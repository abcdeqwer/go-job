package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
	"github.com/abcdeqwer/go-job/internal/dispatch"
	"github.com/abcdeqwer/go-job/internal/server"
	"github.com/abcdeqwer/go-job/internal/store"
	"github.com/abcdeqwer/go-job/internal/testexec"
)

// authHarness stands a scheduler up WITH the authentication interceptor, which the other e2e
// harness deliberately omits — and which is exactly why those tests pass. Without this, the
// entire authentication path would be code nothing ever executes.
type authHarness struct {
	client gojobv1.JobSchedulerClient
	exec   *testexec.Executor
}

func setupAuth(t *testing.T, auth *server.DBAuthenticator, st *store.Store, clock gojob.Clock) *authHarness {
	t.Helper()

	// TWO listeners, because there are two processes.
	//
	// Putting both services behind one interceptor is not a shortcut, it is a different
	// system: the scheduler's outbound contract probe would be intercepted by the scheduler's
	// own inbound authentication and refused. In a real deployment the executor authenticates
	// the scheduler with whatever ITS deployment configured (the -executor-token and
	// -executor-ca flags), which is a separate decision from how the scheduler authenticates
	// executors.
	execLis := bufconn.Listen(1 << 20)
	schedLis := bufconn.Listen(1 << 20)

	dialTo := func(lis *bufconn.Listener) func(string) (*grpc.ClientConn, error) {
		return func(string) (*grpc.ClientConn, error) {
			return grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return lis.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	}

	ex := testexec.New("exec-1", "canary", "bufnet", tenantName, 4)
	ex.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		return "ok", nil
	})
	execSrv := grpc.NewServer()
	gojobv1.RegisterJobExecutorServer(execSrv, ex)
	go func() { _ = execSrv.Serve(execLis) }()
	t.Cleanup(func() { execSrv.Stop(); _ = execLis.Close() })

	disp := dispatch.NewClient(time.Second, 5*time.Second, dispatch.Credentials{})
	disp.SetDialer(dialTo(execLis))

	schedSrv := grpc.NewServer(grpc.UnaryInterceptor(server.UnaryAuthInterceptor(auth)))
	gojobv1.RegisterJobSchedulerServer(schedSrv, server.New(server.Config{
		HeartbeatInterval: 2 * time.Second,
		RegistrationTTL:   30 * time.Second,
		ProgressInterval:  5 * time.Second,
		SilenceDeadline:   30 * time.Second,
	}, staticTenants{st}, disp, alwaysUnfenced{}, clock, func() int { return 1 }, discardLogger()))
	go func() { _ = schedSrv.Serve(schedLis) }()
	t.Cleanup(func() { schedSrv.Stop(); _ = schedLis.Close() })

	cc, err := dialTo(schedLis)("bufnet")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	return &authHarness{client: gojobv1.NewJobSchedulerClient(cc), exec: ex}
}

func withToken(ctx context.Context, tok string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

// An identity with no executor_identity row is REFUSED. An earlier version allowed it whenever
// the table was empty, which in an mTLS installation means any certificate the client CA ever
// signed can register as an arbitrary production tenant and be handed its work.
func TestUnlistedIdentityIsRefused(t *testing.T) {
	h := setup(t)
	ctl, _ := controlDB(t)

	auth := &server.DBAuthenticator{DB: ctl, RequireCredential: true}
	ah := setupAuth(t, auth, h.store, h.clock)

	_, err := ah.client.Register(withToken(context.Background(), "a-token-nobody-issued"),
		&gojobv1.RegisterRequest{
			ExecutorId: "exec-1", Group: "canary", Tenant: tenantName, Address: "bufnet",
		})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("an unissued token got %v, want UNAUTHENTICATED", status.Code(err))
	}

	// And with NO credential at all.
	_, err = ah.client.Register(context.Background(), &gojobv1.RegisterRequest{
		ExecutorId: "exec-1", Group: "canary", Tenant: tenantName, Address: "bufnet",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("an uncredentialed call got %v, want UNAUTHENTICATED", status.Code(err))
	}
}

// A listed identity registers, and one listed for a different GROUP is refused — a canary
// registering as the main group would silently take production traffic.
func TestGroupScopedIdentity(t *testing.T) {
	h := setup(t)
	ctl, _ := controlDB(t)

	const token = "s3cret-token-for-the-canary"
	if _, err := ctl.Exec(`
		INSERT INTO executor_identity (identity, tenant, executor_group, token_sha256, created_at)
		VALUES ('canary-fleet', ?, 'canary', ?, NOW())`,
		tenantName, server.HashToken(token)); err != nil {
		t.Fatal(err)
	}

	auth := &server.DBAuthenticator{DB: ctl, RequireCredential: true}
	ah := setupAuth(t, auth, h.store, h.clock)
	ctx := withToken(context.Background(), token)

	if _, err := ah.client.Register(ctx, &gojobv1.RegisterRequest{
		ExecutorId: "exec-1", Group: "canary", Tenant: tenantName, Address: "bufnet",
	}); err != nil {
		t.Fatalf("a listed identity was refused its own group: %v", err)
	}

	_, err := ah.client.Register(ctx, &gojobv1.RegisterRequest{
		ExecutorId: "exec-2", Group: "main", Tenant: tenantName, Address: "bufnet",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("the canary registered as `main` (%v); a partial rollout would silently take "+
			"production traffic", status.Code(err))
	}
}

// The callbacks carry no group, so the group check cannot apply to them — but they must still
// work for a group-scoped identity, or an executor registers and then loses every callback,
// going stale while looking like a process that died.
func TestGroupScopedIdentityCanUseTheCallbacks(t *testing.T) {
	h := setup(t)
	ctl, _ := controlDB(t)

	const token = "s3cret-token-for-the-canary"
	if _, err := ctl.Exec(`
		INSERT INTO executor_identity (identity, tenant, executor_group, token_sha256, created_at)
		VALUES ('canary-fleet', ?, 'canary', ?, NOW())`,
		tenantName, server.HashToken(token)); err != nil {
		t.Fatal(err)
	}

	auth := &server.DBAuthenticator{DB: ctl, RequireCredential: true}
	ah := setupAuth(t, auth, h.store, h.clock)
	ctx := withToken(context.Background(), token)

	if _, err := ah.client.Register(ctx, &gojobv1.RegisterRequest{
		ExecutorId: "exec-1", Group: "canary", Tenant: tenantName, Address: "bufnet",
	}); err != nil {
		t.Fatal(err)
	}

	resp, err := ah.client.Heartbeat(ctx, &gojobv1.HeartbeatRequest{
		ExecutorId: "exec-1", Tenant: tenantName, Running: 0,
	})
	if err != nil {
		t.Fatalf("a group-scoped identity could not heartbeat its own registration: %v", err)
	}
	if !resp.GetKnown() {
		t.Fatal("the heartbeat reported the registration unknown; it was just made")
	}
}

// A registration is bound to the credential that made it. Knowing an executor id must not be
// enough to keep it alive: a heartbeat is what holds an address routable, so an identity able
// to heartbeat any id could keep a dead process in the routing pool indefinitely, and every
// dispatch to it would burn another job's recovery budget.
func TestHeartbeatIsBoundToTheRegisteringIdentity(t *testing.T) {
	h := setup(t)
	ctl, _ := controlDB(t)

	const mainTok, canaryTok = "token-for-main", "token-for-canary"
	for _, r := range []struct{ name, group, tok string }{
		{"main-fleet", "main", mainTok},
		{"canary-fleet", "canary", canaryTok},
	} {
		if _, err := ctl.Exec(`
			INSERT INTO executor_identity (identity, tenant, executor_group, token_sha256, created_at)
			VALUES (?, ?, ?, ?, NOW())`,
			r.name, tenantName, r.group, server.HashToken(r.tok)); err != nil {
			t.Fatal(err)
		}
	}

	auth := &server.DBAuthenticator{DB: ctl, RequireCredential: true}
	ah := setupAuth(t, auth, h.store, h.clock)

	// main registers.
	if _, err := ah.client.Register(withToken(context.Background(), mainTok),
		&gojobv1.RegisterRequest{
			ExecutorId: "main-1", Group: "main", Tenant: tenantName, Address: "bufnet",
		}); err != nil {
		t.Fatal(err)
	}

	// The canary tries to keep main-1 alive.
	resp, err := ah.client.Heartbeat(withToken(context.Background(), canaryTok),
		&gojobv1.HeartbeatRequest{ExecutorId: "main-1", Tenant: tenantName})
	if err != nil {
		t.Fatalf("heartbeat returned an error rather than an answer: %v", err)
	}
	if resp.GetKnown() {
		t.Fatal("the canary kept another fleet's registration alive; knowing an executor id " +
			"must not be enough to hold its address routable")
	}

	// main can still heartbeat its own.
	resp, err = ah.client.Heartbeat(withToken(context.Background(), mainTok),
		&gojobv1.HeartbeatRequest{ExecutorId: "main-1", Tenant: tenantName})
	if err != nil || !resp.GetKnown() {
		t.Fatalf("the registering identity could not heartbeat its own registration: %v / known=%v",
			err, resp.GetKnown())
	}
}

// The escape hatch has to work, and has to be an explicit choice rather than a consequence of
// an empty table.
func TestUnlistedIdentitiesAllowedWhenExplicitlyEnabled(t *testing.T) {
	h := setup(t)
	ctl, _ := controlDB(t)

	auth := &server.DBAuthenticator{
		DB: ctl, RequireCredential: false, AllowUnlistedIdentities: true,
	}
	ah := setupAuth(t, auth, h.store, h.clock)

	if _, err := ah.client.Register(context.Background(), &gojobv1.RegisterRequest{
		ExecutorId: "exec-1", Group: "canary", Tenant: tenantName, Address: "bufnet",
	}); err != nil {
		t.Fatalf("registration was refused with both escape hatches open: %v", err)
	}
}
