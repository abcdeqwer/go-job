// Package e2e exercises the whole scheduler against a real MySQL.
//
// Everything else in this repository is checked statically or in isolation. These tests are
// the only place the protocol meets an actual database, and they exist because the parts of
// this design that matter most — SKIP LOCKED, affected-row semantics, DATETIME rounding, the
// unique key that makes duplicate materialization a no-op — are precisely the parts a mock
// cannot get wrong and a real engine can.
//
// Set GOJOB_TEST_DSN to run them; without it they skip, because a test that requires Docker
// and fails loudly when it is absent trains people to ignore a red build.
package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
	"github.com/abcdeqwer/go-job/internal/control"
	"github.com/abcdeqwer/go-job/internal/cron"
	"github.com/abcdeqwer/go-job/internal/dispatch"
	"github.com/abcdeqwer/go-job/internal/engine"
	"github.com/abcdeqwer/go-job/internal/server"
	"github.com/abcdeqwer/go-job/internal/store"
	"github.com/abcdeqwer/go-job/internal/testexec"
)

const tenantName = "np"

type harness struct {
	t       *testing.T
	db      *sql.DB
	store   *store.Store
	clock   gojob.Clock
	engine  *engine.Engine
	exec    *testexec.Executor
	disp    *dispatch.Client
	sched   *server.Server
	cancel  context.CancelFunc
	dial    func(string) (*grpc.ClientConn, error)
	connect func()
}

func dsn(t *testing.T) string {
	d := os.Getenv("GOJOB_TEST_DSN")
	if d == "" {
		t.Skip("set GOJOB_TEST_DSN to run the end-to-end tests")
	}
	return d
}

// setup gives each test its own schema, so one test's rows can never explain another's
// result. A shared schema with cleanup between tests fails differently under -count=2.
func setup(t *testing.T) *harness {
	t.Helper()
	base := dsn(t)

	admin, err := sql.Open("mysql", base+"?parseTime=true&loc=UTC&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer admin.Close()

	schema := fmt.Sprintf("gojob_t%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := admin.Exec("CREATE DATABASE " + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		a, err := sql.Open("mysql", base)
		if err == nil {
			_, _ = a.Exec("DROP DATABASE IF EXISTS " + schema)
			a.Close()
		}
	})

	ddl, err := os.ReadFile("../../schema/mysql/tenant/001_tenant.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	db, err := sql.Open("mysql", base+schema+"?parseTime=true&loc=UTC&multiStatements=true&time_zone=%27%2B00%3A00%27")
	if err != nil {
		t.Fatalf("open tenant: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, stmt := range splitStatements(string(ddl)) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply schema: %v\n%s", err, firstLine(stmt))
		}
	}
	if _, err := db.Exec(`INSERT INTO schema_identity (lock_row, tenant, schema_uuid, schema_version, created_at)
	                      VALUES (1, ?, UUID(), ?, NOW())`, tenantName, control.SchemaVersion); err != nil {
		t.Fatalf("mint identity: %v", err)
	}

	// A REAL clock, not a fixed one. Backoffs, poll delays and availability are all business
	// instants compared against this clock, so a frozen one makes every deferred row
	// permanently invisible — a retry scheduled a second out never comes due, and the test
	// hangs in a way that looks exactly like a broken retry path.
	//
	// Determinism here comes from polling for a condition rather than from stopping time.
	clock := gojob.SystemClock{Loc: time.UTC}
	st := store.New(db, clock)

	h := &harness{t: t, db: db, store: st, clock: clock}
	h.startExecutorAndScheduler()
	return h
}

// startExecutorAndScheduler wires a real executor to a real scheduler over an in-process
// connection, so the gRPC contract is genuinely exercised without binding a port.
func (h *harness) startExecutorAndScheduler() {
	t := h.t
	lis := bufconn.Listen(1 << 20)
	dial := func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	h.exec = testexec.New("exec-1", "default", "bufnet", tenantName, 4)
	h.disp = dispatch.NewClient(time.Second, 5*time.Second, dispatch.Credentials{})
	h.disp.SetDialer(dial)

	grpcSrv := grpc.NewServer()
	gojobv1.RegisterJobExecutorServer(grpcSrv, h.exec)

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	schedSrv := server.New(server.Config{
		HeartbeatInterval: 2 * time.Second,
		RegistrationTTL:   30 * time.Second,
		ProgressInterval:  5 * time.Second,
		SilenceDeadline:   30 * time.Second,
	}, staticTenants{h.store}, h.disp, alwaysUnfenced{}, h.clock, func() int { return 1 }, log)
	gojobv1.RegisterJobSchedulerServer(grpcSrv, schedSrv)
	h.sched = schedSrv

	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop(); _ = lis.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(cancel)

	h.engine = engine.New(engine.Config{
		InstanceID:           "test-instance",
		Tenant:               tenantName,
		ScanInterval:         200 * time.Millisecond,
		RecoverInterval:      300 * time.Millisecond,
		ReapInterval:         time.Hour,
		MisfireGrace:         time.Minute,
		ExecutorLiveness:     30 * time.Second,
		ExecutorRetention:    time.Hour,
		PageSize:             50,
		BackoffBase:          time.Second,
		BackoffMax:           5 * time.Second,
		ReconcileDeadline:    2 * time.Second,
		DispatchResendLimit:  3,
		DispatchResendWindow: 5 * time.Second,
	}, h.store, h.disp, h.clock, alwaysUnfenced{}, log)

	// The executor registers through the real gRPC path, so the contract probe, the handler
	// declaration and the in_flight reconciliation are all genuinely exercised.
	h.dial = dial
	h.connect = func() {
		if err := h.exec.Connect(ctx, "bufnet", dial); err != nil {
			t.Fatalf("executor registration: %v", err)
		}
	}
}

type staticTenants struct{ st *store.Store }

func (s staticTenants) Store(string) (*store.Store, bool) { return s.st, true }
func (s staticTenants) Names() []string                   { return []string{tenantName} }

func (s staticTenants) Lookup(string) (*store.Store, server.Availability) {
	return s.st, server.Available
}

type alwaysUnfenced struct{}

func (alwaysUnfenced) Check() error { return nil }

// splitStatements strips comments BEFORE splitting on semicolons.
//
// The other order does not work, and the reason is worth recording: the schema's own comments
// contain semicolons — a header line ends "...; admission asserts it." and a column carries
// "-- defaults; merged with trigger overrides" — so splitting first cuts statements in half at
// a full stop and in the middle of a column list.
func splitStatements(ddl string) []string {
	var kept []string
	for _, l := range strings.Split(ddl, "\n") {
		// Trailing comments count too: `params_json JSON NULL, -- defaults; merged with ...`
		// puts a semicolon inside a column definition.
		if i := strings.Index(l, "--"); i >= 0 {
			l = l[:i]
		}
		if strings.TrimSpace(l) == "" {
			continue
		}
		kept = append(kept, l)
	}
	var out []string
	for _, raw := range strings.Split(strings.Join(kept, "\n"), ";") {
		if s := strings.TrimSpace(raw); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// eventually polls until cond holds, because these tests observe loops rather than calls.
// A fixed sleep is either flaky or slow, and under load it is both.
func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (h *harness) createJob(d gojob.Definition, nextFire time.Time) {
	h.t.Helper()
	if err := h.store.CreateJob(context.Background(), d, nextFire, "test", "a test fixture"); err != nil {
		h.t.Fatalf("create job %q: %v", d.JobName, err)
	}
}

func (h *harness) executions(status string) []store.ExecutionView {
	h.t.Helper()
	rows, _, err := h.store.Executions(context.Background(), store.ExecutionFilter{
		Status: status, Limit: 100,
	})
	if err != nil {
		h.t.Fatalf("list executions: %v", err)
	}
	return rows
}

// compileFor supplies the schedule compiler the store's materialization needs, from the
// definition read inside the transaction.
func compileFor(t *testing.T) store.Compile {
	return func(d gojob.Definition) (store.Schedule, error) {
		return cron.Parse(d.ScheduleExpr)
	}
}

func stringReader(s string) io.Reader { return strings.NewReader(s) }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}
