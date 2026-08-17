package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/admin"
	"github.com/abcdeqwer/go-job/internal/control"
)

// controlDB gives a test its own control schema.
func controlDB(t *testing.T) (*sql.DB, gojob.Clock) {
	t.Helper()
	base := dsn(t)

	adminConn, err := sql.Open("mysql", base+"?parseTime=true&loc=UTC&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer adminConn.Close()

	schema := fmt.Sprintf("gojob_c%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := adminConn.Exec("CREATE DATABASE " + schema); err != nil {
		t.Fatalf("create control schema: %v", err)
	}
	t.Cleanup(func() {
		a, err := sql.Open("mysql", base)
		if err == nil {
			_, _ = a.Exec("DROP DATABASE IF EXISTS " + schema)
			a.Close()
		}
	})

	ddl, err := os.ReadFile("../../schema/mysql/control/001_control.sql")
	if err != nil {
		t.Fatalf("read control schema: %v", err)
	}
	db, err := sql.Open("mysql", base+schema+"?parseTime=true&loc=UTC&multiStatements=true&time_zone=%27%2B00%3A00%27")
	if err != nil {
		t.Fatalf("open control: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, stmt := range splitStatements(string(ddl)) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply control schema: %v\n%s", err, firstLine(stmt))
		}
	}
	return db, gojob.SystemClock{Loc: time.UTC}
}

// Sessions live in the control database so a cluster behaves like one system: signing in
// through one replica must work on the next, and signing OUT must actually revoke — a
// process-local map gives neither.
func TestSessionsAreSharedAcrossReplicas(t *testing.T) {
	db, clock := controlDB(t)
	ctx := context.Background()

	hash, err := admin.HashPassword("a-reasonable-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO admin_user (username, password_hash, role, created_at, updated_at)
		VALUES (?, ?, 'OPERATOR', NOW(), NOW())`, "alice", hash); err != nil {
		t.Fatal(err)
	}

	// Two Auth values are two replicas: separate processes, one database.
	replicaA := admin.NewAuth(db, clock, time.Hour, admin.TrustedHeader{}, false)
	replicaB := admin.NewAuth(db, clock, time.Hour, admin.TrustedHeader{}, false)

	apiA := admin.New(admin.Config{Clock: clock}, nil, nil, alwaysHealthy{}, replicaA, discardLogger())
	apiB := admin.New(admin.Config{Clock: clock}, nil, nil, alwaysHealthy{}, replicaB, discardLogger())

	// Sign in through A.
	rec := httptest.NewRecorder()
	apiA.Handler().ServeHTTP(rec, jsonPost("/api/login",
		`{"username":"alice","password":"a-reasonable-password"}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("login through replica A returned %d: %s", rec.Code, rec.Body)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("login set no session cookie")
	}

	// Use it against B.
	req := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec = httptest.NewRecorder()
	apiB.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("a session issued by replica A returned %d on replica B: %s", rec.Code, rec.Body)
	}

	// Sign out through B; A must stop accepting it.
	logout := httptest.NewRequest(http.MethodPost, "/api/logout", nil)
	for _, c := range cookies {
		logout.AddCookie(c)
	}
	rec = httptest.NewRecorder()
	apiB.Handler().ServeHTTP(rec, logout)
	if rec.Code != http.StatusOK {
		t.Fatalf("logout returned %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/me", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec = httptest.NewRecorder()
	apiA.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("replica A still accepted a session revoked through B (%d); signing out has "+
			"to mean signed out everywhere", rec.Code)
	}
}

// A wrong password must be refused, and the refusal must not distinguish a missing account
// from a wrong one — either message turns the login form into an account enumerator.
func TestLoginRefusesBadCredentials(t *testing.T) {
	db, clock := controlDB(t)
	hash, err := admin.HashPassword("a-reasonable-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO admin_user (username, password_hash, role, created_at, updated_at)
		VALUES ('alice', ?, 'VIEWER', NOW(), NOW())`, hash); err != nil {
		t.Fatal(err)
	}

	auth := admin.NewAuth(db, clock, time.Hour, admin.TrustedHeader{}, false)
	api := admin.New(admin.Config{Clock: clock}, nil, nil, alwaysHealthy{}, auth, discardLogger())

	var bodies []string
	for _, creds := range []string{
		`{"username":"alice","password":"wrong"}`,
		`{"username":"nobody","password":"wrong"}`,
	} {
		rec := httptest.NewRecorder()
		api.Handler().ServeHTTP(rec, jsonPost("/api/login", creds))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("bad credentials returned %d", rec.Code)
		}
		bodies = append(bodies, rec.Body.String())
	}
	if bodies[0] != bodies[1] {
		t.Fatalf("a wrong password and a missing account produce different messages:\n  %s\n  %s",
			bodies[0], bodies[1])
	}
}

// A disabled account must not be able to sign in, whatever its password.
func TestDisabledAccountCannotSignIn(t *testing.T) {
	db, clock := controlDB(t)
	hash, err := admin.HashPassword("a-reasonable-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO admin_user (username, password_hash, role, disabled, created_at, updated_at)
		VALUES ('bob', ?, 'OPERATOR', 1, NOW(), NOW())`, hash); err != nil {
		t.Fatal(err)
	}

	auth := admin.NewAuth(db, clock, time.Hour, admin.TrustedHeader{}, false)
	api := admin.New(admin.Config{Clock: clock}, nil, nil, alwaysHealthy{}, auth, discardLogger())

	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, jsonPost("/api/login",
		`{"username":"bob","password":"a-reasonable-password"}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a disabled account signed in (%d)", rec.Code)
	}
}

// The registry round-trips a tenant, and never returns a DSN in plaintext.
func TestTenantRegistryRoundTrip(t *testing.T) {
	db, clock := controlDB(t)
	ctx := context.Background()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	ctl, err := control.New(db, clock, key)
	if err != nil {
		t.Fatal(err)
	}

	const secretDSN = "gojob:hunter2@tcp(db:3306)/np_scheduler"
	if err := ctl.AddTenant(ctx, "np", secretDSN, "uuid-1", "test", "adding a site"); err != nil {
		t.Fatal(err)
	}

	rows, err := ctl.Tenants(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Name != "np" {
		t.Fatalf("tenants = %+v", rows)
	}
	if rows[0].DSN != secretDSN {
		t.Fatalf("the DSN did not round-trip through encryption: %q", rows[0].DSN)
	}

	// The stored bytes must not contain the password: encryption at rest is the point.
	var stored []byte
	if err := db.QueryRowContext(ctx,
		`SELECT coordination_dsn FROM tenant_registry WHERE tenant = 'np'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if containsBytes(stored, "hunter2") {
		t.Fatal("the stored DSN contains the password in plaintext")
	}

	// A different key must not be able to read it, and must say so rather than returning junk.
	otherKey := make([]byte, 32)
	otherCtl, err := control.New(db, clock, otherKey)
	if err != nil {
		t.Fatal(err)
	}
	got, err := otherCtl.Tenants(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Enabled || got[0].LastError == "" {
		t.Fatalf("a wrong key produced %+v; it must disable the tenant and record why", got)
	}
}

// A DSN change is refused while the tenant is enabled. The disable is what makes the
// quiescence question answerable at all.
func TestDSNChangeRequiresDisableFirst(t *testing.T) {
	db, clock := controlDB(t)
	ctx := context.Background()

	key := make([]byte, 16)
	ctl, err := control.New(db, clock, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.AddTenant(ctx, "np", "u:p@tcp(a:3306)/x", "uuid-1", "test", "adding a site"); err != nil {
		t.Fatal(err)
	}

	err = ctl.SetTenantDSN(ctx, "np", "u:p@tcp(b:3306)/y", "uuid-2", "test", "moving schemas", true, 30*time.Second)
	if err == nil {
		t.Fatal("re-pointed an ENABLED tenant; a cutover must be disable, prove, then change")
	}

	if err := ctl.SetTenantEnabled(ctx, "np", false, "test", "preparing a cutover"); err != nil {
		t.Fatal(err)
	}
	if err := ctl.SetTenantDSN(ctx, "np", "u:p@tcp(b:3306)/y", "uuid-2", "test", "moving schemas", true, 30*time.Second); err != nil {
		t.Fatalf("re-pointing a disabled, quiescent tenant failed: %v", err)
	}

	// The generation must have moved on both operations, because that is what a cutover's
	// acknowledgement check is keyed on.
	rows, err := ctl.Tenants(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Generation < 3 {
		t.Fatalf("generation = %d after an add, a disable and a re-point; each must bump it",
			rows[0].Generation)
	}
	if rows[0].SchemaUUID != "uuid-2" {
		t.Fatalf("schema uuid = %q, want uuid-2", rows[0].SchemaUUID)
	}
}

// Quiescence is refused while anything is outstanding, and a cutover is gated on it.
func TestSetTenantDSNRefusesWhenNotQuiescent(t *testing.T) {
	db, clock := controlDB(t)
	ctx := context.Background()

	ctl, err := control.New(db, clock, make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.AddTenant(ctx, "np", "u:p@tcp(a:3306)/x", "uuid-1", "test", "adding a site"); err != nil {
		t.Fatal(err)
	}
	if err := ctl.SetTenantEnabled(ctx, "np", false, "test", "preparing a cutover"); err != nil {
		t.Fatal(err)
	}

	if err := ctl.SetTenantDSN(ctx, "np", "u:p@tcp(b:3306)/y", "uuid-2", "test", "moving schemas", false, 30*time.Second); err == nil {
		t.Fatal("re-pointed a tenant that was not proven quiescent")
	}
}

type alwaysHealthy struct{}

func (alwaysHealthy) Healthy() bool { return true }

func jsonPost(path, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, stringReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func containsBytes(b []byte, s string) bool {
	return len(b) > 0 && len(s) > 0 && indexBytes(b, s) >= 0
}

func indexBytes(b []byte, s string) int {
	for i := 0; i+len(s) <= len(b); i++ {
		if string(b[i:i+len(s)]) == s {
			return i
		}
	}
	return -1
}

// Admission's clock contract has to catch the two things the clock model rests on: that the
// driver parses DATETIMEs in the business location, and that the two hosts' clocks agree.
//
// The check it replaced asserted the session time zone, which — once ownership moved to
// UTC_TIMESTAMP() — constrained nothing any statement reads. This test exists so the
// replacement is not the same shape of reassurance: it admits a correct pool and refuses a
// pool whose driver would store business columns in a different wall clock.
func TestAdmissionChecksTheClockContract(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	var uuid string
	if err := h.db.QueryRowContext(ctx,
		`SELECT schema_uuid FROM schema_identity WHERE lock_row = 1`).Scan(&uuid); err != nil {
		t.Fatal(err)
	}

	// The harness opens its pool with loc=UTC and runs a UTC business clock: admitted.
	if err := control.Admit(ctx, h.db, tenantName, uuid, time.UTC); err != nil {
		t.Fatalf("a correctly configured pool was refused: %v", err)
	}

	// Same pool, business location eight hours away. Every business column this process
	// wrote would be a UTC wall clock while the design, the UI and every operator reading
	// the table expect Manila — and not one round trip would fail.
	manila := time.FixedZone("Asia/Manila", 8*60*60)
	err := control.Admit(ctx, h.db, tenantName, uuid, manila)
	if err == nil {
		t.Fatal("a pool parsing timestamps in UTC was admitted for a business location eight " +
			"hours away; business columns would silently hold the wrong wall clock")
	}
	if !errors.Is(err, gojob.ErrTimeZone) {
		t.Fatalf("refusal was not reported as a time-zone error: %v", err)
	}
}

// A cutover must be refused by a blocker that appeared AFTER the caller's own check.
//
// The handler's blocker check is a snapshot, and what follows it — verifying the new schema,
// opening a pool — takes time. An instance that was mid-admission during the snapshot has no
// observation row at all, so it blocks nothing; it then finishes, publishes, starts an engine
// against the OLD DSN, and records its observation. If the cutover writes after that, two
// schemas serve one tenant.
//
// The registry now re-reads the gate inside the transaction that moves the DSN, so an
// observation written at any point before the commit is seen.
func TestCutoverRefusesABlockerThatAppearedLate(t *testing.T) {
	db, clock := controlDB(t)
	ctx := context.Background()

	key := make([]byte, 32)
	ctl, err := control.New(db, clock, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.AddTenant(ctx, "np", "u:p@tcp(a:3306)/x", "uuid-1", "test", "adding"); err != nil {
		t.Fatal(err)
	}
	if err := ctl.SetTenantEnabled(ctx, "np", false, "test", "preparing a cutover"); err != nil {
		t.Fatal(err)
	}

	// Generation is now 2. A late instance declares itself at the OLD generation, not
	// quiesced — exactly what an admission in flight records before it publishes.
	if err := ctl.Observe(ctx, "np", "late-instance", 1, false); err != nil {
		t.Fatal(err)
	}

	// The caller says it checked and found nothing, which was true when it looked.
	err = ctl.SetTenantDSN(ctx, "np", "u:p@tcp(b:3306)/y", "uuid-2",
		"test", "moving schemas", true, 30*time.Second)
	if !errors.Is(err, control.ErrNotQuiesced) {
		t.Fatalf("the cutover was accepted with a live blocker: %v", err)
	}

	// Once that instance reports the new generation and quiet, the cutover proceeds.
	if err := ctl.Observe(ctx, "np", "late-instance", 2, true); err != nil {
		t.Fatal(err)
	}
	if err := ctl.SetTenantDSN(ctx, "np", "u:p@tcp(b:3306)/y", "uuid-2",
		"test", "moving schemas", true, 30*time.Second); err != nil {
		t.Fatalf("the cutover was refused after every instance acknowledged: %v", err)
	}
}
