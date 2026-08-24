package admin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/control"
	"github.com/abcdeqwer/go-job/internal/store"
)

// A VIEWER must never satisfy an OPERATOR requirement. Every mutating route in this package
// is guarded by exactly this comparison, so an inverted case here hands every reader the
// ability to trigger production jobs.
func TestRoleAllows(t *testing.T) {
	cases := []struct {
		have, required Role
		want           bool
	}{
		{RoleViewer, RoleViewer, true},
		{RoleOperator, RoleViewer, true},
		{RoleOperator, RoleOperator, true},
		{RoleViewer, RoleOperator, false},
		{"", RoleViewer, false},
		{"", RoleOperator, false},
		{"ADMIN", RoleOperator, false},
		{"admin", RoleOperator, false},
		// Lower case is not normalised here; verify() upper-cases what the database holds, so
		// anything reaching this point in another case is a value nothing produced.
		{"operator", RoleOperator, false},
	}
	for _, c := range cases {
		if got := c.have.allows(c.required); got != c.want {
			t.Errorf("Role(%q).allows(%q) = %v, want %v", c.have, c.required, got, c.want)
		}
	}
}

// New tenants must be created at the schema version this binary admits. The v2 rollout
// upgraded the binary while the tenant databases were still v1, which stopped every job;
// applying the complete embedded migration stream during provisioning prevents the same
// split for every database created after a schema release.
func TestEmbeddedTenantMigrationsIncludeCurrentSchema(t *testing.T) {
	migrations, err := TenantMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var combined strings.Builder
	for i, migration := range migrations {
		if i > 0 && migrations[i-1].Name >= migration.Name {
			t.Fatalf("tenant migrations are not ordered: %q before %q",
				migrations[i-1].Name, migration.Name)
		}
		combined.WriteString(migration.DDL)
		combined.WriteByte('\n')
	}
	ddl := combined.String()
	if !strings.Contains(ddl, "idx_job_execution_retention (status, finished_at, id)") {
		t.Fatal("new-tenant migration stream lacks the execution-retention index required by v2")
	}
	if !strings.Contains(ddl, "ADD COLUMN description VARCHAR(512) NOT NULL DEFAULT ''") {
		t.Fatal("new-tenant migration stream lacks handler descriptions required by v3")
	}
	if !strings.Contains(ddl, "SET schema_version = '"+control.SchemaVersion+"'") {
		t.Fatalf("new-tenant migration stream does not advance schema_identity to required version %s",
			control.SchemaVersion)
	}
}

// "Refused" and "failed" are different things an operator needs told apart: a paused job
// declining a trigger is not an error condition, and showing it as a 500 sends someone to
// read scheduler logs for a decision the scheduler made correctly.
func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		kind   string
	}{
		{"bad request", badRequest("no"), 400, "invalid"},
		{"missing job", store.ErrNoSuchJob, 404, "not_found"},
		{"missing execution", store.ErrNoSuchExecution, 404, "not_found"},
		{"stale edit", store.ErrStaleVersion, 409, "stale"},
		{"not quiesced", control.ErrNotQuiesced, 409, "not_quiesced"},
		{"held by another run", gojob.ErrContended, 409, "refused"},
		{"not runnable", gojob.ErrNotRunnable, 409, "refused"},
		{"protocol misuse", gojob.ErrProtocol, 400, "invalid"},
		{"anything else", errors.New("boom"), 500, "error"},

		// Wrapping must not change the classification: every store method wraps its sentinel
		// with context, and a classification that only matched bare errors would report all
		// of them as 500.
		{"wrapped refusal", errors.Join(errors.New("claiming x"), gojob.ErrContended), 409, "refused"},
	}
	for _, c := range cases {
		status, kind := classify(c.err)
		if status != c.status || kind != c.kind {
			t.Errorf("%s: got %d/%s, want %d/%s", c.name, status, kind, c.status, c.kind)
		}
	}
}

// `cancelled` alone lies by omission. Reaching it because a handler confirmed it stopped, and
// reaching it because a lease expired, are different facts — and showing both as a plain
// "cancelled" invites the assumption that nothing happened, which for a job with external
// effects is the most expensive available wrong assumption.
func TestStatusLabelDistinguishesHowATerminalStateWasReached(t *testing.T) {
	cases := []struct {
		status, reason string
		wantContains   string
	}{
		{"cancelled", "handler_confirmed", "handler confirmed"},
		{"cancelled", "fenced", "side effects unverified"},
		{"dead", "timeout", "runtime cap"},
		{"dead", "budget_exhausted", "attempt budget"},
		{"dead", "permanent_failure", "not retried"},
		{"cancel_requested", "", "still holds its slot"},
		{"skipped", "", "FORBID"},
		{"success", "", "success"},
	}
	for _, c := range cases {
		got := statusLabel(store.ExecutionView{
			Status:         gojob.Status(c.status),
			TerminalReason: c.reason,
		})
		if !strings.Contains(got, c.wantContains) {
			t.Errorf("statusLabel(%s/%s) = %q, want it to mention %q",
				c.status, c.reason, got, c.wantContains)
		}
	}
}

// The two ways of reaching `cancelled` must not render identically. This is the property, not
// the wording.
func TestCancelledLabelsDiffer(t *testing.T) {
	confirmed := statusLabel(store.ExecutionView{Status: "cancelled", TerminalReason: "handler_confirmed"})
	fenced := statusLabel(store.ExecutionView{Status: "cancelled", TerminalReason: "fenced"})
	if confirmed == fenced {
		t.Fatalf("both cancellation paths render as %q; an operator cannot tell whether side "+
			"effects were verified", confirmed)
	}
}

func TestJobBodyValidation(t *testing.T) {
	valid := jobBody{
		JobName: "nightly", HandlerKey: "reports.nightly",
		ScheduleKind: "CRON", ScheduleExpr: "0 0 2 * * *", Reason: "because",
	}

	t.Run("defaults are the decided ones", func(t *testing.T) {
		d, err := valid.toDefinition()
		if err != nil {
			t.Fatal(err)
		}
		if d.Misfire != gojob.DefaultMisfirePolicy {
			t.Errorf("misfire default = %q, want %q", d.Misfire, gojob.DefaultMisfirePolicy)
		}
		if d.Concurrency != gojob.DefaultConcurrencyPolicy {
			t.Errorf("concurrency default = %q, want %q", d.Concurrency, gojob.DefaultConcurrencyPolicy)
		}
		if d.MaxAttempts < 1 || d.Lease < 10*time.Second || d.Timeout < time.Second {
			t.Errorf("defaults produced an invalid definition: %+v", d)
		}
	})

	bad := []struct {
		name string
		mut  func(*jobBody)
	}{
		{"no name", func(b *jobBody) { b.JobName = "" }},
		{"no handler", func(b *jobBody) { b.HandlerKey = "" }},
		{"unknown kind", func(b *jobBody) { b.ScheduleKind = "HOURLY" }},
		{"no expression", func(b *jobBody) { b.ScheduleExpr = "" }},
		{"unknown concurrency", func(b *jobBody) { b.Concurrency = "PARALLEL" }},
		{"unknown misfire", func(b *jobBody) { b.Misfire = "REPLAY" }},
		{"attempts too high", func(b *jobBody) { b.MaxAttempts = 1000 }},
		{"attempts negative", func(b *jobBody) { b.MaxAttempts = -1 }},
		{"lease below the floor", func(b *jobBody) { b.LeaseSeconds = 5 }},
		{"timeout beyond a week", func(b *jobBody) { b.TimeoutSecond = 999999 }},
		{"params are not an object", func(b *jobBody) { b.Params = []byte(`[1,2]`) }},
		{"fixed delay with a cron expression", func(b *jobBody) {
			b.ScheduleKind = "FIXED_DELAY"
			b.ScheduleExpr = "0 0 2 * * *"
		}},
		{"fixed delay of zero", func(b *jobBody) {
			b.ScheduleKind = "FIXED_DELAY"
			b.ScheduleExpr = "0"
		}},
	}
	for _, c := range bad {
		b := valid
		c.mut(&b)
		if _, err := b.toDefinition(); err == nil {
			t.Errorf("%s: accepted", c.name)
		} else if !errors.Is(err, errBadRequest) {
			t.Errorf("%s: %v is not a bad-request error, so the API would report it as a 500",
				c.name, err)
		}
	}
}

// An invalid cron expression must be refused at the API. Accepted here, it becomes a job that
// exists, looks healthy, and silently never runs — and the person who could have fixed it in
// one second is gone by the time anyone notices.
func TestInvalidCronIsRefusedAtCreation(t *testing.T) {
	now := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	if _, err := parseCron(gojob.ScheduleCron, "0 0 0 31 2 *", now); err == nil {
		t.Fatal("accepted a cron expression that can never fire")
	}
	if _, err := parseCron(gojob.ScheduleCron, "not a cron", now); err == nil {
		t.Fatal("accepted a malformed cron expression")
	}
	if _, err := parseCron(gojob.ScheduleFixedDelay, "5000", now); err != nil {
		t.Fatalf("a fixed-delay expression must not be cron-parsed: %v", err)
	}
}

// A job created exactly on its own fire second must fire then, not silently wait a whole
// period. Next is strictly-after, so this is the nanosecond nudge in parseCron.
func TestFirstFireIncludesTheCreationInstant(t *testing.T) {
	now := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)
	got, err := parseCron(gojob.ScheduleCron, "0 0 2 * * *", now)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now) {
		t.Fatalf("first fire = %s, want %s — creating a daily job at exactly its fire second "+
			"must not lose the day", got, now)
	}
}

// A mistyped API path must not return the UI. An HTML body where JSON was expected surfaces
// as a parse error in the client, which sends whoever is debugging it to the wrong place.
func TestUnknownAPIPathIsNotTheUI(t *testing.T) {
	a := &API{}
	rec := httptest.NewRecorder()
	a.ui().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/nope", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /api/nope returned %d, want 404", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "<html") {
		t.Fatal("an unknown API path returned the UI")
	}
}

// A deep link into the UI must open.
func TestDeepLinkServesTheUI(t *testing.T) {
	a := &API{}
	rec := httptest.NewRecorder()
	a.ui().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/executions/some-key", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("deep link returned %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "go-job") {
		t.Fatal("deep link did not serve the UI")
	}
	if got := rec.Header().Get("Content-Security-Policy"); !strings.Contains(got, "frame-ancestors 'none'") {
		t.Errorf("missing frame-ancestors in CSP: %q", got)
	}
}

func TestUIOffersCopyAllHandlerDescriptionsAndIdentityDeletion(t *testing.T) {
	a := &API{}
	rec := httptest.NewRecorder()
	a.ui().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	body := rec.Body.String()
	for _, marker := range []string{
		`id="copyAllJobs"`,
		`/jobs/copy-all`,
		`.copy-target input[type="checkbox"] { width:auto`,
		`id="copyJobGo" disabled`,
		`id="copyAllJobsGo" disabled`,
		`function copyTargetChanged(`,
		`/handler-catalog`,
		`function applyHandlerDescription()`,
		`data-act="delete"`,
		`api("DELETE", "/api/executor-identities"`,
	} {
		if !strings.Contains(body, marker) {
			t.Errorf("admin UI is missing %q", marker)
		}
	}
	if got := strings.Count(body, `<label class="copy-target">`); got != 2 {
		t.Fatalf("copy target row is used %d times, want both single-job and copy-all dialogs", got)
	}
}

// The trusted-header mode is a full authentication bypass for anything that can reach the
// port directly, so it must never be on unless it was switched on.
func TestTrustedHeaderIsOffByDefault(t *testing.T) {
	clock := gojob.NewFixedClock(time.Now(), time.UTC)
	auth := NewAuth(nil, clock, time.Hour, TrustedHeader{}, false)

	r := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	r.Header.Set("X-Forwarded-User", "attacker")
	r.Header.Set("X-Forwarded-Role", "OPERATOR")

	if _, _, ok := auth.identify(r); ok {
		t.Fatal("an identity header was trusted without the mode being enabled")
	}
}

// When it IS enabled, a proxy that sends an identity but no role must under-grant.
func TestTrustedHeaderDefaultsToViewer(t *testing.T) {
	clock := gojob.NewFixedClock(time.Now(), time.UTC)
	auth := NewAuth(nil, clock, time.Hour, TrustedHeader{
		Enabled: true, UserHeader: "X-User", RoleHeader: "X-Role",
	}, false)

	r := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	r.Header.Set("X-User", "alice")
	actor, role, ok := auth.identify(r)
	if !ok || actor != "alice" {
		t.Fatalf("identify = %q/%v/%v", actor, role, ok)
	}
	if role != RoleViewer {
		t.Fatalf("role = %q, want VIEWER — a proxy that sends no role must under-grant", role)
	}

	// An unrecognised role must also fall back rather than being passed through.
	r.Header.Set("X-Role", "SUPERUSER")
	if _, role, _ = auth.identify(r); role != RoleViewer {
		t.Fatalf("unknown role became %q, want VIEWER", role)
	}

	r.Header.Set("X-Role", "operator")
	if _, role, _ = auth.identify(r); role != RoleOperator {
		t.Fatalf("role = %q, want OPERATOR", role)
	}
}

func TestBuiltInLoginRemainsAvailableWithTrustedHeaders(t *testing.T) {
	clock := gojob.NewFixedClock(time.Now(), time.UTC)
	a := &API{auth: NewAuth(nil, clock, time.Hour, TrustedHeader{
		Enabled: true, UserHeader: "X-User", RoleHeader: "X-Role",
	}, false)}
	r := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader("{"))
	r.RemoteAddr = "10.77.0.8:49152"
	rec := httptest.NewRecorder()

	a.login(rec, r)

	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "malformed body") {
		t.Fatalf("login response = %d %s, want malformed body (not disabled)", rec.Code, rec.Body.String())
	}
}

func TestRequireReason(t *testing.T) {
	for _, bad := range []string{"", "   ", "\t\n"} {
		if err := requireReason(bad); err == nil {
			t.Errorf("accepted %q as a reason", bad)
		}
	}
	if err := requireReason("rotating credentials"); err != nil {
		t.Errorf("rejected a real reason: %v", err)
	}
}

// A password for an account that can trigger production jobs must not be four characters.
func TestHashPasswordRejectsShortPasswords(t *testing.T) {
	if _, err := HashPassword("short"); err == nil {
		t.Fatal("accepted a five-character password")
	}
	if _, err := HashPassword("a-reasonable-password"); err != nil {
		t.Fatalf("rejected a reasonable password: %v", err)
	}
}
