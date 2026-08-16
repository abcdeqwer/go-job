// Package admin serves the operator API and the UI that consumes it.
//
// The UI is a client of this API; the API is the contract. Two rules run through all of it:
//
//   - every job, execution and executor route is under an explicit tenant prefix, because a
//     job name is unique only within a tenant and a path without one is ambiguous — or worse,
//     resolved consistently but not as the operator expected;
//   - actor identity is never defaulted, and every mutating call requires a reason. An action
//     that cannot be attributed is refused rather than attributed to a placeholder, and a
//     reason recorded at the moment of action is worth more than a reconstruction later.
package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/control"
	"github.com/abcdeqwer/go-job/internal/cron"
	"github.com/abcdeqwer/go-job/internal/store"
)

// Tenants gives the API access to admitted tenants.
type Tenants interface {
	Store(tenant string) (*store.Store, bool)
	Names() []string
}

// Health reports readiness.
type Health interface{ Healthy() bool }

// Config is what the API needs that is not a dependency.
type Config struct {
	ExecutorLiveness time.Duration
	InstanceLiveness time.Duration
	Clock            gojob.Clock

	// OpenDB opens a coordination schema by DSN. The API needs it to prove an old schema is
	// quiescent and to verify a new one's identity before a cutover — both of which have to
	// talk to the database in question rather than to whatever this replica happens to hold.
	OpenDB func(dsn string) (*sql.DB, error)
}

// API is the operator HTTP surface.
type API struct {
	cfg     Config
	tenants Tenants
	control *control.Store
	health  Health
	auth    *Auth
	log     *slog.Logger
}

// New builds the API.
func New(cfg Config, tenants Tenants, ctl *control.Store, health Health, auth *Auth, log *slog.Logger) *API {
	return &API{cfg: cfg, tenants: tenants, control: ctl, health: health, auth: auth, log: log}
}

// Handler returns the routed HTTP handler.
//
// Routes are declared with Go 1.22 method-and-wildcard patterns, so a path that does not match
// exactly gets a 404 from the mux rather than falling through to a handler that then has to
// re-parse it. Route parsing that lives in two places is route parsing that disagrees.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	// Unauthenticated: probes and login.
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.HandleFunc("POST /api/login", a.login)
	mux.HandleFunc("POST /api/logout", a.logout)

	// Read-only. VIEWER and above.
	a.read(mux, "GET /api/me", a.me)
	a.read(mux, "GET /api/tenants", a.listTenants)
	a.read(mux, "GET /api/tenants/{tenant}/quiescence", a.quiescence)
	a.read(mux, "GET /api/tenants/{tenant}/handlers", a.handlers)
	a.read(mux, "GET /api/tenants/{tenant}/jobs", a.listJobs)
	a.read(mux, "GET /api/tenants/{tenant}/jobs/{name}", a.getJob)
	a.read(mux, "GET /api/tenants/{tenant}/executions", a.listExecutions)
	a.read(mux, "GET /api/tenants/{tenant}/executions/{key}", a.getExecution)
	a.read(mux, "GET /api/tenants/{tenant}/executors", a.listExecutors)
	a.read(mux, "GET /api/tenants/{tenant}/orphans", a.listOrphans)
	a.read(mux, "GET /api/tenants/{tenant}/audit", a.listAudit)

	// Mutating. OPERATOR only.
	a.write(mux, "POST /api/tenants", a.addTenant)
	a.write(mux, "PATCH /api/tenants/{tenant}", a.patchTenant)
	a.write(mux, "PUT /api/tenants/{tenant}/dsn", a.repointTenant)
	a.write(mux, "POST /api/tenants/{tenant}/jobs", a.createJob)
	a.write(mux, "PATCH /api/tenants/{tenant}/jobs/{name}", a.patchJob)
	a.write(mux, "POST /api/tenants/{tenant}/jobs/{name}/pause", a.pauseJob)
	a.write(mux, "POST /api/tenants/{tenant}/jobs/{name}/resume", a.resumeJob)
	a.write(mux, "POST /api/tenants/{tenant}/jobs/{name}/trigger", a.triggerJob)
	a.write(mux, "POST /api/tenants/{tenant}/jobs/{name}/retire", a.retireJob)
	a.write(mux, "POST /api/tenants/{tenant}/executions/{key}/retry", a.retryExecution)
	a.write(mux, "POST /api/tenants/{tenant}/executions/{key}/cancel", a.cancelExecution)

	mux.Handle("/", a.ui())
	return mux
}

func (a *API) read(mux *http.ServeMux, pattern string, h handlerFunc) {
	mux.Handle(pattern, a.auth.Require(RoleViewer, a.wrap(h)))
}

func (a *API) write(mux *http.ServeMux, pattern string, h handlerFunc) {
	mux.Handle(pattern, a.auth.Require(RoleOperator, a.wrap(h)))
}

type handlerFunc func(w http.ResponseWriter, r *http.Request) error

// wrap turns a handler's error into the right status code.
//
// The mapping is central rather than repeated per handler, because "refused" and "failed" are
// different things an operator needs told apart — a paused job rejecting a trigger is not an
// error condition — and a per-handler mapping is how half of them end up as 500.
func (a *API) wrap(h handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := h(w, r)
		if err == nil {
			return
		}
		status, kind := classify(err)
		if status >= 500 {
			a.log.Error("admin request failed",
				"method", r.Method, "path", r.URL.Path, "error", err)
		}
		writeJSON(w, status, map[string]any{"error": err.Error(), "kind": kind})
	})
}

func classify(err error) (int, string) {
	switch {
	case errors.Is(err, errBadRequest):
		return http.StatusBadRequest, "invalid"
	case errors.Is(err, errNotFound), errors.Is(err, store.ErrNoSuchJob),
		errors.Is(err, store.ErrNoSuchExecution):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, store.ErrStaleVersion):
		return http.StatusConflict, "stale"
	case errors.Is(err, control.ErrNotQuiesced):
		return http.StatusConflict, "not_quiesced"
	case errors.Is(err, gojob.ErrContended), errors.Is(err, gojob.ErrNotRunnable):
		// Refused, not failed. The request was understood and declined for a reason the
		// operator can act on, which is a different screen from "something broke".
		return http.StatusConflict, "refused"
	case errors.Is(err, gojob.ErrProtocol):
		return http.StatusBadRequest, "invalid"
	default:
		return http.StatusInternalServerError, "error"
	}
}

var (
	errBadRequest = errors.New("bad request")
	errNotFound   = errors.New("not found")
)

func badRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errBadRequest, fmt.Sprintf(format, args...))
}

// tenantStore resolves the tenant in the path.
func (a *API) tenantStore(r *http.Request) (*store.Store, string, error) {
	name := r.PathValue("tenant")
	if name == "" {
		return nil, "", badRequest("a tenant is required")
	}
	st, ok := a.tenants.Store(name)
	if !ok {
		return nil, "", fmt.Errorf("%w: tenant %q is not admitted", errNotFound, name)
	}
	return st, name, nil
}

// action is the common body of every mutating request.
type action struct {
	Reason string `json:"reason"`
}

// decode reads a request body, bounded.
//
// The bound is not politeness: this endpoint is authenticated but a signed-in viewer posting a
// gigabyte of JSON should not be able to take the scheduler's memory with it.
func decode(r *http.Request, into any) error {
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(into); err != nil {
		return badRequest("malformed body: %v", err)
	}
	return nil
}

func requireReason(reason string) error {
	if strings.TrimSpace(reason) == "" {
		return badRequest("a reason is required; it is recorded in the audit log")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz fails while this instance is fenced from the control database.
//
// It must fail, not merely report: a fenced instance refuses executor callbacks, so leaving it
// in a load balancer's pool turns a recoverable partition into a fraction of every executor's
// calls failing.
func (a *API) readyz(w http.ResponseWriter, r *http.Request) {
	if !a.health.Healthy() {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"status": "fenced from the control database"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (a *API) me(w http.ResponseWriter, r *http.Request) error {
	writeJSON(w, http.StatusOK, map[string]any{
		"actor": ActorFrom(r.Context()),
		"role":  RoleFrom(r.Context()),
	})
	return nil
}

func (a *API) listTenants(w http.ResponseWriter, r *http.Request) error {
	rows, err := a.control.Tenants(r.Context())
	if err != nil {
		return err
	}
	admitted := map[string]bool{}
	for _, n := range a.tenants.Names() {
		admitted[n] = true
	}

	out := make([]map[string]any, 0, len(rows))
	for _, t := range rows {
		out = append(out, map[string]any{
			"tenant":         t.Name,
			"enabled":        t.Enabled,
			"generation":     t.Generation,
			"schema_uuid":    t.SchemaUUID,
			"schema_version": t.SchemaVersion,
			"admitted":       admitted[t.Name],
			"admitted_at":    nullTime(t.AdmittedAt),
			"last_error":     t.LastError,
			// Masked, never plaintext. There is no affordance anywhere that reveals a stored
			// database password, because no legitimate use for one outweighs what it costs
			// when the UI is reachable by one more person than intended.
			"dsn": control.MaskedDSN(t.DSN),
		})
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (a *API) addTenant(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Tenant     string `json:"tenant"`
		DSN        string `json:"dsn"`
		SchemaUUID string `json:"schema_uuid"`
		Reason     string `json:"reason"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	actor := ActorFrom(r.Context())
	if err := a.control.AddTenant(r.Context(), body.Tenant, body.DSN, body.SchemaUUID,
		actor, body.Reason); err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, map[string]string{"tenant": body.Tenant})
	return nil
}

func (a *API) patchTenant(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Enabled *bool  `json:"enabled"`
		Reason  string `json:"reason"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	if body.Enabled == nil {
		return badRequest("enabled is required")
	}
	// The reason travels WITH the action, in one transaction. Recording it afterwards leaves
	// the action committed without it when the second write fails, and returns an error that
	// invites a retry of something that already happened.
	return a.control.SetTenantEnabled(r.Context(), r.PathValue("tenant"), *body.Enabled,
		ActorFrom(r.Context()), body.Reason)
}

// repointTenant changes a coordination DSN, and refuses until the old schema is quiescent.
//
// Quiescence is proven by looking at the OLD schema rather than by counting acknowledgements.
// Acknowledgement can only say who replied; an instance partitioned from the control database
// stops replying while remaining perfectly able to reach the tenant's database, and its held
// rows are visible there even though its acknowledgements are not.
func (a *API) repointTenant(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		DSN        string `json:"dsn"`
		SchemaUUID string `json:"schema_uuid"`
		Reason     string `json:"reason"`

		// AbandonQueued acknowledges that `ready` work in the old schema will be left behind.
		//
		// Queued work cannot GATE a cutover: the tenant must be disabled first, a disabled
		// tenant has no scheduler claiming, and so the count can never fall on its own — a
		// gate on it is a cutover that is permanently unreachable. But abandoning it silently
		// is worse, because the rows simply stop existing as far as the new schema is
		// concerned, and a missed nightly run is discovered days later.
		//
		// So it is neither: the operator is told the number and has to say the number is
		// acceptable.
		AbandonQueued bool `json:"abandon_queued"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	if body.DSN == "" || body.SchemaUUID == "" {
		return badRequest("dsn and schema_uuid are required; the uuid is what the new schema " +
			"must present, and without it a mistyped DSN is undetectable")
	}
	name := r.PathValue("tenant")

	// Quiescence is proven against the OLD schema, by opening it directly.
	//
	// Asking THIS replica's admitted set cannot work: the tenant must be disabled before a
	// cutover, and a disabled tenant has already been retired from every replica — so the
	// check would find no store and pass by default, which is the opposite of what it is for.
	//
	// Acknowledgement counting cannot work either. It says who REPLIED; an instance
	// partitioned from the control database stops replying while remaining perfectly able to
	// reach the tenant's database, and its held rows are visible there even though its
	// acknowledgements are not.
	quiet, queued, err := a.oldSchemaQuiescent(r.Context(), name)
	if err != nil {
		return err
	}
	if queued > 0 && !body.AbandonQueued {
		return fmt.Errorf("%w: %s has %d queued execution(s) that this cutover would leave in "+
			"the old schema, where nothing will ever claim them; re-send with "+
			"\"abandon_queued\": true to accept that",
			control.ErrNotQuiesced, name, queued)
	}

	// BOTH checks, not either. They cover different failures and neither subsumes the other.
	//
	// The schema scan says what is held RIGHT NOW. It cannot see an instance that has not yet
	// polled the disable: such an instance still believes the tenant is enabled and can claim
	// a moment after the scan returns empty.
	//
	// The acknowledgement check closes exactly that window — every live instance has applied
	// the disable generation and reports holding nothing — and it in turn cannot see an
	// instance partitioned from THIS database, which is why that instance self-fences and why
	// the schema scan is still needed.
	tenants, err := a.control.Tenants(r.Context())
	if err != nil {
		return err
	}
	var generation int64
	for _, t := range tenants {
		if t.Name == name {
			generation = t.Generation
		}
	}
	blockers, err := a.control.Blockers(r.Context(), name, generation, a.cfg.InstanceLiveness)
	if err != nil {
		return err
	}
	if len(blockers) > 0 {
		names := make([]string, 0, len(blockers))
		for _, b := range blockers {
			names = append(names, b.InstanceID)
		}
		return fmt.Errorf("%w: %s has not been acknowledged as quiesced by %s",
			control.ErrNotQuiesced, name, strings.Join(names, ", "))
	}

	// And the NEW schema must present the identity it is claimed to have, BEFORE the registry
	// records it. Storing an unverified DSN moves the failure from this request — where an
	// operator is watching and can fix it — to the next admission, where it surfaces as a
	// tenant that silently stopped scheduling.
	if err := a.verifyNewSchema(r.Context(), name, body.DSN, body.SchemaUUID); err != nil {
		return err
	}

	reason := body.Reason
	if queued > 0 {
		reason = fmt.Sprintf("%s (abandoning %d queued execution(s) in the old schema)",
			reason, queued)
	}
	return a.control.SetTenantDSN(r.Context(), name, body.DSN, body.SchemaUUID,
		ActorFrom(r.Context()), reason, quiet)
}

// oldSchemaQuiescent opens the tenant's CURRENT coordination schema and asks it directly.
// It returns whether the schema is quiet AND how much queued work it still holds, because
// those are two different questions with two different answers. Held and in-flight work
// BLOCKS a cutover — it drains by itself, so waiting works. Queued work does not drain,
// because the tenant is disabled; it is returned so the caller can require the operator to
// acknowledge losing it rather than gating on a number that will never move.
func (a *API) oldSchemaQuiescent(ctx context.Context, name string) (bool, int, error) {
	tenants, err := a.control.Tenants(ctx)
	if err != nil {
		return false, 0, err
	}
	for _, t := range tenants {
		if t.Name != name {
			continue
		}
		if t.Enabled {
			return false, 0, fmt.Errorf("%w: %s must be disabled before its DSN can change",
				control.ErrNotQuiesced, name)
		}
		db, err := a.cfg.OpenDB(t.DSN)
		if err != nil {
			return false, 0, fmt.Errorf("%w: cannot open %s to prove it is quiescent: %v",
				control.ErrNotQuiesced, name, err)
		}
		defer db.Close()

		q, err := store.New(db, a.cfg.Clock).Quiescent(ctx)
		if err != nil {
			return false, 0, fmt.Errorf("%w: cannot read %s to prove it is quiescent: %v",
				control.ErrNotQuiesced, name, err)
		}
		if !q.Quiet() {
			return false, q.Queued, fmt.Errorf("%w: %s still holds %d job(s) with %d execution(s) in flight",
				control.ErrNotQuiesced, name, q.Held, q.InFlight)
		}
		return true, q.Queued, nil
	}
	return false, 0, fmt.Errorf("%w: tenant %q", errNotFound, name)
}

// verifyNewSchema refuses a DSN whose schema is not the one it is claimed to be.
func (a *API) verifyNewSchema(ctx context.Context, name, dsn, schemaUUID string) error {
	db, err := a.cfg.OpenDB(dsn)
	if err != nil {
		return badRequest("cannot open the new DSN: %v", err)
	}
	defer db.Close()

	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := control.Admit(probeCtx, db, name, schemaUUID, a.cfg.Clock.Location()); err != nil {
		return badRequest("the new schema was refused: %v", err)
	}
	return nil
}

// quiescence reports who has not acknowledged the current generation, and whether the schema
// itself still holds anything. The API names the blocking instances rather than making an
// operator guess which one is holding things up.
func (a *API) quiescence(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("tenant")
	tenants, err := a.control.Tenants(r.Context())
	if err != nil {
		return err
	}
	var generation int64
	found := false
	for _, t := range tenants {
		if t.Name == name {
			generation, found = t.Generation, true
			break
		}
	}
	if !found {
		return fmt.Errorf("%w: tenant %q", errNotFound, name)
	}

	blockers, err := a.control.Blockers(r.Context(), name, generation, a.cfg.InstanceLiveness)
	if err != nil {
		return err
	}
	body := map[string]any{"generation": generation, "blockers": blockers}
	if st, ok := a.tenants.Store(name); ok {
		q, err := st.Quiescent(r.Context())
		if err != nil {
			return err
		}
		body["schema_quiescent"] = q.Quiet()
		body["held"] = q.Held
		body["in_flight"] = q.InFlight
		// Surfaced, not gated on. Queued work does not block a cutover — a disabled tenant has
		// no scheduler draining it, so gating would make a cutover unreachable — but
		// abandoning it silently is not something an operator should find out afterwards.
		body["queued_and_would_be_abandoned"] = q.Queued
	}
	writeJSON(w, http.StatusOK, body)
	return nil
}

func (a *API) handlers(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	hs, err := st.DeclaredHandlers(r.Context(), a.cfg.ExecutorLiveness)
	if err != nil {
		return err
	}
	if hs == nil {
		hs = []string{}
	}
	writeJSON(w, http.StatusOK, hs)
	return nil
}

// nullTime renders an optional timestamp as RFC3339 or JSON null.
//
// RFC3339 rather than the driver's native time.Time, so every timestamp the API emits has one
// format. A mixture is the sort of thing a UI absorbs with a special case per field until one
// of them is parsed wrongly.
func nullTime(t sql.NullTime) any {
	if !t.Valid {
		return nil
	}
	return t.Time.Format(time.RFC3339)
}

// parseCron validates a schedule and reports its first fire.
//
// Validation happens at the API rather than at materialization, because an invalid expression
// accepted here becomes a job that exists, appears healthy, and silently never runs — and the
// person who could have fixed it in one second is gone by the time anyone notices.
func parseCron(kind gojob.ScheduleKind, expr string, now time.Time) (time.Time, error) {
	if kind != gojob.ScheduleCron {
		return time.Time{}, nil
	}
	e, err := cron.Parse(expr)
	if err != nil {
		return time.Time{}, badRequest("%v", err)
	}
	// A nanosecond back so a job created exactly on its own fire second fires then, rather
	// than silently waiting a whole period.
	next, err := e.Next(now.Add(-time.Nanosecond))
	if err != nil {
		return time.Time{}, badRequest("%v", err)
	}
	return next, nil
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func parseTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, badRequest("%q is not an RFC3339 timestamp", s)
	}
	return &t, nil
}
