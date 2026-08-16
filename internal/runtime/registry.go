// Package runtime wires the pieces together: it polls the tenant registry, admits and
// retires tenants without a restart, and owns the per-tenant engines.
//
// The polling loop is also what keeps this instance UNFENCED. Every successful read refreshes
// the fence; a failing read leaves it to lapse, after which every loop and every executor
// callback stops. That coupling is deliberate — the right to operate and the ability to see
// the registry are the same thing.
package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/admin"
	"github.com/abcdeqwer/go-job/internal/control"
	"github.com/abcdeqwer/go-job/internal/dispatch"
	"github.com/abcdeqwer/go-job/internal/engine"
	"github.com/abcdeqwer/go-job/internal/server"
	"github.com/abcdeqwer/go-job/internal/store"
)

// Options is everything the registry needs to admit a tenant and run it.
type Options struct {
	InstanceID string
	Clock      gojob.Clock

	PollInterval    time.Duration
	StalenessLimit  time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration

	// DrainTimeout bounds how long retiring a tenant waits for its in-flight work. Zero means
	// no wait at all, which is only right for a shutdown that is already out of time.
	DrainTimeout time.Duration

	Engine engine.Config

	// OpenDB is the driver hook. The library never imports a MySQL driver: the host chooses
	// it, along with its TLS settings, its connection parameters and its session time zone —
	// none of which a scheduler should be deciding on a host's behalf.
	OpenDB func(dsn string) (*sql.DB, error)
}

type tenant struct {
	name       string
	generation int64
	db         *sql.DB
	store      *store.Store
	engine     *engine.Engine
	cancel     context.CancelFunc

	// draining means claiming has stopped but callbacks are still served.
	//
	// Retiring a tenant is THREE things, and they cannot happen at the same moment: stop
	// claiming, stop serving callbacks, close the pool. Doing them together removed the
	// routing an executor needs to report a result at the instant the drain began — so every
	// progress report and every result during the drain answered NOT_FOUND, which an executor
	// treats as terminal and discards. The drain could not complete by its own definition, and
	// a success arriving during a disable was lost and later recorded as unknown.
	draining bool
}

// Registry owns the admitted tenants.
type Registry struct {
	opts    Options
	control *control.Store
	fence   *control.Fence
	disp    *dispatch.Client
	log     *slog.Logger

	mu        sync.RWMutex
	tenants   map[string]*tenant
	admitting map[string]bool

	// seen is the newest generation this instance has read for each tenant. Admission is
	// asynchronous, so it publishes against this rather than against what it started with:
	// a slow admission for an old generation must not install its engine after a cutover has
	// already moved the tenant to a new schema, or both would dispatch the same job.
	seen map[string]int64

	wg   sync.WaitGroup
	stop chan struct{}
	once sync.Once
}

// NewRegistry builds the registry. It admits nothing until Run.
func NewRegistry(opts Options, ctl *control.Store, fence *control.Fence,
	disp *dispatch.Client, log *slog.Logger) *Registry {
	return &Registry{
		opts: opts, control: ctl, fence: fence, disp: disp, log: log,
		tenants:   make(map[string]*tenant),
		admitting: make(map[string]bool),
		seen:      make(map[string]int64),
		stop:      make(chan struct{}),
	}
}

// Availability says why a tenant lookup did not produce a store.
type Availability int

const (
	// Available: admitted here, and serving.
	Available Availability = iota

	// Pending: the registry knows this tenant but THIS instance has not admitted it yet, or
	// its admission failed. The distinction matters at the gRPC boundary: NOT_FOUND tells an
	// executor to discard a result, and discarding a real result because one instance happened
	// to be mid-admission loses completed work. UNAVAILABLE tells it to retry.
	Pending

	// Unknown: no such tenant in the registry.
	Unknown
)

// Store implements the lookup both the gRPC server and the admin API use.
//
// A DRAINING tenant is still returned. Its engine has stopped claiming, but the executors it
// dispatched to are still reporting, and those reports are what the drain is waiting for.
func (r *Registry) Store(name string) (*store.Store, bool) {
	st, avail := r.Lookup(name)
	return st, avail == Available
}

// Lookup resolves a tenant and says why, when it cannot.
func (r *Registry) Lookup(name string) (*store.Store, Availability) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if t, ok := r.tenants[name]; ok {
		return t.store, Available
	}
	if _, known := r.seen[name]; known {
		return nil, Pending
	}
	return nil, Unknown
}

// Names lists admitted tenants.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.tenants))
	for n := range r.tenants {
		out = append(out, n)
	}
	return out
}

// Run polls the registry until the context ends.
//
// The first pass is synchronous, so a process that cannot reach the control database fails
// visibly at startup rather than coming up healthy with no tenants and no explanation.
func (r *Registry) Run(ctx context.Context) {
	r.reconcile(ctx)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		t := time.NewTicker(r.opts.PollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stop:
				return
			case <-t.C:
				r.reconcile(ctx)
			}
		}
	}()
}

// Stop drains every tenant.
func (r *Registry) Stop() {
	r.once.Do(func() { close(r.stop) })
	r.wg.Wait()

	r.mu.Lock()
	tenants := r.tenants
	r.tenants = make(map[string]*tenant)
	r.mu.Unlock()

	// Shutdown does NOT release what is still held: another instance is running and will
	// recover it, and releasing early guarantees a second executor may start beside a handler
	// that has not proved it stopped.
	for _, t := range tenants {
		r.retire(context.Background(), t, "shutdown", false)
	}
	_ = r.disp.Close()
}

// reconcile brings the admitted set in line with the registry.
func (r *Registry) reconcile(ctx context.Context) {
	rows, err := r.control.Tenants(ctx)
	if err != nil {
		// The fence is NOT refreshed. Every loop stops within the staleness limit, and
		// readiness drops — which is the whole point: an instance that cannot see the registry
		// must not keep holding work invisibly while a cutover proceeds.
		r.log.Error("reading the tenant registry failed; this instance will fence itself",
			"error", err)
		return
	}
	r.fence.Refresh()

	want := make(map[string]control.Tenant, len(rows))
	r.mu.Lock()
	for _, t := range rows {
		if g := r.seen[t.Name]; t.Generation > g {
			r.seen[t.Name] = t.Generation
		}
		if t.Enabled {
			want[t.Name] = t
		}
	}
	// A tenant that has vanished from the registry entirely stops being tracked, so its
	// generation cannot pin an admission for a row that no longer exists.
	present := make(map[string]bool, len(rows))
	for _, t := range rows {
		present[t.Name] = true
	}
	for name := range r.seen {
		if !present[name] {
			delete(r.seen, name)
		}
	}
	r.mu.Unlock()

	r.mu.RLock()
	have := make(map[string]*tenant, len(r.tenants))
	for k, v := range r.tenants {
		have[k] = v
	}
	r.mu.RUnlock()

	// Retire first, so a disable followed by a re-enable in one pass does not leave two
	// engines briefly running against the same schema.
	//
	// The removal from r.tenants is synchronous — from that moment nothing routes to it — but
	// the DRAIN is not. Draining on this goroutine would block the only registry poll for as
	// long as one tenant's in-flight work takes, and the poll is what refreshes the fence: a
	// one-minute drain under a thirty-second staleness limit self-fences every other tenant on
	// this instance. Admission was moved off the loop for exactly this reason; retirement has
	// the same problem.
	for name, t := range have {
		w, keep := want[name]
		if !keep || w.Generation != t.generation {
			r.mu.Lock()
			if t.draining {
				// Already being drained by an earlier pass.
				r.mu.Unlock()
				delete(have, name)
				continue
			}
			t.draining = true
			r.mu.Unlock()
			reason := "disabled"
			if keep {
				reason = fmt.Sprintf("generation moved %d -> %d", t.generation, w.Generation)
			}
			r.wg.Add(1)
			go func(t *tenant, why string) {
				defer r.wg.Done()
				r.retire(ctx, t, why, true)
			}(t, reason)
			delete(have, name)
		}
	}

	for name, w := range want {
		if _, running := have[name]; running {
			r.observe(ctx, name, w.Generation)
			continue
		}
		// A tenant still draining is NOT re-admitted yet. Starting a new engine beside a drain
		// that is about to release the old engine's held work would let the new one dispatch a
		// row the old one has just returned to `ready`, while the executor that had it may
		// still be running. The next pass admits it, once the drain has finished.
		r.mu.RLock()
		_, stillDraining := r.tenants[name]
		r.mu.RUnlock()
		if stillDraining {
			continue
		}
		// Admission runs OFF the poll loop.
		//
		// It opens a pool, verifies a schema and checks a time zone, each with its own
		// timeout. Doing that inline serialises every tenant behind every other, and three
		// unreachable ones consume the whole staleness limit — after which this instance
		// fences ITSELF and stops scheduling for the tenants that were perfectly healthy. One
		// tenant's database being down must not be able to stop the others.
		if !r.beginAdmit(name) {
			continue // already being admitted by an earlier pass
		}
		go func(t control.Tenant) {
			defer r.endAdmit(t.Name)
			if err := r.admit(ctx, t); err != nil {
				r.log.Error("admitting a tenant failed", "tenant", t.Name, "error", err)
				_ = r.control.RecordAdmission(ctx, t.Name, "", err)
				return
			}
			_ = r.control.RecordAdmission(ctx, t.Name, control.SchemaVersion, nil)
			r.observe(ctx, t.Name, t.Generation)
		}(w)
	}
}

// beginAdmit claims the right to admit a tenant, so successive poll passes do not start a
// second admission for one already in flight — which would open two pools and leave one of
// them leaked behind whichever registration lost.
func (r *Registry) beginAdmit(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.admitting[name] {
		return false
	}
	r.admitting[name] = true
	return true
}

func (r *Registry) endAdmit(name string) {
	r.mu.Lock()
	delete(r.admitting, name)
	r.mu.Unlock()
}

// observe reports what this instance has applied, and whether it is holding anything.
//
// Quiescence is answered from the engine's tracked set rather than from the database, because
// this is a statement about THIS instance: the database says what is held, not by whom.
func (r *Registry) observe(ctx context.Context, name string, generation int64) {
	r.mu.RLock()
	t := r.tenants[name]
	r.mu.RUnlock()

	quiesced := true
	if t != nil {
		quiesced = t.engine.Tracking() == 0
	}
	if err := r.control.Observe(ctx, name, r.opts.InstanceID, generation, quiesced); err != nil {
		r.log.Warn("recording an observation failed", "tenant", name, "error", err)
	}
}

// admit opens a tenant's pool, verifies its schema and starts its engine.
func (r *Registry) admit(ctx context.Context, t control.Tenant) error {
	db, err := r.opts.OpenDB(t.DSN)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	db.SetMaxOpenConns(r.opts.MaxOpenConns)
	db.SetMaxIdleConns(r.opts.MaxIdleConns)
	db.SetConnMaxLifetime(r.opts.ConnMaxLifetime)

	// Bounded, and bounded well under the staleness limit even though this no longer runs on
	// the poll loop: an admission that hangs holds a pool open and a goroutine with it.
	admitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := control.Admit(admitCtx, db, t.Name, t.SchemaUUID, r.opts.Clock.Location()); err != nil {
		_ = db.Close()
		return err
	}

	st := store.New(db, r.opts.Clock)
	cfg := r.opts.Engine
	cfg.Tenant = t.Name
	cfg.InstanceID = r.opts.InstanceID

	runCtx, runCancel := context.WithCancel(ctx)
	// Disarmed once the engine is published and retire() owns the cancel. Anything that leaves
	// before then — a superseded generation, a failure — releases the context here rather than
	// leaking a goroutine per abandoned admission.
	published := false
	defer func() {
		if !published {
			runCancel()
		}
	}()

	eng := engine.New(cfg, st, r.disp, r.opts.Clock, r.fence, r.log)

	// Publish under the lock, against the NEWEST generation seen — not against the one this
	// admission started with.
	//
	// Admission takes time: a pool, a schema check, a time-zone check. In that window the
	// tenant can be disabled, proven quiescent, re-pointed at a different schema and
	// re-enabled, and another replica can already be running the new one. Installing this
	// engine anyway would put two schemas in service for one tenant, each correctly excluding
	// only itself, dispatching the same logical job twice — the exact split brain the whole
	// cutover procedure exists to prevent.
	r.mu.Lock()
	stale := r.seen[t.Name] != t.Generation
	if !stale {
		r.tenants[t.Name] = &tenant{
			name: t.Name, generation: t.Generation,
			db: db, store: st, engine: eng, cancel: runCancel,
		}
	}
	r.mu.Unlock()

	if stale {
		_ = db.Close()
		return fmt.Errorf("admission for %s generation %d was superseded before it completed",
			t.Name, t.Generation)
	}

	published = true
	eng.Start(runCtx)
	r.log.Info("tenant admitted", "tenant", t.Name, "generation", t.Generation,
		"schema", t.SchemaUUID)
	return nil
}

// releaseContext bounds the release of held work, independently of the drain that preceded it.
//
// Separate, and detached, for one reason: the release matters MOST in the case where the drain
// ran out of time. Deriving it from the drain's context meant that whenever the drain timed
// out — the only case where anything is still held — the release began already cancelled and
// failed on its first statement.
func (r *Registry) releaseContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), r.opts.DrainTimeout)
}

// retire stops claiming, waits a bounded time for in-flight work, then closes the pool.
//
// The drain is bounded rather than absent OR unlimited. Closing immediately leaves running
// executors with nowhere to report — their results arrive at a scheduler that no longer has
// the tenant and are answered NOT_FOUND, so perfectly good work is recorded as unknown.
// Waiting forever makes a disable that an operator issued during an incident hang on the one
// job that is stuck.
//
// It does NOT release the leases of whatever is still in flight when the bound elapses. A
// handler that has not proved it stopped keeps its lease until it expires: expiry is not proof
// a handler stopped, but releasing early is a guarantee that a second executor may start while
// the first is still writing.
func (r *Registry) retire(ctx context.Context, t *tenant, why string, releaseHeld bool) {
	r.log.Info("retiring tenant", "tenant", t.name, "reason", why,
		"still_tracking", t.engine.Tracking())

	// The bound covers the WHOLE retirement, and is created first.
	//
	// Bounding only the wait leaves everything before it unbounded: a store call on the
	// tenant's own context can hang, and the tenant then stays `draining` for ever with its
	// callbacks routed and its re-admission blocked.
	drainCtx, cancelDrain := context.WithTimeout(ctx, r.opts.DrainTimeout)
	defer cancelDrain()

	// Stop ACQUIRING work. The heartbeat keeps running, which is the point: leases of what is
	// already in flight have to stay live through the drain, or the owner loses the right to
	// release its own work and — with every engine for this tenant stopping — nothing is left
	// to recover it.
	t.engine.StopClaiming()

	for t.engine.Tracking() > 0 && drainCtx.Err() == nil {
		select {
		case <-time.After(200 * time.Millisecond):
		case <-drainCtx.Done():
		}
	}

	if releaseHeld {
		// Still holding the leases, so this can act as the owner. From the DATABASE, because
		// an exhausted unknown dispatch is held and untracked.
		//
		// Its OWN bound, not the drain's. Passing drainCtx here meant the release was skipped
		// in exactly the case it exists for: the loop above exits either because the tenant
		// went quiet — in which case there is little to release — or because the bound
		// elapsed, and in that second case drainCtx is already cancelled, so every query the
		// release makes fails on its first statement. Every replica retires a disabled tenant,
		// so nothing is left to recover what stayed held: job_state.active_kind stays set, the
		// schema never becomes quiescent, and the DSN cutover that quiescence gates can never
		// proceed.
		//
		// Detached from ctx as well, for the same reason at process scope: a shutdown that
		// cancelled ctx would otherwise cancel the release it just decided to make.
		releaseCtx, cancelRelease := r.releaseContext(ctx)
		t.engine.ReleaseOwnedWork(releaseCtx)
		cancelRelease()
	} else if left := t.engine.Tracking(); left > 0 {
		r.log.Warn("shutting down with work still in flight; those leases will expire and "+
			"another instance will recover them", "tenant", t.name, "in_flight", left)
	}

	// Only NOW does the heartbeat stop, and only now is routing removed and the pool closed.
	// Everything above needed the store to resolve, because callbacks were the point of the
	// drain, and needed the leases live, because the release depends on still owning them.
	t.engine.Stop()

	r.mu.Lock()
	if cur, ok := r.tenants[t.name]; ok && cur == t {
		delete(r.tenants, t.name)
	}
	if releaseHeld {
		// Terminally retired: no longer "admission in progress". Leaving it in `seen` makes
		// Lookup answer Pending, which an executor reads as UNAVAILABLE and retries for ever
		// against a tenant that will never come back unless an operator re-enables it.
		delete(r.seen, t.name)
	}
	r.mu.Unlock()

	t.cancel()
	if err := t.db.Close(); err != nil {
		r.log.Warn("closing a tenant pool failed", "tenant", t.name, "error", err)
	}
}

// Healthy reports readiness, which is exactly "this instance may act".
func (r *Registry) Healthy() bool { return r.fence.Healthy() }

// SchedulerTenants adapts the registry to the gRPC server's narrower view.
//
// Two enums rather than one shared type, because the alternative is the server package
// importing runtime — and runtime already imports the engine, the store and the dispatch
// client. A dependency edge added for an integer constant is a dependency edge.
type SchedulerTenants struct{ R *Registry }

func (s SchedulerTenants) Lookup(name string) (*store.Store, server.Availability) {
	st, avail := s.R.Lookup(name)
	switch avail {
	case Available:
		return st, server.Available
	case Pending:
		return nil, server.Pending
	default:
		return nil, server.Unknown
	}
}

var (
	_ admin.Tenants  = (*Registry)(nil)
	_ admin.Health   = (*Registry)(nil)
	_ server.Tenants = SchedulerTenants{}
)

// ErrNoTenants is returned by a startup check when the registry is empty, so a fresh
// installation says so instead of idling silently.
var ErrNoTenants = errors.New("gojob: the tenant registry is empty")
