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
}

// Registry owns the admitted tenants.
type Registry struct {
	opts    Options
	control *control.Store
	fence   *control.Fence
	disp    *dispatch.Client
	log     *slog.Logger

	mu      sync.RWMutex
	tenants map[string]*tenant

	wg   sync.WaitGroup
	stop chan struct{}
	once sync.Once
}

// NewRegistry builds the registry. It admits nothing until Run.
func NewRegistry(opts Options, ctl *control.Store, fence *control.Fence,
	disp *dispatch.Client, log *slog.Logger) *Registry {
	return &Registry{
		opts: opts, control: ctl, fence: fence, disp: disp, log: log,
		tenants: make(map[string]*tenant),
		stop:    make(chan struct{}),
	}
}

// Store implements the lookup both the gRPC server and the admin API use.
func (r *Registry) Store(name string) (*store.Store, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tenants[name]
	if !ok {
		return nil, false
	}
	return t.store, true
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

	for _, t := range tenants {
		r.retire(t, "shutdown")
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
	for _, t := range rows {
		if t.Enabled {
			want[t.Name] = t
		}
	}

	r.mu.RLock()
	have := make(map[string]*tenant, len(r.tenants))
	for k, v := range r.tenants {
		have[k] = v
	}
	r.mu.RUnlock()

	// Retire first, so a disable followed by a re-enable in one pass does not leave two
	// engines briefly running against the same schema.
	for name, t := range have {
		w, keep := want[name]
		if !keep || w.Generation != t.generation {
			r.mu.Lock()
			delete(r.tenants, name)
			r.mu.Unlock()
			reason := "disabled"
			if keep {
				reason = fmt.Sprintf("generation moved %d -> %d", t.generation, w.Generation)
			}
			r.retire(t, reason)
			delete(have, name)
		}
	}

	for name, w := range want {
		if _, running := have[name]; running {
			r.observe(ctx, name, w.Generation)
			continue
		}
		if err := r.admit(ctx, w); err != nil {
			r.log.Error("admitting a tenant failed", "tenant", name, "error", err)
			_ = r.control.RecordAdmission(ctx, name, "", err)
			continue
		}
		_ = r.control.RecordAdmission(ctx, name, control.SchemaVersion, nil)
		r.observe(ctx, name, w.Generation)
	}
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
	eng := engine.New(cfg, st, r.disp, r.opts.Clock, r.fence, r.log)
	eng.Start(runCtx)

	r.mu.Lock()
	r.tenants[t.Name] = &tenant{
		name: t.Name, generation: t.Generation,
		db: db, store: st, engine: eng, cancel: runCancel,
	}
	r.mu.Unlock()

	r.log.Info("tenant admitted", "tenant", t.Name, "generation", t.Generation,
		"schema", t.SchemaUUID)
	return nil
}

// retire stops a tenant's engine and closes its pool.
//
// It does NOT release the leases of work still in flight. A handler that has not proved it
// stopped keeps its lease until it expires: expiry is not proof a handler stopped, but
// releasing early is a guarantee that a second executor may start while the first is still
// writing.
func (r *Registry) retire(t *tenant, why string) {
	r.log.Info("retiring tenant", "tenant", t.name, "reason", why,
		"still_tracking", t.engine.Tracking())
	t.engine.Stop()
	t.cancel()
	if err := t.db.Close(); err != nil {
		r.log.Warn("closing a tenant pool failed", "tenant", t.name, "error", err)
	}
}

// Healthy reports readiness, which is exactly "this instance may act".
func (r *Registry) Healthy() bool { return r.fence.Healthy() }

var (
	_ admin.Tenants = (*Registry)(nil)
	_ admin.Health  = (*Registry)(nil)
)

// ErrNoTenants is returned by a startup check when the registry is empty, so a fresh
// installation says so instead of idling silently.
var ErrNoTenants = errors.New("gojob: the tenant registry is empty")
