// Package engine runs the scheduler's loops for one tenant.
//
// Every instance runs the same loops and none of them is a leader. Materialization takes the
// state row FOR UPDATE SKIP LOCKED so exactly one instance creates a given fire instant;
// claiming is guarded by the same row; recovery is idempotent under its guards. What makes
// that safe is specified in doc/protocol.md — this package is the part that decides WHEN each
// transaction runs, not what it means.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/cron"
	"github.com/abcdeqwer/go-job/internal/dispatch"
	"github.com/abcdeqwer/go-job/internal/store"
)

// Config is the timing of the loops. Every value has a failure at the wrong setting, so the
// defaults live in the root package's defaults.go with their reasons.
type Config struct {
	InstanceID string
	Tenant     string

	ScanInterval      time.Duration
	RecoverInterval   time.Duration
	ReapInterval      time.Duration
	MisfireGrace      time.Duration
	ExecutorLiveness  time.Duration
	ExecutorRetention time.Duration

	// PageSize bounds every discovery query. Unbounded pages turn a backlog into one very
	// long transaction and a memory spike at the worst possible moment.
	PageSize int

	// BackoffBase and BackoffMax bound retry, contention and orphan deferral. Deterministic
	// base, jitter applied after, so the sequence stays testable.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// ReconcileDeadline bounds recovery's GetExecution call.
	ReconcileDeadline time.Duration

	// DispatchResendLimit and DispatchResendWindow bound the unknown-outcome re-send. Without
	// a bound an execution can be stranded permanently: an executor whose outbound heartbeats
	// still succeed stays registration-live, so it keeps being chosen, while its Run path is
	// unreachable and never answers — the scheduler would re-send for ever, renewing both
	// leases each cycle, so the lease never expires, recovery never runs, and no budget is
	// ever consumed.
	DispatchResendLimit  int
	DispatchResendWindow time.Duration
}

// Fence is the control-plane self-fence. Every loop asks it before doing anything, so a
// partitioned instance stops acting rather than continuing to hold work invisibly.
type Fence interface{ Check() error }

// Engine runs one tenant.
type Engine struct {
	cfg   Config
	store *store.Store
	disp  *dispatch.Client
	clock gojob.Clock
	fence Fence
	log   *slog.Logger

	// tracked holds the executions this instance owns, so the heartbeat loop knows what to
	// renew. It is a cache of what the database already says, never the authority: an entry
	// missing from it costs a recovery cycle, an entry wrongly present is fenced on its next
	// renewal.
	mu      sync.Mutex
	tracked map[int64]store.Holder

	// unhealthy records executors whose dispatch path stopped answering, with the instant they
	// may be tried again.
	//
	// An executor that heartbeats but cannot accept work is the case registration liveness
	// alone cannot see: it stays routable, keeps being chosen, exhausts the re-send bound,
	// recovers as unknown, and is chosen again — until the recovery budget kills the job
	// without one line of business code having run. Suppressing the path is what breaks that
	// loop. It is per instance and in memory on purpose: it is an observation about THIS
	// process's connectivity, and writing it to the database would let one instance's network
	// problem deprioritise an executor every other instance can reach perfectly well.
	healthMu  sync.Mutex
	unhealthy map[string]time.Time

	// schedules caches compiled cron expressions by expression text. Compilation is cheap but
	// happens inside a transaction holding the job's lock, and the cache keeps that window
	// from including a parse.
	schedMu   sync.RWMutex
	schedules map[string]*cron.Expression

	wg   sync.WaitGroup
	stop chan struct{}
	once sync.Once
}

// New builds an engine. It does not start any loops.
func New(cfg Config, st *store.Store, disp *dispatch.Client, clock gojob.Clock, fence Fence, log *slog.Logger) *Engine {
	return &Engine{
		cfg:       cfg,
		store:     st,
		disp:      disp,
		clock:     clock,
		fence:     fence,
		log:       log.With("tenant", cfg.Tenant, "instance", cfg.InstanceID),
		tracked:   make(map[int64]store.Holder),
		unhealthy: make(map[string]time.Time),
		schedules: make(map[string]*cron.Expression),
		stop:      make(chan struct{}),
	}
}

// Start launches every loop.
func (e *Engine) Start(ctx context.Context) {
	loops := []struct {
		name     string
		interval time.Duration
		fn       func(context.Context)
	}{
		{"materialize", e.cfg.ScanInterval, e.materializePass},
		{"drift", e.cfg.ScanInterval * 2, e.driftPass},
		{"dispatch", e.cfg.ScanInterval, e.dispatchPass},
		{"heartbeat", e.heartbeatInterval(), e.heartbeatPass},
		{"recover", e.cfg.RecoverInterval, e.recoverPass},
		{"timeout", e.cfg.RecoverInterval, e.timeoutPass},
		{"silence", e.cfg.RecoverInterval, e.silencePass},
		{"cancel", e.cfg.RecoverInterval, e.cancelPass},
		{"reap", e.cfg.ReapInterval, e.reapPass},
	}
	for _, l := range loops {
		e.wg.Add(1)
		go e.runLoop(ctx, l.name, l.interval, l.fn)
	}
}

// Stop stops claiming and waits for the loops to finish.
//
// It does NOT release the leases of work still in flight. If a handler has not proved it
// stopped, its lease is left to expire rather than released: expiry is not proof a handler
// stopped, but releasing early is a guarantee that a second executor may start while the
// first is still writing.
func (e *Engine) Stop() {
	e.once.Do(func() { close(e.stop) })
	e.wg.Wait()
}

func (e *Engine) heartbeatInterval() time.Duration {
	// A third of the shortest lease any job may configure. The schema's floor is ten seconds,
	// so three seconds is the interval that keeps even that job's lease alive.
	return 3 * time.Second
}

// runLoop ticks fn, with jitter so a fleet restarted together does not synchronise into a
// thundering herd against one state row.
func (e *Engine) runLoop(ctx context.Context, name string, interval time.Duration, fn func(context.Context)) {
	defer e.wg.Done()
	if interval <= 0 {
		interval = time.Second
	}

	// Stagger the first tick across the interval so seven loops in fifty instances do not all
	// fire at the same millisecond.
	select {
	case <-time.After(jitter(interval)):
	case <-e.stop:
		return
	case <-ctx.Done():
		return
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stop:
			return
		case <-t.C:
			if err := e.fence.Check(); err != nil {
				// Fenced. Not an error to log every tick — it is already visible in readiness.
				continue
			}
			e.safely(ctx, name, fn)
		}
	}
}

// markUnhealthy suppresses an executor's dispatch path for a while.
func (e *Engine) markUnhealthy(executorID string) {
	e.healthMu.Lock()
	e.unhealthy[executorID] = time.Now().Add(e.dispatchQuarantine())
	e.healthMu.Unlock()
}

// dispatchHealthy reports whether an executor may be dispatched to.
func (e *Engine) dispatchHealthy(executorID string) bool {
	e.healthMu.Lock()
	defer e.healthMu.Unlock()
	until, ok := e.unhealthy[executorID]
	if !ok {
		return true
	}
	if time.Now().After(until) {
		delete(e.unhealthy, executorID)
		return true
	}
	return false
}

// dispatchQuarantine is how long a blackholed executor is skipped. Long enough to stop the
// loop, short enough that a transient failure does not take an instance out of the fleet for
// an operator's whole afternoon.
func (e *Engine) dispatchQuarantine() time.Duration {
	if e.cfg.ExecutorLiveness > 0 {
		return e.cfg.ExecutorLiveness
	}
	return 30 * time.Second
}

// stopping reports whether this pass must abandon what it is doing.
//
// It is checked between ITEMS, not only at the pass boundary. A recovery pass over a hundred
// rows, each with a five-second reconciliation deadline, runs for minutes — and if control
// connectivity is lost a second after the pass started, a boundary-only check would let it go
// on adopting, resolving and renewing long after the staleness limit said this instance had
// lost the right to act. The fence exists precisely so an invisible owner stops writing.
func (e *Engine) stopping() bool {
	select {
	case <-e.stop:
		return true
	default:
	}
	return e.fence.Check() != nil
}

// safely runs one pass and turns a panic into a logged error rather than a dead loop.
//
// A panic in one pass must not take the scheduler down: the other loops are still holding
// leases, and a process that exits without renewing them turns a bug in the drift scan into a
// recovery cycle for every running job.
func (e *Engine) safely(ctx context.Context, name string, fn func(context.Context)) {
	defer func() {
		if r := recover(); r != nil {
			e.log.Error("scheduler loop panicked", "loop", name, "panic", r)
		}
	}()
	fn(ctx)
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// backoff computes a bounded, deterministic delay, with jitter applied afterwards so the base
// sequence stays testable.
func (e *Engine) backoff(attempt int) int {
	base := e.cfg.BackoffBase
	if base <= 0 {
		base = 5 * time.Second
	}
	max := e.cfg.BackoffMax
	if max <= 0 {
		max = 5 * time.Minute
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	d += jitter(d / 4)
	s := int(d / time.Second)
	if s < 1 {
		s = 1
	}
	return s
}

// compile turns a definition into a schedule, from the definition read INSIDE the
// materialization transaction. The cache is keyed by expression text rather than by job name,
// so an edit produces a different key and cannot be served a stale compilation.
func (e *Engine) compile(def gojob.Definition) (store.Schedule, error) {
	if def.ScheduleKind != gojob.ScheduleCron {
		return nil, fmt.Errorf("%w: %q is not a cron job", gojob.ErrProtocol, def.JobName)
	}
	e.schedMu.RLock()
	got, ok := e.schedules[def.ScheduleExpr]
	e.schedMu.RUnlock()
	if ok {
		return got, nil
	}

	parsed, err := cron.Parse(def.ScheduleExpr)
	if err != nil {
		return nil, fmt.Errorf("job %q: %w", def.JobName, err)
	}
	e.schedMu.Lock()
	e.schedules[def.ScheduleExpr] = parsed
	e.schedMu.Unlock()
	return parsed, nil
}

// track and untrack maintain the heartbeat set.
func (e *Engine) track(h store.Holder) {
	e.mu.Lock()
	e.tracked[h.ExecutionID] = h
	e.mu.Unlock()
}

// untrack stops renewing an execution, but ONLY if the holder currently tracked is the one
// the caller was acting for.
//
// The epoch check is what stops an ABA. A heartbeat pass snapshots holder H1; recovery adopts
// the same execution as H2 and replaces the entry; H1's renewal then fails, correctly, because
// it is fenced — and an unconditional delete would remove H2. The adopted execution would stop
// being renewed and fall into recovery again while its executor is still running.
func (e *Engine) untrack(id int64, epoch int64) {
	e.mu.Lock()
	if cur, ok := e.tracked[id]; ok && cur.FenceEpoch == epoch {
		delete(e.tracked, id)
	}
	e.mu.Unlock()
}

func (e *Engine) holders() []store.Holder {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]store.Holder, 0, len(e.tracked))
	for _, h := range e.tracked {
		out = append(out, h)
	}
	return out
}

// ReleaseOwnedWork resolves everything this instance still holds, for a tenant that is being
// taken away from it.
//
// It is deliberately NOT what process shutdown does. On shutdown, leaving a lease to expire is
// right: another instance is still running and will recover it, and releasing early guarantees
// a second executor may start while the first is still writing.
//
// A tenant being DISABLED is the opposite situation. Every instance retires it, so no engine
// and no pool remain — nothing will ever recover what is left held, the rows stay held for
// ever, and the tenant's quiescence never becomes true. Since quiescence is exactly what a DSN
// cutover waits on, "leave it for recovery" means "the cutover can never proceed".
//
// So the owner resolves its own holdings. It asks each executor to stop on the way out, and
// records the attempt as unknown, because it genuinely is.
func (e *Engine) ReleaseOwnedWork(ctx context.Context) {
	for _, h := range e.holders() {
		if v, err := e.store.ExecutionByKey(ctx, h.ExecutionKey); err == nil {
			e.requestStop(ctx, v, "the tenant was disabled")
		}
		landed, err := e.store.ReleaseAsOwner(ctx, h, e.backoff(0))
		if err != nil {
			if !errors.Is(err, gojob.ErrContended) {
				e.log.Error("releasing owned work on retirement failed",
					"execution", h.ExecutionKey, "error", err)
			}
			continue
		}
		e.untrack(h.ExecutionID, h.FenceEpoch)
		e.log.Warn("released an in-flight execution because the tenant was disabled",
			"execution", h.ExecutionKey, "job", h.JobName, "landed", landed)
	}
}

// Tracking reports how many executions this instance owns. Graceful shutdown and the
// quiescence check both need it, and so does the admin UI.
func (e *Engine) Tracking() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.tracked)
}

// heartbeatPass renews the lease of everything this instance owns.
//
// A renewal that affects zero rows means ownership is lost. The handler context is abandoned,
// fence_lost is emitted, and the execution is dropped from the tracked set — after which this
// instance makes no further write for it. That is what makes a revived zombie harmless.
func (e *Engine) heartbeatPass(ctx context.Context) {
	for _, h := range e.holders() {
		// A renewal after the fence lapsed is the single worst write this instance can make:
		// it keeps an owner alive that the control plane has already concluded is gone.
		if e.stopping() {
			return
		}
		lease := e.leaseSecondsFor(ctx, h.JobName)
		err := e.store.Renew(ctx, h, lease)
		switch {
		case err == nil:

		case errors.Is(err, gojob.ErrFenced):
			// Ownership is provably gone. Abandon the handler context and make no further
			// write for this execution — that is what makes a revived zombie harmless.
			e.log.Warn("fence lost; abandoning execution",
				"job", h.JobName, "execution", h.ExecutionKey, "epoch", h.FenceEpoch)
			e.untrack(h.ExecutionID, h.FenceEpoch)

		default:
			// A transient database error is NOT proof of losing ownership, and dropping the
			// holder here would stop renewing a lease this instance still holds — turning one
			// failed query into a recovery cycle for a healthy execution. Keep it; the lease
			// has room for several missed renewals, and if the database really is gone the
			// lease expires and recovery does the right thing anyway.
			e.log.Error("lease renewal failed; keeping the holder and retrying",
				"job", h.JobName, "execution", h.ExecutionKey, "error", err)
		}
	}
}

// leaseSecondsFor reads a job's configured lease. A read failure falls back to the schema's
// floor rather than to zero: renewing for zero seconds would expire the lease instantly and
// hand a healthy execution to recovery.
func (e *Engine) leaseSecondsFor(ctx context.Context, jobName string) int {
	def, err := e.store.Definition(ctx, jobName)
	if err != nil || def.Lease <= 0 {
		return 30
	}
	return int(def.Lease / time.Second)
}

// reapPass removes registrations of executors that stopped heartbeating long ago, and reports
// jobs no live executor can run.
//
// An orphan is never dispatched and never marked failed — nothing was attempted — so the only
// way anyone learns about it is this log line. That is the difference between noticing in a
// minute that a job has no executor and noticing next week that it stopped running.
func (e *Engine) reapPass(ctx context.Context) {
	if n, err := e.store.ReapExecutors(ctx, e.cfg.ExecutorRetention); err != nil {
		e.log.Error("reaping dead executors failed", "error", err)
	} else if n > 0 {
		e.log.Info("reaped dead executor registrations", "count", n)
	}

	orphans, err := e.store.Orphans(ctx, e.cfg.ExecutorLiveness)
	if err != nil {
		e.log.Error("orphan scan failed", "error", err)
		return
	}
	for _, o := range orphans {
		e.log.Warn("job has no live executor",
			"job", o.JobName, "handler", o.HandlerKey, "group", o.Group)
	}
}
