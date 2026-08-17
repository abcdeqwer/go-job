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
	"sync/atomic"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
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
	tracked map[int64]held

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

	// rotation breaks ties between equally loaded executors, so a stable sort does not send
	// every job to whichever one happened to sort first.
	rotation atomic.Uint64

	// schedules caches compiled cron expressions by expression text. Compilation is cheap but
	// happens inside a transaction holding the job's lock, and the cache keeps that window
	// from including a parse.
	schedMu   sync.RWMutex
	schedules map[string]*cron.Expression

	wg sync.WaitGroup

	// Two signals, because retiring a tenant has to stop acquiring work before it stops
	// holding it. See StopClaiming.
	stopClaim chan struct{}
	stop      chan struct{}
	onceClaim sync.Once
	once      sync.Once
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
		tracked:   make(map[int64]held),
		unhealthy: make(map[string]time.Time),
		schedules: make(map[string]*cron.Expression),
		stopClaim: make(chan struct{}),
		stop:      make(chan struct{}),
	}
}

// Start launches every loop.
//
// The loops are split into two groups because retirement needs them stopped at different
// times. Everything that ACQUIRES work stops first; the heartbeat keeps running, because
// something has to hold the leases of what is already in flight while it drains.
func (e *Engine) Start(ctx context.Context) {
	acquiring := []struct {
		name     string
		interval time.Duration
		fn       func(context.Context)
	}{
		{"materialize", e.cfg.ScanInterval, e.materializePass},
		{"drift", e.cfg.ScanInterval * 2, e.driftPass},
		{"dispatch", e.cfg.ScanInterval, e.dispatchPass},
		{"recover", e.cfg.RecoverInterval, e.recoverPass},
		{"reap", e.cfg.ReapInterval, e.reapPass},
	}
	holding := []struct {
		name     string
		interval time.Duration
		fn       func(context.Context)
	}{
		{"heartbeat", e.heartbeatInterval(), e.heartbeatPass},
		{"timeout", e.cfg.RecoverInterval, e.timeoutPass},
		{"silence", e.cfg.RecoverInterval, e.silencePass},
		{"cancel", e.cfg.RecoverInterval, e.cancelPass},
	}
	for _, l := range acquiring {
		e.wg.Add(1)
		go e.runLoop(ctx, l.name, l.interval, e.stopClaim, l.fn)
	}
	for _, l := range holding {
		e.wg.Add(1)
		go e.runLoop(ctx, l.name, l.interval, e.stop, l.fn)
	}
}

// StopClaiming stops everything that ACQUIRES work, and leaves the heartbeat running.
//
// This is the first step of retiring a tenant, and the ordering is what makes a drain
// possible at all. Stopping the heartbeat first — which is what Stop() does — lets the leases
// of in-flight work expire during the wait, and a lease with the schema's ten-second minimum
// expires well inside any useful drain. The owner then cannot release its own work, because
// an expired holder belongs to recovery; and every engine for that tenant is stopping, so no
// recovery will ever come. The rows stay held for ever and quiescence never becomes true.
func (e *Engine) StopClaiming() {
	e.onceClaim.Do(func() { close(e.stopClaim) })
}

// Stop stops every loop, including the heartbeat, and waits for them to finish.
//
// It does NOT release the leases of work still in flight. If a handler has not proved it
// stopped, its lease is left to expire rather than released: expiry is not proof a handler
// stopped, but releasing early is a guarantee that a second executor may start while the
// first is still writing.
func (e *Engine) Stop() {
	e.StopClaiming()
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
func (e *Engine) runLoop(ctx context.Context, name string, interval time.Duration,
	until <-chan struct{}, fn func(context.Context)) {
	defer e.wg.Done()
	if interval <= 0 {
		interval = time.Second
	}

	// Stagger the first tick across the interval so nine loops in fifty instances do not all
	// fire at the same millisecond.
	select {
	case <-time.After(jitter(interval)):
	case <-until:
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
		case <-until:
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
	case <-e.stopClaim:
		return true
	default:
	}
	return e.fence.Check() != nil
}

// renewing reports whether this instance may still RENEW what it already holds, which is a
// different question from whether it may take on more.
//
// The drain depends on the difference. StopClaiming halts acquisition and leaves the
// heartbeat running deliberately: leases of what is already in flight have to stay live, or
// the owner loses the right to release its own work — and with every engine for that tenant
// retiring, nothing is left to recover it. Checking `stopping()` in the heartbeat collapsed
// the two, so a drain silently stopped renewal, a legal ten-second lease expired inside the
// fifteen-second drain, and ReleaseAsOwner then refused the row because expired work belongs
// to a recovery pass that had already stopped. The job stayed held for ever, and quiescence —
// hence the DSN cutover it gates — became unreachable.
//
// The control fence still applies, and must: renewing after it lapses is the one write that
// preserves an owner nobody can see.
func (e *Engine) renewing() bool {
	select {
	case <-e.stop:
		return false
	default:
	}
	return e.fence.Check() == nil
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

// defaultLease is what a job whose definition carries no lease is renewed and gated under.
//
// One constant for both, because they are two halves of one rule: the heartbeat renews under
// it and the dispatch gate measures against it, and if they disagreed the gate would either
// refuse work whose lease is being renewed normally, or wave through a claim whose lease has
// already lapsed.
const defaultLease = 30 * time.Second

// held is a tracked execution together with the last instant this instance PROVED it owned
// the execution — the claim, or the most recent successful renewal.
//
// The instant is read from time.Now() and only ever used through time.Since, so it carries
// Go's monotonic reading and measures elapsed time even if the host's wall clock steps. It is
// deliberately not the business Clock: that clock is truncated, which strips the monotonic
// reading, and a backward step would then make an old proof look fresh.
type held struct {
	h         store.Holder
	confirmed time.Time
}

// track and untrack maintain the heartbeat set.
//
// `since` is the instant taken BEFORE the database call that proved ownership, not after it.
// Stamping it afterwards is wrong in exactly the case the guard exists for: a process
// suspended between the commit and this line resumes and records a proof dated to the resume,
// so an ownership fact minutes old reads as seconds old, the dispatch gate passes, and the
// stale attempt goes to an executor after another instance has already recovered it. Taken
// before the call, the stamp can only ever be older than the truth, which is the safe
// direction.
func (e *Engine) track(h store.Holder, since time.Time) {
	e.mu.Lock()
	e.tracked[h.ExecutionID] = held{h: h, confirmed: since}
	e.mu.Unlock()
}

// confirmHold records that ownership was proved again, as of `since` — see track.
func (e *Engine) confirmHold(id int64, epoch int64, since time.Time) {
	e.mu.Lock()
	if cur, ok := e.tracked[id]; ok && cur.h.FenceEpoch == epoch && since.After(cur.confirmed) {
		cur.confirmed = since
		e.tracked[id] = cur
	}
	e.mu.Unlock()
}

// holdRemaining reports how much longer this instance's proof of ownership is good enough to
// act on IRREVERSIBLY — which, in this system, means handing the work to an executor.
//
// Every database write is fenced, so a stale instance cannot corrupt state. `Run` is the one
// thing fencing cannot undo: a process frozen past its lease (a stop-the-world pause, a
// suspended VM, a host migration) resumes holding a claim another instance has already
// recovered and re-dispatched, and its Run call starts a second handler for work that is
// already running. The subsequent Accept is correctly refused — far too late, because the
// handler is running by then.
//
// So the send is gated on elapsed local time since ownership was last proved. Four fifths of
// the lease, not all of it: the database's lease started slightly BEFORE this instance learned
// it had one, and a send is not instantaneous. In healthy operation this is never close — the
// heartbeat renews several times per lease — so the margin costs nothing and only bites in
// the case it exists for.
// It returns the REMAINING time rather than a yes/no, because the answer is needed twice: to
// decide whether to send at all, and to bound the send itself. A boolean left a window between
// the check and the call — a pause there resumes with a satisfied check and an unbounded call
// context, and sends anyway. A deadline carried into the context is already expired when the
// process wakes.
func (e *Engine) holdRemaining(id int64, epoch int64, lease time.Duration) time.Duration {
	if lease <= 0 {
		// Same fallback the renewal uses, and it must be the same: a gate stricter than the
		// lease it is gating would refuse every dispatch for a job whose definition carries no
		// lease, which is a stop, not a safety measure.
		lease = defaultLease
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	cur, ok := e.tracked[id]
	if !ok || cur.h.FenceEpoch != epoch {
		return 0
	}
	left := (lease - lease/5) - time.Since(cur.confirmed)
	if left < 0 {
		return 0
	}
	return left
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
	if cur, ok := e.tracked[id]; ok && cur.h.FenceEpoch == epoch {
		delete(e.tracked, id)
	}
	e.mu.Unlock()
}

func (e *Engine) holders() []store.Holder {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]store.Holder, 0, len(e.tracked))
	for _, t := range e.tracked {
		out = append(out, t.h)
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
// It returns whether anything was left held BECAUSE A HANDLER IS STILL RUNNING. That is not a
// failure and not an error: it is the tenant honestly reporting that it is not quiescent yet,
// and the caller has to keep the engine and the pool alive so that handler can still report,
// and so something is left to recover it when it does.
func (e *Engine) ReleaseOwnedWork(ctx context.Context) (deferred bool) {
	// From the DATABASE, not from the tracked map. An unknown-outcome dispatch that exhausted
	// its re-send bound deliberately stops being tracked so recovery can take it — leaving a
	// held, `dispatching` row that no in-memory scan can see. If the tenant is then disabled,
	// the pool closes with that row still held and nothing left to recover it.
	for page := 0; page < 100; page++ {
		owned, err := e.store.OwnedByInstance(ctx, e.cfg.InstanceID, 200)
		if err != nil {
			// Not knowing is not the same as nothing being there. Reporting `false` here told
			// the caller the tenant was clean, after which the pool closes and whatever this
			// query failed to list is stranded with no engine left to find it.
			e.log.Error("listing owned work on retirement failed", "error", err)
			return true
		}
		if len(owned) == 0 {
			break
		}
		progress, held := e.releasePage(ctx, owned)
		deferred = deferred || held
		if !progress {
			// Nothing in the page could be released — every row was contended, failing, or
			// deliberately held. Looping again would spin on the same rows, and something is
			// still there, so say so.
			return true
		}
		if page == 99 {
			e.log.Error("gave up releasing owned work after a hundred pages; rows remain held")
			return true
		}
	}

	// And a last recovery sweep, because "owned by me" is not the whole of what is left.
	//
	// An instance that crashed holding an execution leaves an expired lease that only a
	// recovery pass can clear. If the tenant is disabled before any surviving instance's next
	// recovery tick, every replica runs this owner-local release, finds nothing of its own,
	// and closes its pool. The dead instance's row stays held for ever — and quiescence, which
	// the DSN cutover gates on, can never become true.
	//
	// Owner-local release first, this second: what this instance holds it can end directly,
	// while somebody else's expired work has to go through reconciliation, which is slower and
	// may not reach a conclusion at all.
	// Its answer counts too: an expired row this sweep could not settle is a row nothing will
	// settle once the pool closes.
	return e.recoverStale(ctx, false) || deferred
}

// releasePage releases one page, reporting whether it made any progress.
// askExecutor reconciles one execution during retirement, and returns the WHOLE answer.
//
// Reducing it to "is it running" threw away the one case worth most: an executor that has
// FINISHED the work and whose ReportResult failed just as retirement began still holds the
// authoritative outcome. Answering only true/false meant releasePage then recorded that
// attempt as `dead` with "outcome unknown" — overwriting a known success that was sitting one
// RPC away.
//
// Unreachable is NOT treated as running. That looks like the unsafe direction and is the
// deliberate one: an executor nobody can reach is exactly the case where waiting is unbounded,
// and holding for ever on a process that may have died turns every crashed executor into a
// tenant that can never be re-pointed. Unknown work is ended rather than assumed complete,
// which is the same judgement recovery makes.
func (e *Engine) askExecutor(ctx context.Context, v store.Stale) dispatch.Reconciliation {
	if v.DispatchedTo == "" {
		return dispatch.Reconciliation{}
	}
	addr, ok := e.addressOf(ctx, v.DispatchedTo)
	if !ok {
		return dispatch.Reconciliation{}
	}
	rec := e.disp.GetExecution(ctx, addr, e.cfg.Tenant, v.ExecutionKey, e.cfg.ReconcileDeadline)
	if rec.Reachable && rec.RunToken != v.RunToken {
		// An answer about another attempt, or about none. Same rule as everywhere else: it
		// proves nothing about this one — except that a RUNNING answer still says a handler
		// holds the KEY, which releasePage reads below.
		if rec.State == gojobv1.ExecutionState_EXECUTION_STATE_RUNNING {
			return dispatch.Reconciliation{Reachable: true,
				State: gojobv1.ExecutionState_EXECUTION_STATE_RUNNING}
		}
		return dispatch.Reconciliation{}
	}
	return rec
}

func (e *Engine) releasePage(ctx context.Context, owned []store.Stale) (progress, deferred bool) {
	for _, v := range owned {
		e.requestStop(ctx, v, "the tenant was disabled")

		// A stop REQUEST is not a stopped handler.
		//
		// Cancel acknowledges that the signal was delivered, nothing more: cancellation is
		// cooperative, and a handler not watching its context never sees it. Ending the row
		// anyway frees the job lock and erases every trace of an execution that may still be
		// writing — after which the schema scans clean, the cutover proceeds, and the new
		// schema dispatches the same logical job beside the handler that never stopped.
		// Quiescence would be a forced database transition presented as proof.
		//
		// So this asks first. A RUNNING answer means the row cannot be ended; anything else
		// means no handler is running. It is the same rule the retirement recovery sweep
		// applies to another instance's work — the two were inconsistent, and this was the
		// unsafe side.
		rec := e.askExecutor(ctx, v)
		if rec.Reachable && rec.State == gojobv1.ExecutionState_EXECUTION_STATE_RUNNING {
			e.log.Warn("not releasing an execution whose handler is still running; the tenant "+
				"cannot be certified quiescent until it stops",
				"execution", v.ExecutionKey, "job", v.JobName, "executor", v.DispatchedTo)
			deferred = true
			continue
		}

		h := store.Holder{
			JobName: v.JobName, ExecutionID: v.ID, ExecutionKey: v.ExecutionKey,
			Owner: e.cfg.InstanceID, RunToken: v.RunToken, FenceEpoch: v.FenceEpoch,
		}

		// A real outcome beats a manufactured one. The executor finished and its own
		// ReportResult did not get through; recording `dead` / "outcome unknown" over a
		// success that is one RPC away is a loss this instance is choosing, not suffering.
		if rec.Reachable && rec.State == gojobv1.ExecutionState_EXECUTION_STATE_FINISHED &&
			finishedOutcomeIsUsable(rec.Outcome) {
			e.applyOutcome(ctx, h, rec.Outcome, v.DispatchedTo)
			progress = true
			e.untrack(h.ExecutionID, h.FenceEpoch)
			e.log.Info("adopted the executor's result while retiring the tenant",
				"execution", h.ExecutionKey, "job", h.JobName)
			continue
		}
		landed, err := e.store.ReleaseAsOwner(ctx, h)
		if err != nil {
			if !errors.Is(err, gojob.ErrContended) {
				e.log.Error("releasing owned work on retirement failed",
					"execution", h.ExecutionKey, "error", err)
			}
			continue
		}
		progress = true
		e.untrack(h.ExecutionID, h.FenceEpoch)
		e.log.Warn("ended an in-flight execution because the tenant was disabled",
			"execution", h.ExecutionKey, "job", h.JobName, "landed", landed)
	}
	return progress, deferred
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
		// it keeps an owner alive that the control plane has already concluded is gone. But a
		// DRAIN is not that: it must keep renewing, or it cannot release its own work.
		if !e.renewing() {
			return
		}
		lease := e.leaseSecondsFor(ctx, h.JobName)
		// Before the call, not after: see track.
		proved := time.Now()
		err := e.store.Renew(ctx, h, lease)
		switch {
		case err == nil:
			// Ownership proved again, so the dispatch gate may go on trusting this claim.
			// Without this the gate measures only from the CLAIM, and a routing decision that
			// takes longer than four fifths of the lease is refused while the lease under it
			// is being renewed perfectly well — an unnecessary expiry and recovery cycle for
			// a healthy execution.
			e.confirmHold(h.ExecutionID, h.FenceEpoch, proved)

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
		return int(defaultLease / time.Second)
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
