package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
	"github.com/abcdeqwer/go-job/internal/dispatch"
	"github.com/abcdeqwer/go-job/internal/outcome"
	"github.com/abcdeqwer/go-job/internal/store"
)

// materializePass turns due state rows into executions.
//
// Cron and poll are scanned separately because they are different questions of the same
// table, and one query answering both would need an OR that no index satisfies.
func (e *Engine) materializePass(ctx context.Context) {
	due, err := e.store.DueCron(ctx, e.cfg.PageSize)
	if err != nil {
		e.log.Error("cron due scan failed", "error", err)
	}
	for _, d := range due {
		if e.stopping() {
			return
		}
		res, err := e.store.MaterializeCron(ctx, d.JobName, e.compile, e.cfg.MisfireGrace)
		switch {
		case errors.Is(err, gojob.ErrContended):
			// Another instance is deciding about this job. Normal.
		case err != nil:
			e.log.Error("materializing cron job failed", "job", d.JobName, "error", err)
		case res.Outcome == store.MaterializedExecution:
			e.log.Debug("materialized", "job", d.JobName, "execution", res.ExecutionKey,
				"scheduled_at", res.ScheduledAt)
		}
		if err == nil && res.Missed > 0 {
			e.log.Warn("fire instants were missed",
				"job", d.JobName, "missed", res.Missed, "exact", res.MissedExact,
				"created", res.Outcome == store.MaterializedExecution)
		}
	}

	polls, err := e.store.DuePoll(ctx, e.cfg.PageSize)
	if err != nil {
		e.log.Error("poll due scan failed", "error", err)
		return
	}
	for _, d := range polls {
		if e.stopping() {
			return
		}
		// A poll key carries a fresh monotonic identifier rather than being derived from an
		// instant: a timestamp-derived key would collide with a retained pass after a
		// business-clock shift, and a reusable per-job key would make every pass after the
		// first a duplicate.
		key := store.ExecutionKey("p", d.JobName, uuid.NewString())
		res, err := e.store.MaterializePoll(ctx, d.JobName, key, e.compile)
		switch {
		case errors.Is(err, gojob.ErrContended):
		case err != nil:
			e.log.Error("materializing poll pass failed", "job", d.JobName, "error", err)
		case res.Outcome == store.MaterializedExecution:
			e.log.Debug("poll pass materialized", "job", d.JobName, "execution", res.ExecutionKey)
		}
	}
}

// driftPass recomputes state rows whose definition has moved on.
//
// The due scan cannot be the only reader of config_version, because it visits only rows that
// are already due: an operator changing a weekly job's expression would otherwise see nothing
// happen until the old next_fire_at arrives a week later — the edit accepted, audited,
// displayed in the UI, and silently inert.
func (e *Engine) driftPass(ctx context.Context) {
	names, err := e.store.Drifted(ctx, e.cfg.PageSize)
	if err != nil {
		e.log.Error("drift scan failed", "error", err)
		return
	}
	for _, name := range names {
		if e.stopping() {
			return
		}
		next, err := e.store.Recompute(ctx, name, e.compile)
		if errors.Is(err, gojob.ErrContended) {
			continue
		}
		if err != nil {
			e.log.Error("recomputing a drifted job failed", "job", name, "error", err)
			continue
		}
		e.log.Info("schedule recomputed after a definition change", "job", name, "next_fire_at", next)
	}
}

// dispatchPass claims ready work and hands it to executors.
//
// Manual and scheduled work are discovered by two bounded queries, not one ordered query. A
// single ordered page swaps one starvation for another: with enough ready manual rows, every
// page is manual and cron work is never discovered, however old it gets.
func (e *Engine) dispatchPass(ctx context.Context) {
	for _, manual := range []bool{true, false} {
		candidates, err := e.store.ReadyCandidates(ctx, manual, e.cfg.PageSize)
		if err != nil {
			e.log.Error("candidate discovery failed", "manual", manual, "error", err)
			continue
		}
		for _, c := range candidates {
			if e.stopping() || ctx.Err() != nil {
				return
			}
			e.claimAndDispatch(ctx, c)
		}
	}
}

func (e *Engine) claimAndDispatch(ctx context.Context, c store.Candidate) {
	// The executors are chosen BEFORE the claim, because dispatched_to is written in the claim
	// transaction — before the Run call. Writing it only after the reply creates a window that
	// loses work: the executor accepts, this scheduler dies before recording where it sent the
	// job, and recovery finds dispatched_to unset, concludes the dispatch never landed, and
	// sends the same work elsewhere while the first executor runs.
	//
	// A LIST rather than one, so a refusal can try the next instance. An executor advertising
	// stale headroom would otherwise refuse every dispatch while a healthy one is never chosen.
	def, err := e.store.Definition(ctx, c.JobName)
	if err != nil {
		e.log.Error("reading a candidate's definition failed", "job", c.JobName, "error", err)
		return
	}
	targets, err := e.pickExecutors(ctx, def)
	if err != nil {
		// No live executor. The claim below still runs, so the orphan backoff is applied and
		// the row stops occupying the front of every discovery page.
		e.log.Warn("no live executor for a ready execution",
			"job", c.JobName, "handler", def.HandlerKey, "execution", c.ExecutionKey)
	}

	p := store.ClaimParams{
		ExecutionID:    c.ID,
		JobName:        c.JobName,
		ExecutionKey:   c.ExecutionKey,
		Owner:          e.cfg.InstanceID,
		RunToken:       uuid.NewString(),
		BackoffSeconds: e.backoff(0),
	}
	if len(targets) > 0 {
		p.ExecutorID = targets[0].ExecutorID
	}

	// Re-checked immediately before the claim. Reading the definition and choosing an executor
	// take time, and an instance that lost its right to operate during them must not go on to
	// commit a claim and start new work.
	if e.stopping() {
		return
	}

	res, err := e.store.Claim(ctx, p, e.runnableFor(targets))
	if errors.Is(err, gojob.ErrContended) || errors.Is(err, gojob.ErrMissingState) {
		return
	}
	if err != nil {
		e.log.Error("claim failed", "job", c.JobName, "execution", c.ExecutionKey, "error", err)
		return
	}
	switch res.Outcome {
	case store.ClaimDeferred, store.ClaimSkipped:
		e.log.Debug("candidate not claimed", "job", c.JobName,
			"execution", c.ExecutionKey, "outcome", res.Outcome, "reason", res.Reason)
		return
	case store.ClaimExpired:
		e.log.Warn("execution passed its runtime cap before dispatch",
			"job", c.JobName, "execution", c.ExecutionKey)
		return
	}

	h := store.Holder{
		JobName:      c.JobName,
		ExecutionID:  c.ID,
		ExecutionKey: c.ExecutionKey,
		Owner:        e.cfg.InstanceID,
		RunToken:     p.RunToken,
		FenceEpoch:   res.FenceEpoch,
	}
	// Tracked from the moment the claim commits, not from acceptance: the row holds a lease
	// from now, and an unrenewed `dispatching` row is a recovery cycle nobody needed.
	e.track(h)

	if len(targets) == 0 {
		// Claimed with nowhere to send it. Release immediately rather than holding the job
		// for a lease's worth of nothing.
		e.releaseUndispatchable(ctx, p, res.FenceEpoch, "no live executor")
		return
	}

	// The EXECUTION's snapshot, read after the claim so it carries the timeout_at the claim
	// just set. Its parameters and scheduled_at are what the run was created with; the
	// definition's current values are not the same thing.
	snapshot, err := e.store.ExecutionByKey(ctx, c.ExecutionKey)
	if err != nil {
		e.log.Error("reading the claimed execution failed",
			"execution", c.ExecutionKey, "error", err)
		e.releaseUndispatchable(ctx, p, res.FenceEpoch, "could not read the execution row")
		return
	}
	e.dispatch(ctx, h, res.Definition, snapshot, targets, p)
}

// runnableFor builds the claim's runnability check.
//
// Condition 3 — "does any live executor declare this handler, in this job's group" — is
// answered inside the transaction against the registry, not from the routing decision made
// above it. The two can disagree, and the transaction's answer is the one that must decide,
// because it is the one taken under the job's lock.
func (e *Engine) runnableFor(targets []store.Executor) store.Runnable {
	return func(ctx context.Context, tx *sql.Tx, def gojob.Definition, st store.StateRow) error {
		if !def.Enabled || def.Retired {
			return fmt.Errorf("%w: job %q is disabled or retired", gojob.ErrNotRunnable, def.JobName)
		}
		served, err := store.HandlerIsServed(ctx, tx, def, e.cfg.ExecutorLiveness)
		if err != nil {
			return err
		}
		if !served {
			return fmt.Errorf("%w: no live executor declares handler %q%s",
				gojob.ErrNotRunnable, def.HandlerKey, groupSuffix(def.ExecutorGroup))
		}
		if len(targets) == 0 {
			return fmt.Errorf("%w: no executor was selected for %q", gojob.ErrNotRunnable, def.JobName)
		}
		return nil
	}
}

func groupSuffix(group string) string {
	if group == "" {
		return ""
	}
	return " in group " + group
}

// pickExecutors orders the live executors for a job, best first.
//
// Least-loaded, with the advisory `running` count as the signal. It is advisory on purpose:
// the executor's own refusal is authoritative, and treating a stale count as a hard limit
// would leave capacity unused whenever a heartbeat is a second old. Ties break on the freshest
// heartbeat, so a genuinely idle fleet spreads out.
//
// The full ORDER matters, not just the winner. A refusal moves to the next instance, and an
// executor advertising stale headroom would otherwise refuse every dispatch while a healthy
// one with less advertised room is never tried.
func (e *Engine) pickExecutors(ctx context.Context, def gojob.Definition) ([]store.Executor, error) {
	live, err := e.store.LiveExecutors(ctx, def.HandlerKey, def.ExecutorGroup, e.cfg.ExecutorLiveness)
	if err != nil {
		return nil, err
	}
	if len(live) == 0 {
		return nil, fmt.Errorf("%w: handler %q%s", gojob.ErrNotRunnable,
			def.HandlerKey, groupSuffix(def.ExecutorGroup))
	}
	// An executor whose dispatch path has been blackholed goes to the BACK rather than being
	// removed. Removing it would make a fleet of one unroutable the moment its first dispatch
	// timed out; ordering it last means a healthy instance is always preferred and a
	// quarantined one is still tried when it is the only thing there is.
	sort.SliceStable(live, func(i, j int) bool {
		hi, hj := e.dispatchHealthy(live[i].ExecutorID), e.dispatchHealthy(live[j].ExecutorID)
		if hi != hj {
			return hi
		}
		return headroom(live[i]) > headroom(live[j])
	})
	return live, nil
}

// headroom is how much spare capacity an executor claims to have. An executor reporting no
// capacity at all is treated as having one slot rather than none, so a fleet that never
// reports capacity is still routable — the refusal path handles the rest.
func headroom(x store.Executor) int {
	capacity := x.Capacity
	if capacity <= 0 {
		capacity = 1
	}
	return capacity - x.Running
}

func (e *Engine) releaseUndispatchable(ctx context.Context, p store.ClaimParams, epoch int64, why string) {
	if err := e.store.Refuse(ctx, p, epoch); err != nil {
		e.log.Error("releasing an undispatchable execution failed",
			"job", p.JobName, "execution", p.ExecutionKey, "error", err)
	}
	e.untrack(p.ExecutionID, epoch)
	e.log.Warn("execution returned to ready", "job", p.JobName,
		"execution", p.ExecutionKey, "reason", why)
}

// dispatch sends the Run call and applies its outcome, trying each live executor in turn.
//
// snapshot is the EXECUTION's own row, not the definition. Its parameters and scheduled_at are
// what the run was created with, and sending the definition's current values instead would
// replace a manual trigger's parameter override with the default and tell a delayed daily fire
// that its scheduled time is now — from which a handler derives the wrong business date.
func (e *Engine) dispatch(ctx context.Context, h store.Holder, def gojob.Definition,
	snapshot store.Stale, targets []store.Executor, p store.ClaimParams) {

	params, err := decodeParams(snapshot.Params)
	if err != nil {
		e.log.Error("an execution's stored parameters are not a JSON object",
			"job", def.JobName, "execution", h.ExecutionKey, "error", err)
		e.releaseUndispatchable(ctx, p, h.FenceEpoch, "unparseable parameters")
		return
	}

	remaining := snapshot.RemainingTimeout
	if remaining <= 0 {
		// The cap elapsed between the claim and here. Dispatching now would hand an executor a
		// budget the scheduler has already spent.
		e.log.Warn("runtime cap elapsed before dispatch",
			"job", def.JobName, "execution", h.ExecutionKey)
		e.releaseUndispatchable(ctx, p, h.FenceEpoch, "runtime cap elapsed")
		return
	}

	deadline := time.Now().Add(e.cfg.DispatchResendWindow)

	for i, target := range targets {
		spec := dispatch.RunSpec{
			Address:      target.Address,
			Tenant:       e.cfg.Tenant,
			ExecutionKey: h.ExecutionKey,
			RunToken:     h.RunToken,
			JobName:      def.JobName,
			HandlerKey:   def.HandlerKey,
			Attempt:      snapshot.AttemptNo + 1,
			ScheduledAt:  snapshot.ScheduledAt,
			// SILENCE budget and RUNTIME budget are different bounds, and the runtime one is
			// what REMAINS rather than what was configured: the cap started at the claim, so
			// handing over the full amount after a delayed re-send leaves the executor
			// believing it owns the run for longer than the scheduler will wait.
			SilenceDeadline:  def.Lease,
			RemainingTimeout: remaining,
			Params:           params,
		}

		if i > 0 {
			// Routing moved. dispatched_to must move with it BEFORE the send, or recovery
			// would reconcile with the wrong process.
			if err := e.store.Retarget(ctx, h, target.ExecutorID); err != nil {
				if errors.Is(err, gojob.ErrFenced) {
					e.untrack(h.ExecutionID, h.FenceEpoch)
					return
				}
				// No second Run was made, so the row is provably unstarted — but it is claimed
				// and held, and returning here would leave the heartbeat renewing it until its
				// runtime cap. Release it and let the next pass try again.
				e.log.Error("recording a new dispatch target failed; releasing the claim",
					"execution", h.ExecutionKey, "error", err)
				e.releaseUndispatchable(ctx, p, h.FenceEpoch, "could not record the new target")
				return
			}
		}

		switch e.attemptDispatch(ctx, h, def, spec, target, deadline) {
		case dispatch.Accepted:
			return
		case dispatch.Unknown:
			// The outcome is unknown for THIS executor. Trying another would risk a second
			// handler, so the row stays `dispatching` with its target recorded and recovery
			// reconciles with the executor that may have it.
			e.untrack(h.ExecutionID, h.FenceEpoch)
			return
		}
		// Refused, provably. Try the next instance.
		e.log.Info("executor declined; trying another", "job", def.JobName,
			"execution", h.ExecutionKey, "executor", target.ExecutorID,
			"remaining_targets", len(targets)-i-1)
	}

	// Every live executor declined. Release with a backoff rather than holding the job.
	e.releaseUndispatchable(ctx, p, h.FenceEpoch, "every live executor declined")
}

// attemptDispatch sends to one executor, re-sending on an unknown outcome within the bound.
func (e *Engine) attemptDispatch(ctx context.Context, h store.Holder, def gojob.Definition,
	spec dispatch.RunSpec, target store.Executor, deadline time.Time) dispatch.Answer {

	for attempt := 0; ; attempt++ {
		// Before EVERY send, including the first. The claim's own fence check is separated
		// from this one by a definition read and a routing decision, and at a fence age of
		// 29.9 seconds that gap is enough to cross the limit and still hand work to an
		// executor on behalf of an instance that no longer has the right to.
		if e.stopping() {
			return dispatch.Unknown
		}
		if attempt > 0 {
			// The wall-clock bound is checked BEFORE the send, not after. Checked afterwards
			// it does not bound anything: a call that returns just inside the window is
			// followed by a backoff and then another send that leaves it entirely.
			if time.Now().After(deadline) {
				e.markUnhealthy(target.ExecutorID)
				e.log.Warn("dispatch outcome unknown and the re-send window has closed; "+
					"leaving the row to recovery",
					"execution", h.ExecutionKey, "executor", target.ExecutorID)
				return dispatch.Unknown
			}
			if e.stopping() {
				return dispatch.Unknown
			}
			// Re-read the cap before every re-send. A first call can consume most of what was
			// left, and re-sending the ORIGINAL budget would tell the executor it may run for
			// longer than the scheduler will wait — after which the timeout pass fences and
			// releases the row while that executor still believes it owns the work.
			cur, err := e.store.ExecutionByKey(ctx, h.ExecutionKey)
			if err != nil {
				return dispatch.Unknown
			}
			if cur.RemainingTimeout <= 0 {
				e.log.Warn("runtime cap elapsed mid-dispatch; leaving the row to the timeout scan",
					"execution", h.ExecutionKey, "executor", target.ExecutorID)
				return dispatch.Unknown
			}
			spec.RemainingTimeout = cur.RemainingTimeout
		}

		res := e.disp.Run(ctx, spec)

		switch res.Answer {
		case dispatch.Accepted:
			// ALREADY_EXISTS naming a DIFFERENT token is not this attempt being adopted: it is
			// a collision with an older attempt the scheduler already fenced, whose handler is
			// still running. Treating it as acceptance would record a start that never
			// happened and then wait for a result belonging to someone else.
			if res.HeldToken != "" && res.HeldToken != h.RunToken {
				e.log.Warn("executor holds a different attempt for this execution",
					"execution", h.ExecutionKey, "held", res.HeldToken, "sent", h.RunToken)
				return dispatch.Unknown
			}
			if err := e.store.Accept(ctx, h.ExecutionID, h.RunToken, h.FenceEpoch,
				int(def.Lease/time.Second)); err != nil {
				if errors.Is(err, gojob.ErrFenced) {
					e.untrack(h.ExecutionID, h.FenceEpoch)
					return dispatch.Unknown
				}
				// The executor HAS the work, but this instance could not record it. Leaving the
				// row `dispatching` while continuing to renew it means the silence scan cannot
				// see it — that scan looks at `running` — so it would sit renewed until the
				// hard cap. Stop renewing and let recovery reconcile with the executor, which
				// is the path built for exactly this uncertainty.
				e.log.Error("recording acceptance failed; leaving the row to recovery",
					"execution", h.ExecutionKey, "error", err)
				e.untrack(h.ExecutionID, h.FenceEpoch)
				return dispatch.Unknown
			}
			e.log.Debug("dispatched", "job", def.JobName, "execution", h.ExecutionKey,
				"executor", target.ExecutorID)
			return dispatch.Accepted

		case dispatch.Refused:
			return dispatch.Refused

		default:
			// Outcome unknown. Re-send to the SAME executor, which answers ALREADY_EXISTS if
			// it has it — but bounded, in attempts and in time, and additionally capped by the
			// execution's own runtime budget. Without the bound the row sits `dispatching` for
			// good: the executor stays registration-live so it keeps being chosen, the leases
			// keep being renewed, and no budget is ever consumed.
			if attempt+1 >= e.cfg.DispatchResendLimit {
				// Mark the path unhealthy. An executor that heartbeats but never answers Run
				// stays registration-live, so without this it is chosen again on the next
				// claim, exhausts the bound again, and marches the job to dead through its
				// recovery budget without any business code running.
				e.markUnhealthy(target.ExecutorID)
				e.log.Warn("dispatch outcome unknown after the re-send bound; "+
					"suppressing this executor and leaving the row to recovery",
					"execution", h.ExecutionKey, "executor", target.ExecutorID,
					"attempts", attempt+1, "code", res.Code, "error", res.Err)
				return dispatch.Unknown
			}
			select {
			case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
			case <-ctx.Done():
				return dispatch.Unknown
			case <-e.stop:
				return dispatch.Unknown
			}
		}
	}
}

func decodeParams(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// recoverPass is the only path that clears an expired holder.
func (e *Engine) recoverPass(ctx context.Context) {
	stale, err := e.store.StaleExecutions(ctx, e.cfg.PageSize)
	if err != nil {
		e.log.Error("stale scan failed", "error", err)
		return
	}
	for _, v := range stale {
		if e.stopping() {
			return
		}
		e.recoverOne(ctx, v)
	}
}

func (e *Engine) recoverOne(ctx context.Context, v store.Stale) {
	// Phase 2, outside any transaction. An RPC to a process that may be wedged must never be
	// made while holding a row lock: a connection that stays open and never answers would pin
	// job_state and job_execution indefinitely, blocking completion, cancellation and every
	// other recovery for that job.
	rec := dispatch.Reconciliation{}
	if v.DispatchedTo != "" {
		if addr, ok := e.addressOf(ctx, v.DispatchedTo); ok {
			rec = e.disp.GetExecution(ctx, addr, e.cfg.Tenant, v.ExecutionKey, e.cfg.ReconcileDeadline)
		}
	}

	// An answer about a DIFFERENT run_token describes another attempt entirely. Discard it
	// rather than adopting a state that belongs to someone else.
	if rec.Reachable && rec.RunToken != "" && rec.RunToken != v.RunToken {
		e.log.Warn("executor answered about a different attempt; treating as unknown",
			"execution", v.ExecutionKey, "asked_about", v.RunToken, "answered_about", rec.RunToken)
		rec = dispatch.Reconciliation{}
	}

	// Phase 2 was an RPC with its own deadline. Re-check before phase 3 writes: the fence can
	// have lapsed during it, and adopting or resolving afterwards is precisely the invisible
	// ownership the fence exists to stop.
	if e.stopping() {
		return
	}

	switch {
	case rec.Reachable && rec.State == gojobv1.ExecutionState_EXECUTION_STATE_RUNNING:
		epoch, err := e.store.Adopt(ctx, v, e.cfg.InstanceID, e.leaseSecondsFor(ctx, v.JobName))
		switch {
		case errors.Is(err, store.ErrCapElapsed):
			e.fenceTimedOut(ctx, v)
		case errors.Is(err, gojob.ErrContended):
		case err != nil:
			e.log.Error("adopting a running execution failed",
				"execution", v.ExecutionKey, "error", err)
		default:
			// Same run_token, new epoch: rotating the token would fence a healthy run, while
			// the new epoch is what evicts the dead scheduler's in-flight writes.
			e.track(store.Holder{
				JobName: v.JobName, ExecutionID: v.ID, ExecutionKey: v.ExecutionKey,
				Owner: e.cfg.InstanceID, RunToken: v.RunToken, FenceEpoch: epoch,
			})
			e.log.Info("adopted a running execution from a lost scheduler",
				"execution", v.ExecutionKey, "epoch", epoch)
		}

	case rec.Reachable && rec.State == gojobv1.ExecutionState_EXECUTION_STATE_FINISHED:
		e.adoptResult(ctx, v, rec)

	default:
		landed, err := e.store.Resolve(ctx, v, e.backoff(v.RecoveryCount))
		if errors.Is(err, gojob.ErrContended) {
			return
		}
		if err != nil {
			e.log.Error("resolving an unknown attempt failed",
				"execution", v.ExecutionKey, "error", err)
			return
		}
		e.untrack(v.ID, v.FenceEpoch)
		e.log.Warn("attempt resolved as unknown", "execution", v.ExecutionKey,
			"landed", landed, "recoveries", v.RecoveryCount+1)
	}
}

// adoptResult applies an outcome the executor still remembers.
func (e *Engine) adoptResult(ctx context.Context, v store.Stale, rec dispatch.Reconciliation) {
	// Take ownership before writing the result: the terminal CAS is guarded on the epoch, and
	// the one on the row belongs to the scheduler that died.
	epoch, err := e.store.Adopt(ctx, v, e.cfg.InstanceID, e.leaseSecondsFor(ctx, v.JobName))
	switch {
	case errors.Is(err, store.ErrCapElapsed):
		e.fenceTimedOut(ctx, v)
		return
	case errors.Is(err, gojob.ErrContended):
		return
	case err != nil:
		e.log.Error("adopting a finished execution failed", "execution", v.ExecutionKey, "error", err)
		return
	}

	h := store.Holder{
		JobName: v.JobName, ExecutionID: v.ID, ExecutionKey: v.ExecutionKey,
		Owner: e.cfg.InstanceID, RunToken: v.RunToken, FenceEpoch: epoch,
	}
	e.applyOutcome(ctx, h, rec.Outcome, v.DispatchedTo)
}

// applyOutcome writes a reported result through the shared classification, so a result
// recovered from a surviving executor lands identically to one reported normally.
func (e *Engine) applyOutcome(ctx context.Context, h store.Holder, oc *gojobv1.ExecutionOutcome, executorID string) {
	err := outcome.Apply(ctx, e.store, h, oc, executorID, e.backoff(0))
	if err != nil && !errors.Is(err, gojob.ErrFenced) {
		e.log.Error("recording an outcome failed", "execution", h.ExecutionKey, "error", err)
	}
	e.untrack(h.ExecutionID, h.FenceEpoch)
}

// timeoutPass ends executions past their runtime cap whose leases are still being renewed —
// the population the stale scan cannot see, because a healthy scheduler tracking a handler
// that simply will not finish keeps the lease live indefinitely.
func (e *Engine) timeoutPass(ctx context.Context) {
	over, err := e.store.TimedOut(ctx, e.cfg.PageSize)
	if err != nil {
		e.log.Error("timeout scan failed", "error", err)
		return
	}
	for _, v := range over {
		if e.stopping() {
			return
		}
		e.fenceTimedOut(ctx, v)
	}
}

// silencePass ends executions that stopped reporting while their scheduler kept renewing.
//
// The lease and the silence budget bound different things — how long the SCHEDULER may go
// without renewing, and how long the EXECUTOR may go without reporting — and only the first is
// visible to the stale scan. A healthy scheduler tracking an executor whose progress loop died
// renews the lease indefinitely, so without this pass the row is owned until its runtime cap,
// which for a job capped in hours means nobody learns for hours.
func (e *Engine) silencePass(ctx context.Context) {
	silent, err := e.store.Silent(ctx, e.cfg.PageSize)
	if err != nil {
		e.log.Error("silence scan failed", "error", err)
		return
	}
	for _, v := range silent {
		if e.stopping() {
			return
		}
		e.resolveSilent(ctx, v)
	}
}

// resolveSilent decides what a silent execution actually is, by asking its executor.
//
// Silence is not proof the work stopped. An executor whose progress loop died, or whose
// reports are merely slow, is still running the handler — and fencing it on the strength of a
// missing progress report releases the job lock while that handler writes, after which the
// next execution of the same job starts alongside it.
//
// So this is the same three-phase shape recovery uses, for the same reason: reconcile OUTSIDE
// any transaction, and only treat the attempt as unknown when the executor genuinely cannot
// answer for it.
func (e *Engine) resolveSilent(ctx context.Context, v store.Stale) {
	rec := dispatch.Reconciliation{}
	if v.DispatchedTo != "" {
		if addr, ok := e.addressOf(ctx, v.DispatchedTo); ok {
			rec = e.disp.GetExecution(ctx, addr, e.cfg.Tenant, v.ExecutionKey, e.cfg.ReconcileDeadline)
		}
	}
	if rec.Reachable && rec.RunToken != "" && rec.RunToken != v.RunToken {
		rec = dispatch.Reconciliation{} // an answer about a different attempt
	}
	if e.stopping() {
		return
	}

	switch {
	case rec.Reachable && rec.State == gojobv1.ExecutionState_EXECUTION_STATE_RUNNING:
		// It IS running; its reports were merely late. Extend the silence budget and leave it
		// alone. The runtime cap is untouched, so a handler that never finishes is still
		// bounded — by the cap, which is the bound that means "too long".
		h := store.Holder{
			JobName: v.JobName, ExecutionID: v.ID, ExecutionKey: v.ExecutionKey,
			Owner: e.cfg.InstanceID, RunToken: v.RunToken, FenceEpoch: v.FenceEpoch,
		}
		if err := e.store.ExtendDeadline(ctx, h, e.silenceSeconds()); err != nil &&
			!errors.Is(err, gojob.ErrFenced) {
			e.log.Error("extending a late executor's silence budget failed",
				"execution", v.ExecutionKey, "error", err)
		}
		e.log.Info("executor was late, not lost; silence budget extended",
			"execution", v.ExecutionKey, "executor", v.DispatchedTo)

	case rec.Reachable && rec.State == gojobv1.ExecutionState_EXECUTION_STATE_FINISHED:
		// Completed under the CURRENT holder, not through Adopt. Adopt requires an expired
		// lease — its job is taking work from an owner that is gone — and this instance is
		// still renewing perfectly well. Routing a silent-but-finished executor through it
		// affected zero rows, and when the executor's own result retries ran out, a genuine
		// success was eventually recorded as a timeout.
		h := store.Holder{
			JobName: v.JobName, ExecutionID: v.ID, ExecutionKey: v.ExecutionKey,
			Owner: e.cfg.InstanceID, RunToken: v.RunToken, FenceEpoch: v.FenceEpoch,
		}
		e.applyOutcome(ctx, h, rec.Outcome, v.DispatchedTo)

	default:
		// Unreachable, NOT_FOUND, or an answer about someone else. Only now is the attempt
		// genuinely unknown. Ask it to stop on the way out — it cannot be relied on, but an
		// executor that does comply stops burning resources on work already written off.
		e.requestStop(ctx, v, "no progress within the silence budget")

		err := e.store.ExpireSilent(ctx, v)
		if errors.Is(err, gojob.ErrContended) {
			return
		}
		if err != nil {
			e.log.Error("expiring a silent execution failed",
				"execution", v.ExecutionKey, "error", err)
			return
		}
		e.untrack(v.ID, v.FenceEpoch)
		e.log.Warn("execution ended: silent and unreachable",
			"execution", v.ExecutionKey, "job", v.JobName, "executor", v.DispatchedTo)
	}
}

// silenceSeconds is how long an executor may say nothing. It tracks the executor liveness
// window, because an executor that is not heartbeating at all is a different problem with a
// different scan.
func (e *Engine) silenceSeconds() int {
	s := int(e.cfg.ExecutorLiveness / time.Second)
	if s < 1 {
		s = 30
	}
	return s
}

// cancelPass carries an operator's cancel to the executor holding the work.
//
// RequestCancel only writes a row, and it has to: the operator's request lands on whichever
// scheduler instance the load balancer picked, which is usually not the one holding the
// execution and may not be able to reach the executor at all. Something has to notice the row
// and make the call, and that something is every instance, repeatedly — Cancel is idempotent
// and an executor that no longer has it answers NOT_FOUND, which this treats as done.
func (e *Engine) cancelPass(ctx context.Context) {
	rows, err := e.store.CancelRequested(ctx, e.cfg.PageSize)
	if err != nil {
		e.log.Error("cancel scan failed", "error", err)
		return
	}
	for _, v := range rows {
		if e.stopping() {
			return
		}
		e.requestStop(ctx, v, "cancelled by an operator")
	}
}

// requestStop relays a stop to whichever executor holds an execution.
func (e *Engine) requestStop(ctx context.Context, v store.Stale, reason string) {
	if v.DispatchedTo == "" {
		return
	}
	addr, ok := e.addressOf(ctx, v.DispatchedTo)
	if !ok {
		return
	}
	if err := e.disp.Cancel(ctx, addr, e.cfg.Tenant, v.ExecutionKey, v.RunToken, reason); err != nil {
		e.log.Debug("a stop request was not acknowledged",
			"execution", v.ExecutionKey, "executor", v.DispatchedTo, "error", err)
	}
}

func (e *Engine) fenceTimedOut(ctx context.Context, v store.Stale) {
	// Ask the executor to stop first. It cannot be relied on — cancellation is cooperative and
	// the executor may be wedged — but an executor that DOES comply stops burning resources on
	// work the scheduler has already written off.
	e.requestStop(ctx, v, "runtime cap elapsed")

	err := e.store.FenceTimedOut(ctx, v)
	if errors.Is(err, gojob.ErrContended) {
		return
	}
	if err != nil {
		e.log.Error("fencing a timed-out execution failed", "execution", v.ExecutionKey, "error", err)
		return
	}
	e.untrack(v.ID, v.FenceEpoch)
	e.log.Warn("execution fenced at its runtime cap", "execution", v.ExecutionKey,
		"job", v.JobName, "executor", v.DispatchedTo)
}

// addressOf resolves an executor id to an address for a reconciliation or cancel call.
//
// A registration that has been reaped resolves to nothing, which is the correct answer: there
// is no process to ask, and recovery classifies the attempt unknown.
func (e *Engine) addressOf(ctx context.Context, executorID string) (string, bool) {
	addr, err := e.store.ExecutorAddress(ctx, executorID)
	if err != nil {
		return "", false
	}
	return addr, addr != ""
}
