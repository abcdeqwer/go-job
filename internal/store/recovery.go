package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// Stale is an execution whose lease has lapsed, as read by the non-locking phase-1 scan.
type Stale struct {
	ID             int64
	ExecutionKey   string
	JobName        string
	Status         gojob.Status
	DispatchedTo   string
	RunToken       string
	FenceEpoch     int64
	AttemptNo      int
	MaxAttempts    int
	RecoveryCount  int
	MaxRecoveries  int
	TimeoutExpired bool

	// SilenceExpired is whether the silence budget has lapsed, measured by the database at the
	// moment of the read. Re-read under the lock before acting on it: a progress report during
	// reconciliation moves it, and that report is the evidence the execution is alive.
	SilenceExpired bool

	// LeaseLive is whether ownership has NOT yet lapsed. Once it has, the row belongs to
	// recovery — the single path that clears an expired holder — and no other writer may act
	// as its owner.
	LeaseLive bool

	// RemainingTimeout is what is LEFT of the runtime cap, measured by the database. It is
	// what a dispatch must send, not the job's configured timeout: the cap starts at the
	// claim, so a dispatch delayed by a re-send that hands over the full budget again leaves
	// the executor believing it owns the run for longer than the scheduler will wait — and
	// the difference is a window where the scheduler has fenced and re-dispatched while the
	// first handler is still going.
	RemainingTimeout time.Duration

	// ScheduledAt and Params are the execution's own snapshot. A dispatch must carry these
	// and not the definition's current values: a manual trigger's parameter override would
	// otherwise be replaced by the definition's default, and a delayed daily fire would be
	// told its scheduled time is now — from which a handler derives the wrong business date.
	ScheduledAt time.Time
	Params      []byte
}

// StaleExecutions is recovery phase 1: read, no locks.
//
// Every holder of a job's state row has an execution row — including a fixed-delay pass — so
// there is one scan and one algorithm rather than a second mechanism for pollers that would
// need its own correctness argument.
//
// `lease_until < UTC_TIMESTAMP()` is an ownership comparison and uses the database clock. Comparing a
// lease against a scheduler's own host clock would make ownership depend on skew between
// machines, which is the one thing a distributed lease must not do.
func (s *Store) StaleExecutions(ctx context.Context, limit int) ([]Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), timeout_at), 0), 0),
		       scheduled_at, params_json,
		       (timeout_at IS NOT NULL AND timeout_at < UTC_TIMESTAMP()),
		       (deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()),
		       (lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP())
		FROM job_execution
		WHERE status IN ('dispatching', 'running', 'cancel_requested')
		  AND lease_until < UTC_TIMESTAMP()
		ORDER BY lease_until, id LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("scan stale executions: %w", err)
	}
	defer rows.Close()

	var out []Stale
	for rows.Next() {
		var (
			v             Stale
			remainingSecs int64
			params        []byte
		)
		if err := rows.Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
			&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
			&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
			&remainingSecs, &v.ScheduledAt, &params,
			&v.TimeoutExpired, &v.SilenceExpired, &v.LeaseLive); err != nil {
			return nil, fmt.Errorf("scan stale execution: %w", err)
		}
		v.RemainingTimeout = time.Duration(remainingSecs) * time.Second
		v.Params = params
		out = append(out, v)
	}
	return out, rows.Err()
}

// Reconciliation is the phase-2 answer. Phase 2 happens OUTSIDE any transaction: an RPC to a
// process that may be wedged must never be made while holding a row lock, or a connection
// that stays open and never answers would pin job_state and job_execution indefinitely,
// blocking completion, cancellation and every other recovery for that job.
type Reconciliation int

const (
	// ReconcileUnknown covers NOT_FOUND, unreachable, a lapsed deadline, and an answer about
	// a different run_token. It is the honest classification: nobody can say whether the
	// handler ran, and recording it as failure would claim knowledge no one has.
	ReconcileUnknown Reconciliation = iota

	// ReconcileRunning means the executor is still working. The work was never interrupted.
	ReconcileRunning

	// ReconcileFinished means the executor holds a result to adopt.
	ReconcileFinished
)

// Adopt is recovery phase 3 for an executor that answered RUNNING.
//
// It takes the lease with a NEW fence_epoch and the SAME run_token. Rotating the token would
// fence a healthy run — every ownership-bearing write the executor's handler makes carries
// that token — while a new epoch is what evicts the dead scheduler's in-flight writes. The
// two identifiers exist precisely so this case can keep one and rotate the other.
//
// Returns the new epoch to track under. It returns ErrCapElapsed when the execution outran
// its runtime cap while the reconciliation call was in flight; the caller fences it instead.
// promote says the executor confirmed the handler is RUNNING, so a row still recorded as
// `dispatching` should be moved on exactly as a live Accept would have moved it.
func (s *Store) Adopt(ctx context.Context, v Stale, newOwner string,
	leaseSeconds, silenceSeconds int, promote bool) (int64, error) {
	var epoch int64
	err := s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, v.JobName)
		if err != nil {
			return err
		}
		// Re-verify everything phase 1 read. If any guard fails another instance got here
		// first, and this recovery simply abandons rather than competing.
		if !st.Held() || st.ActiveRunToken.String != v.RunToken || st.FenceEpoch != v.FenceEpoch {
			return fmt.Errorf("%w: job %q was recovered by another instance", gojob.ErrContended, v.JobName)
		}

		// Re-read the execution UNDER THE LOCK, for the same reason Resolve does: phase 2's
		// reconciliation RPC is bounded but not instant, and the row can change during it.
		// The case that matters here is the cap: an executor that answers RUNNING moments
		// after timeout_at has passed would otherwise be adopted and given a fresh lease,
		// keeping an over-budget handler alive and tracked until some later timeout scan
		// noticed. A cap that only applies when nothing races it is not a cap.
		cur, err := lockExecution(ctx, tx, v.ID)
		if err != nil {
			return err
		}
		if cur.RunToken != v.RunToken || cur.FenceEpoch != v.FenceEpoch {
			return fmt.Errorf("%w: execution %d was recovered by another instance", gojob.ErrContended, v.ID)
		}
		if cur.TimeoutExpired {
			return fmt.Errorf("%w: execution %d passed its runtime cap during reconciliation",
				ErrCapElapsed, v.ID)
		}
		epoch = v.FenceEpoch + 1
		now := s.clock.Now()

		res, err := tx.ExecContext(ctx, `
			UPDATE job_state
			SET write_seq = write_seq + 1,
			    active_owner = ?, fence_epoch = ?,
			    lease_until  = TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP()),
			    heartbeat_at = UTC_TIMESTAMP(), updated_at = ?
			WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?
			  AND lease_until < UTC_TIMESTAMP()`,
			newOwner, epoch, leaseSeconds, now, v.JobName, v.RunToken, v.FenceEpoch)
		if err != nil {
			return fmt.Errorf("adopt job lock %q: %w", v.JobName, err)
		}
		if err := assertOne(res, "adopt: job lock"); err != nil {
			return err
		}

		// A `dispatching` row that the executor says is RUNNING is an acceptance the dead
		// scheduler never got to record.
		//
		// Leaving the status alone looked harmless and was not. The silence scan reads only
		// `running` and `cancel_requested`, so an adopted handler that later goes quiet is
		// invisible to it and stays held until its runtime cap — which for a job capped in
		// days means days. `attempt_no` stays zero, so the same failure repeated can run the
		// work more times than max_attempts allows, bounded only by the recovery budget. And
		// deadline_at may still be NULL, so there is no silence budget to elapse at all.
		//
		// promote is false on the FINISHED path: chargeUnacceptedAttempt already accounts for
		// an attempt whose result arrived while the row was still `dispatching`, and charging
		// it here as well would count one run twice.
		// The decision is made HERE, in Go, from the row this transaction already locked —
		// not by testing `status = 'dispatching'` inside the SET clause.
		//
		// MySQL evaluates single-table UPDATE assignments left to right, and each one sees the
		// values written by the ones before it. An earlier version assigned status first and
		// then tested `status = 'dispatching'` in the assignments that followed, which by then
		// read `running`: the row was promoted while attempt_no stayed 0, started_at stayed
		// NULL and deadline_at stayed NULL — so the silence scan, which needs a deadline,
		// could never see it, and the attempt was never charged. Reproducible on MySQL 8.4,
		// and invisible to any test starting from an already-running row.
		//
		// A precomputed flag depends on no column at all, so no ordering can change it.
		promoteNow := promote && cur.Status == gojob.StatusDispatching

		res, err = tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq = write_seq + 1,
			    owner_instance = ?, fence_epoch = ?,
			    attempt_no  = IF(?, attempt_no + 1, attempt_no),
			    started_at  = IF(?, COALESCE(started_at, ?), started_at),
			    deadline_at = IF(?, TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP()), deadline_at),
			    status      = IF(?, 'running', status),
			    lease_until  = TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP()),
			    heartbeat_at = UTC_TIMESTAMP(), updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?`,
			newOwner, epoch,
			promoteNow, promoteNow, now, promoteNow, silenceSeconds, promoteNow,
			leaseSeconds, now, v.ID, v.RunToken, v.FenceEpoch)
		if err != nil {
			return fmt.Errorf("adopt execution %d: %w", v.ID, err)
		}
		return assertOne(res, "adopt: execution")
	})
	if err != nil {
		return 0, err
	}
	return epoch, nil
}

// Resolve is recovery phase 3 for the unknown case: the executor said NOT_FOUND, was
// unreachable, or answered about a different attempt.
//
// A recovery that returns the row to `ready` clears timeout_at, because the re-dispatch that
// follows is a new attempt and the cap is a per-attempt budget. A recovery that ends the
// execution keeps it as evidence.
//
// It does NOT touch attempt_no. That was incremented when the dispatch was accepted, and the
// re-dispatch that follows recovery increments it again — which is exactly the budget a crash
// should cost. Incrementing here as well would exhaust a budget of three in two real handler
// starts. What recovery charges instead is recovery_count, so a handler that reliably kills
// its executor still terminates.
//
// cancel_requested resolves to `cancelled`, never `ready`. Someone asked this run to stop; its
// executor dying is not a reason to start it again.
func (s *Store) Resolve(ctx context.Context, v Stale, backoffSeconds int) (gojob.Status, error) {
	var landed gojob.Status
	err := s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, v.JobName)
		if err != nil {
			return err
		}
		if !st.Held() || st.ActiveRunToken.String != v.RunToken || st.FenceEpoch != v.FenceEpoch {
			return fmt.Errorf("%w: job %q was recovered by another instance", gojob.ErrContended, v.JobName)
		}

		// Re-read the execution UNDER THE LOCK. Phase 1's snapshot was taken before the
		// reconciliation RPC, which is bounded but not instant, and the row can legitimately
		// change during it: an operator can commit `cancel_requested`, or timeout_at can
		// elapse. Deciding from the stale snapshot would send work an operator explicitly
		// cancelled straight back to `ready`.
		//
		// The state row is already locked, so this takes the two rows in the canonical order.
		cur, err := lockExecution(ctx, tx, v.ID)
		if err != nil {
			return err
		}
		if cur.RunToken != v.RunToken || cur.FenceEpoch != v.FenceEpoch {
			return fmt.Errorf("%w: execution %d was recovered by another instance", gojob.ErrContended, v.ID)
		}
		if cur.Status.Terminal() {
			return fmt.Errorf("%w: execution %d reached %s before recovery ran",
				gojob.ErrContended, v.ID, cur.Status)
		}
		now := s.clock.Now()

		// Release the state row under the OLD ownership. This is the step a claim must never
		// perform for itself: if a fresh claim could overwrite an expired holder directly,
		// the previous execution would still be `running` under the old token and this
		// statement would affect zero rows forever.
		res, err := tx.ExecContext(ctx, `
			UPDATE job_state
			SET write_seq = write_seq + 1,
			    active_kind = NULL, active_execution = NULL, active_owner = NULL,
			    active_run_token = NULL, dispatched_to = NULL, lease_until = NULL,
			    last_failure_at = ?, updated_at = ?
			WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?
			  AND lease_until < UTC_TIMESTAMP()`,
			now, now, v.JobName, v.RunToken, v.FenceEpoch)
		if err != nil {
			return fmt.Errorf("release stale job lock %q: %w", v.JobName, err)
		}
		if err := assertOne(res, "resolve: release job lock"); err != nil {
			return err
		}

		landed = resolvedStatus(cur)
		reason := gojob.ReasonFenced
		switch landed {
		case gojob.StatusDead:
			if cur.TimeoutExpired {
				reason = gojob.ReasonTimeout
			} else {
				reason = gojob.ReasonBudgetExhausted
			}
		case gojob.StatusReady:
			reason = ""
		}

		// fence_epoch is incremented on the execution row too, so a revived executor holding
		// the old epoch cannot write to it. recovery_count is the budget this path consumes.
		res, err = tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq = write_seq + 1,
			    status         = ?,
			    terminal_reason = ?,
			    finished_at    = IF(? = 'ready', NULL, ?),
			    available_at   = IF(? = 'ready', TIMESTAMPADD(SECOND, ?, ?), available_at),
			    timeout_at     = IF(? = 'ready', NULL, timeout_at),
			    fence_epoch    = fence_epoch + 1,
			    recovery_count = recovery_count + 1,
			    owner_instance = NULL, dispatched_to = NULL, run_token = NULL,
			    lease_until = NULL, heartbeat_at = NULL, deadline_at = NULL,
			    updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?`,
			string(landed), nullString(string(reason)),
			string(landed), now,
			string(landed), backoffSeconds, now,
			string(landed),
			now, v.ID, v.RunToken, v.FenceEpoch)
		if err != nil {
			return fmt.Errorf("resolve execution %d: %w", v.ID, err)
		}
		if err := assertOne(res, "resolve: execution"); err != nil {
			return err
		}

		// The attempt is recorded as `unknown` rather than `failed`, and under the token it
		// actually ran with — which is why attempt history is keyed by run_token and not by
		// the ordinal this path deliberately did not consume.
		h := Holder{
			JobName:      v.JobName,
			ExecutionID:  v.ID,
			ExecutionKey: v.ExecutionKey,
			RunToken:     v.RunToken,
			FenceEpoch:   v.FenceEpoch,
		}
		if err := recordAttempt(ctx, tx, h, Outcome{
			AttemptOutcome: gojob.AttemptUnknown,
			ExecutorID:     v.DispatchedTo,
			FinishedAt:     now,
			FailureKind:    "lease_expired",
			ResultSummary:  "scheduler lost ownership; executor could not confirm the outcome",
		}, now); err != nil {
			return err
		}

		// A recovery that ENDS the pass restarts the poll clock; one that returns the row to
		// `ready` does not, because the recovered pass is still the outstanding one. Both cases
		// call the same function — the status guard inside it decides.
		return settlePollClock(ctx, tx, h, now)
	})
	if err != nil {
		return "", err
	}
	return landed, nil
}

// lockExecution takes an execution row under the state row already held, which is the
// canonical order. A blocking FOR UPDATE rather than SKIP LOCKED: holding the state row means
// nobody else can legitimately be inside this execution's transition, so a wait here is
// either momentary or a genuine violation worth surfacing rather than skipping.
func lockExecution(ctx context.Context, tx *sql.Tx, id int64) (Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), timeout_at), 0), 0),
		       scheduled_at, params_json,
		       (timeout_at IS NOT NULL AND timeout_at < UTC_TIMESTAMP()),
		       (deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()),
		       (lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP())
		FROM job_execution WHERE id = ? FOR UPDATE`

	var (
		v             Stale
		remainingSecs int64
		params        []byte
	)
	err := tx.QueryRowContext(ctx, q, id).Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
		&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
		&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
		&remainingSecs, &v.ScheduledAt, &params, &v.TimeoutExpired, &v.SilenceExpired, &v.LeaseLive)
	if errors.Is(err, sql.ErrNoRows) {
		return Stale{}, fmt.Errorf("%w: execution %d", ErrNoSuchExecution, id)
	}
	if err != nil {
		return Stale{}, fmt.Errorf("lock execution %d: %w", id, err)
	}
	v.RemainingTimeout = time.Duration(remainingSecs) * time.Second
	v.Params = params
	return v, nil
}

// resolvedStatus decides where an unrecoverable attempt lands.
//
// The timeout check comes first because a runtime cap that only sometimes applies is not a
// cap: an execution past timeout_at is over regardless of how much attempt budget remains.
// It is deliberately not retried — a job that exhausted its entire runtime budget will most
// likely exhaust it again, and retrying is how one slow run becomes an afternoon of them. An
// operator who disagrees can retry it explicitly, which is exactly the judgement a human
// should make and the scheduler should not.
func resolvedStatus(v Stale) gojob.Status {
	if v.Status == gojob.StatusCancelRequested {
		return gojob.StatusCancelled
	}
	if v.TimeoutExpired {
		return gojob.StatusDead
	}
	if v.AttemptNo >= v.MaxAttempts || v.RecoveryCount+1 >= v.MaxRecoveries {
		return gojob.StatusDead
	}
	return gojob.StatusReady
}

// TimedOut finds executions past their runtime cap whose leases are still being renewed —
// the case the stale scan cannot see, because a healthy scheduler tracking a handler that
// simply will not finish keeps the lease live indefinitely.
//
// Without this scan the cap would only apply to executions that also lost their scheduler,
// which is the opposite of the population it exists for.
func (s *Store) TimedOut(ctx context.Context, limit int) ([]Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), timeout_at), 0), 0),
		       scheduled_at, params_json, TRUE,
		       (deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()),
		       (lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP())
		FROM job_execution
		WHERE status IN ('dispatching', 'running', 'cancel_requested')
		  AND timeout_at IS NOT NULL AND timeout_at < UTC_TIMESTAMP()
		ORDER BY timeout_at, id LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("scan timed-out executions: %w", err)
	}
	defer rows.Close()

	var out []Stale
	for rows.Next() {
		var (
			v             Stale
			remainingSecs int64
			params        []byte
		)
		if err := rows.Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
			&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
			&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
			&remainingSecs, &v.ScheduledAt, &params,
			&v.TimeoutExpired, &v.SilenceExpired, &v.LeaseLive); err != nil {
			return nil, fmt.Errorf("scan timed-out execution: %w", err)
		}
		v.RemainingTimeout = time.Duration(remainingSecs) * time.Second
		v.Params = params
		out = append(out, v)
	}
	return out, rows.Err()
}

// FenceTimedOut ends an execution that outran its cap while its lease was still live.
//
// It releases the job lock under the CURRENT ownership rather than waiting for the lease to
// lapse, because the owning scheduler is alive and would otherwise keep renewing forever. The
// executor is asked to stop separately; whether it complies is not something this can prove,
// which is why the terminal reason distinguishes the two ways `dead` is reached.
func (s *Store) FenceTimedOut(ctx context.Context, v Stale) error {
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, v.JobName)
		if err != nil {
			return err
		}
		if !st.Held() || st.ActiveRunToken.String != v.RunToken || st.FenceEpoch != v.FenceEpoch {
			return fmt.Errorf("%w: job %q moved on before the cap was applied", gojob.ErrContended, v.JobName)
		}

		// Re-read under the lock. Between the non-locking timeout scan and this transaction an
		// operator can commit `cancel_requested`, and recording that run as `dead (timeout)`
		// would report a failure to a person who is looking at the cancel they just issued.
		cur, err := lockExecution(ctx, tx, v.ID)
		if err != nil {
			return err
		}
		if cur.RunToken != v.RunToken || cur.FenceEpoch != v.FenceEpoch {
			return fmt.Errorf("%w: execution %d moved on before the cap was applied", gojob.ErrContended, v.ID)
		}
		if cur.Status.Terminal() {
			return fmt.Errorf("%w: execution %d reached %s before the cap was applied",
				gojob.ErrContended, v.ID, cur.Status)
		}

		// A cancel outranks the cap, matching resolvedStatus: `cancelled` names the outcome an
		// operator asked for, and `dead` would report a failure nobody caused. The failure kind
		// still records that the cap elapsed, so the fact is not lost.
		landed, reason := gojob.StatusDead, gojob.ReasonTimeout
		summary := "runtime cap elapsed; side effects unverified"
		if cur.Status == gojob.StatusCancelRequested {
			landed, reason = gojob.StatusCancelled, gojob.ReasonFenced
			summary = "cancel requested, then the runtime cap elapsed; side effects unverified"
		}

		h := Holder{
			JobName:      v.JobName,
			ExecutionID:  v.ID,
			ExecutionKey: v.ExecutionKey,
			RunToken:     v.RunToken,
			FenceEpoch:   v.FenceEpoch,
		}
		if err := releaseJobLock(ctx, tx, h, false, now); err != nil {
			return err
		}

		// The terminal CAS here must NOT carry Complete's `timeout_at >= UTC_TIMESTAMP()` guard: this
		// is the one writer whose whole purpose is to act after the cap has passed.
		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq = write_seq + 1,
			    status = ?, terminal_reason = ?, finished_at = ?,
			    failure_kind = 'timeout',
			    error_message = 'runtime cap elapsed; execution fenced by the scheduler',
			    fence_epoch = fence_epoch + 1,
			    lease_until = NULL, heartbeat_at = NULL, deadline_at = NULL, updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?`,
			string(landed), string(reason), now, now, v.ID, v.RunToken, v.FenceEpoch)
		if err != nil {
			return fmt.Errorf("fence timed-out execution %d: %w", v.ID, err)
		}
		if err := assertOne(res, "fence timed out: terminal CAS"); err != nil {
			return err
		}
		if err := recordAttempt(ctx, tx, h, Outcome{
			AttemptOutcome: gojob.AttemptFenced,
			ExecutorID:     v.DispatchedTo,
			FinishedAt:     now,
			FailureKind:    "timeout",
			ResultSummary:  summary,
		}, now); err != nil {
			return err
		}
		return settlePollClock(ctx, tx, h, now)
	})
}

// ErrNoSuchExecution is returned by lookups for a row that has been retained away.
var ErrNoSuchExecution = errors.New("gojob: no such execution")

// ErrCapElapsed means an execution passed its runtime cap while a reconciliation call was in
// flight, so it must be fenced rather than adopted. It is a distinct error because the caller
// has a different action to take, not merely a different message to log.
var ErrCapElapsed = errors.New("gojob: execution passed its runtime cap during reconciliation")

// ExecutionByKey answers a redelivered result: an executor reporting twice, or reporting
// after its scheduler died, must get the same answer both times rather than have the second
// report treated as a first one.
func (s *Store) ExecutionByKey(ctx context.Context, key string) (Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), timeout_at), 0), 0),
		       scheduled_at, params_json,
		       (timeout_at IS NOT NULL AND timeout_at < UTC_TIMESTAMP()),
		       (deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()),
		       (lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP())
		FROM job_execution WHERE execution_key = ?`

	var (
		v             Stale
		remainingSecs int64
		params        []byte
	)
	err := s.db.QueryRowContext(ctx, q, key).Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
		&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
		&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
		&remainingSecs, &v.ScheduledAt, &params, &v.TimeoutExpired, &v.SilenceExpired, &v.LeaseLive)
	if errors.Is(err, sql.ErrNoRows) {
		return Stale{}, fmt.Errorf("%w: %s", ErrNoSuchExecution, key)
	}
	if err != nil {
		return Stale{}, fmt.Errorf("read execution %s: %w", key, err)
	}
	v.RemainingTimeout = time.Duration(remainingSecs) * time.Second
	v.Params = params
	return v, nil
}

// CancelRequested lists executions an operator has asked to stop.
//
// The scan exists because RequestCancel only writes a row. Something has to carry that request
// to the executor, and it cannot be the API call itself: the operator's request lands on
// whichever scheduler instance the load balancer picked, which is usually not the one holding
// the execution and may not be able to reach the executor at all.
// afterID is a cursor. A cancel is a REQUEST that has to be repeated until the handler stops,
// so rows linger here by design — and a page always taken from the lowest ids means a hundred
// slow or unreachable ones are re-sent on every pass while a cancel issued a minute ago, with
// a higher id, is never sent at all. One group of stuck executors could delay every later
// cancellation indefinitely.
func (s *Store) CancelRequested(ctx context.Context, afterID int64, limit int) ([]Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), timeout_at), 0), 0),
		       scheduled_at, params_json,
		       (timeout_at IS NOT NULL AND timeout_at < UTC_TIMESTAMP()),
		       (deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()),
		       (lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP())
		FROM job_execution
		WHERE status = 'cancel_requested' AND dispatched_to IS NOT NULL AND id > ?
		ORDER BY id LIMIT ?`
	return s.scanStale(ctx, q, afterID, limit)
}

// Silent lists executions whose SILENCE budget has elapsed while their lease is still fresh.
//
// This is the population the stale scan cannot see. deadline_at bounds how long an executor
// may report nothing; lease_until bounds how long the SCHEDULER may go without renewing. A
// healthy scheduler tracking an executor whose progress loop died keeps the lease fresh for
// ever, so without this scan the row is owned until its runtime cap — which for a job capped
// in hours means nobody learns for hours.
//
// The live-lease predicate is what keeps the two scans disjoint, and it is load-bearing. The
// doc comment claimed it from the start while the SQL did not have it, so a row whose lease
// AND silence budget had both elapsed was visible to both passes — and they disagree about
// what to do with it. Silence, finding the executor unreachable, makes it terminally `dead`;
// recovery, with budget remaining, returns it to `ready` for another attempt. Which one a job
// got depended on which loop ticked first.
func (s *Store) Silent(ctx context.Context, limit int) ([]Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), timeout_at), 0), 0),
		       scheduled_at, params_json,
		       (timeout_at IS NOT NULL AND timeout_at < UTC_TIMESTAMP()),
		       (deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()),
		       (lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP())
		FROM job_execution
		WHERE status IN ('running', 'cancel_requested')
		  AND deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()
		  AND lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP()
		ORDER BY deadline_at, id LIMIT ?`
	return s.scanStale(ctx, q, limit)
}

// scanStale runs one of the Stale-shaped queries above.
func (s *Store) scanStale(ctx context.Context, query string, args ...any) ([]Stale, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("scan executions: %w", err)
	}
	defer rows.Close()

	var out []Stale
	for rows.Next() {
		var (
			v             Stale
			remainingSecs int64
			params        []byte
		)
		if err := rows.Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
			&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
			&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
			&remainingSecs, &v.ScheduledAt, &params, &v.TimeoutExpired, &v.SilenceExpired, &v.LeaseLive); err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		v.RemainingTimeout = time.Duration(remainingSecs) * time.Second
		v.Params = params
		out = append(out, v)
	}
	return out, rows.Err()
}

// ExpireSilent ends an execution that stopped reporting while its scheduler kept renewing.
//
// It lands `dead` with failure_kind `silence`, which is deliberately distinct from `timeout`:
// the two say different things to whoever reads it later. `timeout` means the work took too
// long; `silence` means nobody knows what it was doing, and its side effects are unverified in
// a way a completed-but-slow run's are not.
func (s *Store) ExpireSilent(ctx context.Context, v Stale) error {
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, v.JobName)
		if err != nil {
			return err
		}
		if !st.Held() || st.ActiveRunToken.String != v.RunToken || st.FenceEpoch != v.FenceEpoch {
			return fmt.Errorf("%w: job %q moved on", gojob.ErrContended, v.JobName)
		}
		cur, err := lockExecution(ctx, tx, v.ID)
		if err != nil {
			return err
		}
		if cur.RunToken != v.RunToken || cur.FenceEpoch != v.FenceEpoch || cur.Status.Terminal() {
			return fmt.Errorf("%w: execution %d moved on", gojob.ErrContended, v.ID)
		}
		// The deadline must STILL be expired. Phase 2's reconciliation is an RPC with its own
		// timeout, and during it the executor can report progress through another scheduler
		// instance — that report is precisely the evidence this execution is alive. Fencing on
		// the strength of a reading taken before it would release the job lock while the
		// handler is running, and a successor would start beside it.
		if !cur.SilenceExpired {
			return fmt.Errorf("%w: execution %d reported progress during reconciliation",
				gojob.ErrContended, v.ID)
		}

		landed, reason := gojob.StatusDead, gojob.ReasonFenced
		if cur.Status == gojob.StatusCancelRequested {
			landed = gojob.StatusCancelled
		}

		h := Holder{
			JobName: v.JobName, ExecutionID: v.ID, ExecutionKey: v.ExecutionKey,
			RunToken: v.RunToken, FenceEpoch: v.FenceEpoch,
		}
		if err := releaseJobLock(ctx, tx, h, false, now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq = write_seq + 1,
			    status = ?, terminal_reason = ?, finished_at = ?,
			    failure_kind = 'silence',
			    error_message = 'the executor stopped reporting progress within its silence budget',
			    fence_epoch = fence_epoch + 1,
			    lease_until = NULL, heartbeat_at = NULL, deadline_at = NULL, updated_at = ?
			WHERE id = ? AND status IN ('running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?`,
			string(landed), string(reason), now, now, v.ID, v.RunToken, v.FenceEpoch)
		if err != nil {
			return fmt.Errorf("expire silent execution %d: %w", v.ID, err)
		}
		if err := assertOne(res, "expire silent: terminal CAS"); err != nil {
			return err
		}
		if err := recordAttempt(ctx, tx, h, Outcome{
			AttemptOutcome: gojob.AttemptUnknown,
			ExecutorID:     v.DispatchedTo,
			FinishedAt:     now,
			FailureKind:    "silence",
			ResultSummary:  "no progress within the silence budget; side effects unverified",
		}, now); err != nil {
			return err
		}
		return settlePollClock(ctx, tx, h, now)
	})
}

// OwnedByInstance lists every non-terminal execution this instance owns, from the database.
//
// The engine's tracked map is not sufficient for this, and the gap is not hypothetical: an
// unknown-outcome dispatch that exhausts its re-send bound deliberately STOPS being tracked,
// so recovery can take it — leaving a row that is held, `dispatching`, and invisible to any
// in-memory scan. If the tenant is then disabled, the pool closes with that row still held and
// nothing left to recover it, and the schema's quiescence is permanently false.
func (s *Store) OwnedByInstance(ctx context.Context, owner string, limit int) ([]Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), timeout_at), 0), 0),
		       scheduled_at, params_json,
		       (timeout_at IS NOT NULL AND timeout_at < UTC_TIMESTAMP()),
		       (deadline_at IS NOT NULL AND deadline_at < UTC_TIMESTAMP()),
		       (lease_until IS NOT NULL AND lease_until >= UTC_TIMESTAMP())
		FROM job_execution
		WHERE owner_instance = ?
		  AND status IN ('dispatching', 'running', 'cancel_requested')
		ORDER BY id LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, owner, limit)
	if err != nil {
		return nil, fmt.Errorf("scan work owned by %q: %w", owner, err)
	}
	defer rows.Close()

	var out []Stale
	for rows.Next() {
		var (
			v             Stale
			remainingSecs int64
			params        []byte
		)
		if err := rows.Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
			&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
			&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
			&remainingSecs, &v.ScheduledAt, &params,
			&v.TimeoutExpired, &v.SilenceExpired, &v.LeaseLive); err != nil {
			return nil, fmt.Errorf("scan owned execution: %w", err)
		}
		v.RemainingTimeout = time.Duration(remainingSecs) * time.Second
		v.Params = params
		out = append(out, v)
	}
	return out, rows.Err()
}

// ReleaseAsOwner resolves an execution this instance still legitimately owns.
//
// It is the shutdown counterpart of Resolve. Resolve requires an EXPIRED lease, because its
// job is to take work away from an owner that is gone; this one is used by the owner itself,
// when a tenant is being retired out from under it and no scheduler will be left to recover
// what it holds. Leaving the rows held would block the tenant's quiescence for ever, and
// quiescence is what a DSN cutover waits on.
//
// The attempt is recorded `unknown`, not failed: the executor may well still be running, and
// this instance is losing the ability to find out. `cancel_requested` still lands `cancelled`,
// for the same reason it does in recovery.
func (s *Store) ReleaseAsOwner(ctx context.Context, h Holder) (gojob.Status, error) {
	var landed gojob.Status
	err := s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, h.JobName)
		if err != nil {
			return err
		}
		if !st.Held() || st.ActiveRunToken.String != h.RunToken || st.FenceEpoch != h.FenceEpoch {
			return fmt.Errorf("%w: job %q is no longer held under this token", gojob.ErrContended, h.JobName)
		}
		cur, err := lockExecution(ctx, tx, h.ExecutionID)
		if err != nil {
			return err
		}
		if cur.RunToken != h.RunToken || cur.FenceEpoch != h.FenceEpoch || cur.Status.Terminal() {
			return fmt.Errorf("%w: execution %d moved on", gojob.ErrContended, h.ExecutionID)
		}
		// The lease must still be LIVE. Once it has lapsed the row belongs to recovery, which
		// is the single path that clears an expired holder — and recovery may already hold a
		// confirmed result from the executor, obtained outside its transaction. Letting a
		// departing owner win that race would replace a known success with `unknown`.
		if !cur.LeaseLive {
			return fmt.Errorf("%w: execution %d has an expired lease and belongs to recovery",
				gojob.ErrContended, h.ExecutionID)
		}
		now := s.clock.Now()

		if err := releaseJobLock(ctx, tx, h, false, now); err != nil {
			return err
		}

		// TERMINAL, always. resolvedStatus would return `ready` while budgets remain, and a
		// `ready` row in a schema nothing will schedule again is work that is silently
		// abandoned by the cutover it was supposed to be counted by.
		landed = gojob.StatusDead
		if cur.Status == gojob.StatusCancelRequested {
			landed = gojob.StatusCancelled
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq       = write_seq + 1,
			    status          = ?,
			    terminal_reason = ?,
			    finished_at     = ?,
			    failure_kind    = 'tenant_retired',
			    error_message   = 'the tenant was disabled while this attempt was in flight',
			    fence_epoch     = fence_epoch + 1,
			    recovery_count  = recovery_count + 1,
			    owner_instance = NULL, dispatched_to = NULL, run_token = NULL,
			    lease_until = NULL, heartbeat_at = NULL, deadline_at = NULL,
			    updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?`,
			string(landed), string(gojob.ReasonFenced), now,
			now, h.ExecutionID, h.RunToken, h.FenceEpoch)
		if err != nil {
			return fmt.Errorf("release execution %d as its owner: %w", h.ExecutionID, err)
		}
		if err := assertOne(res, "release as owner"); err != nil {
			return err
		}
		if err := recordAttempt(ctx, tx, h, Outcome{
			AttemptOutcome: gojob.AttemptUnknown,
			FinishedAt:     now,
			FailureKind:    "tenant_retired",
			ResultSummary:  "the tenant was disabled while this attempt was in flight; outcome unknown",
		}, now); err != nil {
			return err
		}
		return settlePollClock(ctx, tx, h, now)
	})
	return landed, err
}

// CompleteAsOwner records an outcome an executor reported through reconciliation, while this
// instance still holds the execution.
//
// It exists because Adopt requires an expired lease — its purpose is taking work from an owner
// that is gone — and the silence path reaches a FINISHED executor while its scheduler is still
// renewing perfectly well. Routing that through Adopt affected zero rows and, when the
// executor's own result retries had run out, turned a genuine success into a timeout.
func (s *Store) CompleteAsOwner(ctx context.Context, h Holder, o Outcome, retryable bool, backoffSeconds int) error {
	if retryable {
		return s.Retry(ctx, h, o, backoffSeconds)
	}
	return s.Complete(ctx, h, o)
}
