package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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
}

// StaleExecutions is recovery phase 1: read, no locks.
//
// Every holder of a job's state row has an execution row — including a fixed-delay pass — so
// there is one scan and one algorithm rather than a second mechanism for pollers that would
// need its own correctness argument.
//
// `lease_until < NOW()` is an ownership comparison and uses the database clock. Comparing a
// lease against a scheduler's own host clock would make ownership depend on skew between
// machines, which is the one thing a distributed lease must not do.
func (s *Store) StaleExecutions(ctx context.Context, limit int) ([]Stale, error) {
	const q = `
		SELECT id, execution_key, job_name, status,
		       COALESCE(dispatched_to, ''), COALESCE(run_token, ''), fence_epoch,
		       attempt_no, max_attempts, recovery_count, max_recoveries,
		       (timeout_at IS NOT NULL AND timeout_at < NOW())
		FROM job_execution
		WHERE status IN ('dispatching', 'running', 'cancel_requested')
		  AND lease_until < NOW()
		ORDER BY lease_until, id LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("scan stale executions: %w", err)
	}
	defer rows.Close()

	var out []Stale
	for rows.Next() {
		var v Stale
		if err := rows.Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
			&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
			&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
			&v.TimeoutExpired); err != nil {
			return nil, fmt.Errorf("scan stale execution: %w", err)
		}
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
func (s *Store) Adopt(ctx context.Context, v Stale, newOwner string, leaseSeconds int) (int64, error) {
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
			    lease_until  = TIMESTAMPADD(SECOND, ?, NOW()),
			    heartbeat_at = NOW(), updated_at = ?
			WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?
			  AND lease_until < NOW()`,
			newOwner, epoch, leaseSeconds, now, v.JobName, v.RunToken, v.FenceEpoch)
		if err != nil {
			return fmt.Errorf("adopt job lock %q: %w", v.JobName, err)
		}
		if err := assertOne(res, "adopt: job lock"); err != nil {
			return err
		}

		res, err = tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq = write_seq + 1,
			    owner_instance = ?, fence_epoch = ?,
			    lease_until  = TIMESTAMPADD(SECOND, ?, NOW()),
			    heartbeat_at = NOW(), updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?`,
			newOwner, epoch, leaseSeconds, now, v.ID, v.RunToken, v.FenceEpoch)
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
			  AND lease_until < NOW()`,
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
		       (timeout_at IS NOT NULL AND timeout_at < NOW())
		FROM job_execution WHERE id = ? FOR UPDATE`

	var v Stale
	err := tx.QueryRowContext(ctx, q, id).Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
		&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
		&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries, &v.TimeoutExpired)
	if errors.Is(err, sql.ErrNoRows) {
		return Stale{}, fmt.Errorf("%w: execution %d", ErrNoSuchExecution, id)
	}
	if err != nil {
		return Stale{}, fmt.Errorf("lock execution %d: %w", id, err)
	}
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
		       attempt_no, max_attempts, recovery_count, max_recoveries, TRUE
		FROM job_execution
		WHERE status IN ('dispatching', 'running', 'cancel_requested')
		  AND timeout_at IS NOT NULL AND timeout_at < NOW()
		ORDER BY timeout_at, id LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("scan timed-out executions: %w", err)
	}
	defer rows.Close()

	var out []Stale
	for rows.Next() {
		var v Stale
		if err := rows.Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
			&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
			&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries,
			&v.TimeoutExpired); err != nil {
			return nil, fmt.Errorf("scan timed-out execution: %w", err)
		}
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

		// The terminal CAS here must NOT carry Complete's `timeout_at >= NOW()` guard: this
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
		       (timeout_at IS NOT NULL AND timeout_at < NOW())
		FROM job_execution WHERE execution_key = ?`

	var v Stale
	err := s.db.QueryRowContext(ctx, q, key).Scan(&v.ID, &v.ExecutionKey, &v.JobName, &v.Status,
		&v.DispatchedTo, &v.RunToken, &v.FenceEpoch,
		&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount, &v.MaxRecoveries, &v.TimeoutExpired)
	if errors.Is(err, sql.ErrNoRows) {
		return Stale{}, fmt.Errorf("%w: %s", ErrNoSuchExecution, key)
	}
	if err != nil {
		return Stale{}, fmt.Errorf("read execution %s: %w", key, err)
	}
	return v, nil
}
