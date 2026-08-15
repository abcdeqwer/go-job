package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// Outcome is a finished attempt as the scheduler records it.
type Outcome struct {
	Status         gojob.Status
	TerminalReason gojob.TerminalReason
	AttemptOutcome gojob.AttemptOutcome
	FailureKind    string
	ErrorMessage   string
	ResultSummary  string
	ExecutorID     string
	StartedAt      time.Time
	FinishedAt     time.Time
}

// Complete moves an execution to a terminal state and releases the job lock, in one
// transaction in the canonical order.
//
// Every terminal transition additionally guards `timeout_at >= NOW()`. Without it the outcome
// would depend on which writer reached the database first: a non-conforming executor still
// running past the cap could report success moments before the timeout scanner fenced it, and
// the execution would record success for a run the scheduler had already decided to stop. A
// cap that only sometimes applies is not a cap.
//
// The prior-status set includes cancel_requested because the real outcome wins over a cancel
// that lost the race: an operator can request a cancel a moment after the handler already
// finished, and recording that as `cancelled` would tell an operator the opposite of the
// truth about a job that may have moved money. The audit trail keeps the request, so "we
// asked, but it had already finished" stays visible.
func (s *Store) Complete(ctx context.Context, h Holder, o Outcome) error {
	if !o.Status.Terminal() {
		return fmt.Errorf("%w: Complete called with non-terminal status %q", gojob.ErrProtocol, o.Status)
	}
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		if err := releaseJobLock(ctx, tx, h, o.Status == gojob.StatusSuccess, now); err != nil {
			return err
		}

		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET status = ?, terminal_reason = ?, finished_at = ?,
			    failure_kind = ?, error_message = ?, result_summary = ?,
			    lease_until = NULL, updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?
			  AND (timeout_at IS NULL OR timeout_at >= NOW())`,
			string(o.Status), string(o.TerminalReason), now,
			nullString(o.FailureKind), nullString(o.ErrorMessage), nullString(o.ResultSummary),
			now, h.ExecutionID, h.RunToken, h.FenceEpoch)
		if err != nil {
			return fmt.Errorf("complete execution %d: %w", h.ExecutionID, err)
		}
		n, err := affected(res, "complete: terminal CAS")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: execution %d result refused under token %s epoch %d",
				gojob.ErrFenced, h.ExecutionID, h.RunToken, h.FenceEpoch)
		}
		return recordAttempt(ctx, tx, h, o)
	})
}

// Retry returns a failed attempt to `ready`, or to `dead` when a budget is spent.
//
// Terminality is decided IN SQL rather than by the caller, because attempt_no and
// max_attempts are columns the caller may have read before the attempt was accepted. Deciding
// it here means the decision is made against the values the row actually holds at commit.
//
// backoffSeconds is bounded, and jitter is applied by the caller after computing the base
// delay so the sequence stays testable.
func (s *Store) Retry(ctx context.Context, h Holder, o Outcome, backoffSeconds int) error {
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		// A retry is a failed attempt whether or not the budget is spent, so the state row
		// records a failure either way.
		if err := releaseJobLock(ctx, tx, h, false, now); err != nil {
			return err
		}

		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET status = IF(attempt_no >= max_attempts, 'dead', 'ready'),
			    terminal_reason = IF(attempt_no >= max_attempts, ?, NULL),
			    finished_at     = IF(attempt_no >= max_attempts, ?, NULL),
			    available_at    = IF(attempt_no >= max_attempts,
			                         available_at, TIMESTAMPADD(SECOND, ?, ?)),
			    owner_instance = NULL, dispatched_to = NULL, run_token = NULL,
			    lease_until = NULL, heartbeat_at = NULL, deadline_at = NULL,
			    failure_kind = ?, error_message = ?, updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND run_token = ? AND fence_epoch = ?
			  AND (timeout_at IS NULL OR timeout_at >= NOW())`,
			string(gojob.ReasonBudgetExhausted), now,
			backoffSeconds, now,
			nullString(o.FailureKind), nullString(o.ErrorMessage), now,
			h.ExecutionID, h.RunToken, h.FenceEpoch)
		if err != nil {
			return fmt.Errorf("retry execution %d: %w", h.ExecutionID, err)
		}
		n, err := affected(res, "retry: CAS")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: execution %d retry refused under token %s epoch %d",
				gojob.ErrFenced, h.ExecutionID, h.RunToken, h.FenceEpoch)
		}
		return recordAttempt(ctx, tx, h, o)
	})
}

// releaseJobLock clears the state row under the current ownership. Guarded by token and
// epoch, so a fenced holder releases nothing.
//
// last_success_at and last_failure_at are set from the same statement rather than a second
// one, so a crash cannot leave the lock released and the outcome unrecorded.
func releaseJobLock(ctx context.Context, tx *sql.Tx, h Holder, succeeded bool, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET active_kind = NULL, active_execution = NULL, active_owner = NULL,
		    active_run_token = NULL, dispatched_to = NULL, lease_until = NULL,
		    last_success_at = IF(?, ?, last_success_at),
		    last_failure_at = IF(?, last_failure_at, ?),
		    updated_at = ?
		WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?`,
		succeeded, now, succeeded, now, now,
		h.JobName, h.RunToken, h.FenceEpoch)
	if err != nil {
		return fmt.Errorf("release job lock %q: %w", h.JobName, err)
	}
	n, err := affected(res, "release job lock")
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: job %q lock not held under token %s epoch %d",
			gojob.ErrFenced, h.JobName, h.RunToken, h.FenceEpoch)
	}
	return nil
}

// recordAttempt appends attempt history in the SAME transaction as the transition it
// describes, because a redelivered result is answered from this table: if the row could be
// missing after a commit, a duplicate report would be treated as a first one.
//
// Keyed by (execution_key, run_token), NOT by attempt_no. A token identifies an attempt;
// attempt_no counts budget, and the two differ in a case that occurs in normal operation — an
// accepted attempt whose reply is lost, whose executor then restarts, is classified `unknown`
// and correctly does not consume the ordinal, so two attempts legitimately share one.
//
// ON DUPLICATE KEY makes a redelivery idempotent rather than a constraint violation the
// caller has to interpret.
func recordAttempt(ctx context.Context, tx *sql.Tx, h Holder, o Outcome) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO job_execution_attempt
		    (execution_key, run_token, attempt_no, executor_id,
		     started_at, finished_at, outcome, failure_kind, summary)
		SELECT ?, ?, attempt_no, ?, ?, ?, ?, ?, ?
		FROM job_execution WHERE id = ?
		ON DUPLICATE KEY UPDATE
		    finished_at  = VALUES(finished_at),
		    outcome      = VALUES(outcome),
		    failure_kind = VALUES(failure_kind),
		    summary      = VALUES(summary)`,
		h.ExecutionKey, h.RunToken, nullString(o.ExecutorID),
		nullTime(o.StartedAt), nullTime(o.FinishedAt),
		string(o.AttemptOutcome), nullString(o.FailureKind), nullString(o.ResultSummary),
		h.ExecutionID)
	if err != nil {
		return fmt.Errorf("record attempt %s of execution %d: %w", h.RunToken, h.ExecutionID, err)
	}
	return nil
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// RequestCancel moves a running execution to cancel_requested. It KEEPS the lease and the job
// lock, and the holder keeps heartbeating.
//
// Cancelling a handler's context is a cooperative signal. It does not prove a SQL statement
// finished, a file write completed, or an outbound request returned. Marking the execution
// `cancelled` and releasing the job lock here would let the next execution start while the
// previous handler is still writing — so "we asked it to stop" and "it stopped" are two
// states, not one.
// The request and its audit row are written in one transaction. The audit trail is what
// makes "we asked, but it had already finished" visible after the real outcome wins, so it
// must not be able to go missing while the request itself commits.
func (s *Store) RequestCancel(ctx context.Context, id int64, jobName, executionKey, actor string) error {
	if actor == "" {
		return fmt.Errorf("%w: cancel with no actor", gojob.ErrProtocol)
	}
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET status = 'cancel_requested', updated_at = ?
			WHERE id = ? AND status IN ('dispatching', 'running')`,
			now, id)
		if err != nil {
			return fmt.Errorf("request cancel of execution %d: %w", id, err)
		}
		n, err := affected(res, "request cancel")
		if err != nil {
			return err
		}
		if n == 0 {
			// Not an error the operator caused: the execution finished first, or another
			// operator asked already. The caller reports it as such.
			return fmt.Errorf("%w: execution %d is not in a cancellable state", gojob.ErrContended, id)
		}
		return audit(ctx, tx, now, actor, "cancel_requested", jobName, executionKey,
			"cancel requested; execution keeps its lease until it stops or is fenced")
	})
}

// audit appends an operator action. actor is never defaulted — an action nobody can be held
// to is worse than no record, because it reads as authoritative.
func audit(ctx context.Context, tx *sql.Tx, now time.Time, actor, action, jobName, executionKey, detail string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO job_audit (actor, action, job_name, execution, detail, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		actor, action, nullString(jobName), nullString(executionKey), nullString(detail), now)
	if err != nil {
		return fmt.Errorf("audit %s by %s: %w", action, actor, err)
	}
	return nil
}
