package store

import (
	"context"
	"database/sql"
	"errors"
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
		if err := chargeUnacceptedAttempt(ctx, tx, h, now); err != nil {
			return err
		}

		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq       = write_seq + 1,
			    status          = ?,
			    terminal_reason = ?,
			    finished_at     = ?,
			    failure_kind    = ?,
			    error_message   = ?,
			    result_summary  = ?,
			    lease_until     = NULL,
			    updated_at      = ?
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
		if err := recordAttempt(ctx, tx, h, o, now); err != nil {
			return err
		}
		return settlePollClock(ctx, tx, h, now)
	})
}

// Retry returns a failed attempt to `ready`, or to `dead` when a budget is spent.
//
// Terminality is decided IN SQL rather than by the caller, because attempt_no and
// max_attempts are columns the caller may have read before the attempt was accepted. Deciding
// it here means the decision is made against the values the row actually holds at commit —
// including the charge chargeUnacceptedAttempt may have just applied.
//
// The runtime cap is a PER-ATTEMPT budget, so returning the row to `ready` clears timeout_at
// and the next claim mints a fresh one. Sharing one cap across attempts sounds tidier but is
// not: a job with three attempts and a sixty-second timeout would spend most of that budget on
// its first attempt and give the third a few seconds, which is a guaranteed timeout dressed up
// as a retry.
//
// The `dead` branch keeps timeout_at, because the row is terminal and the value is evidence.
// A REFUSAL deliberately does not clear it — no handler started, so no new budget is earned,
// and clearing it there is how a row that keeps being refused near its cap gets the cap pushed
// forward for ever.
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
		if err := chargeUnacceptedAttempt(ctx, tx, h, now); err != nil {
			return err
		}

		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq       = write_seq + 1,
			    status          = IF(attempt_no >= max_attempts, 'dead', 'ready'),
			    terminal_reason = IF(attempt_no >= max_attempts, ?, NULL),
			    finished_at     = IF(attempt_no >= max_attempts, ?, NULL),
			    available_at    = IF(attempt_no >= max_attempts,
			                         available_at, TIMESTAMPADD(SECOND, ?, ?)),
			    timeout_at      = IF(attempt_no >= max_attempts, timeout_at, NULL),
			    owner_instance  = NULL,
			    dispatched_to   = NULL,
			    run_token       = NULL,
			    lease_until     = NULL,
			    heartbeat_at    = NULL,
			    deadline_at     = NULL,
			    failure_kind    = ?,
			    error_message   = ?,
			    updated_at      = ?
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
		if err := recordAttempt(ctx, tx, h, o, now); err != nil {
			return err
		}
		return settlePollClock(ctx, tx, h, now)
	})
}

// chargeUnacceptedAttempt charges the retry budget for a result that arrived while the
// execution was still `dispatching`.
//
// attempt_no is normally incremented only on acceptance, because a claim is not an attempt.
// But a RESULT is proof a handler started, and results are instance-agnostic: an executor may
// report through a different scheduler than the one that dispatched, and beat that
// scheduler's own record of the acceptance. Without this charge the budget would never be
// consumed on that path, and a handler that fails fast enough to always win that race would
// be retried forever with attempt_no stuck at zero — max_attempts silently not a bound.
//
// Zero rows is the normal case: the row is already `running` and acceptance charged it. This
// is a separate statement rather than a conditional inside the terminal CAS because MySQL
// evaluates a SET list left to right using already-updated values, so an expression reading
// `status` while the same statement assigns it depends on column order — a correctness
// argument no reader should have to reconstruct.
func chargeUnacceptedAttempt(ctx context.Context, tx *sql.Tx, h Holder, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_execution
		SET write_seq  = write_seq + 1,
		    attempt_no = attempt_no + 1,
		    started_at = COALESCE(started_at, ?),
		    updated_at = ?
		WHERE id = ? AND status = 'dispatching'
		  AND run_token = ? AND fence_epoch = ?`,
		now, now, h.ExecutionID, h.RunToken, h.FenceEpoch)
	if err != nil {
		return fmt.Errorf("charge unaccepted attempt on execution %d: %w", h.ExecutionID, err)
	}
	n, err := affected(res, "charge unaccepted attempt")
	if err != nil {
		return err
	}
	if n > 1 {
		return fmt.Errorf("%w: charge affected %d rows", gojob.ErrProtocol, n)
	}
	return nil
}

// releaseJobLock clears the state row under the current ownership. Guarded by token and
// epoch, so a fenced holder releases nothing.
//
// last_success_at and last_failure_at are set from the same statement rather than from a
// follow-up, so a crash cannot leave the lock released and the outcome unrecorded.
func releaseJobLock(ctx context.Context, tx *sql.Tx, h Holder, succeeded bool, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET write_seq        = write_seq + 1,
		    active_kind      = NULL,
		    active_execution = NULL,
		    active_owner     = NULL,
		    active_run_token = NULL,
		    dispatched_to    = NULL,
		    lease_until      = NULL,
		    last_success_at  = IF(?, ?, last_success_at),
		    last_failure_at  = IF(?, last_failure_at, ?),
		    updated_at       = ?
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

// settlePollClock restarts a fixed-delay job's poll loop, and does nothing at all otherwise.
//
// EVERY terminal path calls it unconditionally, and it takes no "is this a poller" parameter,
// because the alternative shape — a *time.Time the caller supplies when it believes the
// outcome ends a pass — is a place to forget. It was forgotten three times in three review
// rounds: once for FORBID, once when a late refusal became terminal, once in recovery. The
// answer is not a fourth reminder; it is that no caller gets to decide.
//
// Two conditions are checked in SQL rather than in Go, so a caller cannot get them wrong:
//
//   - the execution named must actually be a POLL pass. A terminal manual run on a
//     fixed-delay job is not the end of a pass, and moving the clock for it postpones the
//     next real pass by a whole delay;
//   - it must actually be terminal. A retry that went back to `ready` IS the outstanding
//     pass, and restoring the clock would let the due scan materialize a second to race it.
//
// The guard is deliberately not "the job lock is free": FORBID settles the clock while the
// job is still held by whatever won the contention, and every caller holds the state row's
// lock for the rest of its transaction anyway.
func settlePollClock(ctx context.Context, tx *sql.Tx, h Holder, now time.Time) error {
	def, err := readDefinition(ctx, tx, h.JobName)
	if err != nil {
		return err
	}
	if def.ScheduleKind != gojob.ScheduleFixedDelay {
		return nil
	}
	delay, err := def.Delay()
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, `
		UPDATE job_state js
		SET js.write_seq    = js.write_seq + 1,
		    js.next_poll_at = ?,
		    js.updated_at   = ?
		WHERE js.job_name = ?
		  AND EXISTS (SELECT 1 FROM job_execution je
		              WHERE je.id = ? AND je.job_name = js.job_name
		                AND je.trigger_type = 'poll'
		                AND je.status IN ('success', 'dead', 'cancelled', 'skipped'))`,
		now.Add(delay), now, h.JobName, h.ExecutionID)
	if err != nil {
		return fmt.Errorf("settle poll clock for %q: %w", h.JobName, err)
	}
	n, err := affected(res, "settle poll clock")
	if err != nil {
		return err
	}
	if n > 1 {
		return fmt.Errorf("%w: poll clock settle affected %d rows", gojob.ErrProtocol, n)
	}
	return nil
}

// AttemptRecord is one row of attempt history.
type AttemptRecord struct {
	ExecutionKey string
	RunToken     string
	AttemptNo    int
	ExecutorID   string
	Outcome      gojob.AttemptOutcome
	FailureKind  string
	Summary      string
	FinishedAt   sql.NullTime
}

// ErrNoSuchAttempt means no result has been recorded for that (execution_key, run_token).
var ErrNoSuchAttempt = errors.New("gojob: no result recorded for this attempt")

// Attempt answers a redelivered result.
//
// This is the query the gRPC contract's idempotency promise rests on: ReportResult is
// idempotent on (tenant, execution_key, run_token), and a redelivery must return OK with
// already_recorded=true rather than ABORTED. By the time a redelivery arrives the execution
// row has usually moved on — run_token cleared, epoch bumped, status back to `ready` for a
// different attempt — so the execution row cannot answer the question. Attempt history is
// keyed by the token precisely so it can.
func (s *Store) Attempt(ctx context.Context, executionKey, runToken string) (AttemptRecord, error) {
	var a AttemptRecord
	err := s.db.QueryRowContext(ctx, `
		SELECT execution_key, run_token, attempt_no, COALESCE(executor_id, ''),
		       outcome, COALESCE(failure_kind, ''), COALESCE(summary, ''), finished_at
		FROM job_execution_attempt
		WHERE execution_key = ? AND run_token = ?`, executionKey, runToken).Scan(
		&a.ExecutionKey, &a.RunToken, &a.AttemptNo, &a.ExecutorID,
		&a.Outcome, &a.FailureKind, &a.Summary, &a.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AttemptRecord{}, fmt.Errorf("%w: %s/%s", ErrNoSuchAttempt, executionKey, runToken)
	}
	if err != nil {
		return AttemptRecord{}, fmt.Errorf("read attempt %s/%s: %w", executionKey, runToken, err)
	}
	return a, nil
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
// FIRST WRITE WINS. The duplicate-key clause deliberately updates nothing: the recorded
// outcome is the one that won the terminal CAS and therefore the one the execution row acted
// on. Letting a later redelivery overwrite it would make history disagree with the decision
// history is used to explain — an attempt resolved `unknown` by recovery, whose executor
// surfaces hours later with a real result, must still read `unknown`, because `unknown` is
// what the scheduler acted on.
func recordAttempt(ctx context.Context, tx *sql.Tx, h Holder, o Outcome, now time.Time) error {
	// A caller that leaves FinishedAt zero gets the transaction's own instant rather than a
	// NULL. An attempt row with no finish time is the one field an incident review always
	// wants and the easiest for a caller to forget.
	if o.FinishedAt.IsZero() {
		o.FinishedAt = now
	}

	// The ordinal is read first and the insert is a plain VALUES, rather than one
	// INSERT ... SELECT ... ON DUPLICATE KEY UPDATE.
	//
	// With the SELECT in scope, job_execution and job_execution_attempt both have an
	// execution_key, and MySQL rejects the duplicate-key clause as ambiguous — a failure that
	// no amount of reading the statement makes obvious and that only a real database reports.
	// Two plain statements inside one transaction cost a round trip and are impossible to get
	// wrong that way.
	var attemptNo int
	if err := tx.QueryRowContext(ctx,
		`SELECT attempt_no FROM job_execution WHERE id = ?`, h.ExecutionID).Scan(&attemptNo); err != nil {
		return fmt.Errorf("read attempt ordinal of execution %d: %w", h.ExecutionID, err)
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO job_execution_attempt
		    (execution_key, run_token, attempt_no, executor_id,
		     started_at, finished_at, outcome, failure_kind, summary)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE execution_key = execution_key`,
		h.ExecutionKey, h.RunToken, attemptNo, nullString(o.ExecutorID),
		nullTime(o.StartedAt), nullTime(o.FinishedAt),
		string(o.AttemptOutcome), nullString(o.FailureKind), nullString(o.ResultSummary))
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
//
// The request and its audit row are written in one transaction. The audit trail is what makes
// "we asked, but it had already finished" visible after the real outcome wins, so it must not
// be able to go missing while the request itself commits.
func (s *Store) RequestCancel(ctx context.Context, id int64, actor string) error {
	if actor == "" {
		return fmt.Errorf("%w: cancel with no actor", gojob.ErrProtocol)
	}
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		// The audit names the execution this statement actually cancelled, read from the row
		// itself rather than taken from the caller alongside the id. A caller that passed a
		// mismatched name would otherwise produce an audit entry that is wrong and reads as
		// authoritative — the one kind of record worse than none.
		var jobName, executionKey string
		if err := tx.QueryRowContext(ctx,
			`SELECT job_name, execution_key FROM job_execution WHERE id = ?`, id).
			Scan(&jobName, &executionKey); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: execution %d", ErrNoSuchExecution, id)
			}
			return fmt.Errorf("read execution %d for cancel: %w", id, err)
		}

		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq  = write_seq + 1,
			    status     = 'cancel_requested',
			    updated_at = ?
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
