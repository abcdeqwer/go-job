package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// Candidate is one row returned by discovery. Discovery is a non-locking read whose result
// the claim transaction re-verifies, because the canonical order requires the state row to
// be locked first and an ordered locking scan over executions would take the two in the
// opposite order.
type Candidate struct {
	ID           int64
	JobName      string
	ExecutionKey string
}

// ReadyCandidates reads one bounded page of claimable work.
//
// manualFirst selects the class: manual triggers and scheduled work are read by two separate
// bounded queries rather than one query ordered by class. A single ordered page swaps one
// starvation for another — enough ready manual rows and every page is manual, so cron work is
// never discovered however old it gets. Each class getting its own bounded share starves
// neither.
//
// `available_at <= ?` takes the business clock, not NOW(): available_at is a business column,
// and comparing it against the database's clock would be the mixed-clock bug this package
// exists to avoid.
func (s *Store) ReadyCandidates(ctx context.Context, manualFirst bool, limit int) ([]Candidate, error) {
	const q = `SELECT id, job_name, execution_key FROM job_execution
	           WHERE status = 'ready' AND manual_first = ? AND available_at <= ?
	           ORDER BY available_at, id LIMIT ?`

	flag := 0
	if manualFirst {
		flag = 1
	}
	rows, err := s.db.QueryContext(ctx, q, flag, s.clock.Now(), limit)
	if err != nil {
		return nil, fmt.Errorf("discover candidates (manual=%v): %w", manualFirst, err)
	}
	defer rows.Close()

	var out []Candidate
	for rows.Next() {
		var c Candidate
		if err := rows.Scan(&c.ID, &c.JobName, &c.ExecutionKey); err != nil {
			return nil, fmt.Errorf("scan candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// StateRow is job_state as read under the claim's lock.
type StateRow struct {
	JobName         string
	NextFireAt      sql.NullTime
	NextPollAt      sql.NullTime
	OpsPaused       bool
	ActiveKind      sql.NullString
	ActiveExecution sql.NullString
	ActiveOwner     sql.NullString
	ActiveRunToken  sql.NullString
	DispatchedTo    sql.NullString
	FenceEpoch      int64
	ConfigVersion   int64
}

// Held reports whether some execution currently owns this job.
func (r StateRow) Held() bool { return r.ActiveKind.Valid }

const selectStateForUpdateSkipLocked = `
	SELECT job_name, next_fire_at, next_poll_at, ops_paused,
	       active_kind, active_execution, active_owner, active_run_token,
	       dispatched_to, fence_epoch, config_version
	FROM job_state WHERE job_name = ? FOR UPDATE SKIP LOCKED`

// lockState takes the job's state row, which IS the job's lock.
//
// SKIP LOCKED rather than a blocking wait: a skip means another scheduler instance is
// already deciding about this job, and blocking would serialise every instance behind it for
// no gain. The two zero-row outcomes are deliberately different errors — skipped means
// contention, absent means a broken installation — because reporting a missing state row as
// contention turns an installation that cannot schedule into one that merely looks idle.
func lockState(ctx context.Context, tx *sql.Tx, jobName string) (StateRow, error) {
	var r StateRow
	err := tx.QueryRowContext(ctx, selectStateForUpdateSkipLocked, jobName).Scan(
		&r.JobName, &r.NextFireAt, &r.NextPollAt, &r.OpsPaused,
		&r.ActiveKind, &r.ActiveExecution, &r.ActiveOwner, &r.ActiveRunToken,
		&r.DispatchedTo, &r.FenceEpoch, &r.ConfigVersion)
	if errors.Is(err, sql.ErrNoRows) {
		// SKIP LOCKED and "no such row" are indistinguishable in the result set, so this is
		// resolved with a second, non-skipping existence check rather than guessed at.
		var exists int
		probe := tx.QueryRowContext(ctx, `SELECT 1 FROM job_state WHERE job_name = ?`, jobName).Scan(&exists)
		if errors.Is(probe, sql.ErrNoRows) {
			return StateRow{}, fmt.Errorf("%w: job %q", gojob.ErrMissingState, jobName)
		}
		if probe != nil {
			return StateRow{}, fmt.Errorf("probe state row %q: %w", jobName, probe)
		}
		return StateRow{}, fmt.Errorf("%w: job %q state row is locked elsewhere", gojob.ErrContended, jobName)
	}
	if err != nil {
		return StateRow{}, fmt.Errorf("lock state row %q: %w", jobName, err)
	}
	return r, nil
}

const selectDefinition = `
	SELECT job_name, handler_key, executor_group, schedule_kind, schedule_expr,
	       enabled, retired, concurrency_policy, misfire_policy,
	       max_attempts, max_recoveries, lease_seconds, timeout_seconds,
	       params_json, description, version
	FROM job_definition WHERE job_name = ?`

// readDefinition reads configuration WITHOUT a lock, so an operator's edit never contends
// with a running job. Divergence is reconciled through config_version by the drift scan, not
// by making the hot path wait on a human's transaction.
func readDefinition(ctx context.Context, tx *sql.Tx, jobName string) (gojob.Definition, error) {
	var (
		d        gojob.Definition
		group    sql.NullString
		params   []byte
		desc     sql.NullString
		leaseSec int
		toSec    int
	)
	err := tx.QueryRowContext(ctx, selectDefinition, jobName).Scan(
		&d.JobName, &d.HandlerKey, &group, &d.ScheduleKind, &d.ScheduleExpr,
		&d.Enabled, &d.Retired, &d.Concurrency, &d.Misfire,
		&d.MaxAttempts, &d.MaxRecoveries, &leaseSec, &toSec,
		&params, &desc, &d.Version)
	if errors.Is(err, sql.ErrNoRows) {
		// A foreign key makes this impossible while the state row exists, so it is a broken
		// installation rather than a race.
		return gojob.Definition{}, fmt.Errorf("%w: job %q has a state row but no definition",
			gojob.ErrProtocol, jobName)
	}
	if err != nil {
		return gojob.Definition{}, fmt.Errorf("read definition %q: %w", jobName, err)
	}
	d.ExecutorGroup = group.String
	d.Params = params
	d.Description = desc.String
	d.Lease = time.Duration(leaseSec) * time.Second
	d.Timeout = time.Duration(toSec) * time.Second
	return d, nil
}

// Runnable answers claim step 2. It is a callback rather than a query in this package
// because its third condition — "does any LIVE executor declare this handler, in this job's
// group" — depends on the scheduler's liveness window, which is configuration rather than
// schema. It runs inside the claim transaction and must not perform I/O outside it.
//
// Returning an error wrapping gojob.ErrNotRunnable rejects the candidate with a backoff;
// any other error aborts the claim.
type Runnable func(ctx context.Context, tx *sql.Tx, def gojob.Definition, st StateRow) error

// ClaimParams is everything the claim transaction needs that it cannot read for itself.
type ClaimParams struct {
	ExecutionID int64
	JobName     string

	// ExecutionKey is what job_state.active_execution records. It holds the key rather than
	// the row id because the key is what every other table and every log line names, and a
	// state row pointing at an opaque integer would need a join to be legible mid-incident.
	ExecutionKey string

	// Owner is this scheduler instance. ExecutorID is the executor chosen for the dispatch,
	// recorded BEFORE the Run call is made — see Claim's doc comment.
	Owner      string
	ExecutorID string
	RunToken   string

	// BackoffSeconds is how far available_at moves when the candidate is rejected for
	// contention or unrunnability. Bounded, so a rejected row stops occupying the front of an
	// ordered, bounded discovery page instead of blocking it forever.
	BackoffSeconds int
}

// ClaimOutcome says what the claim transaction did. All three COMMIT: a rejection writes a
// backoff or a terminal `skipped`, and rolling that back would leave the candidate at the
// front of the next discovery page, spinning against a busy job at full rate.
type ClaimOutcome int

const (
	// ClaimAcquired means the job lock is held and the execution is `dispatching`. The caller
	// dispatches only after this commits.
	ClaimAcquired ClaimOutcome = iota

	// ClaimDeferred means the candidate was rejected — held by another execution, paused,
	// or unrunnable — and its available_at has been pushed forward.
	ClaimDeferred

	// ClaimSkipped means FORBID marked this occurrence terminal.
	ClaimSkipped
)

// ClaimResult is what a claim transaction committed.
type ClaimResult struct {
	Outcome ClaimOutcome

	// FenceEpoch is set only for ClaimAcquired. Every subsequent write for this attempt
	// carries it.
	FenceEpoch int64

	// Definition is the configuration as read inside the claim transaction. The lease and
	// timeout applied to this attempt come from here, not from the caller — a caller that
	// read the definition earlier could otherwise claim under one configuration and track
	// under another.
	Definition gojob.Definition

	// Reason is nil for ClaimAcquired and otherwise wraps gojob.ErrContended or
	// gojob.ErrNotRunnable, so the caller can log and meter the rejection without having to
	// treat a committed decision as a failed call.
	Reason error
}

// Claim is the whole of doc/protocol.md §2, as one short transaction.
//
// It commits `dispatching`, NOT `running`, and does not touch attempt_no. An attempt is a
// handler start, and at this point nothing has started: the chosen executor may still answer
// RESOURCE_EXHAUSTED because it is busy or UNAVAILABLE because it is shutting down. Charging
// a retry budget for an executor's capacity would march a job to `dead` without one line of
// business code having run.
//
// dispatched_to is written here, before the Run call, on both rows. Writing it only after the
// reply creates a window that loses work: the executor accepts, this scheduler dies before
// recording where it sent the job, and recovery finds dispatched_to unset, concludes the
// dispatch never landed, and sends the same work elsewhere while the first executor runs.
// Recording the intended target before an irreversible send is the general rule — the network
// call is the point of no return, so everything needed to reason about it afterwards must
// already be durable.
//
// A rejection is a COMMITTED decision, not a failed call. It is reported through
// ClaimResult.Outcome rather than by returning an error, because returning an error would
// roll back the very write that makes the rejection safe — the backoff that stops the
// candidate spinning at the front of the next discovery page, or the terminal `skipped` row
// FORBID just wrote. An error from this method means the transaction did not commit at all.
//
// Dispatch happens only after this commits, using ClaimResult.Definition's lease and timeout.
func (s *Store) Claim(ctx context.Context, p ClaimParams, runnable Runnable) (ClaimResult, error) {
	var out ClaimResult
	err := s.tx(ctx, func(tx *sql.Tx) error {
		// 1. The state row first, always.
		st, err := lockState(ctx, tx, p.JobName)
		if err != nil {
			return err
		}

		// 2. Configuration and runnability. A failure here is a logged rejection with a
		//    backoff, not contention.
		def, err := readDefinition(ctx, tx, p.JobName)
		if err != nil {
			return err
		}
		out.Definition = def

		// ops_paused is a RUNNABILITY condition, not contention. Reporting a paused job as
		// "someone else is running it" is the conflation doc/protocol.md §4 forbids: it turns
		// a job an operator deliberately stopped into one that merely looks busy, and the
		// distinction is the whole content of the alert.
		if st.OpsPaused {
			out.Outcome, out.Reason = ClaimDeferred,
				fmt.Errorf("%w: job %q is paused by an operator", gojob.ErrNotRunnable, p.JobName)
			return deferCandidate(ctx, tx, p.ExecutionID, p.BackoffSeconds, s.clock.Now())
		}
		if rErr := runnable(ctx, tx, def, st); rErr != nil {
			if !errors.Is(rErr, gojob.ErrNotRunnable) {
				return rErr
			}
			out.Outcome, out.Reason = ClaimDeferred, rErr
			return deferCandidate(ctx, tx, p.ExecutionID, p.BackoffSeconds, s.clock.Now())
		}

		// A held job is the ONLY zero-row outcome that is ordinary. It is decided here,
		// after the lock proved the row exists and after every operational condition has
		// been ruled out, so that a missing row or a paused job can never be misreported as
		// "someone else is running it".
		if st.Held() {
			return s.applyContention(ctx, tx, &out, p, def, st)
		}

		out.Outcome = ClaimAcquired
		out.FenceEpoch = st.FenceEpoch + 1
		epoch := out.FenceEpoch
		leaseSeconds := int(def.Lease / time.Second)
		timeoutSeconds := int(def.Timeout / time.Second)

		// 3. Acquire the job lock. The guard is `active_kind IS NULL`, NOT "or the lease
		//    expired": a claim never steals an expired holder. If it could, the previous
		//    execution would still be `running` under the old token, and recovery's later
		//    attempt to release the state row under that token would affect zero rows,
		//    leaving a `running` row no path can resolve. Recovery is the single reclaim
		//    path, and it costs one recovery interval of latency after a crash.
		res, err := tx.ExecContext(ctx, `
			UPDATE job_state
			SET active_kind      = 'EXECUTION',
			    active_execution = ?,
			    active_owner     = ?,
			    active_run_token = ?,
			    dispatched_to    = ?,
			    fence_epoch      = ?,
			    lease_until      = TIMESTAMPADD(SECOND, ?, NOW()),
			    heartbeat_at     = NOW(),
			    updated_at       = ?
			WHERE job_name = ? AND ops_paused = 0 AND active_kind IS NULL`,
			p.ExecutionKey, p.Owner, p.RunToken, nullString(p.ExecutorID),
			epoch, leaseSeconds, s.clock.Now(), p.JobName)
		if err != nil {
			return fmt.Errorf("acquire job lock %q: %w", p.JobName, err)
		}
		if err := assertOne(res, "claim: acquire job lock"); err != nil {
			return err
		}

		// 4. Then the execution row. Under the canonical order nobody else can hold it, so
		//    zero rows here is an assertion failure rather than a skip.
		//
		//    timeout_at is set HERE and never extended. It is a hard runtime cap, and a cap
		//    computed at acceptance would not exist for a row that never gets that far — a
		//    `dispatching` row whose executor never answers would have no budget at all.
		res, err = tx.ExecContext(ctx, `
			UPDATE job_execution
			SET status         = 'dispatching',
			    owner_instance = ?,
			    dispatched_to  = ?,
			    run_token      = ?,
			    fence_epoch    = ?,
			    lease_until    = TIMESTAMPADD(SECOND, ?, NOW()),
			    heartbeat_at   = NOW(),
			    timeout_at     = TIMESTAMPADD(SECOND, ?, NOW()),
			    updated_at     = ?
			WHERE id = ? AND status = 'ready' AND attempt_no < max_attempts`,
			p.Owner, nullString(p.ExecutorID), p.RunToken, epoch,
			leaseSeconds, timeoutSeconds, s.clock.Now(), p.ExecutionID)
		if err != nil {
			return fmt.Errorf("claim execution %d: %w", p.ExecutionID, err)
		}
		return assertOne(res, "claim: mark dispatching")
	})
	if err != nil {
		return ClaimResult{}, err
	}
	return out, nil
}

// applyContention runs the concurrency policy, having already proved under the lock that the
// job is genuinely held. It records the decision on out and returns nil, so the transaction
// commits what it wrote.
func (s *Store) applyContention(ctx context.Context, tx *sql.Tx, out *ClaimResult, p ClaimParams, def gojob.Definition, st StateRow) error {
	now := s.clock.Now()

	// FORBID never applies to a manual trigger. It exists to stop a schedule piling up on
	// itself; silently discarding an operator's explicit request is the opposite of what
	// pressing the button means.
	if def.Concurrency == gojob.PolicyForbid {
		manual, err := isManual(ctx, tx, p.ExecutionID)
		if err != nil {
			return err
		}
		if !manual {
			res, err := tx.ExecContext(ctx, `
				UPDATE job_execution
				SET status = 'skipped', terminal_reason = ?, finished_at = ?,
				    result_summary = ?, lease_until = NULL, updated_at = ?
				WHERE id = ? AND status = 'ready'`,
				string(gojob.ReasonOperator), now,
				fmt.Sprintf("skipped by FORBID; job held by %s", st.ActiveExecution.String),
				now, p.ExecutionID)
			if err != nil {
				return fmt.Errorf("skip execution %d: %w", p.ExecutionID, err)
			}
			if err := assertOne(res, "claim: FORBID skip"); err != nil {
				return err
			}
			out.Outcome = ClaimSkipped
			out.Reason = fmt.Errorf("%w: job %q held by %s; occurrence skipped by FORBID",
				gojob.ErrContended, p.JobName, st.ActiveExecution.String)
			return nil
		}
	}

	// QUEUE, and every manual trigger: leave it ready and push available_at forward so a
	// blocked claim does not spin against a busy job.
	out.Outcome = ClaimDeferred
	out.Reason = fmt.Errorf("%w: job %q held by %s", gojob.ErrContended, p.JobName, st.ActiveExecution.String)
	return deferCandidate(ctx, tx, p.ExecutionID, p.BackoffSeconds, now)
}

// deferCandidate pushes a rejected candidate's available_at forward.
//
// Leaving it untouched would make it a permanent head-of-line block: discovery is an ordered,
// bounded page, so a handful of old unrunnable rows would fill every page forever and newer
// runnable work would never be seen. The row stays `ready` and stays visible as an orphan; it
// just stops occupying the front of the queue.
func deferCandidate(ctx context.Context, tx *sql.Tx, id int64, backoffSeconds int, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_execution
		SET available_at = TIMESTAMPADD(SECOND, ?, ?), updated_at = ?
		WHERE id = ? AND status = 'ready'`,
		backoffSeconds, now, now, id)
	if err != nil {
		return fmt.Errorf("defer execution %d: %w", id, err)
	}
	// Zero rows is possible and harmless: the row may have been resolved between discovery
	// and this transaction. Only more than one would be a violation.
	n, err := affected(res, "claim: defer candidate")
	if err != nil {
		return err
	}
	if n > 1 {
		return fmt.Errorf("%w: defer candidate affected %d rows", gojob.ErrProtocol, n)
	}
	return nil
}

func isManual(ctx context.Context, tx *sql.Tx, id int64) (bool, error) {
	var manual bool
	err := tx.QueryRowContext(ctx, `SELECT manual_first FROM job_execution WHERE id = ?`, id).Scan(&manual)
	if err != nil {
		return false, fmt.Errorf("read trigger class of execution %d: %w", id, err)
	}
	return manual, nil
}

// Accept records that an executor took the work. This is the ONLY place attempt_no is
// incremented — not at claim, not by recovery. It counts handler starts, and a dispatch that
// was refused, or never answered, did not start one.
//
// started_at uses COALESCE so a re-send answered with ALREADY_EXISTS does not rewrite the
// first start, and deadline_at — the silence budget — is set from NOW() because it is an
// ownership column extended by progress reports.
func (s *Store) Accept(ctx context.Context, id int64, runToken string, epoch int64, silenceSeconds int) error {
	now := s.clock.Now()
	res, err := s.db.ExecContext(ctx, `
		UPDATE job_execution
		SET status      = 'running',
		    attempt_no  = attempt_no + 1,
		    started_at  = COALESCE(started_at, ?),
		    deadline_at = TIMESTAMPADD(SECOND, ?, NOW()),
		    updated_at  = ?
		WHERE id = ? AND status = 'dispatching'
		  AND run_token = ? AND fence_epoch = ?`,
		now, silenceSeconds, now, id, runToken, epoch)
	if err != nil {
		return fmt.Errorf("accept execution %d: %w", id, err)
	}
	n, err := affected(res, "accept")
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: execution %d accepted under token %s epoch %d", gojob.ErrFenced, id, runToken, epoch)
	}
	return nil
}

// Refuse returns a claimed-but-unaccepted execution to `ready` and releases the job lock,
// consuming NO attempt budget. RESOURCE_EXHAUSTED, UNAVAILABLE and FAILED_PRECONDITION all
// land here: the executor was busy, shutting down, or does not have the handler, and none of
// those is a handler start.
//
// dispatched_to is cleared on both rows because the send provably did not land — which is
// what distinguishes this from the transport-error case, where the outcome is unknown and the
// row deliberately stays `dispatching` with its target recorded.
func (s *Store) Refuse(ctx context.Context, p ClaimParams, epoch int64) error {
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE job_state
			SET active_kind = NULL, active_execution = NULL, active_owner = NULL,
			    active_run_token = NULL, dispatched_to = NULL,
			    lease_until = NULL, updated_at = ?
			WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?`,
			now, p.JobName, p.RunToken, epoch)
		if err != nil {
			return fmt.Errorf("release job lock %q: %w", p.JobName, err)
		}
		n, err := affected(res, "refuse: release job lock")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: job %q under token %s epoch %d", gojob.ErrFenced, p.JobName, p.RunToken, epoch)
		}

		res, err = tx.ExecContext(ctx, `
			UPDATE job_execution
			SET status = 'ready',
			    available_at   = TIMESTAMPADD(SECOND, ?, ?),
			    owner_instance = NULL, dispatched_to = NULL, run_token = NULL,
			    lease_until = NULL, heartbeat_at = NULL, updated_at = ?
			WHERE id = ? AND status = 'dispatching'
			  AND run_token = ? AND fence_epoch = ?`,
			p.BackoffSeconds, now, now, p.ExecutionID, p.RunToken, epoch)
		if err != nil {
			return fmt.Errorf("return execution %d to ready: %w", p.ExecutionID, err)
		}
		return assertOne(res, "refuse: return to ready")
	})
}
