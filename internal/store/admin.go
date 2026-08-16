package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// JobView is a job as the admin API presents it: configuration plus the hot state that says
// what it is actually doing.
type JobView struct {
	gojob.Definition

	NextFireAt    sql.NullTime
	NextPollAt    sql.NullTime
	OpsPaused     bool
	ActiveExec    string
	ActiveOwner   string
	LastSuccessAt sql.NullTime
	LastFailureAt sql.NullTime
	ConfigVersion int64
}

func scanJob(rows interface{ Scan(...any) error }) (JobView, error) {
	var (
		v        JobView
		params   []byte
		leaseSec int
		toSec    int
	)
	err := rows.Scan(&v.JobName, &v.HandlerKey, &v.ExecutorGroup, &v.ScheduleKind, &v.ScheduleExpr,
		&v.Enabled, &v.Retired, &v.Concurrency, &v.Misfire,
		&v.MaxAttempts, &v.MaxRecoveries, &leaseSec, &toSec,
		&params, &v.Description, &v.Version, &v.UpdatedBy,
		&v.NextFireAt, &v.NextPollAt, &v.OpsPaused,
		&v.ActiveExec, &v.ActiveOwner, &v.LastSuccessAt, &v.LastFailureAt, &v.ConfigVersion)
	if err != nil {
		return JobView{}, err
	}
	v.Params = params
	v.Lease = time.Duration(leaseSec) * time.Second
	v.Timeout = time.Duration(toSec) * time.Second
	return v, nil
}

// Jobs lists every job with its effective state.
func (s *Store) Jobs(ctx context.Context) ([]JobView, error) {
	const q = `
		SELECT d.job_name, d.handler_key, COALESCE(d.executor_group, ''), d.schedule_kind, d.schedule_expr,
		       d.enabled, d.retired, d.concurrency_policy, d.misfire_policy,
		       d.max_attempts, d.max_recoveries, d.lease_seconds, d.timeout_seconds,
		       d.params_json, COALESCE(d.description, ''), d.version, COALESCE(d.updated_by, ''),
		       s.next_fire_at, s.next_poll_at, s.ops_paused,
		       COALESCE(s.active_execution, ''), COALESCE(s.active_owner, ''),
		       s.last_success_at, s.last_failure_at, s.config_version
		FROM job_definition d JOIN job_state s ON s.job_name = d.job_name
		ORDER BY d.job_name`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var out []JobView
	for rows.Next() {
		v, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ErrNoSuchJob means the job does not exist in this tenant.
var ErrNoSuchJob = errors.New("gojob: no such job")

// Job reads one job.
func (s *Store) Job(ctx context.Context, name string) (JobView, error) {
	const q = `
		SELECT d.job_name, d.handler_key, COALESCE(d.executor_group, ''), d.schedule_kind, d.schedule_expr,
		       d.enabled, d.retired, d.concurrency_policy, d.misfire_policy,
		       d.max_attempts, d.max_recoveries, d.lease_seconds, d.timeout_seconds,
		       d.params_json, COALESCE(d.description, ''), d.version, COALESCE(d.updated_by, ''),
		       s.next_fire_at, s.next_poll_at, s.ops_paused,
		       COALESCE(s.active_execution, ''), COALESCE(s.active_owner, ''),
		       s.last_success_at, s.last_failure_at, s.config_version
		FROM job_definition d JOIN job_state s ON s.job_name = d.job_name
		WHERE d.job_name = ?`

	row := s.db.QueryRowContext(ctx, q, name)
	v, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return JobView{}, fmt.Errorf("%w: %s", ErrNoSuchJob, name)
	}
	if err != nil {
		return JobView{}, fmt.Errorf("read job %q: %w", name, err)
	}
	return v, nil
}

// CreateJob inserts a definition and its state row in ONE transaction.
//
// A definition without a state row is inert — both scheduling scans read job_state and a
// claim fails closed without it — so creating them separately would leave a job that appears
// in the UI, accepts edits, and never runs. There is no repair path for that except noticing.
//
// nextFire is the job's first fire instant, computed by the caller from the schedule it just
// validated; for a fixed-delay job it is ignored and the poll clock starts at `now`, because
// the delay is the gap BETWEEN passes and there is no previous pass to measure from.
func (s *Store) CreateJob(ctx context.Context, d gojob.Definition, nextFire time.Time, actor string) error {
	if actor == "" {
		return fmt.Errorf("%w: creating a job with no actor", gojob.ErrProtocol)
	}
	now := s.clock.Now()

	return s.tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO job_definition
			    (job_name, handler_key, executor_group, schedule_kind, schedule_expr,
			     enabled, retired, concurrency_policy, misfire_policy,
			     max_attempts, max_recoveries, lease_seconds, timeout_seconds,
			     params_json, description, version, updated_by, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
			d.JobName, d.HandlerKey, nullString(d.ExecutorGroup), string(d.ScheduleKind), d.ScheduleExpr,
			d.Enabled, string(d.Concurrency), string(d.Misfire),
			d.MaxAttempts, d.MaxRecoveries, int(d.Lease/time.Second), int(d.Timeout/time.Second),
			nullBytes(d.Params), nullString(d.Description), actor, now, now)
		if err != nil {
			if isDuplicateKey(err) {
				return fmt.Errorf("%w: job %q already exists", gojob.ErrProtocol, d.JobName)
			}
			return fmt.Errorf("create job %q: %w", d.JobName, err)
		}

		var fireArg, pollArg any
		if d.Enabled {
			if d.ScheduleKind == gojob.ScheduleCron {
				fireArg = nextFire
			} else {
				pollArg = now
			}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO job_state (job_name, next_fire_at, next_poll_at, config_version, updated_at)
			VALUES (?, ?, ?, 1, ?)`,
			d.JobName, fireArg, pollArg, now); err != nil {
			return fmt.Errorf("create state row for %q: %w", d.JobName, err)
		}
		return audit(ctx, tx, now, actor, "job_created", d.JobName, "",
			fmt.Sprintf("handler=%s schedule=%s %s", d.HandlerKey, d.ScheduleKind, d.ScheduleExpr))
	})
}

// UpdateJob edits a definition under an optimistic version check.
//
// A stale version is refused rather than silently overwritten: two operators editing the same
// job from two tabs is ordinary, and the loser learning that their change was discarded five
// minutes later is not.
//
// The version bump is what the drift scan notices, so an edit takes effect within seconds
// regardless of when the job was next due.
func (s *Store) UpdateJob(ctx context.Context, d gojob.Definition, expectVersion int64, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("%w: editing a job needs an actor and a reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()

	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE job_definition
			SET handler_key = ?, executor_group = ?, schedule_kind = ?, schedule_expr = ?,
			    enabled = ?, concurrency_policy = ?, misfire_policy = ?,
			    max_attempts = ?, max_recoveries = ?, lease_seconds = ?, timeout_seconds = ?,
			    params_json = ?, description = ?,
			    version = version + 1, updated_by = ?, updated_at = ?
			WHERE job_name = ? AND version = ?`,
			d.HandlerKey, nullString(d.ExecutorGroup), string(d.ScheduleKind), d.ScheduleExpr,
			d.Enabled, string(d.Concurrency), string(d.Misfire),
			d.MaxAttempts, d.MaxRecoveries, int(d.Lease/time.Second), int(d.Timeout/time.Second),
			nullBytes(d.Params), nullString(d.Description), actor, now,
			d.JobName, expectVersion)
		if err != nil {
			return fmt.Errorf("update job %q: %w", d.JobName, err)
		}
		n, err := affected(res, "update job")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: job %q is not at version %d", ErrStaleVersion, d.JobName, expectVersion)
		}
		return audit(ctx, tx, now, actor, "job_updated", d.JobName, "", reason)
	})
}

// ErrStaleVersion means an edit was made against a version the job no longer holds.
var ErrStaleVersion = errors.New("gojob: job was modified by someone else")

// SetPaused pauses or resumes a job.
//
// It takes the state row's lock — the same lock a claim takes — so a pause that races a claim
// resolves deterministically. Without the lock the two interleave, and the answer to "did the
// pause stop that run" depends on microseconds.
func (s *Store) SetPaused(ctx context.Context, jobName string, paused bool, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("%w: pausing a job needs an actor and a reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()

	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := lockState(ctx, tx, jobName); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE job_state
			SET write_seq = write_seq + 1, ops_paused = ?, updated_at = ?
			WHERE job_name = ?`, paused, now, jobName)
		if err != nil {
			return fmt.Errorf("set paused on %q: %w", jobName, err)
		}
		if err := assertOne(res, "set paused"); err != nil {
			return err
		}
		action := "job_resumed"
		if paused {
			action = "job_paused"
		}
		return audit(ctx, tx, now, actor, action, jobName, "", reason)
	})
}

// Retire marks a job permanently finished.
//
// It is separate from disabling because the two mean different things to an operator: a
// disabled job is expected back, and a retired one is not. Retiring also stops the orphan
// alert, which is the point — a job nobody intends to run again should not page anyone.
func (s *Store) Retire(ctx context.Context, jobName, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("%w: retiring a job needs an actor and a reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()

	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE job_definition
			SET retired = 1, enabled = 0, version = version + 1, updated_by = ?, updated_at = ?
			WHERE job_name = ? AND retired = 0`, actor, now, jobName)
		if err != nil {
			return fmt.Errorf("retire %q: %w", jobName, err)
		}
		n, err := affected(res, "retire job")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: job %q is already retired or does not exist", gojob.ErrProtocol, jobName)
		}

		// Outstanding work has to be resolved, not left. Retiring only the definition leaves
		// ready rows non-terminal and permanently deferred — the definition is unrunnable, so
		// every claim rejects them with a backoff for ever — and leaves running work with
		// nobody expecting it. Both show in the UI as a retired job that is somehow still busy.
		//
		// `ready` goes straight to `cancelled`: nothing was attempted, so there is nothing to
		// prove stopped.
		if _, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq = write_seq + 1,
			    status = 'cancelled', terminal_reason = ?, finished_at = ?,
			    result_summary = 'the job was retired before this run started',
			    updated_at = ?
			WHERE job_name = ? AND status = 'ready'`,
			string(gojob.ReasonRetired), now, now, jobName); err != nil {
			return fmt.Errorf("cancel pending work of %q: %w", jobName, err)
		}

		// Anything in flight goes to `cancel_requested`, which KEEPS its lease and its slot.
		// Marking it cancelled here would release the job while a handler is still writing,
		// which is the overlap the whole protocol exists to prevent — retirement is not a
		// reason to make an exception.
		if _, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq = write_seq + 1,
			    status = 'cancel_requested', updated_at = ?
			WHERE job_name = ? AND status IN ('dispatching', 'running')`,
			now, jobName); err != nil {
			return fmt.Errorf("stop in-flight work of %q: %w", jobName, err)
		}

		return audit(ctx, tx, now, actor, "job_retired", jobName, "", reason)
	})
}

// Trigger queues a manual run, idempotently on requestID.
//
// The idempotency is not a nicety. A double-clicked button, or a client that resent after a
// timeout, is the single most likely repeat under stress, and it is the one operation where a
// repeat means running the work twice. A second call with the same request id returns the
// execution the first one created.
//
// manual_first is set, which does two things: the manual row is discovered by its own bounded
// page, and materialization for the job is suspended while it is ready — so the operator's run
// acquires the job at the next release instead of losing indefinitely to a fast poller.
func (s *Store) Trigger(ctx context.Context, jobName, requestID, actor, reason string, params []byte) (string, error) {
	if actor == "" || reason == "" {
		return "", fmt.Errorf("%w: a manual trigger needs an actor and a reason", gojob.ErrProtocol)
	}
	if requestID == "" {
		return "", fmt.Errorf("%w: a manual trigger needs a request_id so a repeat cannot run it twice",
			gojob.ErrProtocol)
	}

	// Answer a repeat before doing anything else.
	if key, ok, err := s.executionByRequest(ctx, requestID); err != nil {
		return "", err
	} else if ok {
		return key, nil
	}

	now := s.clock.Now()
	key := fmt.Sprintf("m:%s:%s", jobName, requestID)
	if len(key) > 160 {
		key = key[:160]
	}

	err := s.tx(ctx, func(tx *sql.Tx) error {
		def, err := readDefinition(ctx, tx, jobName)
		if err != nil {
			return err
		}
		if def.Retired {
			return fmt.Errorf("%w: job %q is retired", gojob.ErrNotRunnable, jobName)
		}
		merged := def.Params
		if len(params) > 0 {
			merged = params
		}
		created, err := insertExecution(ctx, tx, executionRow{
			Key:           key,
			JobName:       jobName,
			TriggerType:   gojob.TriggerManual,
			RequestID:     requestID,
			ScheduledAt:   now,
			AvailableAt:   now,
			MaxAttempts:   def.MaxAttempts,
			MaxRecoveries: def.MaxRecoveries,
			Params:        merged,
		}, now)
		if err != nil {
			return err
		}
		if !created {
			// Two callers raced on the same request id. Both get the same execution, which is
			// exactly what idempotency promises.
			return nil
		}
		return audit(ctx, tx, now, actor, "job_triggered", jobName, key, reason)
	})
	if err != nil {
		return "", err
	}
	return key, nil
}

func (s *Store) executionByRequest(ctx context.Context, requestID string) (string, bool, error) {
	var key string
	err := s.db.QueryRowContext(ctx,
		`SELECT execution_key FROM job_execution WHERE request_id = ?`, requestID).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("look up request %q: %w", requestID, err)
	}
	return key, true, nil
}

// AuthorizedRetry returns a dead execution to `ready` and raises its attempt budget.
//
// Both halves matter. Returning it to `ready` without raising max_attempts produces a row
// that is immediately dead again on its next claim — the claim guard is
// `attempt_no < max_attempts` — so the button would appear to do nothing.
//
// The runtime cap is cleared too, because this is a new attempt and the cap is a per-attempt
// budget; leaving an elapsed one would have the next claim end the row instead of running it.
func (s *Store) AuthorizedRetry(ctx context.Context, key, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("%w: an authorized retry needs an actor and a reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()

	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE job_execution
			SET write_seq       = write_seq + 1,
			    status          = 'ready',
			    max_attempts    = attempt_no + 1,
			    available_at    = ?,
			    terminal_reason = NULL,
			    finished_at     = NULL,
			    timeout_at      = NULL,
			    updated_at      = ?
			WHERE execution_key = ? AND status = 'dead'`,
			now, now, key)
		if err != nil {
			return fmt.Errorf("retry execution %q: %w", key, err)
		}
		n, err := affected(res, "authorized retry")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: execution %q is not dead", gojob.ErrProtocol, key)
		}
		return audit(ctx, tx, now, actor, "execution_retried", "", key, reason)
	})
}

// ExecutionView is one row of the execution list.
type ExecutionView struct {
	ID             int64
	Key            string
	JobName        string
	TriggerType    string
	Status         gojob.Status
	AttemptNo      int
	MaxAttempts    int
	RecoveryCount  int
	ScheduledAt    time.Time
	AvailableAt    time.Time
	StartedAt      sql.NullTime
	FinishedAt     sql.NullTime
	OwnerInstance  string
	DispatchedTo   string
	FailureKind    string
	TerminalReason string
	ResultSummary  string
	ErrorMessage   string
}

// ExecutionFilter narrows the execution list.
//
// Every field is applied in SQL. Filtering a page in memory would make the visible rows, the
// total and any export disagree with each other, and the disagreement grows with the backlog
// — which is exactly when someone is looking.
type ExecutionFilter struct {
	JobName string
	Status  string
	From    *time.Time
	To      *time.Time
	Limit   int
	Offset  int
}

// Executions lists executions matching a filter, newest first, with the total.
//
// Two literal statements rather than one built from fragments, chosen by whether a job name
// was given. The reason is the index: `idx_job_execution_history (job_name, scheduled_at, id)`
// is only usable when job_name is an equality, and a `(? = ” OR job_name = ?)` predicate that
// covers both cases in one statement is usable for neither — which on a table holding months
// of history is a full scan every time an operator opens the page.
//
// The remaining filters ARE expressed that way, deliberately: status has a handful of distinct
// values, and scheduled_at follows job_name in the same index, so neither costs a scan.
//
// Everything is applied in SQL. Filtering a page in memory would make the visible rows, the
// total and any export disagree, and the disagreement grows with the backlog — which is
// exactly when someone is looking.
func (s *Store) Executions(ctx context.Context, f ExecutionFilter) ([]ExecutionView, int, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	from, to := timeBounds(f)

	var (
		total int
		rows  *sql.Rows
		err   error
	)
	if f.JobName != "" {
		if err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM job_execution
			WHERE job_name = ?
			  AND (? = '' OR status = ?)
			  AND scheduled_at >= ? AND scheduled_at <= ?`,
			f.JobName, f.Status, f.Status, from, to).Scan(&total); err != nil {
			return nil, 0, fmt.Errorf("count executions: %w", err)
		}
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, execution_key, job_name, trigger_type, status,
			       attempt_no, max_attempts, recovery_count,
			       scheduled_at, available_at, started_at, finished_at,
			       COALESCE(owner_instance, ''), COALESCE(dispatched_to, ''),
			       COALESCE(failure_kind, ''), COALESCE(terminal_reason, ''),
			       COALESCE(result_summary, ''), COALESCE(error_message, '')
			FROM job_execution
			WHERE job_name = ?
			  AND (? = '' OR status = ?)
			  AND scheduled_at >= ? AND scheduled_at <= ?
			ORDER BY scheduled_at DESC, id DESC LIMIT ? OFFSET ?`,
			f.JobName, f.Status, f.Status, from, to, limit, f.Offset)
	} else {
		if err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM job_execution
			WHERE (? = '' OR status = ?)
			  AND scheduled_at >= ? AND scheduled_at <= ?`,
			f.Status, f.Status, from, to).Scan(&total); err != nil {
			return nil, 0, fmt.Errorf("count executions: %w", err)
		}
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, execution_key, job_name, trigger_type, status,
			       attempt_no, max_attempts, recovery_count,
			       scheduled_at, available_at, started_at, finished_at,
			       COALESCE(owner_instance, ''), COALESCE(dispatched_to, ''),
			       COALESCE(failure_kind, ''), COALESCE(terminal_reason, ''),
			       COALESCE(result_summary, ''), COALESCE(error_message, '')
			FROM job_execution
			WHERE (? = '' OR status = ?)
			  AND scheduled_at >= ? AND scheduled_at <= ?
			ORDER BY scheduled_at DESC, id DESC LIMIT ? OFFSET ?`,
			f.Status, f.Status, from, to, limit, f.Offset)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()

	var out []ExecutionView
	for rows.Next() {
		var v ExecutionView
		if err := rows.Scan(&v.ID, &v.Key, &v.JobName, &v.TriggerType, &v.Status,
			&v.AttemptNo, &v.MaxAttempts, &v.RecoveryCount,
			&v.ScheduledAt, &v.AvailableAt, &v.StartedAt, &v.FinishedAt,
			&v.OwnerInstance, &v.DispatchedTo, &v.FailureKind, &v.TerminalReason,
			&v.ResultSummary, &v.ErrorMessage); err != nil {
			return nil, 0, fmt.Errorf("scan execution: %w", err)
		}
		out = append(out, v)
	}
	return out, total, rows.Err()
}

// timeBounds turns an open-ended filter into a closed range, so the query needs no optional
// predicate on the column its index is ordered by. The defaults are far outside any real
// scheduled_at, which is what makes "no bound given" and "a bound covering everything" the
// same query plan.
func timeBounds(f ExecutionFilter) (time.Time, time.Time) {
	from := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2999, 12, 31, 23, 59, 59, 0, time.UTC)
	if f.From != nil {
		from = *f.From
	}
	if f.To != nil {
		to = *f.To
	}
	return from, to
}

// Attempts reads an execution's attempt history, oldest first.
func (s *Store) Attempts(ctx context.Context, key string) ([]AttemptRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT execution_key, run_token, attempt_no, COALESCE(executor_id, ''),
		       outcome, COALESCE(failure_kind, ''), COALESCE(summary, ''), finished_at
		FROM job_execution_attempt WHERE execution_key = ?
		ORDER BY attempt_no, finished_at`, key)
	if err != nil {
		return nil, fmt.Errorf("read attempts of %q: %w", key, err)
	}
	defer rows.Close()

	var out []AttemptRecord
	for rows.Next() {
		var a AttemptRecord
		if err := rows.Scan(&a.ExecutionKey, &a.RunToken, &a.AttemptNo, &a.ExecutorID,
			&a.Outcome, &a.FailureKind, &a.Summary, &a.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AuditEntry is one recorded operator action.
type AuditEntry struct {
	ID        int64
	Actor     string
	Action    string
	JobName   string
	Execution string
	Detail    string
	CreatedAt time.Time
}

// Audit lists operator actions, newest first.
func (s *Store) AuditLog(ctx context.Context, jobName, actor string, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, actor, action, COALESCE(job_name, ''), COALESCE(execution, ''),
		       COALESCE(detail, ''), created_at
		FROM job_audit
		WHERE (? = '' OR job_name = ?) AND (? = '' OR actor = ?)
		ORDER BY id DESC LIMIT ?`,
		jobName, jobName, actor, actor, limit)
	if err != nil {
		return nil, fmt.Errorf("read audit log: %w", err)
	}
	defer rows.Close()

	var out []AuditEntry
	for rows.Next() {
		var a AuditEntry
		if err := rows.Scan(&a.ID, &a.Actor, &a.Action, &a.JobName, &a.Execution,
			&a.Detail, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan audit entry: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeclaredHandlers lists what live executors currently declare, for the job-creation picker.
//
// It is a convenience, not a constraint: a handler whose executor is down or not yet deployed
// must still be nameable, or a job could never be created before its executor ships.
func (s *Store) DeclaredHandlers(ctx context.Context, liveness time.Duration) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT h.handler_key
		FROM job_executor_handler h
		JOIN job_executor e ON e.executor_id = h.executor_id
		WHERE e.heartbeat_at >= TIMESTAMPADD(SECOND, ?, NOW())
		ORDER BY h.handler_key`, -seconds(liveness))
	if err != nil {
		return nil, fmt.Errorf("list declared handlers: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, fmt.Errorf("scan handler: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// Quiescent reports whether anything is still held in this schema.
//
// It is the mechanical half of a DSN cutover: acknowledgement can only say who REPLIED, while
// this says whether anything is actually held. An instance partitioned from the control
// database stops reporting but is still perfectly able to reach this one, so its rows are
// visible here even though its acknowledgements are not.
func (s *Store) Quiescent(ctx context.Context) (bool, error) {
	var held, outstanding int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_state WHERE active_kind IS NOT NULL`).Scan(&held); err != nil {
		return false, fmt.Errorf("count held jobs: %w", err)
	}
	// `ready` counts too. It is not held by anyone, but it IS work this schema still expects
	// to run: leaving it behind on a cutover either loses it, or runs it later if anything
	// ever points at the old schema again. A cutover that silently drops queued work is not a
	// cutover an operator can reason about.
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_execution
		 WHERE status IN ('ready', 'dispatching', 'running', 'cancel_requested')`).Scan(&outstanding); err != nil {
		return false, fmt.Errorf("count outstanding executions: %w", err)
	}
	return held == 0 && outstanding == 0, nil
}
