package store

import (
	"context"
	"database/sql"
	"encoding/json"
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
func (s *Store) CreateJob(ctx context.Context, d gojob.Definition, nextFire time.Time, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("%w: creating a job needs an actor and a reason", gojob.ErrProtocol)
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
		// The operator's OWN words, not a rendering of the row they just created. The row is
		// already in job_definition; what the audit log is for is why.
		return audit(ctx, tx, now, actor, "job_created", d.JobName, "",
			fmt.Sprintf("%s (handler=%s schedule=%s %s)",
				reason, d.HandlerKey, d.ScheduleKind, d.ScheduleExpr))
	})
}

// JobSeed is one validated definition plus the first cron fire computed by the API.
type JobSeed struct {
	Definition gojob.Definition
	NextFire   time.Time
}

// CopyJobs creates every missing seed in one target-tenant transaction. Existing names are
// skipped and never overwritten: tenant-local edits are authority for that tenant, and a
// bulk copy must not silently erase them.
func (s *Store) CopyJobs(ctx context.Context, seeds []JobSeed, source, actor, reason string) (created, skipped []string, err error) {
	if source == "" || actor == "" || reason == "" {
		return nil, nil, fmt.Errorf("%w: copying jobs needs a source, actor and reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()
	err = s.tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT job_name FROM job_definition FOR UPDATE`)
		if err != nil {
			return fmt.Errorf("lock target job definitions: %w", err)
		}
		existing := map[string]bool{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return fmt.Errorf("scan target job name: %w", err)
			}
			existing[name] = true
		}
		if err := rows.Close(); err != nil {
			return err
		}

		for _, seed := range seeds {
			d := seed.Definition
			if existing[d.JobName] {
				skipped = append(skipped, d.JobName)
				continue
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO job_definition
				    (job_name, handler_key, executor_group, schedule_kind, schedule_expr,
				     enabled, retired, concurrency_policy, misfire_policy,
				     max_attempts, max_recoveries, lease_seconds, timeout_seconds,
				     params_json, description, version, updated_by, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
				d.JobName, d.HandlerKey, nullString(d.ExecutorGroup), string(d.ScheduleKind), d.ScheduleExpr,
				d.Enabled, string(d.Concurrency), string(d.Misfire),
				d.MaxAttempts, d.MaxRecoveries, int(d.Lease/time.Second), int(d.Timeout/time.Second),
				nullBytes(d.Params), nullString(d.Description), actor, now, now); err != nil {
				return fmt.Errorf("copy job %q from %s: %w", d.JobName, source, err)
			}

			var fireArg, pollArg any
			if d.Enabled {
				if d.ScheduleKind == gojob.ScheduleCron {
					fireArg = seed.NextFire
				} else {
					pollArg = now
				}
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO job_state (job_name, next_fire_at, next_poll_at, config_version, updated_at)
				VALUES (?, ?, ?, 1, ?)`, d.JobName, fireArg, pollArg, now); err != nil {
				return fmt.Errorf("create copied state row for %q: %w", d.JobName, err)
			}
			if err := audit(ctx, tx, now, actor, "job_created", d.JobName, "",
				fmt.Sprintf("copied from tenant %s: %s (handler=%s schedule=%s %s)",
					source, reason, d.HandlerKey, d.ScheduleKind, d.ScheduleExpr)); err != nil {
				return err
			}
			created = append(created, d.JobName)
			existing[d.JobName] = true
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return created, skipped, nil
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

// JobDescriptionChange is one operator-visible description replacement. The handler key is
// included so the preview shows exactly which live code declaration supplies the new text.
type JobDescriptionChange struct {
	JobName    string `json:"job_name"`
	HandlerKey string `json:"handler_key"`
	Before     string `json:"before"`
	After      string `json:"after"`
}

// JobDescriptionSync is both the preview and execution result for a description-only sync.
// Retired jobs are outside the operation: the jobs screen and copy flow both define the active
// catalogue as non-retired definitions.
type JobDescriptionSync struct {
	Changes   []JobDescriptionChange `json:"changes"`
	Missing   []string               `json:"missing"`
	Unchanged int                    `json:"unchanged"`
}

// PlanJobDescriptionSync compares current definitions with descriptions declared by live
// executors. An empty/missing declaration is reported rather than used to erase operator text.
func (s *Store) PlanJobDescriptionSync(ctx context.Context, descriptions map[string]string) (JobDescriptionSync, error) {
	jobs, err := s.Jobs(ctx)
	if err != nil {
		return JobDescriptionSync{}, err
	}
	return descriptionSyncPlan(jobs, descriptions), nil
}

func descriptionSyncPlan(jobs []JobView, descriptions map[string]string) JobDescriptionSync {
	result := JobDescriptionSync{
		Changes: []JobDescriptionChange{},
		Missing: []string{},
	}
	for _, job := range jobs {
		if job.Retired {
			continue
		}
		next := descriptions[job.HandlerKey]
		if next == "" {
			result.Missing = append(result.Missing, job.JobName)
			continue
		}
		if job.Description == next {
			result.Unchanged++
			continue
		}
		result.Changes = append(result.Changes, JobDescriptionChange{
			JobName: job.JobName, HandlerKey: job.HandlerKey,
			Before: job.Description, After: next,
		})
	}
	return result
}

// SyncJobDescriptions changes only job_definition.description plus the mandatory optimistic
// version/audit metadata. Schedules, enabled state, policies, parameters and job_state are not
// selected or assigned by this transaction, so they cannot be accidentally rewritten from a
// stale browser snapshot.
func (s *Store) SyncJobDescriptions(ctx context.Context, descriptions map[string]string,
	actor, reason string) (JobDescriptionSync, error) {
	if actor == "" || reason == "" {
		return JobDescriptionSync{}, fmt.Errorf("%w: syncing job descriptions needs an actor and a reason", gojob.ErrProtocol)
	}
	for key, description := range descriptions {
		if len(description) > 512 {
			return JobDescriptionSync{}, fmt.Errorf("%w: handler %q description exceeds 512 characters", gojob.ErrProtocol, key)
		}
	}

	now := s.clock.Now()
	result := JobDescriptionSync{Changes: []JobDescriptionChange{}, Missing: []string{}}
	err := s.tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT job_name, handler_key, retired, COALESCE(description, ''), version
			FROM job_definition
			ORDER BY job_name
			FOR UPDATE`)
		if err != nil {
			return fmt.Errorf("lock job descriptions: %w", err)
		}
		type row struct {
			name, handler, description string
			retired                    bool
			version                    int64
		}
		var jobs []row
		for rows.Next() {
			var job row
			if err := rows.Scan(&job.name, &job.handler, &job.retired, &job.description, &job.version); err != nil {
				rows.Close()
				return fmt.Errorf("scan locked job description: %w", err)
			}
			jobs = append(jobs, job)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate locked job descriptions: %w", err)
		}
		if err := rows.Close(); err != nil {
			return err
		}

		for _, job := range jobs {
			if job.retired {
				continue
			}
			next := descriptions[job.handler]
			if next == "" {
				result.Missing = append(result.Missing, job.name)
				continue
			}
			if job.description == next {
				result.Unchanged++
				continue
			}
			res, err := tx.ExecContext(ctx, `
				UPDATE job_definition
				SET description = ?, version = version + 1, updated_by = ?, updated_at = ?
				WHERE job_name = ? AND version = ?`,
				next, actor, now, job.name, job.version)
			if err != nil {
				return fmt.Errorf("sync description for job %q: %w", job.name, err)
			}
			if err := assertOne(res, "sync job description"); err != nil {
				return err
			}
			if err := audit(ctx, tx, now, actor, "job_description_synced", job.name, "",
				fmt.Sprintf("%s (handler=%s)", reason, job.handler)); err != nil {
				return err
			}
			result.Changes = append(result.Changes, JobDescriptionChange{
				JobName: job.name, HandlerKey: job.handler,
				Before: job.description, After: next,
			})
		}
		return nil
	})
	if err != nil {
		return JobDescriptionSync{}, err
	}
	return result, nil
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
		// The state row first, in the canonical order.
		//
		// Without it, retirement races materialization: a materializer holding the lock has
		// already read the old definition, and inserts its execution after this transaction's
		// cancellation scan has run. That row is then `ready` on a retired job — permanently
		// deferred, because every claim rejects it as unrunnable, and permanently blocking the
		// schema's quiescence.
		if _, err := lockState(ctx, tx, jobName); err != nil {
			return err
		}
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
	if key, ok, err := s.executionByRequest(ctx, requestID, jobName); err != nil {
		return "", err
	} else if ok {
		return key, nil
	}

	now := s.clock.Now()
	key := ExecutionKey("m", jobName, requestID)

	err := s.tx(ctx, func(tx *sql.Tx) error {
		// The state row first, so this serialises with Retire. Reading the definition without
		// it lets a trigger see `retired = false`, pause, and insert its row after retirement
		// has already cancelled everything it could see — leaving a `ready` execution on a
		// retired job that no claim will ever run and that blocks the schema's quiescence.
		if _, err := lockState(ctx, tx, jobName); err != nil {
			return err
		}
		def, err := readDefinition(ctx, tx, jobName)
		if err != nil {
			return err
		}
		if def.Retired {
			return fmt.Errorf("%w: job %q is retired", gojob.ErrNotRunnable, jobName)
		}
		// MERGED, not replaced. An operator overriding one field expects the rest of the job's
		// configuration to stand: defaults {"region":"PH","batch":500} with an override
		// {"batch":100} must dispatch both fields, or a manual run silently loses the region
		// and does something different from every scheduled one.
		merged, err := mergeParams(def.Params, params)
		if err != nil {
			return err
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
			// exactly what idempotency promises — but only if it IS the same execution. The
			// loser must read back the row the winner actually created rather than returning
			// the key it computed for itself: with the same job those agree, and with a reused
			// id against another job they do not, and returning the computed one hands out a
			// key for a row that does not exist.
			var owner string
			if err := tx.QueryRowContext(ctx,
				`SELECT execution_key, job_name FROM job_execution WHERE request_id = ?`,
				requestID).Scan(&key, &owner); err != nil {
				return fmt.Errorf("read the winning execution for request %q: %w", requestID, err)
			}
			if owner != jobName {
				return fmt.Errorf("%w: request_id %q was already used to trigger %q, not %q",
					gojob.ErrProtocol, requestID, owner, jobName)
			}
			return nil
		}
		return audit(ctx, tx, now, actor, "job_triggered", jobName, key, reason)
	})
	if err != nil {
		return "", err
	}
	return key, nil
}

// mergeParams overlays an override on a job's defaults, one level deep.
//
// One level, not recursive: a nested object in an override replaces the nested object in the
// defaults. Deep-merging reads as more helpful and is not — there would be no way to REMOVE a
// nested field, and "why is this key still here" is a worse afternoon than "I had to write the
// whole object".
func mergeParams(defaults, override []byte) ([]byte, error) {
	if len(override) == 0 {
		return defaults, nil
	}
	if len(defaults) == 0 {
		return override, nil
	}
	var base, top map[string]json.RawMessage
	if err := json.Unmarshal(defaults, &base); err != nil {
		return nil, fmt.Errorf("%w: the job's stored parameters are not a JSON object", gojob.ErrProtocol)
	}
	if err := json.Unmarshal(override, &top); err != nil {
		return nil, fmt.Errorf("%w: the override is not a JSON object", gojob.ErrProtocol)
	}
	// A top-level `null` unmarshals into a nil map without error, and would silently mean "no
	// override" — which is a different request from the one that was made.
	if top == nil {
		return nil, fmt.Errorf("%w: the override is JSON null, not an object", gojob.ErrProtocol)
	}
	if base == nil {
		base = map[string]json.RawMessage{}
	}
	for k, v := range top {
		base[k] = v
	}
	out, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("encode merged parameters: %w", err)
	}
	// The MERGED size is what is bounded, not each half. Two disjoint forty-kilobyte objects
	// each pass a per-object cap and produce eighty kilobytes, copied onto the execution row
	// and sent on every dispatch.
	if len(out) > MaxParamsBytes {
		return nil, fmt.Errorf("%w: the merged parameters are %d bytes; the limit is %d",
			gojob.ErrProtocol, len(out), MaxParamsBytes)
	}
	return out, nil
}

// MaxParamsBytes bounds a parameter object. They are copied onto every execution row and sent
// on every dispatch, so a megabyte here is a megabyte per run, for ever.
const MaxParamsBytes = 64 << 10

// executionByRequest answers a repeat, and refuses a request id reused for a DIFFERENT job.
//
// An idempotency key identifies one request, not one string. Reused across jobs it produced
// two silent failures: the fast path returned job A's execution key while serving a trigger
// for job B and created nothing, and a race returned B's computed key for a row that does not
// exist — so an operator got an accepted response, and an execution key, for work that will
// never run.
//
// A reuse against another job is therefore a conflict rather than a repeat. The caller has
// made a mistake, and the only useful answer is to say so.
func (s *Store) executionByRequest(ctx context.Context, requestID, jobName string) (string, bool, error) {
	var key, owner string
	err := s.db.QueryRowContext(ctx,
		`SELECT execution_key, job_name FROM job_execution WHERE request_id = ?`,
		requestID).Scan(&key, &owner)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("look up request %q: %w", requestID, err)
	}
	if owner != jobName {
		return "", false, fmt.Errorf("%w: request_id %q was already used to trigger %q, not %q",
			gojob.ErrProtocol, requestID, owner, jobName)
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

	// Which job this execution belongs to is read BEFORE the transaction, not inside it.
	//
	// Inside, it would touch job_execution before job_state, which is the canonical order
	// reversed — and a transaction taking the two rows in the opposite order to completion is
	// the deadlock that order exists to prevent. Reading it outside can be stale, so the
	// transaction re-verifies it under the lock.
	jobName, err := s.jobOf(ctx, key)
	if err != nil {
		return err
	}

	return s.tx(ctx, func(tx *sql.Tx) error {
		// Under the job's lock, for the same reason a trigger is: reviving a dead execution
		// after its job was retired produces exactly the stranded `ready` row retirement exists
		// to prevent.
		if _, err := lockState(ctx, tx, jobName); err != nil {
			return err
		}
		def, err := readDefinition(ctx, tx, jobName)
		if err != nil {
			return err
		}
		if def.Retired {
			return fmt.Errorf("%w: job %q is retired; its executions cannot be revived",
				gojob.ErrNotRunnable, jobName)
		}

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
			WHERE execution_key = ? AND job_name = ? AND status = 'dead'`,
			now, now, key, jobName)
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

// jobOf names the job an execution belongs to, outside any transaction.
//
// Outside, because reading job_execution inside a transaction that then locks job_state takes
// the two in the reverse of the canonical order. The caller re-verifies under the lock, so a
// stale answer costs a refused action rather than a wrong one.
func (s *Store) jobOf(ctx context.Context, key string) (string, error) {
	var jobName string
	err := s.db.QueryRowContext(ctx,
		`SELECT job_name FROM job_execution WHERE execution_key = ?`, key).Scan(&jobName)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrNoSuchExecution, key)
	}
	if err != nil {
		return "", fmt.Errorf("read execution %q: %w", key, err)
	}
	return jobName, nil
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

// HandlerMetadata is operator-facing code metadata from a live executor. Key is dispatch
// authority; Description is explanatory only.
type HandlerMetadata struct {
	Key         string
	Description string
}

// DeclaredHandlers lists what live executors currently declare, for compatibility with API
// clients that predate handler descriptions.
//
// It is a convenience, not a constraint: a handler whose executor is down or not yet deployed
// must still be nameable, or a job could never be created before its executor ships.
func (s *Store) DeclaredHandlers(ctx context.Context, liveness time.Duration) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT h.handler_key
		FROM job_executor_handler h
		JOIN job_executor e ON e.executor_id = h.executor_id
		WHERE e.heartbeat_at >= TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP())
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

// DeclaredHandlerMetadata lists each live handler once. During a rolling deployment two live
// executor revisions may describe the same key differently; the freshest heartbeat wins so
// the creation form describes the code most recently admitted by the scheduler.
func (s *Store) DeclaredHandlerMetadata(ctx context.Context, liveness time.Duration) ([]HandlerMetadata, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT h.handler_key, COALESCE(h.description, '')
		FROM job_executor_handler h
		JOIN job_executor e ON e.executor_id = h.executor_id
		WHERE e.heartbeat_at >= TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP())
		ORDER BY h.handler_key, e.heartbeat_at DESC, e.executor_id`, -seconds(liveness))
	if err != nil {
		return nil, fmt.Errorf("list declared handler metadata: %w", err)
	}
	defer rows.Close()

	var out []HandlerMetadata
	seen := map[string]bool{}
	for rows.Next() {
		var h HandlerMetadata
		if err := rows.Scan(&h.Key, &h.Description); err != nil {
			return nil, fmt.Errorf("scan handler metadata: %w", err)
		}
		if seen[h.Key] {
			continue
		}
		seen[h.Key] = true
		out = append(out, h)
	}
	return out, rows.Err()
}

// Quiescence is what a schema still has outstanding.
type Quiescence struct {
	// Held is the number of jobs some execution currently owns, and InFlight the executions
	// actually running. Together they are what a DSN cutover must wait for: two schemas in
	// service for one tenant would each correctly exclude only themselves.
	Held     int
	InFlight int

	// Queued is `ready` work — created, not started, owned by nobody. It does NOT block a
	// cutover, because a disabled tenant has no scheduler draining it and gating on it would
	// make a cutover permanently unreachable. It is reported instead, because abandoning it
	// silently is not something an operator should discover afterwards.
	Queued int
}

// Quiet reports whether a cutover may proceed.
func (q Quiescence) Quiet() bool { return q.Held == 0 && q.InFlight == 0 }

// Quiescent counts what this schema still has outstanding.
//
// It is the mechanical half of a DSN cutover: acknowledgement can only say who REPLIED, while
// this says what is actually held. An instance partitioned from the control database stops
// replying but is still perfectly able to reach this one, so its rows are visible here even
// though its acknowledgements are not.
func (s *Store) Quiescent(ctx context.Context) (Quiescence, error) {
	var q Quiescence
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_state WHERE active_kind IS NOT NULL`).Scan(&q.Held); err != nil {
		return q, fmt.Errorf("count held jobs: %w", err)
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_execution
		 WHERE status IN ('dispatching', 'running', 'cancel_requested')`).Scan(&q.InFlight); err != nil {
		return q, fmt.Errorf("count in-flight executions: %w", err)
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_execution WHERE status = 'ready'`).Scan(&q.Queued); err != nil {
		return q, fmt.Errorf("count queued executions: %w", err)
	}
	return q, nil
}
