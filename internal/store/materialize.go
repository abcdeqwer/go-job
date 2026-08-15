package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// Schedule is the caller's cron engine, injected so this package holds no schedule semantics.
// Next answers "the first fire strictly after t"; Latest answers "the most recent fire at or
// before t"; CountBetween counts fires in (from, to] up to a limit, reporting whether the
// count is exact.
type Schedule interface {
	Next(after time.Time) (time.Time, error)
	Latest(at time.Time, horizon time.Duration) (time.Time, bool, error)
	CountBetween(from, to time.Time, limit int) (int, bool, error)
}

// DueJob is a state row the scan found due.
type DueJob struct {
	JobName string
	DueAt   time.Time
}

// DueCron finds cron jobs whose next fire instant has arrived.
//
// `next_fire_at <= ?` takes the business clock: next_fire_at is a business column, computed
// from the expression in the configured Location, and comparing it against the database's
// NOW() is the mixed-clock bug this package exists to avoid.
//
// Correctness never depends on this scan being prompt. A lost timer callback costs at most
// one scan interval, because the state row still says the job is due; the in-process timer
// heap only reduces latency.
func (s *Store) DueCron(ctx context.Context, limit int) ([]DueJob, error) {
	return s.due(ctx, `
		SELECT job_name, next_fire_at FROM job_state
		WHERE next_fire_at IS NOT NULL AND next_fire_at <= ? AND ops_paused = 0
		ORDER BY next_fire_at LIMIT ?`, limit)
}

// DuePoll finds fixed-delay jobs whose delay has elapsed. next_poll_at is NULL while a pass
// is outstanding, which is what reserves the loop, so a NULL row is simply not due.
func (s *Store) DuePoll(ctx context.Context, limit int) ([]DueJob, error) {
	return s.due(ctx, `
		SELECT job_name, next_poll_at FROM job_state
		WHERE next_poll_at IS NOT NULL AND next_poll_at <= ? AND ops_paused = 0
		ORDER BY next_poll_at LIMIT ?`, limit)
}

// due runs one of the two scans above.
//
// The two queries are written out rather than generated from a column name. A statement
// assembled with fmt.Sprintf reaches this package's static checks with `%s` where its columns
// should be, so the check that business columns are never compared against NOW() would inspect
// `%s <= ?` and conclude nothing — the same blind spot as concatenation, one level quieter.
func (s *Store) due(ctx context.Context, query string, limit int) ([]DueJob, error) {
	rows, err := s.db.QueryContext(ctx, query, s.clock.Now(), limit)
	if err != nil {
		return nil, fmt.Errorf("scan due jobs: %w", err)
	}
	defer rows.Close()

	var out []DueJob
	for rows.Next() {
		var d DueJob
		if err := rows.Scan(&d.JobName, &d.DueAt); err != nil {
			return nil, fmt.Errorf("scan due row: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MaterializeOutcome says what a materialization transaction did.
type MaterializeOutcome int

const (
	// MaterializedExecution created an execution row and advanced the clock.
	MaterializedExecution MaterializeOutcome = iota

	// MaterializedSkipped advanced the clock without creating anything — the SKIP misfire
	// policy, or a job that is disabled or retired.
	MaterializedSkipped

	// MaterializedSuspended did nothing at all, because a manual execution for this job is
	// waiting. Materialization resumes when it reaches a terminal state.
	MaterializedSuspended
)

// MaterializeResult is what a materialization transaction committed.
type MaterializeResult struct {
	Outcome      MaterializeOutcome
	ExecutionKey string
	ScheduledAt  time.Time
	NextFireAt   time.Time

	// Missed counts fire instants that passed while nothing was running. MissedExact is false
	// when the count was truncated, so an outage is reported as "at least N" rather than as a
	// number that quietly understates it.
	Missed      int
	MissedExact bool
}

// misfireHorizon bounds how far back a catch-up fire may be found. A year is far past any
// outage worth catching up on, and it stops a job that has been disabled for years from
// materializing an execution dated to when it was last enabled.
const misfireHorizon = 366 * 24 * time.Hour

// missedCountLimit caps the walk that counts missed instants. The count is a metric — nothing
// branches on its exact value — so truncating it is cheaper than being precise about a
// week-long outage of a per-second job.
const missedCountLimit = 1000

// MaterializeCron creates the execution for a due fire instant and advances next_fire_at, in
// one transaction.
//
// grace is the misfire threshold: a fire instant within grace of now counts as ON TIME, not
// as missed. Without it SKIP is unusable — a scan running a second late on a per-second job
// would see one missed instant plus one due instant on every pass, skip both, and the job
// would never run at all. grace should be at least the scan interval plus expected jitter.
//
// The state row is taken FOR UPDATE SKIP LOCKED, so two schedulers that both decide an
// instant is due do not both reach the insert: the loser skips this job on this pass and on
// its next pass sees a next_fire_at the winner already advanced. The unique key on
// execution_key stays as defence in depth for paths that do not hold this lock — a manual
// trigger, a retried transaction — not as the primary guard against concurrent scanners.
func (s *Store) MaterializeCron(ctx context.Context, jobName string, sched Schedule, grace time.Duration) (MaterializeResult, error) {
	var out MaterializeResult
	err := s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, jobName)
		if err != nil {
			return err
		}
		def, err := readDefinition(ctx, tx, jobName)
		if err != nil {
			return err
		}
		now := s.clock.Now()

		if !def.Enabled || def.Retired || def.ScheduleKind != gojob.ScheduleCron {
			// Stop the row being due forever. A later enable bumps the definition's version,
			// and the config-drift scan recomputes next_fire_at from the current schedule —
			// which is also what stops a re-enabled job replaying its dormant instants.
			out.Outcome = MaterializedSkipped
			return clearFireClock(ctx, tx, jobName, def.Version, now)
		}

		// Drift is reconciled BEFORE deciding what is due, never after.
		//
		// next_fire_at was computed from the definition as it stood at config_version. If the
		// definition has moved on, that instant belongs to a schedule nobody uses any more:
		// materializing it would run a job on Monday because Monday is what the old expression
		// said, and then stamp the new version onto the row as though it had been honoured.
		// Recompute and let the next pass decide.
		if st.ConfigVersion != def.Version {
			next, err := sched.Next(now)
			if err != nil {
				return fmt.Errorf("recompute next fire for %q: %w", jobName, err)
			}
			out.Outcome, out.NextFireAt = MaterializedSkipped, next
			return advanceFireClock(ctx, tx, jobName, next, def.Version, now)
		}

		// While a manual execution for this job is `ready`, materialization is suspended.
		// Separate bounded discovery pages guarantee a manual row is EXAMINED every pass, but
		// examining is not acquiring: a poll materializer can win the state-row lock each time
		// the previous pass releases it, and the operator's run waits indefinitely. Suspending
		// materialization makes the bound one in-flight execution plus its own duration, and
		// it cannot deadlock, because the manual execution's own budgets and timeout make it
		// terminal either way.
		suspended, err := manualPending(ctx, tx, jobName)
		if err != nil {
			return err
		}
		if suspended {
			out.Outcome = MaterializedSuspended
			return nil
		}

		if !st.NextFireAt.Valid {
			return fmt.Errorf("%w: cron job %q has no next_fire_at", gojob.ErrProtocol, jobName)
		}
		due := st.NextFireAt.Time

		// A fire at or after now-grace is ON TIME. Only strictly older instants are missed.
		//
		// All instants are whole seconds, so the half-open window [due, onTimeFrom) is
		// expressed to CountBetween — whose window is (from, to] — as (due-1s, onTimeFrom-1s].
		// Getting that endpoint wrong puts a fire landing exactly on the grace boundary into
		// the missed set, and under SKIP a job that is reliably one grace-period late would be
		// skipped on every single pass.
		onTimeFrom := now.Add(-grace)
		out.Missed, out.MissedExact, err = sched.CountBetween(
			due.Add(-time.Second), onTimeFrom.Add(-time.Second), missedCountLimit)
		if err != nil {
			return fmt.Errorf("count missed fires for %q: %w", jobName, err)
		}

		fireAt := due
		create := true
		switch {
		case out.Missed == 0:
			// On time. Nothing special.
		case def.Misfire == gojob.MisfireSkip:
			// SKIP runs nothing from the past. Unbounded replay is not on offer, and an hour
			// of downtime must not become an hour of catch-up executions arriving at once.
			create = false
		default:
			// FIRE_ONCE creates exactly one catch-up, for the LATEST MISSED instant — the last
			// fire strictly before the grace boundary, not the last fire before now. Using
			// Latest(now) would jump over the instants inside the grace window, which are on
			// time and have not been materialized yet, and silently drop them.
			latest, ok, err := sched.Latest(onTimeFrom.Add(-time.Second), misfireHorizon)
			if err != nil {
				return fmt.Errorf("find latest missed fire for %q: %w", jobName, err)
			}
			if !ok {
				create = false
			} else {
				fireAt = latest
			}
		}

		// The clock advances past the instant this pass DEALT WITH, not past now.
		//
		// Advancing to Next(now) would erase every instant between the one materialized and
		// now — for a job firing faster than the scan interval, most of them, with no execution
		// row and no missed count to show for it. Advancing to Next(fireAt) leaves the backlog
		// on the row, so successive passes drain it one instant at a time and anything that
		// ages past grace is then handled by the misfire policy, visibly.
		//
		// SKIP is the deliberate exception: its whole meaning is to abandon the past.
		anchor := fireAt
		if !create && def.Misfire == gojob.MisfireSkip {
			anchor = now
		}
		next, err := sched.Next(anchor)
		if err != nil {
			return fmt.Errorf("compute next fire for %q: %w", jobName, err)
		}
		out.NextFireAt = next

		if create {
			out.ExecutionKey = CronExecutionKey(jobName, fireAt)
			out.ScheduledAt = fireAt
			out.Outcome = MaterializedExecution
			created, err := insertExecution(ctx, tx, executionRow{
				Key:           out.ExecutionKey,
				JobName:       jobName,
				TriggerType:   gojob.TriggerCron,
				ScheduledAt:   fireAt,
				AvailableAt:   fireAt,
				MaxAttempts:   def.MaxAttempts,
				MaxRecoveries: def.MaxRecoveries,
				Params:        def.Params,
			}, now)
			if err != nil {
				return err
			}
			if !created {
				// Another path already materialized this instant. The clock still advances —
				// leaving it would make the row due forever.
				out.Outcome = MaterializedSkipped
			}
		} else {
			out.Outcome = MaterializedSkipped
		}

		return advanceFireClock(ctx, tx, jobName, next, def.Version, now)
	})
	if err != nil {
		return MaterializeResult{}, err
	}
	return out, nil
}

// MaterializePoll starts one pass of a fixed-delay job and reserves the loop.
//
// Clearing next_poll_at in this transaction is what makes the loop a loop. A poll key is
// fresh each pass rather than derived from an instant, so the unique key that stops two
// schedulers materializing one cron fire does not stop them materializing two passes.
// Leaving the column due until the result arrives would let a second scanner lock the row a
// moment later and create a second pass; both would be `ready`, so the second would run the
// instant the first finished — no delay, and repeated scans would build a backlog of passes
// over one queue.
//
// executionKey is supplied by the caller because a poll key carries a fresh monotonic
// identifier rather than being derived from anything this transaction knows. Deriving it from
// a timestamp would collide with a retained pass after a business-clock shift, and reusing a
// per-job key would make every pass after the first a duplicate.
func (s *Store) MaterializePoll(ctx context.Context, jobName, executionKey string) (MaterializeResult, error) {
	var out MaterializeResult
	err := s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, jobName)
		if err != nil {
			return err
		}
		def, err := readDefinition(ctx, tx, jobName)
		if err != nil {
			return err
		}
		now := s.clock.Now()

		// Re-verify due-ness under the lock, not just non-NULL-ness.
		//
		// The due scan does not lock, so between it and this transaction another replica can
		// materialize a pass, have it complete, and set next_poll_at to completion + delay.
		// Accepting any non-NULL value would then start a second pass immediately: the passes
		// would not overlap — the job lock still guarantees that — but the configured delay
		// would simply not have been waited, which is the entire content of a fixed-delay
		// schedule.
		//
		// Checked FIRST, because every branch below writes next_poll_at and the guarded write
		// would assert against a row that legitimately has nothing left to clear.
		if !st.NextPollAt.Valid || st.NextPollAt.Time.After(now) {
			out.Outcome = MaterializedSuspended
			return nil
		}

		if !def.Enabled || def.Retired {
			out.Outcome = MaterializedSkipped
			return clearPollClock(ctx, tx, jobName, now)
		}

		suspended, err := manualPending(ctx, tx, jobName)
		if err != nil {
			return err
		}
		if suspended {
			out.Outcome = MaterializedSuspended
			return nil
		}

		out.ExecutionKey = executionKey
		out.ScheduledAt = now
		out.Outcome = MaterializedExecution
		created, err := insertExecution(ctx, tx, executionRow{
			Key:           executionKey,
			JobName:       jobName,
			TriggerType:   gojob.TriggerPoll,
			ScheduledAt:   now,
			AvailableAt:   now,
			MaxAttempts:   def.MaxAttempts,
			MaxRecoveries: def.MaxRecoveries,
			Params:        def.Params,
		}, now)
		if err != nil {
			return err
		}
		if !created {
			return fmt.Errorf("%w: poll key %q already exists; keys must be fresh per pass",
				gojob.ErrProtocol, executionKey)
		}
		return clearPollClock(ctx, tx, jobName, now)
	})
	if err != nil {
		return MaterializeResult{}, err
	}
	return out, nil
}

// Recompute rewrites next_fire_at from the current schedule and clears the config drift.
//
// It is reached from three triggers — the due scan, the config-drift scan and a business
// clock change — but there is only one implementation, so the three cannot disagree about
// what a recomputed row looks like.
func (s *Store) Recompute(ctx context.Context, jobName string, sched Schedule) (time.Time, error) {
	var next time.Time
	err := s.tx(ctx, func(tx *sql.Tx) error {
		st, err := lockState(ctx, tx, jobName)
		if err != nil {
			return err
		}
		def, err := readDefinition(ctx, tx, jobName)
		if err != nil {
			return err
		}
		now := s.clock.Now()

		if !def.Enabled || def.Retired {
			return parkBothClocks(ctx, tx, jobName, def.Version, now)
		}

		if def.ScheduleKind == gojob.ScheduleFixedDelay {
			// A poller has no fire instant, so next_fire_at is cleared — but next_poll_at must
			// be RESTARTED, not left alone. Disabling a due poller clears it, and NULL means "a
			// pass is outstanding", so a re-enabled job would sit excluded from the due scan
			// forever with nothing to end the pass that never existed.
			//
			// The distinction between "outstanding" and "stranded" is not in job_state, so it
			// is answered where it actually lives: whether the job has a non-terminal execution.
			outstanding, err := passOutstanding(ctx, tx, jobName)
			if err != nil {
				return err
			}
			if st.NextPollAt.Valid || outstanding {
				return clearFireClockKeepingPoll(ctx, tx, jobName, def.Version, now)
			}
			return restartPollClock(ctx, tx, jobName, def.Version, now)
		}

		next, err = sched.Next(now)
		if err != nil {
			return fmt.Errorf("recompute next fire for %q: %w", jobName, err)
		}
		return advanceFireClock(ctx, tx, jobName, next, def.Version, now)
	})
	return next, err
}

// passOutstanding reports whether the job has an execution that has not reached a terminal
// state. It is the authority on whether a NULL next_poll_at means "a pass is running" or "the
// clock was parked and nothing will ever restart it".
func passOutstanding(ctx context.Context, tx *sql.Tx, jobName string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `
		SELECT 1 FROM job_execution
		WHERE job_name = ?
		  AND status IN ('ready', 'dispatching', 'running', 'cancel_requested')
		LIMIT 1`, jobName).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check outstanding pass for %q: %w", jobName, err)
	}
	return true, nil
}

// Drifted lists jobs whose state row was computed from a definition that has since changed.
//
// The due scan cannot be the only reader of config_version, because it only visits rows that
// are already due: an operator who changes a weekly job's expression would otherwise see
// nothing happen until the old next_fire_at arrives a week later — the edit accepted,
// audited, displayed in the UI, and silently inert.
func (s *Store) Drifted(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT s.job_name FROM job_state s
		JOIN job_definition d ON d.job_name = s.job_name
		WHERE s.config_version <> d.version
		ORDER BY s.job_name LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("scan drifted jobs: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan drifted job: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func advanceFireClock(ctx context.Context, tx *sql.Tx, jobName string, next time.Time, version int64, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET write_seq      = write_seq + 1,
		    next_fire_at   = ?,
		    config_version = ?,
		    updated_at     = ?
		WHERE job_name = ?`,
		next, version, now, jobName)
	if err != nil {
		return fmt.Errorf("advance fire clock for %q: %w", jobName, err)
	}
	return assertOne(res, "materialize: advance fire clock")
}

// clearFireClock parks a job that must not fire — disabled, retired, or not a cron job at all.
//
// It advances config_version along with the clock. Without that the drift scan, which selects
// rows where config_version <> the definition's version, would return this job on every pass
// forever: recomputation would park it again, leave the version behind again, and the scan
// would find it again. A cheap query run in a tight loop is still a loop.
func clearFireClock(ctx context.Context, tx *sql.Tx, jobName string, version int64, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET write_seq      = write_seq + 1,
		    next_fire_at   = NULL,
		    config_version = ?,
		    updated_at     = ?
		WHERE job_name = ?`, version, now, jobName)
	if err != nil {
		return fmt.Errorf("clear fire clock for %q: %w", jobName, err)
	}
	return assertOne(res, "materialize: clear fire clock")
}

// parkBothClocks stops a disabled or retired job being due on either scan.
func parkBothClocks(ctx context.Context, tx *sql.Tx, jobName string, version int64, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET write_seq      = write_seq + 1,
		    next_fire_at   = NULL,
		    next_poll_at   = NULL,
		    config_version = ?,
		    updated_at     = ?
		WHERE job_name = ?`, version, now, jobName)
	if err != nil {
		return fmt.Errorf("park clocks for %q: %w", jobName, err)
	}
	return assertOne(res, "materialize: park both clocks")
}

// clearFireClockKeepingPoll parks the cron clock of a fixed-delay job without disturbing an
// outstanding pass's reservation.
func clearFireClockKeepingPoll(ctx context.Context, tx *sql.Tx, jobName string, version int64, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET write_seq      = write_seq + 1,
		    next_fire_at   = NULL,
		    config_version = ?,
		    updated_at     = ?
		WHERE job_name = ?`, version, now, jobName)
	if err != nil {
		return fmt.Errorf("clear fire clock for %q: %w", jobName, err)
	}
	return assertOne(res, "materialize: clear fire clock keeping poll")
}

// restartPollClock makes a poller due immediately.
//
// A poller starts at once rather than after one delay, because the delay is defined as the gap
// BETWEEN passes and there is no previous pass to measure from. Waiting would make a
// newly-enabled poller look broken for exactly as long as its delay.
func restartPollClock(ctx context.Context, tx *sql.Tx, jobName string, version int64, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET write_seq      = write_seq + 1,
		    next_fire_at   = NULL,
		    next_poll_at   = ?,
		    config_version = ?,
		    updated_at     = ?
		WHERE job_name = ?`, now, version, now, jobName)
	if err != nil {
		return fmt.Errorf("restart poll clock for %q: %w", jobName, err)
	}
	return assertOne(res, "materialize: restart poll clock")
}

func clearPollClock(ctx context.Context, tx *sql.Tx, jobName string, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE job_state
		SET write_seq    = write_seq + 1,
		    next_poll_at = NULL,
		    updated_at   = ?
		WHERE job_name = ? AND next_poll_at IS NOT NULL`, now, jobName)
	if err != nil {
		return fmt.Errorf("clear poll clock for %q: %w", jobName, err)
	}
	return assertOne(res, "materialize: clear poll clock")
}

// manualPending reports whether an operator's run is waiting for this job.
func manualPending(ctx context.Context, tx *sql.Tx, jobName string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `
		SELECT 1 FROM job_execution
		WHERE job_name = ? AND manual_first = 1 AND status = 'ready' LIMIT 1`, jobName).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check pending manual run for %q: %w", jobName, err)
	}
	return true, nil
}

type executionRow struct {
	Key           string
	JobName       string
	TriggerType   gojob.TriggerType
	RequestID     string
	ScheduledAt   time.Time
	AvailableAt   time.Time
	MaxAttempts   int
	MaxRecoveries int
	Params        []byte
}

// insertExecution writes a new execution. It returns false when the key already exists, which
// for a cron instant is the correct, expected outcome of two writers racing — one row and one
// no-op, never two runs.
//
// INSERT IGNORE is deliberately NOT used: it also swallows a NOT NULL violation, a bad
// foreign key and a truncated value, turning a broken write into a silent skip. The duplicate
// is distinguished by inspecting the error instead.
func insertExecution(ctx context.Context, tx *sql.Tx, r executionRow, now time.Time) (bool, error) {
	manualFirst := 0
	if r.TriggerType == gojob.TriggerManual {
		manualFirst = 1
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO job_execution
		    (execution_key, job_name, trigger_type, manual_first, request_id,
		     scheduled_at, available_at, status, attempt_no, recovery_count,
		     max_attempts, max_recoveries, params_json, fence_epoch, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', 0, 0, ?, ?, ?, 0, ?, ?)`,
		r.Key, r.JobName, string(r.TriggerType), manualFirst, nullString(r.RequestID),
		r.ScheduledAt, r.AvailableAt, r.MaxAttempts, r.MaxRecoveries, nullBytes(r.Params),
		now, now)
	if err != nil {
		if isDuplicateKey(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert execution %q: %w", r.Key, err)
	}
	return true, nil
}

// isDuplicateKey recognises MySQL error 1062 without importing a driver.
//
// The library takes a *sql.DB the caller opened, so it never chooses the driver and cannot
// type-assert to its error type. Matching on the message is unpleasant but it is the only
// thing available at this layer, and the alternative — treating every insert error as a
// duplicate — would hide a genuine failure as a benign race.
func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 1062") || strings.Contains(msg, "Duplicate entry")
}

func nullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

// CronExecutionKey derives the idempotency key for a cron fire instant.
//
// It uses the instant's Unix second, not its wall clock. A wall-clock key would collide
// during a fall-back hour, where two real instants share one local time — and a collision
// there is silent, because a duplicate key is exactly how this design expresses "already
// materialized". The instant is unambiguous, and it also means re-pointing a job at a
// different time zone produces genuinely different keys rather than accidental matches.
func CronExecutionKey(jobName string, fireAt time.Time) string {
	return fmt.Sprintf("c:%s:%d", jobName, fireAt.Unix())
}
