package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// seconds renders a duration for TIMESTAMPADD, rounding up so a sub-second window never
// collapses to zero and makes every registration instantly stale.
func seconds(d time.Duration) int {
	s := int((d + time.Second - 1) / time.Second)
	if s < 1 {
		return 1
	}
	return s
}

// Executor is one registered executor PROCESS.
//
// Registrations live in the tenant's database rather than in scheduler memory. With a
// scheduler cluster an in-memory registry would give each instance a different view of which
// executors exist, and routing would depend on which instance happened to decide — so two
// instances could disagree about whether a job is even runnable.
type Executor struct {
	ExecutorID      string
	Group           string
	Address         string
	ContractVersion string
	Revision        string
	Capacity        int
	Running         int
	Capabilities    string
	Handlers        []string
	StartedAt       time.Time
	HeartbeatAt     time.Time
}

// Register records an executor and the handlers it declares, replacing any previous
// declaration for the same executor id.
//
// The handler set is replaced rather than merged: a redeploy that DROPS a handler must be
// visible immediately, because the job that used it is now an orphan and the whole point of
// the orphan alert is to say so within a heartbeat rather than next week.
//
// executor_id is unique per PROCESS, so a restart mints a new one. That is deliberate — an
// id reused across restarts would let a stale registration for a process that no longer
// exists keep a job looking runnable.
func (s *Store) Register(ctx context.Context, e Executor) error {
	if e.ExecutorID == "" || e.Group == "" || e.Address == "" {
		return fmt.Errorf("%w: registration needs an id, a group and an address", gojob.ErrProtocol)
	}
	if len(e.Handlers) == 0 {
		return fmt.Errorf("%w: executor %q declares no handlers", gojob.ErrProtocol, e.ExecutorID)
	}

	return s.tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO job_executor
			    (executor_id, executor_group, address, contract_version, revision,
			     capacity, running, capabilities, started_at, heartbeat_at)
			VALUES (?, ?, ?, ?, ?, ?, 0, ?, NOW(), NOW())
			ON DUPLICATE KEY UPDATE
			    executor_group   = VALUES(executor_group),
			    address          = VALUES(address),
			    contract_version = VALUES(contract_version),
			    revision         = VALUES(revision),
			    capacity         = VALUES(capacity),
			    capabilities     = VALUES(capabilities),
			    heartbeat_at     = NOW()`,
			e.ExecutorID, e.Group, e.Address, e.ContractVersion, e.Revision,
			e.Capacity, nullString(e.Capabilities))
		if err != nil {
			return fmt.Errorf("register executor %q: %w", e.ExecutorID, err)
		}

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM job_executor_handler WHERE executor_id = ?`, e.ExecutorID); err != nil {
			return fmt.Errorf("clear handlers of %q: %w", e.ExecutorID, err)
		}

		// One statement per handler rather than a generated multi-row VALUES list. A built
		// statement is invisible to this package's static SQL checks, and the transaction
		// already guarantees the set is applied whole or not at all — which was the only
		// reason to want a single statement.
		for _, h := range e.Handlers {
			if h == "" {
				return fmt.Errorf("%w: executor %q declares an empty handler key", gojob.ErrProtocol, e.ExecutorID)
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO job_executor_handler (executor_id, handler_key) VALUES (?, ?)`,
				e.ExecutorID, h); err != nil {
				return fmt.Errorf("declare handler %q of %q: %w", h, e.ExecutorID, err)
			}
		}
		return nil
	})
}

// ExecutorHeartbeat keeps a registration alive and reports the executor's current load.
//
// A false return means the registration has lapsed and the executor must call Register again.
// That is the only recovery path, and it is deliberate: an executor whose row was reaped has
// no handlers declared, so silently re-creating the row here would leave it registered and
// unroutable.
func (s *Store) ExecutorHeartbeat(ctx context.Context, executorID string, running int) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE job_executor
		SET write_seq = write_seq + 1, heartbeat_at = NOW(), running = ?
		WHERE executor_id = ?`,
		running, executorID)
	if err != nil {
		return false, fmt.Errorf("heartbeat executor %q: %w", executorID, err)
	}
	n, err := affected(res, "executor heartbeat")
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Deregister removes an executor that is shutting down cleanly, so routing stops choosing it
// immediately rather than after a liveness window of failed dispatches.
func (s *Store) Deregister(ctx context.Context, executorID string) error {
	// The handler rows go with it through ON DELETE CASCADE.
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM job_executor WHERE executor_id = ?`, executorID); err != nil {
		return fmt.Errorf("deregister executor %q: %w", executorID, err)
	}
	return nil
}

// LiveExecutors returns the executors that can run a handler, newest heartbeat first.
//
// group empty means any group declaring the handler; naming one restricts to it. That
// distinction is not cosmetic: a handler-only check reports a group-bound job as runnable
// while the only executors declaring its handler are in the wrong group — runnable in the UI,
// undispatchable in practice.
//
// liveness is the window, and it is a scheduler configuration rather than a schema constant
// because it has to be a multiple of whatever heartbeat interval the executors were built
// with.
//
// The comparison is against the DATABASE clock, like every other liveness decision in this
// package. An executor's heartbeat is received by whichever scheduler instance the load
// balancer picked, so writing it from that instance's own clock would make "is this executor
// alive" depend on skew between scheduler hosts — an instance running fast would keep a dead
// executor routable, and one running slow would declare a healthy fleet orphaned.
func (s *Store) LiveExecutors(ctx context.Context, handlerKey, group string, liveness time.Duration) ([]Executor, error) {
	const q = `
		SELECT e.executor_id, e.executor_group, e.address, e.contract_version, e.revision,
		       e.capacity, e.running, COALESCE(e.capabilities, ''), e.started_at, e.heartbeat_at
		FROM job_executor e
		JOIN job_executor_handler h ON h.executor_id = e.executor_id
		WHERE h.handler_key = ?
		  AND e.heartbeat_at >= TIMESTAMPADD(SECOND, ?, NOW())
		  AND (? = '' OR e.executor_group = ?)
		ORDER BY e.heartbeat_at DESC, e.executor_id`

	rows, err := s.db.QueryContext(ctx, q, handlerKey, -seconds(liveness), group, group)
	if err != nil {
		return nil, fmt.Errorf("find live executors for %q: %w", handlerKey, err)
	}
	defer rows.Close()

	var out []Executor
	for rows.Next() {
		var e Executor
		if err := rows.Scan(&e.ExecutorID, &e.Group, &e.Address, &e.ContractVersion, &e.Revision,
			&e.Capacity, &e.Running, &e.Capabilities, &e.StartedAt, &e.HeartbeatAt); err != nil {
			return nil, fmt.Errorf("scan executor: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AllExecutors lists every registration for the admin UI, live or not, so an operator can see
// a process that stopped heartbeating rather than merely see it vanish.
func (s *Store) AllExecutors(ctx context.Context) ([]Executor, error) {
	const q = `
		SELECT e.executor_id, e.executor_group, e.address, e.contract_version, e.revision,
		       e.capacity, e.running, COALESCE(e.capabilities, ''), e.started_at, e.heartbeat_at,
		       COALESCE(GROUP_CONCAT(h.handler_key ORDER BY h.handler_key SEPARATOR ','), '')
		FROM job_executor e
		LEFT JOIN job_executor_handler h ON h.executor_id = e.executor_id
		GROUP BY e.executor_id, e.executor_group, e.address, e.contract_version, e.revision,
		         e.capacity, e.running, e.capabilities, e.started_at, e.heartbeat_at
		ORDER BY e.executor_group, e.executor_id`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list executors: %w", err)
	}
	defer rows.Close()

	var out []Executor
	for rows.Next() {
		var (
			e        Executor
			handlers string
		)
		if err := rows.Scan(&e.ExecutorID, &e.Group, &e.Address, &e.ContractVersion, &e.Revision,
			&e.Capacity, &e.Running, &e.Capabilities, &e.StartedAt, &e.HeartbeatAt, &handlers); err != nil {
			return nil, fmt.Errorf("scan executor: %w", err)
		}
		if handlers != "" {
			e.Handlers = strings.Split(handlers, ",")
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Orphan is a job no live executor can run.
type Orphan struct {
	JobName    string
	HandlerKey string
	Group      string
}

// Orphans lists enabled, unretired jobs for which no live executor declares the handler in
// the required group.
//
// This is the query the orphan alert is built on, and it is the difference between noticing
// in a minute that a job has no executor and noticing next week that it stopped running. An
// orphan is never dispatched and never marked failed — nothing was attempted.
func (s *Store) Orphans(ctx context.Context, liveness time.Duration) ([]Orphan, error) {
	const q = `
		SELECT d.job_name, d.handler_key, COALESCE(d.executor_group, '')
		FROM job_definition d
		WHERE d.enabled = 1 AND d.retired = 0
		  AND NOT EXISTS (
		      SELECT 1 FROM job_executor_handler h
		      JOIN job_executor e ON e.executor_id = h.executor_id
		      WHERE h.handler_key = d.handler_key
		        AND e.heartbeat_at >= TIMESTAMPADD(SECOND, ?, NOW())
		        AND (d.executor_group IS NULL OR e.executor_group = d.executor_group))
		ORDER BY d.job_name`

	rows, err := s.db.QueryContext(ctx, q, -seconds(liveness))
	if err != nil {
		return nil, fmt.Errorf("scan orphan jobs: %w", err)
	}
	defer rows.Close()

	var out []Orphan
	for rows.Next() {
		var o Orphan
		if err := rows.Scan(&o.JobName, &o.HandlerKey, &o.Group); err != nil {
			return nil, fmt.Errorf("scan orphan: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// ReapExecutors deletes registrations that stopped heartbeating long ago.
//
// The retention window is much larger than the liveness window on purpose. Liveness decides
// routing; reaping decides history. Deleting a registration the moment it goes stale would
// erase the evidence an operator needs at exactly the moment they need it — "the executor
// that was running this job is gone" is a more useful screen than a job whose executor simply
// never appears.
func (s *Store) ReapExecutors(ctx context.Context, retention time.Duration) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM job_executor WHERE heartbeat_at < TIMESTAMPADD(SECOND, ?, NOW())`,
		-seconds(retention))
	if err != nil {
		return 0, fmt.Errorf("reap dead executors: %w", err)
	}
	return affected(res, "reap executors")
}

// HandlerIsServed answers runnability condition 3 inside the claim transaction.
//
// It is a separate, narrower query than LiveExecutors because the claim needs a boolean and
// not a list: the executor actually chosen is picked outside the transaction, by routing that
// also considers capacity and recent dispatch health, and doing that work while holding the
// job's lock would hold it across a decision that does not need it.
func HandlerIsServed(ctx context.Context, tx *sql.Tx, def gojob.Definition, liveness time.Duration) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `
		SELECT 1 FROM job_executor_handler h
		JOIN job_executor e ON e.executor_id = h.executor_id
		WHERE h.handler_key = ?
		  AND e.heartbeat_at >= TIMESTAMPADD(SECOND, ?, NOW())
		  AND (? = '' OR e.executor_group = ?)
		LIMIT 1`,
		def.HandlerKey, -seconds(liveness), def.ExecutorGroup, def.ExecutorGroup).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check live executor for handler %q: %w", def.HandlerKey, err)
	}
	return true, nil
}
