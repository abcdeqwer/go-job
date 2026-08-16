// Package store holds every statement that touches a tenant's coordination schema.
//
// It is one package rather than several because the correctness argument in doc/protocol.md
// is about the statements as a set: the canonical lock order, the guards each transition
// carries, and which clock each column is compared against. Splitting claim, lease,
// completion and recovery across packages would let one of them be changed without its
// counterpart being read.
//
// Two rules hold everywhere in this package, and every deviation is a defect:
//
//   - Ownership columns — lease_until, heartbeat_at, deadline_at, timeout_at — are written and
//     compared using UTC_TIMESTAMP(), never a Go time and never NOW(). Ownership must not
//     depend on clock skew between hosts, and NOW() is not sufficient for that: it returns the
//     SESSION's wall clock, so two instances whose sessions resolved the business zone to
//     different offsets — one pool opened before a DST transition and one after — read NOW()
//     an hour apart, and one sees the other's live lease as expired. UTC_TIMESTAMP() is the
//     same instant in every session, always.
//   - Business columns — scheduled_at, available_at, started_at, finished_at, created_at,
//     updated_at — are written from the caller's business Clock. Admission asserts that every
//     connection's session time zone equals the configured business Location, so the two are
//     the same wall clock; what differs is which source is authoritative.
//
// Mixing them is the specific bug that makes a scheduler fire an hour early after a zone
// change, and it is invisible in any test that runs both clocks on one machine.
package store

import (
	"context"
	"database/sql"
	"fmt"

	gojob "github.com/abcdeqwer/go-job"
)

// Store runs the protocol against one tenant's schema. The connection IS the tenant
// boundary: there is no tenant column and no tenant predicate, so a query that forgets one
// cannot reach another tenant's rows.
type Store struct {
	db    *sql.DB
	clock gojob.Clock
}

// New wraps a pool already pointed at a tenant schema. It does not validate the schema;
// admission does that once, before a tenant is ever scheduled.
func New(db *sql.DB, clock gojob.Clock) *Store {
	return &Store{db: db, clock: clock}
}

// DB exposes the pool for admission and for read-only reporting queries.
func (s *Store) DB() *sql.DB { return s.db }

// tx runs fn in a transaction, rolling back on any error and on panic.
//
// Every caller in this package holds locks in the canonical order — job_state, then
// job_execution — so a transaction here never waits on a lock a sibling transaction holds
// in the reverse order.
func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}

// affected returns a statement's affected-row count.
//
// MySQL reports rows CHANGED, not rows matched, unless the connection sets CLIENT_FOUND_ROWS
// — and go-job cannot require that, because DSNs come from the tenant registry and a flag
// silently missing from one of them would break exactly one tenant, invisibly.
//
// So "zero rows means the guard failed" is not free. Every other column a guarded write
// touches can legitimately be assigned the value it already holds: DATETIME columns are
// whole-second, so a heartbeat or progress report redelivered inside the same database
// second writes lease_until, deadline_at and updated_at back unchanged, and MySQL reports
// zero. Read as fencing, that would abort a healthy twenty-hour handler because one response
// packet was lost.
//
// **Every guarded UPDATE in this package therefore increments write_seq**, which no other
// writer touches and which cannot be assigned its current value. That single column is what
// makes zero-rows-means-guard-failed true unconditionally. A new guarded statement without
// it is a defect, and TestEveryUpdateIsGuarded fails the build for one.
func affected(res sql.Result, stmt string) (int64, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: rows affected: %w", stmt, err)
	}
	return n, nil
}

// assertOne fails when a statement that must have applied did not.
//
// This is the difference between a protocol violation and ordinary contention. Contention is
// detected at a specific, named step — the state-row SELECT ... SKIP LOCKED, or a guarded CAS
// whose token no longer matches — and is reported as gojob.ErrContended or gojob.ErrFenced by
// the caller. Anywhere else, zero rows means the canonical lock order was not held or the row
// vanished, and silently continuing would corrupt ownership.
func assertOne(res sql.Result, stmt string) error {
	n, err := affected(res, stmt)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: %s affected %d rows, expected exactly 1", gojob.ErrProtocol, stmt, n)
	}
	return nil
}

// nullString renders an optional column.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
