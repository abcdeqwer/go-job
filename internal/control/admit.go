package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// SchemaVersion is the coordination schema this build requires.
//
// Admission fails CLOSED on a mismatch: no silent degradation, no partial feature set, no
// writing to a column that may not exist. The consequence is a real contract — an upgrade
// needing new columns is a migration you apply first, and the release notes say so. That is
// the price of not running DDL at runtime, and it is the right price for a component holding
// a lock in someone else's production database.
const SchemaVersion = "1"

// Admit verifies that a coordination schema is the one this tenant is supposed to be using,
// is the version this build understands, and speaks the same wall clock.
//
// The three checks are ordered deliberately. Identity comes first because it is the one that
// catches a mistyped DSN, and a mistyped DSN pointing at another tenant's schema would pass
// both of the others.
func Admit(ctx context.Context, db *sql.DB, tenant, expectUUID string, loc *time.Location) error {
	var (
		gotTenant  string
		gotUUID    string
		gotVersion string
	)
	err := db.QueryRowContext(ctx,
		`SELECT tenant, schema_uuid, schema_version FROM schema_identity WHERE lock_row = 1`).
		Scan(&gotTenant, &gotUUID, &gotVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s has no identity row; it is an empty or unprovisioned schema",
			gojob.ErrSchemaIdentity, tenant)
	}
	if err != nil {
		return fmt.Errorf("%w: reading identity of %s: %v", gojob.ErrSchemaIdentity, tenant, err)
	}

	if gotTenant != tenant {
		return fmt.Errorf("%w: registry says %q, the schema says %q — the DSN points at another tenant",
			gojob.ErrSchemaIdentity, tenant, gotTenant)
	}
	if gotUUID != expectUUID {
		return fmt.Errorf("%w: %s expects schema %s, found %s — a restored snapshot, or a schema re-provisioned without updating the registry",
			gojob.ErrSchemaIdentity, tenant, expectUUID, gotUUID)
	}
	if gotVersion != SchemaVersion {
		return fmt.Errorf("%w: %s is schema version %s, this build requires %s; apply the migration first",
			gojob.ErrSchemaVersion, tenant, gotVersion, SchemaVersion)
	}

	return assertSessionZone(ctx, db, tenant, loc)
}

// assertSessionZone refuses a connection whose session time zone is not the business one.
//
// Every business column in the coordination schema is a naked DATETIME, and the protocol
// compares those against values this process computes in `loc`. If the session zone differs,
// the two disagree by the offset between them — and the symptom is a job firing eight hours
// early, at 2am, months after the mistake was made. Catching it at admission costs one query.
func assertSessionZone(ctx context.Context, db *sql.DB, tenant string, loc *time.Location) error {
	// Compare the OFFSET, not the zone name: MySQL may be configured with a named zone, a
	// numeric offset, or SYSTEM, and only the offset is comparable across all three.
	var dbNow time.Time
	if err := db.QueryRowContext(ctx, `SELECT NOW()`).Scan(&dbNow); err != nil {
		return fmt.Errorf("%w: reading NOW() for %s: %v", gojob.ErrTimeZone, tenant, err)
	}

	// The driver parses a DATETIME with whatever location the connection was configured with.
	// What matters is that the WALL CLOCK the database reports matches the wall clock this
	// process would compute for the same instant, so compare the rendered wall time.
	local := time.Now().In(loc)
	skew := local.Sub(time.Date(
		dbNow.Year(), dbNow.Month(), dbNow.Day(),
		dbNow.Hour(), dbNow.Minute(), dbNow.Second(), 0, loc))

	if skew < 0 {
		skew = -skew
	}
	// A minute of tolerance. This check is looking for a WHOLE-HOUR class of error — a session
	// in UTC while the business runs in Asia/Manila — not for ordinary clock drift, and a tight
	// bound here would make the scheduler refuse to start over a few seconds of NTP wander.
	if skew > time.Minute {
		return fmt.Errorf("%w: %s reports %s but the business location %s says %s (%s apart); "+
			"set the session time zone on this DSN",
			gojob.ErrTimeZone, tenant, dbNow.Format("15:04:05"), loc, local.Format("15:04:05"), skew)
	}
	return nil
}

// Fence tracks whether this instance may still act.
//
// The control database is a lease on the right to operate. An instance that has not
// successfully read the registry within the staleness limit stops claiming, stops
// materializing, stops renewing leases and drops readiness — for every tenant — resuming when
// the control database returns.
//
// The alternative, letting a partitioned instance keep renewing so nothing is stranded, gets
// it exactly backwards. Nothing IS stranded: its leases expire, and any other instance can
// still reach the tenant database and recover them. What renewal would preserve is precisely
// the thing being ruled out — an owner nobody can see, still holding work, while the API
// concludes it is gone and lets a DSN cutover proceed.
type Fence struct {
	limit time.Duration
	clock gojob.Clock

	mu       sync.RWMutex
	lastRead time.Time
}

// NewFence starts fenced: an instance has not read the registry until it has.
func NewFence(clock gojob.Clock, limit time.Duration) *Fence {
	return &Fence{limit: limit, clock: clock}
}

// Refresh records a successful registry read.
func (f *Fence) Refresh() {
	f.mu.Lock()
	f.lastRead = f.clock.Now()
	f.mu.Unlock()
}

// Check returns nil while this instance may act, and ErrControlStale once it may not.
func (f *Fence) Check() error {
	f.mu.RLock()
	last := f.lastRead
	f.mu.RUnlock()

	if last.IsZero() {
		return fmt.Errorf("%w: the registry has never been read", gojob.ErrControlStale)
	}
	if age := f.clock.Now().Sub(last); age > f.limit {
		return fmt.Errorf("%w: last registry read was %s ago, limit is %s",
			gojob.ErrControlStale, age.Truncate(time.Second), f.limit)
	}
	return nil
}

// Healthy reports readiness. A fenced instance must fail its readiness probe, so a load
// balancer stops sending it executor callbacks it would refuse anyway.
func (f *Fence) Healthy() bool { return f.Check() == nil }
