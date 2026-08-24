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
// Admission fails CLOSED on a mismatch after the runtime has had an opportunity to apply the
// embedded additive migrations. A schema newer than this binary is never downgraded.
const SchemaVersion = "3"

// Admit verifies that a coordination schema is the one this tenant is supposed to be using,
// is the version this build understands, and speaks the same wall clock.
//
// The three checks are ordered deliberately. Identity comes first because it is the one that
// catches a mistyped DSN, and a mistyped DSN pointing at another tenant's schema would pass
// both of the others.
func Admit(ctx context.Context, db *sql.DB, tenant, expectUUID string, loc *time.Location) error {
	gotVersion, err := IdentityVersion(ctx, db, tenant, expectUUID)
	if err != nil {
		return err
	}
	if gotVersion != SchemaVersion {
		return fmt.Errorf("%w: %s is schema version %s, this build requires %s",
			gojob.ErrSchemaVersion, tenant, gotVersion, SchemaVersion)
	}

	return assertClockContract(ctx, db, tenant, loc)
}

// IdentityVersion validates that the registry points at the intended tenant schema and
// returns its current version. Runtime migration calls this before executing any DDL.
func IdentityVersion(ctx context.Context, db *sql.DB, tenant, expectUUID string) (string, error) {
	var (
		gotTenant  string
		gotUUID    string
		gotVersion string
	)
	err := db.QueryRowContext(ctx,
		`SELECT tenant, schema_uuid, schema_version FROM schema_identity WHERE lock_row = 1`).
		Scan(&gotTenant, &gotUUID, &gotVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s has no identity row; it is an empty or unprovisioned schema",
			gojob.ErrSchemaIdentity, tenant)
	}
	if err != nil {
		return "", fmt.Errorf("%w: reading identity of %s: %v", gojob.ErrSchemaIdentity, tenant, err)
	}

	if gotTenant != tenant {
		return "", fmt.Errorf("%w: registry says %q, the schema says %q — the DSN points at another tenant",
			gojob.ErrSchemaIdentity, tenant, gotTenant)
	}
	if gotUUID != expectUUID {
		return "", fmt.Errorf("%w: %s expects schema %s, found %s — a restored snapshot, or a schema re-provisioned without updating the registry",
			gojob.ErrSchemaIdentity, tenant, expectUUID, gotUUID)
	}
	return gotVersion, nil
}

// assertClockContract checks the two things the clock model actually depends on.
//
// It replaces an earlier session-time-zone assertion that no longer proves anything. Once
// ownership moved to UTC_TIMESTAMP(), the session zone stopped participating: ownership
// columns are written and compared with a function that returns the same instant in every
// session, business columns are written and compared as values this process computes, and no
// column in the schema carries a CURRENT_TIMESTAMP default. A check that constrains something
// nothing reads is worse than no check — it reads as protection, and it fails deployments
// over a setting that cannot cause the error it names.
//
// What DOES matter:
//
//  1. The driver must parse DATETIME columns in the business Location. Business columns are
//     naked DATETIMEs written as Go times and read back as Go times, so the driver's `loc`
//     IS the wall clock they hold. A DSN missing parseTime, or carrying a different loc, does
//     not corrupt a single round trip — which is why it survives testing — but it means the
//     stored wall clock is not the one the design, the admin UI and any operator reading the
//     table assume.
//
//  2. This host's UTC clock and the database's must agree. Ownership instants come from the
//     database and business instants from this process, so a badly skewed host materializes
//     executions at instants that bear no relation to the leases guarding them.
func assertClockContract(ctx context.Context, db *sql.DB, tenant string, loc *time.Location) error {
	var dbUTC time.Time
	if err := db.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP()`).Scan(&dbUTC); err != nil {
		// Scanning a DATETIME into time.Time is exactly what parseTime=true enables, so a
		// scan failure here is the missing-parseTime case as much as a connection failure.
		return fmt.Errorf("%w: reading the database clock for %s (does the DSN set parseTime=true?): %v",
			gojob.ErrTimeZone, tenant, err)
	}

	// The driver tags every parsed DATETIME with the DSN's `loc`, so the value carries the
	// answer — no arithmetic, no tolerance, no guessing.
	if got := dbUTC.Location().String(); got != loc.String() {
		return fmt.Errorf("%w: %s parses timestamps in %s but the business location is %s; "+
			"set loc=%s on this DSN",
			gojob.ErrTimeZone, tenant, got, loc, loc)
	}

	// dbUTC was read as UTC wall clock and tagged with loc, so recover the instant by reading
	// its wall-clock fields back as UTC rather than trusting the tag.
	asUTC := time.Date(dbUTC.Year(), dbUTC.Month(), dbUTC.Day(),
		dbUTC.Hour(), dbUTC.Minute(), dbUTC.Second(), 0, time.UTC)
	skew := time.Now().UTC().Sub(asUTC)
	if skew < 0 {
		skew = -skew
	}
	// A minute of tolerance: this looks for a broken host clock, not for NTP wander, and a
	// tight bound would refuse to start a scheduler over a few seconds of drift.
	if skew > time.Minute {
		return fmt.Errorf("%w: %s's clock and this host's are %s apart (database UTC %s, host UTC %s); "+
			"ownership instants come from the database and business instants from here",
			gojob.ErrTimeZone, tenant, skew.Round(time.Second),
			asUTC.Format("15:04:05"), time.Now().UTC().Format("15:04:05"))
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
// The instants here come from time.Now() and are only ever read through time.Since, so they
// carry Go's monotonic reading and measure elapsed time regardless of what the wall clock
// does. They deliberately do NOT come from gojob.Clock: that is business time, it is truncated
// to whole seconds — which strips the monotonic reading — and it can legitimately be shifted.
// A backward wall-clock step would then make an arbitrarily old registry read look fresh, and
// this instance would keep claiming and renewing after the control plane had written it off,
// which is the exact state a DSN cutover assumes cannot exist.
type Fence struct {
	limit time.Duration

	mu       sync.RWMutex
	lastRead time.Time
}

// NewFence starts fenced: an instance has not read the registry until it has.
//
// It takes no Clock. The staleness of a registry read is a liveness question and it is
// answered in monotonic time — see the type comment.
func NewFence(limit time.Duration) *Fence {
	return &Fence{limit: limit}
}

// Refresh records a successful registry read.
func (f *Fence) Refresh() {
	f.mu.Lock()
	f.lastRead = time.Now()
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
	if age := time.Since(last); age > f.limit {
		return fmt.Errorf("%w: last registry read was %s ago, limit is %s",
			gojob.ErrControlStale, age.Truncate(time.Second), f.limit)
	}
	return nil
}

// Healthy reports readiness. A fenced instance must fail its readiness probe, so a load
// balancer stops sending it executor callbacks it would refuse anyway.
func (f *Fence) Healthy() bool { return f.Check() == nil }
