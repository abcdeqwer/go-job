package gojob

import (
	"sync"
	"time"
)

// Clock supplies business time: the clock cron expressions are evaluated in and business
// windows are measured by.
//
// It is deliberately NOT the clock that decides ownership. Two clocks exist in this system
// and they are never interchangeable:
//
//   - business time — this interface — decides fire instants, availability and cutoffs.
//   - ownership time — always the database's UTC_TIMESTAMP() — decides lease expiry, silence
//     deadlines and the runtime cap. No Go code supplies it, which is the point: it cannot
//     be affected by a host clock, by skew between machines, or by a test shifting time.
//     UTC_TIMESTAMP() rather than NOW(): NOW() is the SESSION's wall clock, so two instances
//     whose sessions resolved a zone to different offsets — one pool opened before a DST
//     transition, one after — read it an hour apart, and one sees the other's live lease as
//     expired. UTC_TIMESTAMP() is the same instant in every session, always.
//
// Mixing them is the failure this separation exists to prevent. Availability written in a
// shifted business clock and compared against database time makes every execution either
// permanently invisible or immediately due.
type Clock interface {
	// Now returns current business time, truncated to a whole second.
	//
	// Whole seconds are required rather than preferred: execution keys are derived from
	// this value, and sub-second precision would let two callers produce two keys for what
	// is logically one fire.
	Now() time.Time

	// Location is the business time zone. Cron expressions are evaluated in it, and every
	// tenant's DSN must parse timestamps in it — admission checks that rather than letting it
	// surface as an eight-hour scheduling error. The database SESSION zone is not constrained
	// and does not need to be; nothing in the protocol reads the session clock.
	Location() *time.Location
}

// SystemClock is business time as wall time in a fixed location.
type SystemClock struct{ Loc *time.Location }

func (c SystemClock) Now() time.Time           { return time.Now().In(c.Loc).Truncate(time.Second) }
func (c SystemClock) Location() *time.Location { return c.Loc }

// FixedClock pins business time and can be moved deliberately. The differential replay
// harness uses it to run a handler as of an arbitrary historical instant; tests use it to make
// scheduling deterministic without sleeping.
//
// It is safe for concurrent use because the thing reading it is usually a scheduler loop in
// another goroutine while the thing moving it is a test — the exact shape that makes an
// unguarded field a flake nobody can reproduce.
//
// Moving a business clock is not free: every cron next_fire_at computed under the old one is
// wrong, so a shift must be followed by recomputing every cron state row for the tenant. That
// is why this is a testing and replay facility, and why production uses SystemClock.
type FixedClock struct {
	mu  sync.RWMutex
	at  time.Time
	loc *time.Location
}

// NewFixedClock pins business time at `at`, rendered in `loc`.
func NewFixedClock(at time.Time, loc *time.Location) *FixedClock {
	if loc == nil {
		loc = time.UTC
	}
	return &FixedClock{at: at, loc: loc}
}

func (c *FixedClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.at.In(c.loc).Truncate(time.Second)
}

func (c *FixedClock) Location() *time.Location {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loc
}

// Set moves business time to an absolute instant.
func (c *FixedClock) Set(t time.Time) {
	c.mu.Lock()
	c.at = t
	c.mu.Unlock()
}

// Advance moves business time forward by d.
func (c *FixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

// AsUTC re-tags an OWNERSHIP timestamp that a driver mislabelled.
//
// Ownership columns are written with UTC_TIMESTAMP(), so their digits are UTC. The MySQL driver,
// however, tags every bare DATETIME it reads with the DSN's `loc` — the BUSINESS location — so
// the scanned time.Time carries an offset the value never had, and the instant it reports is
// wrong by exactly that offset.
//
// It lives here, exported, because forgetting to call it has now been the same bug twice: once
// on job_executor.heartbeat_at and once on tenant_observation.observed_at. Both packages that
// read ownership columns already depend on this one, and a helper nobody can find is a helper
// the next reader will not call.
//
// The error is invisible in a UTC deployment — which is what the test harness runs in — so a
// test that proves anything here has to open its connection in a non-UTC location.
func AsUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(),
		t.Nanosecond(), time.UTC)
}
