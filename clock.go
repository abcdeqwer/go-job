package gojob

import "time"

// Clock supplies business time: the clock cron expressions are evaluated in and business
// windows are measured by.
//
// It is deliberately NOT the clock that decides ownership. Two clocks exist in this system
// and they are never interchangeable:
//
//   - business time — this interface — decides fire instants, availability and cutoffs.
//   - ownership time — always the database's NOW() — decides lease expiry, silence
//     deadlines and the runtime cap. No Go code supplies it, which is the point: it cannot
//     be affected by a host clock, by skew between machines, or by a test shifting time.
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
	// tenant's database session must agree with it — admission checks that rather than
	// letting it surface as an eight-hour scheduling error.
	Location() *time.Location
}

// SystemClock is business time as wall time in a fixed location.
type SystemClock struct{ Loc *time.Location }

func (c SystemClock) Now() time.Time           { return time.Now().In(c.Loc).Truncate(time.Second) }
func (c SystemClock) Location() *time.Location { return c.Loc }

// FixedClock pins business time. The differential replay harness uses it to run a handler
// as of an arbitrary historical instant; tests use it to make scheduling deterministic.
type FixedClock struct {
	At  time.Time
	Loc *time.Location
}

func (c FixedClock) Now() time.Time           { return c.At.In(c.Loc).Truncate(time.Second) }
func (c FixedClock) Location() *time.Location { return c.Loc }
