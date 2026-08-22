package gojob

import "time"

// Tunables whose values are decisions rather than implementation details.
//
// They live together, and each states what goes wrong at the wrong value, because every one
// of them was chosen after a specific failure was demonstrated. A number scattered through
// the call sites is a number nobody revisits.

// DefaultScanInterval is how often each instance looks for due jobs.
//
// Correctness never depends on it: a lost timer callback costs at most one interval, because
// the state row still says the job is due. It bounds dispatch latency, and it bounds how fast
// a backlog of missed instants can drain — one instant per pass per job.
const DefaultScanInterval = 5 * time.Second

// DefaultMisfireGrace derives the misfire threshold from the scan interval.
//
// A fire instant is "missed" only once it is older than this. Without a threshold the misfire
// policy is unusable: a scan running one second late on a `* * * * * *` job sees one past
// instant plus the due one, calls both missed, and under SKIP creates nothing — on every
// pass, for ever, reporting no error while the missed counter climbs.
//
// The floor of a minute matches Quartz's default and is what most people already expect. The
// three-interval term is what stops that expectation quietly breaking when someone widens the
// scan: at a 60s interval a fixed 60s grace leaves no room at all for jitter, and the same
// starvation returns in a milder, harder-to-see form.
func DefaultMisfireGrace(scanInterval time.Duration) time.Duration {
	if g := 3 * scanInterval; g > time.Minute {
		return g
	}
	return time.Minute
}

// DefaultMisfirePolicy is what a job gets when its creator does not choose.
//
// FIRE_ONCE, not SKIP. Both are literal: SKIP advances to the first future fire and runs
// nothing from the past, which means a five-minute outage costs a daily job its whole day.
// That is a real choice some jobs want — one whose work is only meaningful at its instant —
// but it is a surprising default, and the surprise arrives during an incident, when a run
// that should have caught up silently did not.
//
// A job that genuinely must not catch up says so explicitly.
const DefaultMisfirePolicy = MisfireFireOnce

// DefaultConcurrencyPolicy is QUEUE: a due occurrence waits for the running one rather than
// being discarded. FORBID discards, which is the right answer only when a skipped occurrence
// is genuinely worthless.
const DefaultConcurrencyPolicy = PolicyQueue

// ReconcileDeadline bounds recovery's GetExecution call. It runs outside any transaction —
// an RPC to a process that may be wedged must never be made while holding a row lock — and
// "unreachable" means this elapsed. A defined outcome, not a vague one.
const ReconcileDeadline = 5 * time.Second

// DefaultExecutionSuccessRetention is how long successful executions remain visible in
// history. Success is the high-volume ordinary case, so it has the shorter window.
const DefaultExecutionSuccessRetention = 15 * 24 * time.Hour

// DefaultExecutionOtherRetention is the audit window for every other terminal outcome:
// dead, cancelled and skipped. Non-terminal executions are never eligible for retention.
const DefaultExecutionOtherRetention = 30 * 24 * time.Hour

// DefaultRetentionBatchSize bounds the rows removed by one tenant retention pass. Cleanup
// runs repeatedly, so a backlog drains without turning one sweep into a long transaction.
const DefaultRetentionBatchSize = 100

// MisfireHorizon bounds how far back a catch-up fire may be found. Far past any outage worth
// catching up on, and it stops a job dormant for years from materializing an execution dated
// to when it was last enabled.
const MisfireHorizon = 366 * 24 * time.Hour
