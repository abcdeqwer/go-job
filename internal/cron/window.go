package cron

import (
	"fmt"
	"time"
)

// probeWindows bound how far back Latest looks, finest first.
//
// The point is not the horizon itself but the walk length. Latest works by finding an anchor
// A with Next(A) <= at, then walking forward; the number of steps in that walk is the number
// of fires between A and `at`. Starting with the finest window keeps that small for a
// fast schedule — a per-second expression matches the one-minute anchor and walks sixty
// times, never the 31,536,000 a naive walk from a year-old next_fire_at would take.
var probeWindows = []time.Duration{
	time.Minute,
	time.Hour,
	24 * time.Hour,
	7 * 24 * time.Hour,
	31 * 24 * time.Hour,
	366 * 24 * time.Hour,
}

// walkCap bounds the forward walk inside Latest. It is far above any legitimate result for
// the anchor that matched — the finest window that contains a fire — and exists so a
// pathological expression fails loudly instead of spinning.
const walkCap = 10_000

// Latest returns the most recent fire instant at or before `at`, searching back at most
// `horizon`. ok is false when the expression has no fire in that window.
//
// It exists because misfire handling needs to know what the last fire WAS, and a cron
// expression only knows how to go forwards. Enumerating forwards from a stale next_fire_at is
// not an option: a per-second job whose scheduler was down for a week is 604,800 steps.
func (e *Expression) Latest(at time.Time, horizon time.Duration) (time.Time, bool, error) {
	var anchor time.Time
	var found bool
	for _, w := range probeWindows {
		if w > horizon {
			break
		}
		cand, err := e.Next(at.Add(-w))
		if err != nil {
			return time.Time{}, false, err
		}
		if !cand.After(at) {
			anchor, found = cand, true
			break
		}
	}
	if !found {
		// None of the standard windows contained a fire; try the caller's horizon exactly,
		// so a horizon between two windows, or larger than the largest, still works.
		cand, err := e.Next(at.Add(-horizon))
		if err != nil {
			return time.Time{}, false, err
		}
		if cand.After(at) {
			return time.Time{}, false, nil
		}
		anchor = cand
	}

	last := anchor
	for i := 0; ; i++ {
		if i > walkCap {
			return time.Time{}, false, fmt.Errorf(
				"cron %q: more than %d fires between the anchor and %s; the expression is denser than any schedule this is designed for",
				e.src, walkCap, at.Format(time.RFC3339))
		}
		nxt, err := e.Next(last)
		if err != nil {
			return time.Time{}, false, err
		}
		if nxt.After(at) {
			return last, true, nil
		}
		last = nxt
	}
}

// CountBetween returns how many fires fall in (from, to], stopping at limit.
//
// exact is false when the count was truncated, so a caller can record "at least N missed"
// rather than a number that quietly understates an outage. The count is a metric, not a
// control input — nothing branches on its exact value — which is why truncating it is
// acceptable and walking a week of per-second fires to be precise is not.
func (e *Expression) CountBetween(from, to time.Time, limit int) (n int, exact bool, err error) {
	cur := from
	for n < limit {
		nxt, err := e.Next(cur)
		if err != nil {
			return n, false, err
		}
		if nxt.After(to) {
			return n, true, nil
		}
		n++
		cur = nxt
	}
	return n, false, nil
}
