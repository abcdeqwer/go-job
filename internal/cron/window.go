package cron

import (
	"time"
)

// Latest returns the most recent fire instant at or before `at`, searching back at most
// `horizon`. ok is false when the expression has no fire in that window.
//
// It exists because misfire handling needs to know what the last fire WAS, and a cron
// expression only knows how to go forwards.
//
// The implementation bisects over time rather than enumerating fires, so its cost depends on
// the LENGTH of the horizon and not at all on how many times the expression fires inside it.
// That distinction is the whole point. An enumerating version has to be capped, and any cap
// is wrong for some legitimate schedule: `* * 0-3 1 * *` fires 14,400 times a month and
// `* * * 1-7 * *` fires 604,800 times, both perfectly valid, and a cap sized past them is a
// cap that no longer catches anything. Roughly twenty-five Next calls cover a full year to
// the second, whatever the expression.
//
// The bisection rests on Next being monotonic: for any t before the last fire L, Next(t) <= L
// <= at, and for any t at or after L, Next(t) > at — otherwise a fire later than L would exist
// in the window. So the largest t with Next(t) <= at is L minus one second, and Next of it is
// L exactly.
func (e *Expression) Latest(at time.Time, horizon time.Duration) (time.Time, bool, error) {
	// A nanosecond back, because Next is strictly after its argument: the documented window is
	// the closed interval [at-horizon, at], and starting at the boundary itself would exclude a
	// fire landing exactly on it. For a leap-day expression asked one year and a day later,
	// that single instant is the only candidate there is.
	lo := at.Add(-horizon).Add(-time.Nanosecond)

	first, err := e.Next(lo)
	if err != nil {
		return time.Time{}, false, err
	}
	if first.After(at) {
		return time.Time{}, false, nil // no fire in the window at all
	}

	// Invariant: Next(lo) <= at, and hi is the exclusive upper bound of the search.
	hi := at
	for hi.Sub(lo) > time.Second {
		mid := lo.Add(hi.Sub(lo) / 2)
		nxt, err := e.Next(mid)
		if err != nil {
			return time.Time{}, false, err
		}
		if nxt.After(at) {
			hi = mid
		} else {
			lo = mid
		}
	}

	last, err := e.Next(lo)
	if err != nil {
		return time.Time{}, false, err
	}
	return last, true, nil
}

// CountBetween returns how many fires fall in (from, to], stopping at limit.
//
// exact is false when the count was truncated, so a caller can record "at least N missed"
// rather than a number that quietly understates an outage. The count is a metric, not a
// control input — nothing branches on its exact value — which is why truncating it is
// acceptable and walking a week of per-second fires to be precise is not.
//
// Reaching the limit is not the same as truncating. A window holding exactly `limit` fires is
// counted exactly, so the extra probe below is what stops the common "I sized the limit to the
// answer" case from being reported as an unknown overflow.
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
	nxt, err := e.Next(cur)
	if err != nil {
		return n, false, err
	}
	return n, nxt.After(to), nil
}
