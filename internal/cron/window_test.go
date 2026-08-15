package cron

import (
	"testing"
	"time"
)

func TestLatest(t *testing.T) {
	utc := time.UTC
	cases := []struct {
		expr, at, want string
		horizon        time.Duration
	}{
		// On a boundary: `at` itself is a fire, and Latest is inclusive.
		{"0 0 * * * *", "2026-03-01 10:00:00", "2026-03-01 10:00:00", 24 * time.Hour},
		{"0 0 * * * *", "2026-03-01 10:30:00", "2026-03-01 10:00:00", 24 * time.Hour},
		{"* * * * * *", "2026-03-01 10:00:00", "2026-03-01 10:00:00", time.Hour},
		{"0 0 2 * * *", "2026-03-01 10:00:00", "2026-03-01 02:00:00", 24 * time.Hour},
		{"0 0 2 * * *", "2026-03-01 01:00:00", "2026-02-28 02:00:00", 24 * time.Hour},
		// Sparse: the last leap day before a date three years later.
		{"0 0 0 29 2 *", "2031-06-01 00:00:00", "2028-02-29 00:00:00", 5 * 366 * 24 * time.Hour},
	}
	for _, c := range cases {
		e := MustParse(c.expr)
		got, ok, err := e.Latest(at(t, utc, c.at), c.horizon)
		if err != nil {
			t.Errorf("Latest(%q, %s): %v", c.expr, c.at, err)
			continue
		}
		if !ok {
			t.Errorf("Latest(%q, %s): no fire found, want %s", c.expr, c.at, c.want)
			continue
		}
		if want := at(t, utc, c.want); !got.Equal(want) {
			t.Errorf("Latest(%q, %s) = %s, want %s", c.expr, c.at,
				got.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	}
}

// The horizon is a CLOSED interval, so a fire landing exactly on its far edge is in scope.
func TestLatestIncludesTheHorizonBoundary(t *testing.T) {
	e := MustParse("0 0 0 29 2 *")
	ref := at(t, time.UTC, "2029-03-01 00:00:00")
	// Exactly 366 days back from 2029-03-01 is 2028-03-01; widen by one more day so the only
	// candidate, 2028-02-29, sits precisely on the boundary.
	horizon := ref.Sub(at(t, time.UTC, "2028-02-29 00:00:00"))

	got, ok, err := e.Latest(ref, horizon)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatal("a fire exactly at at-horizon was excluded; the window is closed at both ends")
	}
	if want := at(t, time.UTC, "2028-02-29 00:00:00"); !got.Equal(want) {
		t.Fatalf("Latest = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestLatestNoFireInHorizon(t *testing.T) {
	e := MustParse("0 0 0 29 2 *") // leap day only
	_, ok, err := e.Latest(at(t, time.UTC, "2026-06-01 00:00:00"), time.Hour)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if ok {
		t.Fatal("found a leap-day fire within one hour of a June date")
	}
}

// Latest must never return an instant after `at`, which is the property misfire handling
// depends on: firing a catch-up for an instant that has not happened yet would run a job
// early and then run it again when it comes due.
func TestLatestIsNeverInTheFuture(t *testing.T) {
	utc := time.UTC
	for _, expr := range []string{"* * * * * *", "0 * * * * *", "0 0 * * * *", "0 0 2 * * MON"} {
		e := MustParse(expr)
		for _, s := range []string{"2026-03-01 10:00:00", "2026-03-01 10:00:01", "2026-03-01 23:59:59"} {
			ref := at(t, utc, s)
			got, ok, err := e.Latest(ref, 366*24*time.Hour)
			if err != nil || !ok {
				t.Fatalf("Latest(%q, %s): ok=%v err=%v", expr, s, ok, err)
			}
			if got.After(ref) {
				t.Errorf("Latest(%q, %s) = %s, which is in the future", expr, s, got.Format(time.RFC3339))
			}
			// And it must be the true latest: the next fire after it is strictly after ref.
			nxt, err := e.Next(got)
			if err != nil {
				t.Fatal(err)
			}
			if !nxt.After(ref) {
				t.Errorf("Latest(%q, %s) = %s, but %s also fires at or before it",
					expr, s, got.Format(time.RFC3339), nxt.Format(time.RFC3339))
			}
		}
	}
}

// A week-long outage on a per-second schedule must not turn into 604,800 iterations. The
// bisection's cost tracks the horizon, so the age of the stale instant does not matter.
func TestLatestIsCheapForADenseScheduleAfterALongOutage(t *testing.T) {
	e := MustParse("* * * * * *")
	ref := at(t, time.UTC, "2026-03-08 10:00:00")

	start := time.Now()
	got, ok, err := e.Latest(ref, 366*24*time.Hour)
	elapsed := time.Since(start)

	if err != nil || !ok {
		t.Fatalf("Latest: ok=%v err=%v", ok, err)
	}
	if !got.Equal(ref) {
		t.Fatalf("Latest = %s, want %s", got.Format(time.RFC3339), ref.Format(time.RFC3339))
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("Latest took %s; the search is enumerating fires rather than bisecting time", elapsed)
	}
}

// Bursty schedules are where an enumerating implementation dies, and where each raised cap
// merely moves the failure to a denser expression. The two here fire 14,400 and 604,800 times
// per burst; the bisection does not care, because its cost tracks the horizon and not the
// number of fires inside it.
//
// Getting this wrong is not a slow path — misfire handling calls Latest, so an error leaves
// the row due forever, retrying and failing.
func TestLatestHandlesBurstySchedules(t *testing.T) {
	cases := []struct{ expr, at, want string }{
		// Every second for four hours on the 1st: 14,400 fires per month.
		{"* * 0-3 1 * *", "2026-02-15 12:00:00", "2026-02-01 03:59:59"},
		// Every second for the first seven days: 604,800 fires per month.
		{"* * * 1-7 * *", "2026-02-15 12:00:00", "2026-02-07 23:59:59"},
	}
	for _, c := range cases {
		e := MustParse(c.expr)
		start := time.Now()
		got, ok, err := e.Latest(at(t, time.UTC, c.at), 366*24*time.Hour)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("Latest(%q): %v", c.expr, err)
			continue
		}
		if !ok {
			t.Errorf("Latest(%q): no fire found", c.expr)
			continue
		}
		if want := at(t, time.UTC, c.want); !got.Equal(want) {
			t.Errorf("Latest(%q) = %s, want %s", c.expr, got.Format(time.RFC3339), want.Format(time.RFC3339))
		}
		if elapsed > 100*time.Millisecond {
			t.Errorf("Latest(%q) took %s; cost must track the horizon, not the fire count",
				c.expr, elapsed)
		}
	}
}

// The bisection must agree with brute-force enumeration, including at the boundaries where an
// off-by-one is invisible to a hand-picked expectation.
func TestLatestAgreesWithEnumeration(t *testing.T) {
	utc := time.UTC
	for _, expr := range []string{"0 * * * * *", "*/7 * * * * *", "0 0 2 * * MON-FRI", "0 30 1 1,15 * *"} {
		e := MustParse(expr)
		for _, s := range []string{
			"2026-03-01 10:00:00", "2026-03-01 10:00:01", "2026-03-02 02:00:00",
			"2026-03-15 01:30:00", "2026-03-15 01:29:59", "2026-02-28 23:59:59",
		} {
			ref := at(t, utc, s)

			// Three days, not a year: the brute-force side has to enumerate every fire, and
			// `*/7` alone produces half a million in forty days. The bisection's correctness
			// does not depend on horizon length, so a short one tests the same logic.
			const horizon = 3 * 24 * time.Hour
			got, ok, err := e.Latest(ref, horizon)
			if err != nil {
				t.Fatalf("Latest(%q, %s): %v", expr, s, err)
			}

			// Brute force: walk forward from the horizon, keeping the last fire <= ref.
			var want time.Time
			var found bool
			// The same nanosecond nudge as Latest: an oracle that repeats the implementation's
			// boundary assumption agrees with it while both are wrong.
			cur := ref.Add(-horizon).Add(-time.Nanosecond)
			for i := 0; i < 200_000; i++ {
				nxt, err := e.Next(cur)
				if err != nil {
					t.Fatal(err)
				}
				if nxt.After(ref) {
					break
				}
				want, found, cur = nxt, true, nxt
			}

			if ok != found {
				t.Fatalf("Latest(%q, %s): ok=%v but enumeration found=%v", expr, s, ok, found)
			}
			if found && !got.Equal(want) {
				t.Errorf("Latest(%q, %s) = %s, enumeration says %s",
					expr, s, got.Format(time.RFC3339), want.Format(time.RFC3339))
			}
		}
	}
}

func TestCountBetween(t *testing.T) {
	utc := time.UTC
	e := MustParse("0 * * * * *") // every minute

	from := at(t, utc, "2026-03-01 10:00:00")
	to := at(t, utc, "2026-03-01 10:10:00")

	// (from, to] — the left end is exclusive, so 10:01 through 10:10 is ten.
	n, exact, err := e.CountBetween(from, to, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !exact || n != 10 {
		t.Fatalf("CountBetween = %d (exact=%v), want 10 exact", n, exact)
	}

	// Truncation is reported rather than silently understated.
	n, exact, err = e.CountBetween(from, to, 4)
	if err != nil {
		t.Fatal(err)
	}
	if exact || n != 4 {
		t.Fatalf("CountBetween with limit 4 = %d (exact=%v), want 4 truncated", n, exact)
	}

	// A limit that happens to equal the true count is NOT truncation. Reporting it as
	// truncated would turn "ten missed instants" into "at least ten" for the most likely
	// limit anyone would pick.
	n, exact, err = e.CountBetween(from, to, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !exact || n != 10 {
		t.Fatalf("CountBetween with limit 10 = %d (exact=%v), want 10 exact", n, exact)
	}

	// An empty window is zero, not one.
	n, exact, err = e.CountBetween(from, from, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !exact || n != 0 {
		t.Fatalf("CountBetween over an empty window = %d (exact=%v), want 0 exact", n, exact)
	}
}
