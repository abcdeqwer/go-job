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

// A week-long outage on a per-second schedule must not turn into 604,800 iterations. The walk
// is bounded by the finest probe window that contains a fire, so this is a sixty-step search
// whatever the age of the stale instant.
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
		t.Fatalf("Latest took %s; the anchor probe is not bounding the walk", elapsed)
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

	// An empty window is zero, not one.
	n, exact, err = e.CountBetween(from, from, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !exact || n != 0 {
		t.Fatalf("CountBetween over an empty window = %d (exact=%v), want 0 exact", n, exact)
	}
}
