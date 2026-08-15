package cron

import (
	"testing"
	"time"
)

func mustLoad(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Skipf("zone %s unavailable: %v", name, err)
	}
	return loc
}

func at(t *testing.T, loc *time.Location, s string) time.Time {
	t.Helper()
	v, err := time.ParseInLocation("2006-01-02 15:04:05", s, loc)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return v
}

func next(t *testing.T, expr string, from time.Time) time.Time {
	t.Helper()
	e, err := Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q): %v", expr, err)
	}
	got, err := e.Next(from)
	if err != nil {
		t.Fatalf("Next(%q, %s): %v", expr, from, err)
	}
	return got
}

func TestParseRejects(t *testing.T) {
	cases := []struct {
		expr string
		why  string
	}{
		{"* * * * *", "five fields"},
		{"* * * * * * *", "seven fields"},
		{"0 0 0 L * *", "L unsupported"},
		{"0 0 0 15W * *", "W unsupported"},
		{"0 0 0 ? * MON#2", "# and ? unsupported"},
		{"0 0 0 ? * MON", "? unsupported even alone"},
		{"60 * * * * *", "second out of range"},
		{"* 60 * * * *", "minute out of range"},
		{"* * 24 * * *", "hour out of range"},
		{"* * * 32 * *", "day-of-month out of range"},
		{"* * * * 13 *", "month out of range"},
		{"* * * * * 8", "day-of-week out of range"},
		{"0 0 0 31 2 *", "February has no 31st"},
		{"0 0 0 30 2 *", "February has no 30th"},
		{"0 0 0 31 4,6,9,11 *", "none of those months has a 31st"},
		{"*/0 * * * * *", "zero step"},
		{"5-1 * * * * *", "inverted range"},
		{"0 0 0 1,, * *", "empty list element"},
		{"0 0 0 * FOO *", "unknown month name"},
		{"0 0 0 * * FUNDAY", "unknown day name"},
	}
	for _, c := range cases {
		if _, err := Parse(c.expr); err == nil {
			t.Errorf("Parse(%q) succeeded; expected rejection (%s)", c.expr, c.why)
		}
	}
}

func TestParseAccepts(t *testing.T) {
	// 31 February is impossible, but the ORed day-of-week rescues it.
	for _, expr := range []string{
		"0 0 0 * * *",
		"*/15 * * * * *",
		"0 0 2 * * *",
		"0 30 3 * * MON-FRI",
		"0 0 0 1 JAN *",
		"0 0 0 31 2 MON",
		"0 0 0 29 2 *", // leap years only, but reachable
		"5/15 * * * * *",
		"0 0 0 * * 7",
		"0 0 0 * * 0",
	} {
		if _, err := Parse(expr); err != nil {
			t.Errorf("Parse(%q): %v", expr, err)
		}
	}
}

func TestNextBasic(t *testing.T) {
	utc := time.UTC
	cases := []struct {
		expr, from, want string
	}{
		{"0 * * * * *", "2026-03-01 10:00:00", "2026-03-01 10:01:00"},
		{"0 * * * * *", "2026-03-01 10:00:30", "2026-03-01 10:01:00"},
		{"*/15 * * * * *", "2026-03-01 10:00:00", "2026-03-01 10:00:15"},
		{"*/15 * * * * *", "2026-03-01 10:00:50", "2026-03-01 10:01:00"},
		{"0 0 2 * * *", "2026-03-01 10:00:00", "2026-03-02 02:00:00"},
		{"0 0 2 * * *", "2026-03-01 01:00:00", "2026-03-01 02:00:00"},
		{"0 0 0 1 * *", "2026-03-15 10:00:00", "2026-04-01 00:00:00"},
		{"0 0 0 29 2 *", "2026-01-01 00:00:00", "2028-02-29 00:00:00"},
		{"5/15 * * * * *", "2026-03-01 10:00:00", "2026-03-01 10:00:05"},
		{"5/15 * * * * *", "2026-03-01 10:00:05", "2026-03-01 10:00:20"},
		{"0 0 12 * * MON", "2026-03-01 00:00:00", "2026-03-02 12:00:00"}, // 2026-03-02 is a Monday
		{"0 0 0 * JAN *", "2026-06-01 00:00:00", "2027-01-01 00:00:00"},
	}
	for _, c := range cases {
		got := next(t, c.expr, at(t, utc, c.from))
		want := at(t, utc, c.want)
		if !got.Equal(want) {
			t.Errorf("Next(%q, %s) = %s, want %s", c.expr, c.from, got.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	}
}

// Next must be strictly after its argument, or a scheduler that materializes from the last
// fire would loop on one instant forever.
func TestNextIsStrictlyAfter(t *testing.T) {
	utc := time.UTC
	from := at(t, utc, "2026-03-01 10:00:00")
	got := next(t, "0 0 10 * * *", from)
	if !got.After(from) {
		t.Fatalf("Next returned %s, which is not after %s", got, from)
	}
	if want := at(t, utc, "2026-03-02 10:00:00"); !got.Equal(want) {
		t.Fatalf("Next = %s, want %s", got, want)
	}
}

// The OR rule: with both day fields restricted, either one matching is enough.
func TestDayOfMonthOrDayOfWeek(t *testing.T) {
	utc := time.UTC
	// 2026-03-02 is a Monday; the 1st is a Sunday.
	// "1st of the month OR any Monday" must fire on both the 1st and the 2nd.
	got := next(t, "0 0 0 1 * MON", at(t, utc, "2026-02-28 00:00:00"))
	if want := at(t, utc, "2026-03-01 00:00:00"); !got.Equal(want) {
		t.Fatalf("first = %s, want %s (day-of-month arm)", got, want)
	}
	got = next(t, "0 0 0 1 * MON", got)
	if want := at(t, utc, "2026-03-02 00:00:00"); !got.Equal(want) {
		t.Fatalf("second = %s, want %s (day-of-week arm)", got, want)
	}
}

// A day field that matches everything is not a restriction, so `*/1` must not widen the OR
// into "every day". This is the Vixie cron trap the package comment calls out.
func TestStepOneDayOfMonthIsNotRestriction(t *testing.T) {
	utc := time.UTC
	// Only Mondays, despite `*/1` in day-of-month.
	from := at(t, utc, "2026-03-02 12:00:00") // a Monday
	got := next(t, "0 0 12 */1 * MON", from)
	if want := at(t, utc, "2026-03-09 12:00:00"); !got.Equal(want) {
		t.Fatalf("got %s, want %s — `*/1` must not turn the OR into every day", got, want)
	}
	// Same for an explicit full range.
	got = next(t, "0 0 12 1-31 * MON", from)
	if want := at(t, utc, "2026-03-09 12:00:00"); !got.Equal(want) {
		t.Fatalf("got %s, want %s — `1-31` must not turn the OR into every day", got, want)
	}
}

func TestSundayIsZeroAndSeven(t *testing.T) {
	utc := time.UTC
	from := at(t, utc, "2026-03-02 00:00:00") // Monday
	a := next(t, "0 0 0 * * 0", from)
	b := next(t, "0 0 0 * * 7", from)
	c := next(t, "0 0 0 * * SUN", from)
	if !a.Equal(b) || !a.Equal(c) {
		t.Fatalf("0=%s 7=%s SUN=%s — all three must mean Sunday", a, b, c)
	}
	if want := at(t, utc, "2026-03-08 00:00:00"); !a.Equal(want) {
		t.Fatalf("got %s, want %s", a, want)
	}
}

func TestNamedMonthsAndDays(t *testing.T) {
	utc := time.UTC
	byName := next(t, "0 0 0 1 mar *", at(t, utc, "2026-01-15 00:00:00"))
	byNum := next(t, "0 0 0 1 3 *", at(t, utc, "2026-01-15 00:00:00"))
	if !byName.Equal(byNum) {
		t.Fatalf("named %s != numeric %s", byName, byNum)
	}
}

// Spring forward: 2026-03-08, America/New_York skips 02:00:00–02:59:59 local.
// A 02:30 job must fire exactly once, at the instant the gap ends (03:00:00 EDT).
func TestSpringForwardFiresOnceAtGapEnd(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	from := at(t, ny, "2026-03-08 00:00:00")

	got := next(t, "0 30 2 * * *", from)
	want := at(t, ny, "2026-03-08 03:00:00")
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s (gap end)", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}

	// And exactly once: the following fire is the next day, not another instant that day.
	got2 := next(t, "0 30 2 * * *", got)
	want2 := at(t, ny, "2026-03-09 02:30:00")
	if !got2.Equal(want2) {
		t.Fatalf("got %s, want %s", got2.Format(time.RFC3339), want2.Format(time.RFC3339))
	}
}

// Several wall times inside one gap must not collapse into several fires at the same
// instant — the sequence has to stay strictly increasing.
func TestSpringForwardCollapsedWallTimesDoNotRepeat(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	cur := at(t, ny, "2026-03-08 01:59:00")
	seen := map[int64]bool{}
	for i := 0; i < 5; i++ {
		nxt := next(t, "0 0,15,30,45 * * * *", cur)
		if !nxt.After(cur) {
			t.Fatalf("step %d: %s is not after %s", i, nxt, cur)
		}
		if seen[nxt.Unix()] {
			t.Fatalf("step %d: repeated instant %s", i, nxt.Format(time.RFC3339))
		}
		seen[nxt.Unix()] = true
		cur = nxt
	}
	// The first fire after 01:59:00 must be 03:00:00 EDT — 02:00/02:15/02:30/02:45 do not
	// exist, and all of them resolve to the gap end.
	first := next(t, "0 0,15,30,45 * * * *", at(t, ny, "2026-03-08 01:59:00"))
	if want := at(t, ny, "2026-03-08 03:00:00"); !first.Equal(want) {
		t.Fatalf("first = %s, want %s", first.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

// Fall back: 2026-11-01, America/New_York repeats 01:00:00–01:59:59 local.
// A 01:30 job must fire once, on the FIRST occurrence (EDT, UTC-4).
func TestFallBackFiresOnceOnFirstOccurrence(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	from := at(t, ny, "2026-11-01 00:00:00")

	got := next(t, "0 30 1 * * *", from)
	if _, off := got.Zone(); off != -4*3600 {
		t.Fatalf("got %s with offset %d; want the first (EDT, -14400) occurrence",
			got.Format(time.RFC3339), off)
	}
	if want := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}

	// Once only: the second 01:30 of that day is skipped, so the next fire is the following
	// day — 01:30 EST, which is 06:30 UTC now that the offset has moved.
	got2 := next(t, "0 30 1 * * *", got)
	want2 := time.Date(2026, 11, 2, 6, 30, 0, 0, time.UTC)
	if !got2.Equal(want2) {
		t.Fatalf("got %s, want %s", got2.Format(time.RFC3339), want2.Format(time.RFC3339))
	}
}

// The repeated hour must be emitted once, not twice. Counting fires proves nothing — an
// hourly schedule yields one fire per real hour either way — so this asserts the exact
// instants, where the dropped 06:00 UTC (the second 01:00 local) is visible.
func TestFallBackHourIsNotEmittedTwice(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	cur := at(t, ny, "2026-11-01 00:30:00") // 04:30 UTC, still EDT
	end := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	var got []time.Time
	for {
		nxt := next(t, "0 0 * * * *", cur) // top of every hour
		if !nxt.Before(end) {
			break
		}
		if !nxt.After(cur) {
			t.Fatalf("%s is not after %s", nxt, cur)
		}
		got = append(got, nxt)
		cur = nxt
		if len(got) > 100 {
			t.Fatal("runaway")
		}
	}

	want := []time.Time{
		time.Date(2026, 11, 1, 5, 0, 0, 0, time.UTC), // 01:00 EDT — first occurrence
		// 06:00 UTC is 01:00 EST, the second occurrence, and must NOT appear.
		time.Date(2026, 11, 1, 7, 0, 0, 0, time.UTC), // 02:00 EST
		time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC), // 03:00 EST
	}
	if len(got) != len(want) {
		t.Fatalf("got %d fires %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("fire %d = %s, want %s", i, got[i].Format(time.RFC3339), want[i].Format(time.RFC3339))
		}
	}
}

// A half-hour DST shift must be handled by the same code path as a one-hour shift.
// Australia/Lord_Howe moves by 30 minutes.
func TestHalfHourDSTShift(t *testing.T) {
	lh := mustLoad(t, "Australia/Lord_Howe")
	// Lord Howe springs forward on the first Sunday in October: 02:00 -> 02:30.
	from := at(t, lh, "2026-10-04 00:00:00")
	got := next(t, "0 15 2 * * *", from) // 02:15 does not exist that day

	// Asserting only the hour and minute would pass on 02:30 of any LATER day, which is the
	// ordinary case and proves nothing about the gap. The date is the whole point.
	y, mo, d := got.Date()
	if y != 2026 || mo != time.October || d != 4 {
		t.Fatalf("got %s; the gap-end fire must be on 2026-10-04, not a later day",
			got.Format(time.RFC3339))
	}
	if got.Hour() != 2 || got.Minute() != 30 || got.Second() != 0 {
		t.Fatalf("got %s, want the 02:30:00 gap end", got.Format(time.RFC3339))
	}
	if _, off := got.Zone(); off != 11*3600 {
		t.Fatalf("got %s with offset %d; the gap end is on the far side of the transition (+11)",
			got.Format(time.RFC3339), off)
	}

	// And exactly once: the next fire is the following day's real 02:15.
	nxt := next(t, "0 15 2 * * *", got)
	y, mo, d = nxt.Date()
	if y != 2026 || mo != time.October || d != 5 || nxt.Hour() != 2 || nxt.Minute() != 15 {
		t.Fatalf("next fire = %s, want 2026-10-05 02:15", nxt.Format(time.RFC3339))
	}
}

// The location travels with the argument: the same expression in two zones yields two
// different instants, and each renders to the configured wall time in its own zone.
func TestLocationComesFromArgument(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	mnl := mustLoad(t, "Asia/Manila")

	a := next(t, "0 0 2 * * *", at(t, ny, "2026-06-01 00:00:00"))
	b := next(t, "0 0 2 * * *", at(t, mnl, "2026-06-01 00:00:00"))
	if a.Equal(b) {
		t.Fatal("02:00 in New York and 02:00 in Manila are not the same instant")
	}
	if a.Hour() != 2 || b.Hour() != 2 {
		t.Fatalf("wall clocks %s / %s: both should read 02:00", a.Format(time.RFC3339), b.Format(time.RFC3339))
	}
	if a.Location() != ny || b.Location() != mnl {
		t.Fatalf("locations %v / %v not carried through", a.Location(), b.Location())
	}
}

// Sub-second input must not produce a fire in the past, and must not skip the next second.
func TestSubSecondInput(t *testing.T) {
	utc := time.UTC
	base := at(t, utc, "2026-03-01 10:00:00")
	from := base.Add(500 * time.Millisecond)
	got := next(t, "* * * * * *", from)
	if want := base.Add(time.Second); !got.Equal(want) {
		t.Fatalf("got %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
	if got.Nanosecond() != 0 {
		t.Fatalf("fire instant %s carries sub-second precision", got.Format(time.RFC3339Nano))
	}
}

// A leap-day schedule must find the next leap year rather than give up.
func TestLeapDay(t *testing.T) {
	utc := time.UTC
	got := next(t, "0 0 0 29 2 *", at(t, utc, "2028-02-29 00:00:00"))
	if want := at(t, utc, "2032-02-29 00:00:00"); !got.Equal(want) {
		t.Fatalf("got %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

// The longest gap any satisfiable expression can have: 2100 is a common year under the
// Gregorian century rule, so a leap-day schedule jumps eight years across it. A search cap
// sized for "surely five years is enough" turns this valid schedule into an error, in 2096.
func TestLeapDayAcrossTheCenturyRule(t *testing.T) {
	utc := time.UTC
	got := next(t, "0 0 0 29 2 *", at(t, utc, "2096-02-29 00:00:00"))
	if want := at(t, utc, "2104-02-29 00:00:00"); !got.Equal(want) {
		t.Fatalf("got %s, want %s — 2100 is not a leap year", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestStringRoundTrips(t *testing.T) {
	const expr = "0 */5 * * * MON-FRI"
	e := MustParse(expr)
	if e.String() != expr {
		t.Fatalf("String() = %q, want %q", e.String(), expr)
	}
}

func BenchmarkNextEveryMinute(b *testing.B) {
	e := MustParse("0 * * * * *")
	t := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _ = e.Next(t)
	}
}

func BenchmarkNextSparse(b *testing.B) {
	e := MustParse("0 0 0 29 2 *")
	t := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _ = e.Next(t)
	}
}
