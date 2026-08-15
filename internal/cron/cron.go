// Package cron implements the six-field dialect specified in doc/scheduling.md §1.
//
//	second  minute  hour  day-of-month  month  day-of-week
//
// with steps (*/15), ranges (1-5), lists (1,3,5) and named weekdays and months.
//
// The dialect is fixed rather than pluggable. A scheduler with two cron dialects has two
// sets of edge cases, and the cases where they differ are exactly the ones nobody tests.
//
// Syntax is the easy half. The rules below are the ones two conforming implementations
// would otherwise disagree about, which is why they are settled here rather than left to
// whoever writes the code:
//
//   - day-of-month AND day-of-week both restricted: OR, as Vixie cron and Quartz do.
//   - a field is "restricted" if it excludes at least one value it could match, so `*/1`
//     and `1-31` count as unrestricted and do not silently widen the OR to every day.
//   - day-of-week: 0 and 7 both mean Sunday.
//   - L, W, # and ? are NOT supported, and are rejected rather than silently ignored.
//   - a field combination that can never match is rejected at parse rather than never
//     firing.
//   - nonexistent local time (spring forward): fire once, at the instant the gap ends.
//   - ambiguous local time (fall back): fire at the FIRST occurrence only.
//
// The two DST rules exist because business timestamps are stored without an offset. During
// a fall-back hour two real instants share one wall time, so an execution key derived from
// wall time would collide and the second occurrence would be silently deduplicated. Firing
// once makes that deliberate rather than accidental.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	idxSecond = iota
	idxMinute
	idxHour
	idxDOM
	idxMonth
	idxDOW
	numFields
)

var fieldNames = [numFields]string{"second", "minute", "hour", "day-of-month", "month", "day-of-week"}
var fieldBounds = [numFields][2]int{{0, 59}, {0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}

var monthNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
	"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

var dayNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
}

// Expression is a parsed cron expression. The zero value is not usable; use Parse.
type Expression struct {
	src string

	second [60]bool
	minute [60]bool
	hour   [24]bool
	dom    [32]bool // 1..31
	month  [13]bool // 1..12
	dow    [7]bool  // 0..6, Sunday = 0

	// domRestricted and dowRestricted drive the OR rule: when both are restricted a day
	// matches if EITHER does, and when one is unrestricted the other alone decides.
	domRestricted bool
	dowRestricted bool
}

func (e *Expression) String() string { return e.src }

// Parse reads a six-field expression.
func Parse(expr string) (*Expression, error) {
	fields := strings.Fields(expr)
	if len(fields) != numFields {
		return nil, fmt.Errorf("cron %q: expected %d fields, got %d", expr, numFields, len(fields))
	}
	for _, f := range fields {
		if strings.ContainsAny(f, "LW#?") {
			return nil, fmt.Errorf("cron %q: L, W, # and ? are not supported", expr)
		}
	}

	e := &Expression{src: expr}
	for _, f := range []struct {
		idx   int
		set   []bool
		names map[string]int
	}{
		{idxSecond, e.second[:], nil},
		{idxMinute, e.minute[:], nil},
		{idxHour, e.hour[:], nil},
		{idxDOM, e.dom[:], nil},
		{idxMonth, e.month[:], monthNames},
	} {
		if err := parseInto(fields[f.idx], f.idx, f.set, f.names); err != nil {
			return nil, err
		}
	}

	dow := make([]bool, 8)
	if err := parseInto(fields[idxDOW], idxDOW, dow, dayNames); err != nil {
		return nil, err
	}
	// 0 and 7 both mean Sunday.
	copy(e.dow[:], dow[:7])
	if dow[7] {
		e.dow[0] = true
	}

	// Restricted means "excludes something it could have matched". Deriving this from the
	// parsed set rather than from the literal text is what stops `*/1` in day-of-month from
	// widening the OR to every day — the trap that makes `0 0 12 */1 * MON` fire daily in
	// Vixie cron.
	e.domRestricted = !allSet(e.dom[1:32])
	e.dowRestricted = !allSet(e.dow[:])

	if err := e.rejectUnsatisfiable(); err != nil {
		return nil, err
	}
	return e, nil
}

// MustParse is Parse for expressions known good at compile time.
func MustParse(expr string) *Expression {
	e, err := Parse(expr)
	if err != nil {
		panic(err)
	}
	return e
}

func parseInto(field string, idx int, set []bool, names map[string]int) error {
	lo, hi := fieldBounds[idx][0], fieldBounds[idx][1]
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return fmt.Errorf("cron %s: empty list element", fieldNames[idx])
		}
		step := 1
		if slash := strings.IndexByte(part, '/'); slash >= 0 {
			s, err := strconv.Atoi(part[slash+1:])
			if err != nil || s < 1 {
				return fmt.Errorf("cron %s: bad step %q", fieldNames[idx], part[slash+1:])
			}
			step = s
			part = part[:slash]
		}

		var from, to int
		switch {
		case part == "*":
			from, to = lo, hi
		case strings.ContainsRune(part, '-'):
			bits := strings.SplitN(part, "-", 2)
			var err error
			if from, err = atom(bits[0], idx, names); err != nil {
				return err
			}
			if to, err = atom(bits[1], idx, names); err != nil {
				return err
			}
			if from > to {
				return fmt.Errorf("cron %s: range %q is inverted", fieldNames[idx], part)
			}
		default:
			v, err := atom(part, idx, names)
			if err != nil {
				return err
			}
			from, to = v, v
			if step > 1 {
				// `5/15` means "from 5, every 15" — the same reading as `5-59/15`.
				to = hi
			}
		}
		for v := from; v <= to; v += step {
			set[v] = true
		}
	}
	return nil
}

func atom(s string, idx int, names map[string]int) (int, error) {
	s = strings.TrimSpace(s)
	if names != nil {
		if v, ok := names[strings.ToUpper(s)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("cron %s: %q is not a number or a known name", fieldNames[idx], s)
	}
	lo, hi := fieldBounds[idx][0], fieldBounds[idx][1]
	if v < lo || v > hi {
		return 0, fmt.Errorf("cron %s: %d is outside %d..%d", fieldNames[idx], v, lo, hi)
	}
	return v, nil
}

// rejectUnsatisfiable refuses expressions that can never fire, rather than accepting them
// and leaving an operator to wonder for a month why a job never ran. `0 0 0 31 2 *` is the
// canonical case: February has no 31st.
func (e *Expression) rejectUnsatisfiable() error {
	if !anySet(e.second[:]) || !anySet(e.minute[:]) || !anySet(e.hour[:]) || !anySet(e.month[:]) {
		return fmt.Errorf("cron %q: a field matches nothing", e.src)
	}
	if !anySet(e.dom[1:32]) && !anySet(e.dow[:]) {
		return fmt.Errorf("cron %q: no day can ever match", e.src)
	}
	// A restricted day-of-week rescues an impossible day-of-month, because the two are ORed.
	if !e.domRestricted || (e.dowRestricted && anySet(e.dow[:])) {
		return nil
	}
	for m := 1; m <= 12; m++ {
		if !e.month[m] {
			continue
		}
		for d := 1; d <= daysIn(m, 2024); d++ { // 2024 is a leap year: the permissive case
			if e.dom[d] {
				return nil
			}
		}
	}
	return fmt.Errorf("cron %q: no day-of-month in the selected months can ever match", e.src)
}

func anySet(s []bool) bool {
	for _, v := range s {
		if v {
			return true
		}
	}
	return false
}

func allSet(s []bool) bool {
	for _, v := range s {
		if !v {
			return false
		}
	}
	return true
}

func daysIn(month, year int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// matchesDay applies the OR rule.
func (e *Expression) matchesDay(year, month, day int) bool {
	// Weekday is a property of the calendar date, not of any zone, so compute it in UTC
	// rather than materializing an instant we may end up discarding.
	wd := int(time.Date(year, time.Month(month), day, 12, 0, 0, 0, time.UTC).Weekday())
	dom := e.dom[day]
	dow := e.dow[wd]
	switch {
	case e.domRestricted && e.dowRestricted:
		return dom || dow
	case e.domRestricted:
		return dom
	case e.dowRestricted:
		return dow
	default:
		return true
	}
}

// searchLimit bounds Next so an expression that somehow matches nothing returns an error
// instead of spinning. Parse-time rejection should mean it is never reached.
//
// Twelve years, not five. The largest gap a SATISFIABLE expression can have is eight years:
// `0 0 0 29 2 *` fires on 2096-02-29 and then not until 2104-02-29, because 2100 is a common
// year under the Gregorian century rule. A five-year cap turns that valid schedule into an
// error — and only in 2096, which is exactly the kind of bound nobody tests.
//
// The cost of the larger limit is nothing in practice: the search walks calendar fields, so a
// year that matches no month advances twelve times, not thirty-one million.
const searchLimit = 12 * 366 * 24 * time.Hour

// Next returns the first fire instant strictly after `after`, in `after`'s location.
//
// The search walks calendar fields rather than adding real durations, so the field match is
// decided in wall-clock terms and the zone is applied once, at the end. That ordering is
// what makes the two DST rules expressible at all: a wall time can match the expression and
// still not exist.
func (e *Expression) Next(after time.Time) (time.Time, error) {
	loc := after.Location()
	start := after.Add(time.Second - time.Duration(after.Nanosecond()))
	limit := civilOf(after.Add(searchLimit))

	t := civilOf(start)
	for {
		if t.after(limit) {
			return time.Time{}, fmt.Errorf("cron %q: no fire instant within %s of %s",
				e.src, searchLimit, after.Format(time.RFC3339))
		}
		switch {
		case !e.month[t.month]:
			t = t.nextMonth()
		case !e.matchesDay(t.year, t.month, t.day):
			t = t.nextDay()
		case !e.hour[t.hour]:
			t = t.nextHour()
		case !e.minute[t.min]:
			t = t.nextMinute()
		case !e.second[t.sec]:
			t = t.nextSecond()
		default:
			if got := resolve(t, loc); got.After(after) {
				return got, nil
			}
			// Only reachable when a resolved wall time lands at or before `after` — a
			// repeated hour, or a gap collapsing several wall times onto one instant.
			// Advance the calendar cursor and keep looking; never recurse.
			t = t.nextSecond()
		}
	}
}

// resolve turns a matched wall-clock value into a real instant, applying the two DST rules.
//
// A wall time may correspond to two instants (fall back), one (the ordinary case), or none
// (spring forward). Go's time.Date normalises forward for the third case, and its choice in
// the ambiguous case is explicitly documented as not guaranteed — so neither is relied on.
func resolve(c civil, loc *time.Location) time.Time {
	naive := c.utc()

	// Any transition bracketing this wall time leaves the offset a day earlier and the
	// offset a day later as the only two candidates. Interpreting the wall time under each
	// and keeping those that render back to it distinguishes all three cases.
	_, offBefore := naive.Add(-24 * time.Hour).In(loc).Zone()
	_, offAfter := naive.Add(24 * time.Hour).In(loc).Zone()

	var best time.Time
	found := false
	for _, off := range [2]int{offBefore, offAfter} {
		cand := naive.Add(-time.Duration(off) * time.Second)
		if c.equalsWall(cand.In(loc)) && (!found || cand.Before(best)) {
			best, found = cand, true
		}
	}
	if found {
		// One or two candidates: the earliest is the answer. For an ambiguous wall time that
		// is the first occurrence, which is the documented rule.
		return best.In(loc)
	}
	// No candidate: the wall time does not exist. Fire at the instant the gap ends.
	return gapEnd(c, loc)
}

// gapEnd finds the first instant whose wall clock is at or after c, when c itself falls
// inside a spring-forward gap. Within the search window the wall clock is monotonic — it
// jumps but never goes backwards — so a bisection is exact. Go exposes no transition table,
// and a second-granularity bisection over four days costs about twenty zone lookups.
//
// The bisection runs over integer Unix seconds rather than time.Time, so it converges on
// the transition instant itself rather than merely within a second of it. An off-by-a-
// fraction here would put a sub-second component on a fire instant and make it unequal to
// the same wall time computed anywhere else.
func gapEnd(c civil, loc *time.Location) time.Time {
	naive := c.utc().Unix()
	lo := naive - 48*3600 // renders before c under any real offset
	hi := naive + 48*3600 // renders at or after c under any real offset
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if c.afterWall(time.Unix(mid, 0).In(loc)) {
			lo = mid
		} else {
			hi = mid
		}
	}
	return time.Unix(hi, 0).In(loc)
}

// civil is a wall-clock value with no zone, so the search can walk calendar fields without
// real durations getting in the way.
type civil struct {
	year, month, day, hour, min, sec int
}

func civilOf(t time.Time) civil {
	y, mo, d := t.Date()
	hh, mm, ss := t.Clock()
	return civil{y, int(mo), d, hh, mm, ss}
}

func (c civil) utc() time.Time {
	return time.Date(c.year, time.Month(c.month), c.day, c.hour, c.min, c.sec, 0, time.UTC)
}

func (c civil) equalsWall(t time.Time) bool { return c == civilOf(t) }

// afterWall reports whether c is strictly later on the wall clock than t.
func (c civil) afterWall(t time.Time) bool { return c.after(civilOf(t)) }

func (c civil) after(o civil) bool {
	if c.year != o.year {
		return c.year > o.year
	}
	if c.month != o.month {
		return c.month > o.month
	}
	if c.day != o.day {
		return c.day > o.day
	}
	if c.hour != o.hour {
		return c.hour > o.hour
	}
	if c.min != o.min {
		return c.min > o.min
	}
	return c.sec > o.sec
}

func (c civil) nextSecond() civil {
	c.sec++
	if c.sec > 59 {
		c.sec = 0
		return c.rollMinute()
	}
	return c
}

func (c civil) nextMinute() civil {
	c.sec = 0
	return c.rollMinute()
}

func (c civil) rollMinute() civil {
	c.min++
	if c.min > 59 {
		c.min = 0
		return c.rollHour()
	}
	return c
}

func (c civil) nextHour() civil {
	c.sec, c.min = 0, 0
	return c.rollHour()
}

func (c civil) rollHour() civil {
	c.hour++
	if c.hour > 23 {
		c.hour = 0
		return c.rollDay()
	}
	return c
}

func (c civil) nextDay() civil {
	c.sec, c.min, c.hour = 0, 0, 0
	return c.rollDay()
}

func (c civil) rollDay() civil {
	c.day++
	if c.day > daysIn(c.month, c.year) {
		c.day = 1
		c.month++
		if c.month > 12 {
			c.month = 1
			c.year++
		}
	}
	return c
}

func (c civil) nextMonth() civil {
	c.sec, c.min, c.hour, c.day = 0, 0, 0, 1
	c.month++
	if c.month > 12 {
		c.month = 1
		c.year++
	}
	return c
}
