package engine

import (
	"testing"
	"time"

	"github.com/abcdeqwer/go-job/internal/store"
)

// Backoff must be bounded and must never be zero. A zero backoff makes a rejected candidate
// due again immediately, which is the spin the backoff exists to prevent.
func TestBackoffIsBoundedAndPositive(t *testing.T) {
	e := &Engine{cfg: Config{BackoffBase: 5 * time.Second, BackoffMax: time.Minute}}

	prev := 0
	for attempt := 0; attempt < 20; attempt++ {
		got := e.backoff(attempt)
		if got < 1 {
			t.Fatalf("backoff(%d) = %d; a zero backoff makes the candidate due again immediately", attempt, got)
		}
		// Jitter is 25% of the base, so the ceiling is max plus that.
		if maxAllowed := 60 + 15; got > maxAllowed {
			t.Fatalf("backoff(%d) = %ds, above the %ds bound", attempt, got, maxAllowed)
		}
		if attempt > 0 && got < prev/4 {
			t.Fatalf("backoff(%d) = %ds collapsed from %ds; the sequence must be monotonic up to jitter",
				attempt, got, prev)
		}
		prev = got
	}
}

// An unconfigured engine must still produce a usable backoff rather than zero.
func TestBackoffDefaultsWhenUnconfigured(t *testing.T) {
	e := &Engine{}
	if got := e.backoff(0); got < 1 {
		t.Fatalf("backoff with no configuration = %d, want at least 1s", got)
	}
}

// Routing prefers headroom, and an executor that reports no capacity at all is still
// routable: treating a missing capacity as zero would make a fleet that never reports one
// permanently unusable, when the executor's own refusal already handles overload.
func TestHeadroom(t *testing.T) {
	cases := []struct {
		name string
		in   store.Executor
		want int
	}{
		{"idle", store.Executor{Capacity: 8, Running: 0}, 8},
		{"half", store.Executor{Capacity: 8, Running: 4}, 4},
		{"full", store.Executor{Capacity: 8, Running: 8}, 0},
		{"over", store.Executor{Capacity: 8, Running: 10}, -2},
		{"unreported capacity is one slot", store.Executor{Capacity: 0, Running: 0}, 1},
		{"unreported capacity, busy", store.Executor{Capacity: 0, Running: 1}, 0},
	}
	for _, c := range cases {
		if got := headroom(c.in); got != c.want {
			t.Errorf("%s: headroom = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestGroupSuffix(t *testing.T) {
	if got := groupSuffix(""); got != "" {
		t.Errorf("groupSuffix(\"\") = %q, want empty", got)
	}
	if got := groupSuffix("canary"); got != " in group canary" {
		t.Errorf("groupSuffix(canary) = %q", got)
	}
}

func TestDecodeParams(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantLen int
		wantErr bool
	}{
		{"absent", "", 0, false},
		{"null", "null", 0, false},
		{"empty object", "{}", 0, false},
		{"object", `{"a":1,"b":"x"}`, 2, false},
		{"array is not a parameter set", `[1,2]`, 0, true},
		{"malformed", `{`, 0, true},
	}
	for _, c := range cases {
		got, err := decodeParams([]byte(c.in))
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected an error", c.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got == nil {
			t.Errorf("%s: returned a nil map; the dispatch path would send no params block", c.name)
			continue
		}
		if len(got) != c.wantLen {
			t.Errorf("%s: %d params, want %d", c.name, len(got), c.wantLen)
		}
	}
}

// jitter must stay inside its interval and must tolerate a zero one.
func TestJitter(t *testing.T) {
	if got := jitter(0); got != 0 {
		t.Fatalf("jitter(0) = %s, want 0", got)
	}
	if got := jitter(-time.Second); got != 0 {
		t.Fatalf("jitter(negative) = %s, want 0", got)
	}
	for i := 0; i < 1000; i++ {
		got := jitter(time.Second)
		if got < 0 || got >= time.Second {
			t.Fatalf("jitter(1s) = %s, outside [0, 1s)", got)
		}
	}
}
