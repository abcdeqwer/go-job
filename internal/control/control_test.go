package control

import (
	"errors"
	"strings"
	"testing"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

func TestMaskedDSN(t *testing.T) {
	cases := []struct{ in, want string }{
		{"gojob:s3cr3t@tcp(db:3306)/np_scheduler", "gojob:***@tcp(db:3306)/np_scheduler"},
		{"gojob@tcp(db:3306)/np", "gojob:***@tcp(db:3306)/np"},
		// A password containing an @ must not shorten the mask: the LAST @ separates userinfo.
		{"gojob:p@ss@tcp(db:3306)/np", "gojob:***@tcp(db:3306)/np"},
		{"nonsense", "(unparseable)"},
	}
	for _, c := range cases {
		if got := MaskedDSN(c.in); got != c.want {
			t.Errorf("MaskedDSN(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// A masked DSN must never contain the password, whatever shape it has. This is the property,
// not the exact formatting.
func TestMaskedDSNNeverLeaksThePassword(t *testing.T) {
	for _, dsn := range []string{
		"u:hunter2@tcp(h:3306)/s",
		"u:p@ss:w0rd@tcp(h:3306)/s",
		"user:@tcp(h:3306)/s",
	} {
		masked := MaskedDSN(dsn)
		for _, secret := range []string{"hunter2", "p@ss:w0rd"} {
			if strings.Contains(dsn, secret) && strings.Contains(masked, secret) {
				t.Errorf("MaskedDSN(%q) = %q, which still contains the password", dsn, masked)
			}
		}
	}
}

// An instance that has never read the registry is fenced, not trusted. Starting unfenced
// would let a process that cannot reach the control database begin claiming immediately,
// which is the exact window a DSN cutover has to rule out.
func TestFenceStartsFenced(t *testing.T) {
	f := NewFence(30 * time.Second)

	if f.Healthy() {
		t.Fatal("a fence that has never been refreshed reports healthy")
	}
	if err := f.Check(); !errors.Is(err, gojob.ErrControlStale) {
		t.Fatalf("Check() = %v, want ErrControlStale", err)
	}
}

// The fence expires on ELAPSED time and recovers on a refresh.
//
// Real durations, deliberately short. The staleness of a registry read is a liveness
// question, so it is answered in monotonic time — which means it cannot be driven by a
// fakeable clock, and a test that drove one would be exercising something the fence no longer
// consults.
func TestFenceExpiresAndRecovers(t *testing.T) {
	const limit = 40 * time.Millisecond
	f := NewFence(limit)

	f.Refresh()
	if err := f.Check(); err != nil {
		t.Fatalf("immediately after a refresh: %v", err)
	}

	time.Sleep(3 * limit)
	if err := f.Check(); !errors.Is(err, gojob.ErrControlStale) {
		t.Fatalf("%s after a refresh with a %s limit: %v, want ErrControlStale", 3*limit, limit, err)
	}
	if f.Healthy() {
		t.Fatal("a stale fence reports healthy; readiness must drop so callbacks stop arriving")
	}

	// The control database comes back.
	f.Refresh()
	if err := f.Check(); err != nil {
		t.Fatalf("after the registry became readable again: %v", err)
	}
}

// A backward step in business time must not un-expire the fence.
//
// This is the failure the monotonic reading exists to prevent. An instance that loses the
// control database while its host clock steps backward would, measured on the wall clock, see
// a small or even negative age and keep claiming and renewing past the staleness limit —
// while the control plane, whose observation of it has expired, concludes it is gone and lets
// a DSN cutover proceed. The old schema and the new one then dispatch the same job.
func TestFenceIgnoresBusinessClockSteps(t *testing.T) {
	const limit = 40 * time.Millisecond
	clock := gojob.NewFixedClock(time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), time.UTC)

	f := NewFence(limit)
	f.Refresh()

	// The business clock jumps an hour backward, exactly as a corrected host clock would.
	clock.Set(clock.Now().Add(-time.Hour))

	time.Sleep(3 * limit)
	if err := f.Check(); !errors.Is(err, gojob.ErrControlStale) {
		t.Fatalf("the fence did not expire after a backward business-clock step: %v", err)
	}
}
