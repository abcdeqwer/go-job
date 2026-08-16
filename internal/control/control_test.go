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
	clock := gojob.NewFixedClock(time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), time.UTC)
	f := NewFence(clock, 30*time.Second)

	if f.Healthy() {
		t.Fatal("a fence that has never been refreshed reports healthy")
	}
	if err := f.Check(); !errors.Is(err, gojob.ErrControlStale) {
		t.Fatalf("Check() = %v, want ErrControlStale", err)
	}
}

func TestFenceExpiresAndRecovers(t *testing.T) {
	start := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	clock := gojob.NewFixedClock(start, time.UTC)
	f := NewFence(clock, 30*time.Second)

	f.Refresh()
	if err := f.Check(); err != nil {
		t.Fatalf("immediately after a refresh: %v", err)
	}

	// Inside the limit.
	clock.Set(start.Add(29 * time.Second))
	if err := f.Check(); err != nil {
		t.Fatalf("29s after a refresh with a 30s limit: %v", err)
	}

	// Past it.
	clock.Set(start.Add(31 * time.Second))
	if err := f.Check(); !errors.Is(err, gojob.ErrControlStale) {
		t.Fatalf("31s after a refresh with a 30s limit: %v, want ErrControlStale", err)
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
