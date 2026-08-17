package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
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

// A claim's proof of ownership expires in ELAPSED time, and a stale proof must not be
// dispatched on.
//
// This is the one place fencing cannot help. Every write this instance makes is guarded, so a
// stale instance corrupts nothing — but `Run` is not a write, it is an irreversible external
// side effect. A process frozen past its lease (a stop-the-world pause, a suspended VM, a
// host migration) resumes holding a claim another instance has already recovered, reconciled
// and re-dispatched. Its control fence is fine — its last registry read was seconds ago by
// the clock it kept — so nothing else stops it handing the same work to an executor. The
// Accept that follows is correctly refused, long after two handlers are running.
func TestHoldRemainingGatesDispatch(t *testing.T) {
	e := &Engine{tracked: map[int64]held{}}
	h := store.Holder{ExecutionID: 7, FenceEpoch: 3, ExecutionKey: "job:1"}

	const lease = 50 * time.Millisecond
	e.track(h)
	if e.holdRemaining(h.ExecutionID, h.FenceEpoch, lease) <= 0 {
		t.Fatal("a claim made a moment ago is not considered fresh")
	}

	// An execution this instance does not track, or tracks at another epoch, is never fresh:
	// both mean the proof belongs to somebody else.
	if e.holdRemaining(999, h.FenceEpoch, lease) > 0 {
		t.Error("an untracked execution reported a live hold")
	}
	if e.holdRemaining(h.ExecutionID, h.FenceEpoch+1, lease) > 0 {
		t.Error("a hold at a different epoch reported live; that entry belongs to another attempt")
	}

	// Past the lease, with no renewal in between.
	time.Sleep(lease)
	if e.holdRemaining(h.ExecutionID, h.FenceEpoch, lease) > 0 {
		t.Fatal("a claim older than its lease is still considered dispatchable; a frozen " +
			"process would hand out work another instance has already recovered")
	}

	// A successful renewal is what makes it fresh again — that is the heartbeat's other job.
	e.confirmHold(h.ExecutionID, h.FenceEpoch)
	if e.holdRemaining(h.ExecutionID, h.FenceEpoch, lease) <= 0 {
		t.Fatal("a renewed lease did not restore the hold; a slow routing decision would be " +
			"refused even while its lease is being renewed normally")
	}

	// And a renewal for a DIFFERENT epoch must not refresh this entry.
	time.Sleep(lease)
	e.confirmHold(h.ExecutionID, h.FenceEpoch+1)
	if e.holdRemaining(h.ExecutionID, h.FenceEpoch, lease) > 0 {
		t.Fatal("a renewal at another epoch refreshed this hold")
	}
}

// The heartbeat's two guards and its one obligation, read off the source.
//
// This is a source-reading test because nothing else can reach the seam. The renewal path
// needs a live store, and the gate's dependence on it only shows up when a routing decision
// outlives four fifths of a lease — a case no functional test provokes. So the one thing that
// would make the gate wrong stays invisible: a gate refreshed only at claim time passes every
// test in this repository and then refuses healthy work in production.
//
// It exists because that is exactly what happened. The refresh was written into a patch aimed
// at the wrong file, applied to nothing, and shipped in a commit that described it — leaving
// holdRemaining measuring from the claim alone.
func TestHeartbeatGuardsAndObligations(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	calls := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Name.Name != "heartbeatPass" || fd.Body == nil {
					continue
				}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
							calls[sel.Sel.Name] = true
						}
					}
					return true
				})
			}
		}
	}
	if !calls["confirmHold"] {
		t.Error("heartbeatPass does not call confirmHold; the dispatch gate would then measure " +
			"only from the claim, and a routing decision slower than four fifths of a lease " +
			"would be refused while that lease is being renewed normally")
	}
	if !calls["renewing"] {
		t.Error("heartbeatPass does not guard on renewing(); without it the heartbeat has no " +
			"control-fence check at all, and a fenced instance would keep an owner alive that " +
			"the control plane has already written off")
	}
	if calls["stopping"] {
		t.Error("heartbeatPass guards on stopping(), which is also true during a DRAIN. " +
			"Renewal must continue through a drain: a legal ten-second lease otherwise expires " +
			"inside the fifteen-second drain window, the release refuses it as recovery's " +
			"business, and every recovery pass for that tenant has already stopped")
	}
}

// Draining must not stop lease renewal.
//
// StopClaiming and Stop are deliberately different: the first halts acquisition and leaves
// the heartbeat running, because leases of what is already in flight have to stay live or the
// owner loses the right to release its own work — and with every engine for that tenant
// retiring, nothing is left to recover it. Reading the claim signal in the heartbeat collapsed
// the two, so a drain silently stopped renewing: a legal ten-second lease expired inside the
// fifteen-second drain, ReleaseAsOwner then refused the row because expired work belongs to a
// recovery pass that had already stopped, and the job stayed held for ever — taking quiescence,
// and the DSN cutover it gates, with it.
func TestDrainingStopsClaimingButNotRenewing(t *testing.T) {
	e := &Engine{
		tracked:   map[int64]held{},
		stopClaim: make(chan struct{}),
		stop:      make(chan struct{}),
		fence:     alwaysHealthy{},
	}

	if e.stopping() || !e.renewing() {
		t.Fatal("a running engine should be both claiming and renewing")
	}

	e.StopClaiming()
	if !e.stopping() {
		t.Fatal("StopClaiming did not stop claiming")
	}
	if !e.renewing() {
		t.Fatal("StopClaiming stopped renewal; the drain cannot then release its own work, " +
			"and nothing else is left to recover it")
	}

	e.Stop()
	if e.renewing() {
		t.Fatal("Stop did not stop renewal")
	}
}

type alwaysHealthy struct{}

func (alwaysHealthy) Check() error  { return nil }
func (alwaysHealthy) Healthy() bool { return true }
