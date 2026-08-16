package runtime

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/abcdeqwer/go-job/internal/dispatch"
)

// The release of held work must not inherit the drain's expiry.
//
// Retirement drains for a bounded time and then releases whatever is still held. Those two
// bounds were once one context, which made the release a no-op in precisely the case it
// exists for: the drain ends either because the tenant went quiet — in which case there is
// nothing to release — or because the bound elapsed, and then the shared context is already
// cancelled and the release fails on its first query.
//
// What is stranded then is not recoverable by anybody. Every replica retires a disabled
// tenant, so no engine and no pool remain: job_state.active_kind stays set for ever, the
// schema never becomes quiescent, and the DSN cutover that quiescence gates can never run.
func TestReleaseContextSurvivesAnExpiredDrain(t *testing.T) {
	r := &Registry{opts: Options{DrainTimeout: time.Minute}}

	// The parent is cancelled, standing in for both cases that produced the bug: a drain
	// whose bound elapsed, and a process shutdown that cancelled the registry's context.
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	ctx, done := r.releaseContext(parent)
	defer done()

	if err := ctx.Err(); err != nil {
		t.Fatalf("the release context was born cancelled (%v); every query it makes fails "+
			"immediately and the tenant's held work is stranded", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("the release context has no deadline; retirement would be unbounded")
	}
	if until := time.Until(deadline); until <= 0 || until > time.Minute {
		t.Fatalf("release deadline is %s away, want (0, 1m]", until)
	}
}

// Stop must close the door on admissions still in flight.
//
// Admission runs off the poll loop, so it can be most of the way through — pool opened,
// schema verified — when Stop runs. Untracked and unfenced, it then publishes its tenant into
// the map Stop has just emptied and starts its engine: a registry that reports itself stopped
// goes on claiming and dispatching, against a dispatch client Stop has already closed, with a
// database pool nothing will ever close.
//
// Two things prevent it, and both are needed. The wait makes Stop's snapshot of the tenant map
// come after every admission has finished; the flag makes an admission that finishes anyway
// refuse to publish instead of racing the snapshot.
func TestStopFencesAdmissionsInFlight(t *testing.T) {
	r := &Registry{
		tenants:   map[string]*tenant{},
		admitting: map[string]bool{},
		seen:      map[string]int64{},
		stop:      make(chan struct{}),
		disp:      dispatch.NewClient(time.Second, time.Second, dispatch.Credentials{}),
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// An admission in flight: the goroutine is registered exactly as reconcile registers one.
	started, release := make(chan struct{}), make(chan struct{})
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		close(started)
		<-release
	}()
	<-started

	stopped := make(chan struct{})
	go func() { r.Stop(); close(stopped) }()

	// Stop must not have finished while the admission is outstanding.
	select {
	case <-stopped:
		t.Fatal("Stop returned while an admission was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	// It must already be refusing publications, though — that is what an admission past its
	// schema check reads before installing an engine.
	r.mu.Lock()
	fenced := r.stopped
	r.mu.Unlock()
	if !fenced {
		t.Fatal("Stop had not yet fenced publication; an admission finishing here would " +
			"install an engine into a registry that is shutting down")
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the admission finished")
	}
}

// Every goroutine this package starts must go through goTracked.
//
// Stop's wait is worth exactly as much as the registration behind it, and the registration is
// the part that is easy to leave out — a bare `go func` compiles, passes every test, and only
// shows itself as a pool that outlives shutdown or an engine that starts after it. A test that
// spawns its own stand-in goroutine cannot catch that, because the thing it would be checking
// is whether the REAL call site registered, so this reads the source.
func TestEveryGoroutineIsTracked(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil || fd.Name.Name == "goTracked" {
					continue // goTracked is the one place a `go` statement belongs
				}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if g, ok := n.(*ast.GoStmt); ok {
						t.Errorf("%s: %s starts a goroutine directly; use goTracked, or Stop "+
							"will return while this work is still running",
							fset.Position(g.Pos()), fd.Name.Name)
					}
					return true
				})
			}
		}
	}
}
