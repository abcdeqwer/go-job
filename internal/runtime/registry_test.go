package runtime

import (
	"context"
	"testing"
	"time"
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
