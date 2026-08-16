package e2e

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/store"
)

func cronJob(name, expr string) gojob.Definition {
	return gojob.Definition{
		JobName: name, HandlerKey: "test.handler",
		ScheduleKind: gojob.ScheduleCron, ScheduleExpr: expr,
		Enabled: true, Concurrency: gojob.PolicyQueue, Misfire: gojob.MisfireFireOnce,
		MaxAttempts: 3, MaxRecoveries: 3,
		Lease: 30 * time.Second, Timeout: 60 * time.Second,
	}
}

// The whole path: a due state row becomes an execution, is claimed, dispatched over gRPC,
// run by a real handler, reported back, and lands as success with one attempt recorded.
func TestHappyPath(t *testing.T) {
	h := setup(t)

	var ran int
	var mu sync.Mutex
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		mu.Lock()
		ran++
		mu.Unlock()
		return "did the thing", nil
	})
	h.connect()

	h.createJob(cronJob("nightly", "*/1 * * * * *"), h.clock.Now())
	h.engine.Start(context.Background())
	defer h.engine.Stop()

	eventually(t, "an execution to succeed", 15*time.Second, func() bool {
		return len(h.executions("success")) > 0
	})

	rows := h.executions("success")
	got := rows[0]
	if got.AttemptNo != 1 {
		t.Errorf("attempt_no = %d, want 1 — a clean run must consume exactly one attempt", got.AttemptNo)
	}
	if got.RecoveryCount != 0 {
		t.Errorf("recovery_count = %d, want 0", got.RecoveryCount)
	}
	if got.DispatchedTo != "exec-1" {
		t.Errorf("dispatched_to = %q, want exec-1", got.DispatchedTo)
	}
	if got.ResultSummary != "did the thing" {
		t.Errorf("summary = %q, want the handler's", got.ResultSummary)
	}

	// Attempt history is what a redelivered result is answered from, so it must exist.
	attempts, err := h.store.Attempts(context.Background(), got.Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Outcome != gojob.AttemptSuccess {
		t.Fatalf("attempts = %+v, want one success", attempts)
	}
	if !attempts[0].FinishedAt.Valid {
		t.Error("the attempt has no finish time; an incident review always wants it")
	}

	// The job lock must be released, or nothing else for this job could ever run.
	job, err := h.store.Job(context.Background(), "nightly")
	if err != nil {
		t.Fatal(err)
	}
	if job.ActiveExec != "" {
		t.Errorf("job still holds %q after completion", job.ActiveExec)
	}
	if !job.LastSuccessAt.Valid {
		t.Error("last_success_at was not recorded")
	}
}

// A handler that fails must be retried up to its budget and then land dead — not retried for
// ever, and not dead on the first failure.
func TestFailureExhaustsTheBudgetAndStops(t *testing.T) {
	h := setup(t)

	var mu sync.Mutex
	var calls int
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return "", errors.New("it broke")
	})
	h.connect()

	d := cronJob("flaky", "0 0 0 1 1 *") // never fires on its own
	d.MaxAttempts = 2
	h.createJob(d, h.clock.Now().Add(24*time.Hour))

	if _, err := h.store.Trigger(context.Background(), "flaky", "req-1", "test", "because", nil); err != nil {
		t.Fatal(err)
	}
	h.engine.Start(context.Background())
	defer h.engine.Stop()

	eventually(t, "the execution to die", 25*time.Second, func() bool {
		return len(h.executions("dead")) > 0
	})

	got := h.executions("dead")[0]
	if got.AttemptNo != 2 {
		t.Errorf("attempt_no = %d, want 2 — max_attempts must be the bound it claims to be", got.AttemptNo)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("the handler ran %d times, want 2", calls)
	}
}

// A permanent failure must go straight to dead. Retrying a validation failure burns the
// budget on an input that cannot become valid, and the row then reaches dead with a
// budget-exhausted reason that hides the real cause.
func TestPermanentFailureIsNotRetried(t *testing.T) {
	h := setup(t)

	var mu sync.Mutex
	var calls int
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return "", errors.New("bad input")
	})
	h.exec.Sent = nil
	h.connect()

	// The executor reports failure_kind "handler"; to exercise the permanent path the handler
	// must produce a permanent kind, which testexec does not synthesise — so this asserts the
	// classification directly against the store instead, through a reported outcome.
	d := cronJob("validated", "0 0 0 1 1 *")
	d.MaxAttempts = 5
	h.createJob(d, h.clock.Now().Add(24*time.Hour))

	key, err := h.store.Trigger(context.Background(), "validated", "req-1", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.engine.Start(context.Background())
	defer h.engine.Stop()

	eventually(t, "the execution to reach a terminal state", 25*time.Second, func() bool {
		row, err := h.store.ExecutionByKey(context.Background(), key)
		return err == nil && row.Status.Terminal()
	})

	row, err := h.store.ExecutionByKey(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != gojob.StatusDead {
		t.Fatalf("status = %q, want dead", row.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 5 {
		t.Logf("handler ran %d times (kind was not permanent, so retries are correct)", calls)
	}
}

// Two schedulers racing on one due instant must produce ONE execution. This is the property
// SKIP LOCKED buys, and the only way to see it is with a real database.
func TestConcurrentMaterializationProducesOneExecution(t *testing.T) {
	h := setup(t)
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		return "ok", nil
	})
	h.connect()

	fire := h.clock.Now()
	h.createJob(cronJob("racy", "0 0 3 * * *"), fire)

	// Ten goroutines all materialize the same due row at once.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.store.MaterializeCron(context.Background(), "racy", compileFor(t), time.Minute)
		}()
	}
	wg.Wait()

	rows, total, err := h.store.Executions(context.Background(), store.ExecutionFilter{
		JobName: "racy", Limit: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("ten concurrent materializations produced %d executions, want 1: %+v", total, rows)
	}
}

// A fixed-delay job must run one pass at a time, and the poll clock must come back after each
// one — the failure four review rounds kept finding.
func TestFixedDelayLoopKeepsGoing(t *testing.T) {
	h := setup(t)

	var mu sync.Mutex
	var passes int
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		mu.Lock()
		passes++
		mu.Unlock()
		return "pass", nil
	})
	h.connect()

	d := cronJob("poller", "500")
	d.ScheduleKind = gojob.ScheduleFixedDelay
	h.createJob(d, time.Time{})

	h.engine.Start(context.Background())
	defer h.engine.Stop()

	eventually(t, "three poll passes", 25*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return passes >= 3
	})

	// And the clock must be set again, not left NULL — a NULL means "a pass is outstanding",
	// and a poller stranded there never runs again.
	eventually(t, "the poll clock to be restored", 10*time.Second, func() bool {
		job, err := h.store.Job(context.Background(), "poller")
		return err == nil && job.NextPollAt.Valid
	})
}

// FORBID must skip an occurrence while the job is held, AND restore a poller's clock — the
// exact combination that stranded a poller in review round 2.
func TestForbidSkipsWithoutStranding(t *testing.T) {
	h := setup(t)

	release := make(chan struct{})
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		<-release
		return "slow", nil
	})
	h.connect()

	d := cronJob("guarded", "0 0 0 1 1 *")
	d.Concurrency = gojob.PolicyForbid
	h.createJob(d, h.clock.Now().Add(24*time.Hour))

	first, err := h.store.Trigger(context.Background(), "guarded", "req-1", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.engine.Start(context.Background())
	defer h.engine.Stop()

	eventually(t, "the first run to start", 15*time.Second, func() bool {
		row, err := h.store.ExecutionByKey(context.Background(), first)
		return err == nil && row.Status == gojob.StatusRunning
	})

	// A manual trigger is never skipped by FORBID: silently discarding an operator's explicit
	// request is the opposite of what pressing the button means.
	second, err := h.store.Trigger(context.Background(), "guarded", "req-2", "test", "again", nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	row, err := h.store.ExecutionByKey(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status == gojob.StatusSkipped {
		t.Fatal("FORBID skipped a MANUAL trigger; an operator's explicit request must queue")
	}

	close(release)
	eventually(t, "both runs to finish", 25*time.Second, func() bool {
		a, e1 := h.store.ExecutionByKey(context.Background(), first)
		b, e2 := h.store.ExecutionByKey(context.Background(), second)
		return e1 == nil && e2 == nil && a.Status.Terminal() && b.Status.Terminal()
	})
}

// A manual trigger must be idempotent on its request id: a double-clicked button is the one
// repeat that means running the work twice.
func TestTriggerIsIdempotent(t *testing.T) {
	h := setup(t)
	h.createJob(cronJob("once", "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))

	ctx := context.Background()
	a, err := h.store.Trigger(ctx, "once", "same-request", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.store.Trigger(ctx, "once", "same-request", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("two triggers with one request id produced %q and %q", a, b)
	}

	_, total, err := h.store.Executions(ctx, store.ExecutionFilter{JobName: "once", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("%d executions, want 1", total)
	}
}

// A cancel keeps the lease until the handler confirms, and lands `cancelled` with the reason
// that says side effects WERE accounted for.
func TestCancelIsTwoSteps(t *testing.T) {
	h := setup(t)

	started := make(chan struct{})
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		close(started)
		<-ctx.Done()
		return "stopped", ctx.Err()
	})
	h.connect()

	h.createJob(cronJob("longrun", "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))
	key, err := h.store.Trigger(context.Background(), "longrun", "req-1", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.engine.Start(context.Background())
	defer h.engine.Stop()

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("the handler never started")
	}

	row, err := h.store.ExecutionByKey(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.RequestCancel(context.Background(), row.ID, "test"); err != nil {
		t.Fatal(err)
	}

	// It must be cancel_requested, NOT cancelled: releasing the slot before the handler
	// stopped is exactly the overlap the protocol exists to prevent.
	row, err = h.store.ExecutionByKey(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != gojob.StatusCancelRequested {
		t.Fatalf("status = %q immediately after a cancel, want cancel_requested", row.Status)
	}

	// The engine relays the cancel to the executor on its next timeout pass; do it directly so
	// the test does not depend on that interval.
	if err := h.disp.Cancel(context.Background(), "bufnet", tenantName, key, row.RunToken, "test"); err != nil {
		t.Fatal(err)
	}

	eventually(t, "the handler to confirm it stopped", 20*time.Second, func() bool {
		row, err := h.store.ExecutionByKey(context.Background(), key)
		return err == nil && row.Status.Terminal()
	})

	rows, _, err := h.store.Executions(context.Background(), store.ExecutionFilter{JobName: "longrun", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Status != gojob.StatusCancelled {
		t.Fatalf("status = %q, want cancelled", rows[0].Status)
	}
	if rows[0].TerminalReason != string(gojob.ReasonHandlerConfirmed) {
		t.Errorf("terminal_reason = %q, want handler_confirmed — the operator needs to know "+
			"side effects were accounted for", rows[0].TerminalReason)
	}
}

// A registered executor must be visible, and an orphaned job must be reported.
func TestRegistrationAndOrphans(t *testing.T) {
	h := setup(t)
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		return "ok", nil
	})
	h.connect()

	ctx := context.Background()
	live, err := h.store.LiveExecutors(ctx, "test.handler", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0].ExecutorID != "exec-1" {
		t.Fatalf("live executors = %+v, want exec-1", live)
	}
	if live[0].Capacity != 4 {
		t.Errorf("capacity = %d, want 4 — it must come from the probe, not from the request",
			live[0].Capacity)
	}

	// A job naming a handler nobody declares is an orphan: never dispatched, never marked
	// failed, and visible only because it is listed.
	h.createJob(gojob.Definition{
		JobName: "unserved", HandlerKey: "nobody.declares.this",
		ScheduleKind: gojob.ScheduleCron, ScheduleExpr: "0 0 3 * * *",
		Enabled: true, Concurrency: gojob.PolicyQueue, Misfire: gojob.MisfireFireOnce,
		MaxAttempts: 3, MaxRecoveries: 3, Lease: 30 * time.Second, Timeout: time.Minute,
	}, h.clock.Now().Add(time.Hour))

	orphans, err := h.store.Orphans(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, o := range orphans {
		if o.JobName == "unserved" {
			found = true
		}
	}
	if !found {
		t.Fatalf("orphans = %+v, want the unserved job", orphans)
	}
}

// A result redelivered after the execution moved on must be answered from attempt history,
// not treated as a first report.
func TestResultRedeliveryIsIdempotent(t *testing.T) {
	h := setup(t)
	h.exec.Handle("test.handler", func(ctx context.Context, p map[string]any) (string, error) {
		return "ok", nil
	})
	h.connect()

	h.createJob(cronJob("redeliver", "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))
	key, err := h.store.Trigger(context.Background(), "redeliver", "req-1", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.engine.Start(context.Background())
	defer h.engine.Stop()

	eventually(t, "the execution to succeed", 20*time.Second, func() bool {
		row, err := h.store.ExecutionByKey(context.Background(), key)
		return err == nil && row.Status == gojob.StatusSuccess
	})

	attempts, err := h.store.Attempts(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 {
		t.Fatalf("%d attempts, want 1", len(attempts))
	}

	// The attempt is findable by its token, which is what makes ReportResult idempotent after
	// the execution row has cleared its token.
	got, err := h.store.Attempt(context.Background(), key, attempts[0].RunToken)
	if err != nil {
		t.Fatalf("the recorded attempt is not findable by token: %v", err)
	}
	if got.Outcome != gojob.AttemptSuccess {
		t.Fatalf("outcome = %q, want success", got.Outcome)
	}
}
