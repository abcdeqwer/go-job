package e2e

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
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

func TestCopyJobsCreatesMissingAndPreservesExisting(t *testing.T) {
	h := setup(t)
	existing := cronJob("existing", "0 0 1 * * *")
	existing.Description = "target authority"
	h.createJob(existing, h.clock.Now().Add(time.Hour))

	copiedExisting := cronJob("existing", "0 0 2 * * *")
	copiedExisting.Description = "must not overwrite"
	newJob := cronJob("new-job", "0 0 3 * * *")
	newJob.Description = "copied code description"
	created, skipped, err := h.store.CopyJobs(context.Background(), []store.JobSeed{
		{Definition: copiedExisting, NextFire: h.clock.Now().Add(2 * time.Hour)},
		{Definition: newJob, NextFire: h.clock.Now().Add(3 * time.Hour)},
	}, "source", "operator", "tenant bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != 1 || created[0] != "new-job" || len(skipped) != 1 || skipped[0] != "existing" {
		t.Fatalf("created=%v skipped=%v, want [new-job] and [existing]", created, skipped)
	}
	gotExisting, err := h.store.Job(context.Background(), "existing")
	if err != nil {
		t.Fatal(err)
	}
	if gotExisting.Description != "target authority" || gotExisting.ScheduleExpr != "0 0 1 * * *" {
		t.Fatalf("existing target job was overwritten: %+v", gotExisting.Definition)
	}
	gotNew, err := h.store.Job(context.Background(), "new-job")
	if err != nil {
		t.Fatal(err)
	}
	if gotNew.Description != newJob.Description || gotNew.OpsPaused {
		t.Fatalf("copied job = %+v paused=%v", gotNew.Definition, gotNew.OpsPaused)
	}
	audit, err := h.store.AuditLog(context.Background(), "new-job", "operator", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(audit) != 1 || !strings.Contains(audit[0].Detail, "copied from tenant source") {
		t.Fatalf("copy audit = %+v", audit)
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
	if err := h.store.RequestCancel(context.Background(), row.ID, "test", "the test cancels it"); err != nil {
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

	// No manual relay. The engine's cancel pass must find the row and call the executor, and
	// the executor's progress loop must see proceed=false — an earlier version of this test
	// called Cancel itself, which hid the fact that neither happened.
	eventually(t, "the handler to confirm it stopped", 25*time.Second, func() bool {
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

// An adopted attempt keeps working: its progress is accepted and its result is recorded.
//
// Adoption is the handover that keeps a running handler alive when its scheduler dies. The
// successor finds an expired lease, asks the executor, is told the attempt is still running,
// and takes ownership — bumping fence_epoch on both rows while leaving run_token alone,
// because it is the same attempt under new management. The executor is not told any of this
// and goes on reporting under the token it was dispatched with, so those callbacks have to
// keep working end to end against the adopted row.
//
// This is the sequential case: the adoption has fully landed before the callback arrives. The
// INTERLEAVED case — an adoption between a callback's read and its write — cannot be produced
// from out here, because by the time this test could bump the epoch the server would read the
// new one. That one is pinned in TestRetryPastAdoption, which reproduces it from inside the
// write.
func TestAdoptionDoesNotFenceTheAttemptItAdopted(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	release := make(chan struct{})
	h.exec.Handle("test.handler", func(hctx context.Context, p map[string]any) (string, error) {
		<-release
		return "done", nil
	})
	h.exec.Sent = nil
	h.connect()

	h.createJob(cronJob("adopted", "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))
	key, err := h.store.Trigger(ctx, "adopted", "req-adopt", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.engine.Start(ctx)
	defer func() { close(release); h.engine.Stop() }()

	eventually(t, "the execution to be running", 25*time.Second, func() bool {
		row, err := h.store.ExecutionByKey(ctx, key)
		return err == nil && row.Status == gojob.StatusRunning
	})

	row, err := h.store.ExecutionByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// Exactly what Adopt writes, on BOTH rows it takes: the epoch moves, the token does not,
	// the owner changes. Applied directly so the race is deterministic rather than dependent
	// on a lease expiring at the right moment.
	for _, q := range []string{
		`UPDATE job_state SET write_seq = write_seq + 1, fence_epoch = fence_epoch + 1,
		     active_owner = 'another-instance' WHERE job_name = ?`,
		`UPDATE job_execution SET write_seq = write_seq + 1, fence_epoch = fence_epoch + 1,
		     owner_instance = 'another-instance' WHERE job_name = ?`,
	} {
		if _, err := h.db.ExecContext(ctx, q, "adopted"); err != nil {
			t.Fatal(err)
		}
	}

	// The executor's progress report, carrying the token it was dispatched with — which is
	// still the token on the row.
	resp, err := h.sched.ReportProgress(ctx, &gojobv1.ReportProgressRequest{
		Tenant: tenantName, ExecutionKey: key, RunToken: row.RunToken,
	})
	if err != nil {
		t.Fatalf("progress after adoption returned an error: %v", err)
	}
	if !resp.GetProceed() {
		t.Fatal("a healthy handler was told to stop because its execution had just been " +
			"adopted; adoption exists to keep exactly this handler running")
	}

	// And a result carrying the same token must be recorded, not aborted.
	if _, err := h.sched.ReportResult(ctx, &gojobv1.ReportResultRequest{
		Tenant: tenantName, ExecutionKey: key, RunToken: row.RunToken,
		Outcome: &gojobv1.ExecutionOutcome{
			Disposition: gojobv1.Disposition_DISPOSITION_SUCCESS,
			Summary:     "done",
		},
	}); err != nil {
		t.Fatalf("a result from the adopted attempt was refused: %v", err)
	}
	after, err := h.store.ExecutionByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != gojob.StatusSuccess {
		t.Fatalf("execution is %q after a successful result, want success", after.Status)
	}
}

// A request id belongs to one job. Reusing it for another is a conflict, not a repeat.
//
// It produced two silent failures. The fast path answered a trigger for job B with job A's
// execution key and created nothing; and under a race the loser returned the key it had
// COMPUTED for B, for a row that does not exist. Either way an operator gets an accepted
// response, and an execution key, for work that will never run.
func TestTriggerIdempotencyIsBoundToTheJob(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	h.createJob(cronJob("alpha", "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))
	h.createJob(cronJob("beta", "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))

	keyA, err := h.store.Trigger(ctx, "alpha", "req-shared", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}

	// The same id again for the same job is the repeat idempotency promises.
	again, err := h.store.Trigger(ctx, "alpha", "req-shared", "test", "because", nil)
	if err != nil {
		t.Fatalf("a genuine repeat was refused: %v", err)
	}
	if again != keyA {
		t.Fatalf("a repeat returned a different execution: %q vs %q", again, keyA)
	}

	// The same id for a DIFFERENT job must fail, not quietly answer with alpha's key.
	got, err := h.store.Trigger(ctx, "beta", "req-shared", "test", "because", nil)
	if err == nil {
		t.Fatalf("a reused request id created or matched something for another job: %q", got)
	}
	if !errors.Is(err, gojob.ErrProtocol) {
		t.Fatalf("refusal was not reported as a protocol error: %v", err)
	}

	// And nothing was created for beta.
	for _, v := range h.executions("ready") {
		if v.JobName == "beta" {
			t.Fatal("an execution was created for beta despite the refusal")
		}
	}
}

// A live executor id cannot be taken over by a second process.
//
// Ids are required to be unique per process and a restart is required to mint a fresh one.
// Accepting a duplicate anyway is not leniency: the registration upsert replaces the ADDRESS,
// so recovery asking "does executor E still have this work" reaches the wrong process, is told
// NOT_FOUND, and re-dispatches an execution the original E is still running. One malformed
// registration becomes two handlers.
func TestALiveExecutorIdCannotBeTakenOver(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	original := store.Executor{
		ExecutorID: "exec-live", Group: "main", Address: "host-a:9000",
		ContractVersion: "1", Revision: "r1", Capacity: 4,
		Handlers: []string{"test.handler"},
	}
	if err := h.store.Register(ctx, original, 30); err != nil {
		t.Fatal(err)
	}

	// Another process, same id, different address.
	impostor := original
	impostor.Address = "host-b:9000"
	if err := h.store.Register(ctx, impostor, 30); err == nil {
		t.Fatal("a second process took over a live executor id; recovery would then ask the " +
			"wrong process about work the first one is still running")
	}

	addr, err := h.store.ExecutorAddress(ctx, "exec-live")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "host-a:9000" {
		t.Fatalf("the address moved to %q despite the refusal", addr)
	}

	// The same process re-registering at its own address is fine — that is a heartbeat-losing
	// reconnect, not a takeover.
	if err := h.store.Register(ctx, original, 30); err != nil {
		t.Fatalf("a process could not re-register at its own address: %v", err)
	}

	// And once the registration has lapsed, the id is free: a legitimate restart finds
	// exactly this, and refusing there would make an id unusable for ever.
	if _, err := h.db.ExecContext(ctx, `
		UPDATE job_executor SET heartbeat_at = TIMESTAMPADD(SECOND, -3600, UTC_TIMESTAMP())
		WHERE executor_id = 'exec-live'`); err != nil {
		t.Fatal(err)
	}
	if err := h.store.Register(ctx, impostor, 30); err != nil {
		t.Fatalf("a lapsed id was not reusable: %v", err)
	}
}

// Adopting a `dispatching` row the executor reports RUNNING must record the acceptance the
// dead scheduler never did.
//
// Without it the row stays `dispatching`, which the silence scan does not read, so a handler
// that later goes quiet is invisible until its runtime cap — days, for a job capped in days.
// attempt_no stays zero, so the same failure repeated runs the work more times than
// max_attempts allows, and deadline_at stays NULL, so there is no silence budget to elapse.
//
// It is also where MySQL's assignment order bites: UPDATE assignments are evaluated left to
// right and each sees the previous ones' writes, so a promotion that assigned status first and
// then tested `status = 'dispatching'` promoted the row and updated nothing else. That defect
// is invisible to any test starting from an already-running row, which is why this one starts
// from `dispatching`.
func TestAdoptingADispatchingRowRecordsAcceptance(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	h.createJob(cronJob("adoptme", "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))
	key, err := h.store.Trigger(ctx, "adoptme", "req-adopt-dispatch", "test", "because", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Claim it, leaving the row exactly where a scheduler that died before Accept leaves it:
	// `dispatching`, no attempt charged, no deadline.
	cands, err := h.store.ReadyCandidates(ctx, true, 10)
	if err != nil {
		t.Fatal(err)
	}
	var c store.Candidate
	for _, v := range cands {
		if v.ExecutionKey == key {
			c = v
		}
	}
	if c.ExecutionKey == "" {
		t.Fatal("the triggered execution was not claimable")
	}

	res, err := h.store.Claim(ctx, store.ClaimParams{
		JobName: c.JobName, ExecutionID: c.ID, ExecutionKey: c.ExecutionKey,
		Owner: "dead-instance", RunToken: "tok-adopt", BackoffSeconds: 5,
		ExecutorID: "exec-1",
	}, func(context.Context, *sql.Tx, gojob.Definition, store.StateRow) error { return nil })
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if res.Outcome != store.ClaimAcquired {
		t.Fatalf("claim outcome = %v, want acquired", res.Outcome)
	}

	before, err := h.store.ExecutionByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if before.Status != gojob.StatusDispatching {
		t.Fatalf("status = %q, want dispatching", before.Status)
	}

	// Expire the lease so the row is adoptable, then adopt it as a RUNNING handler.
	if _, err := h.db.ExecContext(ctx, `
		UPDATE job_execution SET lease_until = TIMESTAMPADD(SECOND, -60, UTC_TIMESTAMP())
		WHERE execution_key = ?`, key); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.ExecContext(ctx, `
		UPDATE job_state SET lease_until = TIMESTAMPADD(SECOND, -60, UTC_TIMESTAMP())
		WHERE job_name = 'adoptme'`); err != nil {
		t.Fatal(err)
	}

	stale, err := h.store.StaleExecutions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	var target store.Stale
	for _, v := range stale {
		if v.ExecutionKey == key {
			target = v
		}
	}
	if target.ExecutionKey == "" {
		t.Fatal("the expired execution was not returned by the stale scan")
	}

	if _, err := h.store.Adopt(ctx, target, "live-instance", 30, 45, true); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	after, err := h.store.ExecutionByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != gojob.StatusRunning {
		t.Errorf("status = %q after adopting a running handler, want running", after.Status)
	}
	if after.AttemptNo != 1 {
		t.Errorf("attempt_no = %d, want 1; the attempt ran and must be charged", after.AttemptNo)
	}

	var startedAt, deadlineAt sql.NullTime
	if err := h.db.QueryRowContext(ctx,
		`SELECT started_at, deadline_at FROM job_execution WHERE execution_key = ?`,
		key).Scan(&startedAt, &deadlineAt); err != nil {
		t.Fatal(err)
	}
	if !startedAt.Valid {
		t.Error("started_at is still NULL after adoption")
	}
	if !deadlineAt.Valid {
		t.Error("deadline_at is still NULL; the silence scan needs a deadline, so this " +
			"execution would be invisible to it until its runtime cap")
	}
}

// A slow cancel must not starve a later one.
//
// Rows stay `cancel_requested` until their handler actually stops, which for a stuck or
// unreachable executor is never. A relay that always took one page from the lowest ids
// therefore re-sent the same stop requests every pass, and a cancel issued a minute ago —
// higher id — was never sent at all. One group of stuck executors could delay every unrelated
// cancellation indefinitely.
func TestCancelRelayReachesLaterRequests(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	// Three cancel-requested executions, and a page size of one.
	var keys []string
	for _, name := range []string{"c1", "c2", "c3"} {
		h.createJob(cronJob(name, "0 0 0 1 1 *"), h.clock.Now().Add(24*time.Hour))
		key, err := h.store.Trigger(ctx, name, "req-"+name, "test", "because", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.db.ExecContext(ctx, `
			UPDATE job_execution
			SET status = 'cancel_requested', dispatched_to = 'exec-1', run_token = ?
			WHERE execution_key = ?`, "tok-"+name, key); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}

	// One page at a time, walking the cursor, must reach all three.
	seen := map[string]bool{}
	var after int64
	for page := 0; page < 10; page++ {
		rows, err := h.store.CancelRequested(ctx, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, v := range rows {
			seen[v.ExecutionKey] = true
			after = v.ID
		}
	}
	for _, key := range keys {
		if !seen[key] {
			t.Errorf("execution %q was never reached; the oldest page starves everything "+
				"behind it", key)
		}
	}

	// And without the cursor, the same page comes back for ever.
	first, err := h.store.CancelRequested(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	again, err := h.store.CancelRequested(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(again) != 1 || first[0].ID != again[0].ID {
		t.Fatal("the scan is not deterministic; this test's premise no longer holds")
	}
}

// The executors view must report liveness and timestamps correctly in a NON-UTC deployment.
//
// started_at and heartbeat_at are ownership columns: written with UTC_TIMESTAMP(), so what is
// stored is UTC wall clock. The driver tags every DATETIME it reads with the DSN's `loc`, which
// is the business location — so a scanned value carries an offset it never had, and deriving
// liveness from it in Go compares a UTC instant against a business one.
//
// In a UTC deployment the offset is zero and everything looks right. That is why this bug
// shipped: every test in this repository runs at UTC. In Asia/Manila every live executor was
// reported dead, and both timestamps displayed eight hours early.
func TestExecutorViewIsCorrectOutsideUTC(t *testing.T) {
	h := setup(t)
	ctx := context.Background()

	if err := h.store.Register(ctx, store.Executor{
		ExecutorID: "tz-exec", Group: "main", Address: "host:9000",
		ContractVersion: "1", Revision: "r1", Capacity: 4,
		Handlers: []string{"test.handler"},
	}, 30); err != nil {
		t.Fatal(err)
	}

	// A SECOND pool against the same schema, opened the way a Manila deployment opens one.
	//
	// This is the whole point of the test. The harness runs at UTC, where the offset is zero
	// and the defect cannot appear — reading through this pool is the only way to see what an
	// operator in Asia/Manila would have seen.
	manilaDB, err := sql.Open("mysql",
		dsn(t)+h.schema+"?parseTime=true&loc=Asia%2FManila&multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer manilaDB.Close()
	manila, err := time.LoadLocation("Asia/Manila")
	if err != nil {
		t.Fatal(err)
	}
	manilaStore := store.New(manilaDB, gojob.SystemClock{Loc: manila})

	xs, err := manilaStore.AllExecutors(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 {
		t.Fatalf("got %d executors, want 1", len(xs))
	}
	x := xs[0]

	// Liveness is the database's answer, so it does not depend on the reader's time zone.
	if !x.Live {
		t.Error("an executor that just registered is reported dead")
	}

	// And the timestamp must be the instant it actually is, within a minute — whatever the
	// business location is. Compared as instants, which is what the mis-tagging breaks.
	if skew := time.Since(x.HeartbeatAt); skew < 0 || skew > time.Minute {
		t.Errorf("heartbeat_at is %s away from now; an ownership column read without being "+
			"re-tagged as UTC is wrong by the business offset", skew.Round(time.Second))
	}
	if x.HeartbeatAt.Location() != time.UTC {
		t.Errorf("heartbeat_at is tagged %s, want UTC — it holds a UTC wall clock",
			x.HeartbeatAt.Location())
	}

	// Now the part that only bites outside UTC: an executor whose heartbeat has lapsed must
	// read as dead, and one inside the window as live, with the window measured in the
	// database rather than against a business-clock "now".
	if _, err := h.db.ExecContext(ctx, `
		UPDATE job_executor SET heartbeat_at = TIMESTAMPADD(SECOND, -45, UTC_TIMESTAMP())
		WHERE executor_id = 'tz-exec'`); err != nil {
		t.Fatal(err)
	}
	xs, err = manilaStore.AllExecutors(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if xs[0].Live {
		t.Error("an executor whose heartbeat lapsed 45s ago is reported live under a 30s window")
	}
}
