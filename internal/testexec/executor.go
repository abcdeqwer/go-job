// Package testexec is a conforming executor, used by the end-to-end test and by anyone who
// wants to see what implementing the contract actually looks like.
//
// It implements JobExecutor completely, because there is no partial implementation: the whole
// point of generating from the contract is that a missing method is a build failure rather
// than something discovered at registration. Reading this file is the shortest description of
// what an executor in any language has to do.
package testexec

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
)

// Handler is business code. It receives parameters and a context that is cancelled when the
// scheduler asks it to stop, and returns a summary or an error.
type Handler func(ctx context.Context, params map[string]any) (summary string, err error)

// Executor hosts handlers and talks to a scheduler.
type Executor struct {
	gojobv1.UnimplementedJobExecutorServer

	ID       string
	Group    string
	Address  string
	Tenant   string
	Capacity int
	Revision string

	handlers map[string]Handler

	mu      sync.Mutex
	running map[string]*execution // by execution_key
	done    map[string]*finished  // remembered outcomes

	sched gojobv1.JobSchedulerClient
	// Sent is a hook the test uses to observe reported results.
	Sent func(key string, oc *gojobv1.ExecutionOutcome)

	// lastReportErr is the last error reporting a result, kept because a silently swallowed
	// report failure looks exactly like a scheduler that never received anything — and the
	// two send whoever is debugging to opposite ends of the system.
	lastReportErr error
}

// LastReportError returns why the most recent result report failed, if it did.
func (e *Executor) LastReportError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastReportErr
}

type execution struct {
	runToken  string
	cancel    context.CancelFunc
	startedAt time.Time
}

type finished struct {
	runToken string
	outcome  *gojobv1.ExecutionOutcome
	at       time.Time
}

// New builds an executor.
func New(id, group, address, tenant string, capacity int) *Executor {
	return &Executor{
		ID: id, Group: group, Address: address, Tenant: tenant,
		Capacity: capacity, Revision: "test",
		handlers: make(map[string]Handler),
		running:  make(map[string]*execution),
		done:     make(map[string]*finished),
	}
}

// Handle registers business code under a handler key.
func (e *Executor) Handle(key string, h Handler) { e.handlers[key] = h }

// Describe reports what this executor is and can do. The scheduler calls it during
// registration, before routing any work.
func (e *Executor) Describe(ctx context.Context, _ *gojobv1.DescribeRequest) (*gojobv1.DescribeResponse, error) {
	keys := make([]string, 0, len(e.handlers))
	for k := range e.handlers {
		keys = append(keys, k)
	}
	return &gojobv1.DescribeResponse{
		ContractVersion: "1",
		HandlerKeys:     keys,
		Capacity:        int32(e.Capacity),
		Revision:        e.Revision,
	}, nil
}

// Run accepts one execution and returns IMMEDIATELY. The handler starts in the background;
// an OK response is a promise that a result will eventually be reported.
//
// Answering only after the work finished would make every dispatch a long-lived RPC, which is
// the design this contract exists to avoid: the scheduler would hold a connection for the
// duration of a twenty-hour job and learn nothing from it that progress reports do not say
// better.
func (e *Executor) Run(ctx context.Context, req *gojobv1.RunRequest) (*gojobv1.RunResponse, error) {
	h, ok := e.handlers[req.GetHandlerKey()]
	if !ok {
		// Refused in the body, for the same reason as capacity: this executor knows for
		// certain it did not take the work, and only the body can say so unambiguously.
		return &gojobv1.RunResponse{
			ExecutionKey:  req.GetExecutionKey(),
			Refused:       true,
			RefusalReason: "no handler " + req.GetHandlerKey(),
		}, nil
	}

	e.mu.Lock()
	if cur, held := e.running[req.GetExecutionKey()]; held {
		e.mu.Unlock()
		// ALREADY_EXISTS carries the token actually held, because "already held" is two
		// different situations: a re-send of THIS attempt after a lost reply, which the
		// scheduler adopts, and a new attempt colliding with an older one it already fenced,
		// which it must not.
		st, _ := status.New(codes.AlreadyExists, "already running").
			WithDetails(&gojobv1.ExecutionHeld{HeldRunToken: cur.runToken})
		return nil, st.Err()
	}
	if len(e.running) >= e.Capacity {
		e.mu.Unlock()
		// A refusal is the RESPONSE, not a status code. A status cannot tell the scheduler
		// apart from a transport failure after delivery, and it would have to read the
		// ambiguous case as unknown — which costs a re-send cycle for something this executor
		// knows for certain.
		return &gojobv1.RunResponse{
			ExecutionKey:  req.GetExecutionKey(),
			Refused:       true,
			RefusalReason: "at capacity",
		}, nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	e.running[req.GetExecutionKey()] = &execution{
		runToken: req.GetRunToken(), cancel: cancel, startedAt: time.Now(),
	}
	e.mu.Unlock()

	go e.execute(runCtx, req, h)

	return &gojobv1.RunResponse{
		ExecutionKey: req.GetExecutionKey(),
		RunToken:     req.GetRunToken(),
	}, nil
}

func (e *Executor) execute(ctx context.Context, req *gojobv1.RunRequest, h Handler) {
	// The runtime cap arrives as a DURATION, not an instant, because this process's clock is
	// not the scheduler's. Applying it as a deadline here is what makes a conforming executor
	// report DISPOSITION_TIMED_OUT itself rather than waiting to be fenced.
	if s := req.GetRemainingTimeoutSeconds(); s > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(s)*time.Second)
		defer cancel()
	}

	params := map[string]any{}
	if p := req.GetParams(); p != nil && p.GetValues() != nil {
		params = p.GetValues().AsMap()
	}

	// Progress on a timer, independent of the handler. The handlers most likely to run for
	// hours are the least able to check in, so an executor that only reported when its
	// business code remembered to would leave exactly those looking dead.
	//
	// It also carries cancellation: proceed=false is how a stop reaches a handler that is not
	// watching its context, and is the one channel guaranteed to reach a long run.
	progressCtx, stopProgress := context.WithCancel(ctx)
	ctx, cancelForStop := context.WithCancel(ctx)
	go e.reportProgress(progressCtx, req, cancelForStop)
	defer stopProgress()

	summary, err := h(ctx, params)
	oc := &gojobv1.ExecutionOutcome{Summary: summary, DidWork: true}
	switch {
	case err == nil:
		oc.Disposition = gojobv1.Disposition_DISPOSITION_SUCCESS
	case ctx.Err() == context.DeadlineExceeded:
		oc.Disposition = gojobv1.Disposition_DISPOSITION_TIMED_OUT
		oc.FailureKind = "timeout"
		oc.ErrorDetail = err.Error()
	case ctx.Err() == context.Canceled:
		// The handler confirmed it stopped. This is the only path that reaches `cancelled`
		// with side effects actually accounted for.
		oc.Disposition = gojobv1.Disposition_DISPOSITION_STOPPED
		oc.ErrorDetail = err.Error()
	default:
		oc.Disposition = gojobv1.Disposition_DISPOSITION_FAILED
		oc.FailureKind = "handler"
		oc.ErrorDetail = err.Error()
	}

	e.mu.Lock()
	delete(e.running, req.GetExecutionKey())
	e.done[req.GetExecutionKey()] = &finished{
		runToken: req.GetRunToken(), outcome: oc, at: time.Now(),
	}
	e.mu.Unlock()

	e.report(req, oc)
}

// report delivers the result, retrying until the scheduler acknowledges it.
//
// ABORTED means this attempt was fenced by a different token: discard and do NOT retry, the
// work has already been superseded. Anything else is retried, because a lost response must
// not become a lost result.
func (e *Executor) report(req *gojobv1.RunRequest, oc *gojobv1.ExecutionOutcome) {
	if e.Sent != nil {
		e.Sent(req.GetExecutionKey(), oc)
	}
	if e.sched == nil {
		return
	}
	for attempt := 0; attempt < 10; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := e.sched.ReportResult(ctx, &gojobv1.ReportResultRequest{
			Tenant:       e.Tenant,
			ExecutionKey: req.GetExecutionKey(),
			RunToken:     req.GetRunToken(),
			Outcome:      oc,
		})
		cancel()

		e.mu.Lock()
		e.lastReportErr = err
		e.mu.Unlock()

		if err == nil || status.Code(err) == codes.Aborted || status.Code(err) == codes.NotFound {
			return
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
}

// reportProgress extends the execution's silence budget until the handler ends.
func (e *Executor) reportProgress(ctx context.Context, req *gojobv1.RunRequest, stop context.CancelFunc) {
	if e.sched == nil {
		return
	}
	interval := time.Duration(req.GetSilenceDeadlineSeconds()) * time.Second / 3
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := e.sched.ReportProgress(callCtx, &gojobv1.ReportProgressRequest{
				Tenant:       e.Tenant,
				ExecutionKey: req.GetExecutionKey(),
				RunToken:     req.GetRunToken(),
			})
			cancel()
			if err == nil && !resp.GetProceed() {
				// Cancelled, or fenced. Either way this attempt must stop.
				stop()
				return
			}
		}
	}
}

// Cancel signals a stop. Acknowledgement means the stop was SIGNALLED, not that work has
// ceased — the result still arrives when the handler actually ends.
func (e *Executor) Cancel(ctx context.Context, req *gojobv1.CancelRequest) (*gojobv1.CancelResponse, error) {
	e.mu.Lock()
	cur, ok := e.running[req.GetExecutionKey()]
	e.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "not running here")
	}
	if req.GetRunToken() != "" && req.GetRunToken() != cur.runToken {
		return nil, status.Error(codes.NotFound, "a different attempt is running here")
	}
	cur.cancel()
	return &gojobv1.CancelResponse{}, nil
}

// GetExecution answers what happened to an execution the scheduler lost track of.
//
// NOT_FOUND means UNKNOWN, never "it did not run": this process may have restarted since, and
// the work may have run partially or fully before it died. Making NOT_FOUND mean "did not
// run" would require persisting execution state durably here, which is real work to build a
// worse answer than the one that already exists — the handler's own idempotency key, in
// business data.
func (e *Executor) GetExecution(ctx context.Context, req *gojobv1.GetExecutionRequest) (*gojobv1.GetExecutionResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if cur, ok := e.running[req.GetExecutionKey()]; ok {
		return &gojobv1.GetExecutionResponse{
			State:     gojobv1.ExecutionState_EXECUTION_STATE_RUNNING,
			RunToken:  cur.runToken,
			StartedAt: cur.startedAt.Format(time.RFC3339),
		}, nil
	}
	if fin, ok := e.done[req.GetExecutionKey()]; ok {
		return &gojobv1.GetExecutionResponse{
			State:    gojobv1.ExecutionState_EXECUTION_STATE_FINISHED,
			RunToken: fin.runToken,
			Outcome:  fin.outcome,
		}, nil
	}
	return nil, status.Error(codes.NotFound, "unknown execution")
}

// Connect registers with a scheduler and keeps the registration alive.
//
// The in_flight list is declared on every registration, which is how "did that actually run?"
// is answered after either side restarts — more reliable than the scheduler asking, because
// only this process knows what it is really doing, and it costs one field on a message that
// is sent anyway.
func (e *Executor) Connect(ctx context.Context, schedulerAddr string, dial func(string) (*grpc.ClientConn, error)) error {
	cc, err := dial(schedulerAddr)
	if err != nil {
		return fmt.Errorf("connect to scheduler: %w", err)
	}
	e.sched = gojobv1.NewJobSchedulerClient(cc)

	resp, err := e.sched.Register(ctx, &gojobv1.RegisterRequest{
		ExecutorId: e.ID,
		Group:      e.Group,
		Tenant:     e.Tenant,
		Address:    e.Address,
		InFlight:   e.inFlight(),
	})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// Anything the scheduler fenced is abandoned WITHOUT reporting a result: it has already
	// resolved those, and a late result would be refused anyway.
	for _, key := range resp.GetFenced() {
		e.mu.Lock()
		if cur, ok := e.running[key]; ok {
			cur.cancel()
			delete(e.running, key)
		}
		e.mu.Unlock()
	}

	interval := time.Duration(resp.GetHeartbeatIntervalSeconds()) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go e.heartbeat(ctx, schedulerAddr, dial, interval)
	return nil
}

func (e *Executor) inFlight() []*gojobv1.InFlight {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]*gojobv1.InFlight, 0, len(e.running))
	for key, cur := range e.running {
		out = append(out, &gojobv1.InFlight{ExecutionKey: key, RunToken: cur.runToken})
	}
	return out
}

// heartbeat keeps the registration alive, and re-registers when told the registration lapsed.
//
// `known=false` is not an error: a reaped registration is the ordinary consequence of a long
// pause, and re-registering is the only recovery — an executor whose row was reaped has no
// handlers declared, so a heartbeat that silently re-created the row would leave it registered
// and unroutable.
func (e *Executor) heartbeat(ctx context.Context, addr string, dial func(string) (*grpc.ClientConn, error), interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.mu.Lock()
			running := int32(len(e.running))
			e.mu.Unlock()

			hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := e.sched.Heartbeat(hbCtx, &gojobv1.HeartbeatRequest{
				ExecutorId: e.ID, Tenant: e.Tenant, Running: running,
			})
			cancel()
			if err == nil && !resp.GetKnown() {
				_ = e.Connect(ctx, addr, dial)
			}
		}
	}
}

// Running reports how many handlers are in flight, for tests.
func (e *Executor) Running() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.running)
}
