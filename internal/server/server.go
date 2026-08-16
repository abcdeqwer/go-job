// Package server hosts the half of the contract the SCHEDULER implements: the calls
// executors make inward.
//
// Every one of them is instance-agnostic. An executor posts to whichever scheduler the load
// balancer picks, and that instance writes to MySQL guarded by run_token and fence_epoch —
// so a result can land on an instance that has never heard of the execution and still be
// applied correctly, and the instance that dispatched it can die without stranding anything.
//
// The consequence worth stating: none of these handlers may consult in-process state to
// decide what a call means. Every decision is made against the database.
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
	"github.com/abcdeqwer/go-job/internal/outcome"
	"github.com/abcdeqwer/go-job/internal/store"
)

// Prober probes an executor's own contract before its registration is accepted.
//
// It is an interface rather than a direct dependency on the dispatch client so this package
// does not import the outbound half of the contract just to call one method — and so a test
// can register an executor without one listening.
type Prober interface {
	Describe(ctx context.Context, address string) (*gojobv1.DescribeResponse, error)
}

// Tenants resolves a tenant name to its coordination store.
//
// It returns false for a tenant that is unknown, disabled or not yet admitted — all three of
// which must look identical from outside. Telling an unauthenticated caller which tenant
// names exist is a small leak, and telling it which are merely disabled is a larger one.
type Tenants interface {
	Store(tenant string) (*store.Store, bool)
}

// Fence is the control-plane self-fence. A partitioned instance refuses callbacks rather than
// applying them, because it may no longer be the instance the registry believes is running.
type Fence interface{ Check() error }

// Config is the timing this server advertises to executors.
//
// Advertising it, rather than letting each executor choose, is what makes the liveness window
// mean the same thing across a polyglot fleet: a Java executor that heartbeats every thirty
// seconds against a scheduler expecting five is a fleet that looks permanently half-dead.
type Config struct {
	HeartbeatInterval time.Duration
	RegistrationTTL   time.Duration
	ProgressInterval  time.Duration

	// SilenceDeadline is how long an executor may report nothing before the scheduler treats
	// the execution as lost. Returned on every progress report so an executor whose handler
	// runs for hours keeps a current value even if its registration was renewed meanwhile.
	SilenceDeadline time.Duration
}

// Server implements gojobv1.JobSchedulerServer.
type Server struct {
	gojobv1.UnimplementedJobSchedulerServer

	cfg     Config
	tenants Tenants
	prober  Prober
	fence   Fence
	clock   gojob.Clock
	log     *slog.Logger

	// backoffSeconds is the delay a retryable result gets. It is a function so the value
	// tracks the scheduler's configured schedule rather than being frozen at construction.
	backoffSeconds func() int
}

// New builds the scheduler-side service.
func New(cfg Config, tenants Tenants, prober Prober, fence Fence, clock gojob.Clock,
	backoffSeconds func() int, log *slog.Logger) *Server {
	if backoffSeconds == nil {
		backoffSeconds = func() int { return 30 }
	}
	return &Server{cfg: cfg, tenants: tenants, prober: prober, fence: fence,
		clock: clock, backoffSeconds: backoffSeconds, log: log}
}

// resolve finds a tenant's store, or returns the gRPC error the caller should see.
func (s *Server) resolve(tenant string) (*store.Store, error) {
	if err := s.fence.Check(); err != nil {
		// UNAVAILABLE, not INTERNAL: it is a retryable condition, and the executor should try
		// another scheduler instance rather than give up on the call.
		return nil, status.Error(codes.Unavailable, "scheduler is fenced from the control database")
	}
	if tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant is required")
	}
	st, ok := s.tenants.Store(tenant)
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown tenant")
	}
	return st, nil
}

// Register records an executor and reconciles what it says it is holding.
//
// The in_flight list is the answer to "did that actually run?" after a restart, and it is
// more reliable than asking: only the executor knows what it is really doing, and it costs
// one field on a message that is sent anyway.
//
// The response's `fenced` list names the executions the scheduler no longer recognises —
// because the token was rotated, the row moved on, or it never existed. An executor must
// abandon those handlers without reporting a result: the scheduler has already resolved them,
// and a late result would be refused as fenced anyway.
func (s *Server) Register(ctx context.Context, req *gojobv1.RegisterRequest) (*gojobv1.RegisterResponse, error) {
	st, err := s.resolve(req.GetTenant())
	if err != nil {
		return nil, err
	}
	if req.GetExecutorId() == "" || req.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "executor_id and address are required")
	}

	// What the executor claims about itself is not recorded. Handlers, capacity, revision and
	// contract version all come from the probe below — calling BACK to the address it gave,
	// and taking the answer from the process actually listening there.
	//
	// A registration that trusted the request body would let an executor declare handlers it
	// cannot run: routing would then send it work, it would answer FAILED_PRECONDITION for
	// ever, and the job would look runnable while never running.
	e := store.Executor{
		ExecutorID: req.GetExecutorId(),
		Group:      req.GetGroup(),
		Address:    req.GetAddress(),
	}
	if e.Group == "" {
		e.Group = "default"
	}

	desc, err := s.prober.Describe(ctx, req.GetAddress())
	if err != nil {
		// A registration that cannot be probed is refused rather than accepted with a
		// question mark. An executor in the routing pool that cannot be reconciled with is
		// precisely the state recovery has no answer for.
		s.log.Warn("refusing a registration that failed the contract probe",
			"executor", req.GetExecutorId(), "address", req.GetAddress(), "error", err)
		return nil, status.Errorf(codes.FailedPrecondition, "contract probe failed: %v", err)
	}
	e.Handlers = desc.HandlerKeys
	e.Capacity = int(desc.Capacity)
	e.Revision = desc.Revision
	e.ContractVersion = desc.ContractVersion

	if err := st.Register(ctx, e); err != nil {
		return nil, status.Errorf(codes.Internal, "record registration: %v", err)
	}

	fenced, err := s.reconcileInFlight(ctx, st, req.GetInFlight())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reconcile in-flight work: %v", err)
	}

	s.log.Info("executor registered", "tenant", req.GetTenant(), "executor", e.ExecutorID,
		"group", e.Group, "handlers", len(e.Handlers), "in_flight", len(req.GetInFlight()),
		"fenced", len(fenced))

	return &gojobv1.RegisterResponse{
		HeartbeatIntervalSeconds: int32(roundUpSeconds(s.cfg.HeartbeatInterval)),
		RegistrationTtlSeconds:   int32(roundUpSeconds(s.cfg.RegistrationTTL)),
		ProgressIntervalSeconds:  int32(roundUpSeconds(s.cfg.ProgressInterval)),
		Fenced:                   fenced,
	}, nil
}

// reconcileInFlight decides which of an executor's claimed executions it may keep.
//
// An execution is still the executor's if the row exists, is non-terminal, and carries the
// same run_token. Anything else is fenced: a rotated token means recovery already resolved
// this attempt and something else may be running it, and a terminal row means the outcome is
// already recorded.
func (s *Server) reconcileInFlight(ctx context.Context, st *store.Store, claims []*gojobv1.InFlight) ([]string, error) {
	var fenced []string
	for _, c := range claims {
		if c.GetExecutionKey() == "" {
			continue
		}
		row, err := st.ExecutionByKey(ctx, c.GetExecutionKey())
		if errors.Is(err, store.ErrNoSuchExecution) {
			fenced = append(fenced, c.GetExecutionKey())
			continue
		}
		if err != nil {
			return nil, err
		}
		if row.Status.Terminal() || row.RunToken == "" || row.RunToken != c.GetRunToken() {
			fenced = append(fenced, c.GetExecutionKey())
		}
	}
	return fenced, nil
}

// Heartbeat keeps a registration alive.
//
// `known=false` means the registration lapsed and the executor must call Register again. It
// is not an error: a reaped executor is an ordinary consequence of a long pause, and turning
// it into an RPC failure would make a recoverable state look like an outage.
func (s *Server) Heartbeat(ctx context.Context, req *gojobv1.HeartbeatRequest) (*gojobv1.HeartbeatResponse, error) {
	st, err := s.resolve(req.GetTenant())
	if err != nil {
		return nil, err
	}
	known, err := st.ExecutorHeartbeat(ctx, req.GetExecutorId(), int(req.GetRunning()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
	}
	return &gojobv1.HeartbeatResponse{Known: known}, nil
}

// ReportProgress extends an execution's silence budget and says whether to keep going.
//
// `proceed=false` is how a cancel reaches a handler that is not watching its context, and how
// a fenced execution learns to stop. It is deliberately advisory — the scheduler cannot make
// a handler stop, and the row keeps its lease until the handler says it did or the cap fences
// it.
//
// The deadline this extends bounds SILENCE, not runtime. A twenty-hour handler reporting
// every minute keeps extending it and never touches the runtime cap, which is what lets a
// long job be both supervised and bounded.
func (s *Server) ReportProgress(ctx context.Context, req *gojobv1.ReportProgressRequest) (*gojobv1.ReportProgressResponse, error) {
	st, err := s.resolve(req.GetTenant())
	if err != nil {
		return nil, err
	}
	row, err := st.ExecutionByKey(ctx, req.GetExecutionKey())
	if errors.Is(err, store.ErrNoSuchExecution) {
		return &gojobv1.ReportProgressResponse{Proceed: false}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read execution: %v", err)
	}

	// A cancelled run is told to stop HERE, not only through the outbound Cancel call.
	//
	// ReportProgress is the one call a long handler makes on a timer, so it is the channel a
	// cancel is guaranteed to reach — an outbound Cancel needs the executor to be reachable
	// from a scheduler, and a handler that is not watching its context never sees it at all.
	// Answering proceed=true here told every cancelled handler to carry on, AND renewed its
	// silence budget while doing so, so it ran to natural completion.
	if row.Status == gojob.StatusCancelRequested {
		return &gojobv1.ReportProgressResponse{Proceed: false}, nil
	}

	h := store.Holder{
		JobName:      row.JobName,
		ExecutionID:  row.ID,
		ExecutionKey: row.ExecutionKey,
		RunToken:     req.GetRunToken(),
		FenceEpoch:   row.FenceEpoch,
	}
	if err := st.ExtendDeadline(ctx, h, roundUpSeconds(s.cfg.SilenceDeadline)); err != nil {
		if errors.Is(err, gojob.ErrFenced) {
			// Not an RPC failure: the executor asked a reasonable question and the answer is
			// "stop". Returning an error would leave it retrying a call whose answer will not
			// change.
			return &gojobv1.ReportProgressResponse{Proceed: false}, nil
		}
		return nil, status.Errorf(codes.Internal, "extend deadline: %v", err)
	}
	return &gojobv1.ReportProgressResponse{
		Proceed:                true,
		SilenceDeadlineSeconds: int32(roundUpSeconds(s.cfg.SilenceDeadline)),
	}, nil
}

// ReportResult records a terminal outcome.
//
// Idempotent on (tenant, execution_key, run_token): a redelivery of a result already recorded
// returns OK with already_recorded=true, so a lost response never turns into a spurious
// ABORTED that an executor would treat as "discard and do not retry".
//
// The order matters. Attempt history is checked FIRST, because by the time a redelivery
// arrives the execution row has usually moved on — token cleared, epoch bumped, status back
// to ready for a different attempt — and the execution row cannot answer the question.
func (s *Server) ReportResult(ctx context.Context, req *gojobv1.ReportResultRequest) (*gojobv1.ReportResultResponse, error) {
	st, err := s.resolve(req.GetTenant())
	if err != nil {
		return nil, err
	}
	if req.GetExecutionKey() == "" || req.GetRunToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "execution_key and run_token are required")
	}
	if req.GetOutcome() == nil {
		return nil, status.Error(codes.InvalidArgument, "outcome is required")
	}
	// An unspecified disposition is refused rather than absorbed. The proto requires one, and
	// treating a missing one as a retryable failure consumes an attempt and reruns real work
	// because a client sent an empty message — which is a bug in the caller that should be
	// reported to the caller, not paid for by the job.
	if req.GetOutcome().GetDisposition() == gojobv1.Disposition_DISPOSITION_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument,
			"outcome.disposition is required; it is what says whether the work succeeded")
	}

	if _, err := st.Attempt(ctx, req.GetExecutionKey(), req.GetRunToken()); err == nil {
		return &gojobv1.ReportResultResponse{AlreadyRecorded: true}, nil
	} else if !errors.Is(err, store.ErrNoSuchAttempt) {
		return nil, status.Errorf(codes.Internal, "read attempt history: %v", err)
	}

	row, err := st.ExecutionByKey(ctx, req.GetExecutionKey())
	if errors.Is(err, store.ErrNoSuchExecution) {
		return nil, status.Error(codes.NotFound, "unknown execution")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read execution: %v", err)
	}
	if row.RunToken != req.GetRunToken() {
		// A different attempt owns this execution now. ABORTED tells the executor to discard
		// and not retry, which is right: whatever it did has already been superseded.
		return nil, status.Error(codes.Aborted, "this attempt has been fenced")
	}

	h := store.Holder{
		JobName:      row.JobName,
		ExecutionID:  row.ID,
		ExecutionKey: row.ExecutionKey,
		RunToken:     row.RunToken,
		FenceEpoch:   row.FenceEpoch,
	}
	if err := outcome.Apply(ctx, st, h, req.GetOutcome(), row.DispatchedTo, s.backoffSeconds()); err != nil {
		if errors.Is(err, gojob.ErrFenced) {
			return nil, status.Error(codes.Aborted, "this attempt has been fenced")
		}
		return nil, status.Errorf(codes.Internal, "record result: %v", err)
	}
	return &gojobv1.ReportResultResponse{}, nil
}

func roundUpSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	s := int((d + time.Second - 1) / time.Second)
	if s < 1 {
		return 1
	}
	return s
}
