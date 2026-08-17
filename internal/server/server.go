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
	Describe(ctx context.Context, address, tenant string) (*gojobv1.DescribeResponse, error)
}

// Tenants resolves a tenant name to its coordination store.
//
// It returns false for a tenant that is unknown, disabled or not yet admitted — all three of
// which must look identical from outside. Telling an unauthenticated caller which tenant
// names exist is a small leak, and telling it which are merely disabled is a larger one.
type Tenants interface {
	// Lookup resolves a tenant, and says why when it cannot. The distinction is load-bearing:
	// see resolve.
	Lookup(tenant string) (*store.Store, Availability)
}

// Availability mirrors the registry's, so this package does not import it.
type Availability int

const (
	Available Availability = iota
	Pending
	Unknown
)

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
	st, avail := s.tenants.Lookup(tenant)
	switch avail {
	case Available:
		return st, nil
	case Pending:
		// UNAVAILABLE, not NOT_FOUND, and the difference is a lost result.
		//
		// An executor treats NOT_FOUND as terminal and discards what it was reporting. This
		// instance being mid-admission, or having failed one, says nothing about whether the
		// work happened — so answering NOT_FOUND would throw away a real result and leave
		// recovery to record unknown and possibly rerun completed work. UNAVAILABLE tells the
		// executor to retry, and a load balancer to try another instance.
		return nil, status.Error(codes.Unavailable, "this scheduler has not admitted that tenant yet")
	default:
		return nil, status.Error(codes.NotFound, "unknown tenant")
	}
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
		// Recorded so every later callback can be bound to the credential that registered
		// this process, rather than to whoever happens to know its id.
		Identity: IdentityFrom(ctx).Subject,
	}
	if e.Group == "" {
		e.Group = "default"
	}

	desc, err := s.prober.Describe(ctx, req.GetAddress(), req.GetTenant())
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

	// Reconcile BEFORE the row exists.
	//
	// Registering first makes the executor routable the instant it commits, so another
	// scheduler can dispatch to it while this call is still deciding what it may keep — and if
	// reconciliation then fails, the executor is told its registration failed, may never start
	// its heartbeat loop, and is nonetheless in the routing pool of every other instance.
	//
	// Reconciliation only reads, so doing it first costs nothing and cannot leave a half-state.
	fenced, err := s.reconcileInFlight(ctx, st, req.GetInFlight())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reconcile in-flight work: %v", err)
	}

	if err := st.Register(ctx, e); err != nil {
		return nil, status.Errorf(codes.Internal, "record registration: %v", err)
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
	// The identity is part of the predicate, not merely logged. A heartbeat is what keeps an
	// address routable, so an identity able to heartbeat any id it knows could keep a dead
	// process in the routing pool indefinitely — every dispatch to it returns unknown, and the
	// job burns its recovery budget without ever running.
	//
	// A false answer here is "re-register", which is exactly the right instruction for both an
	// executor whose registration was reaped and one whose credential no longer matches.
	known, err := st.ExecutorHeartbeat(ctx, req.GetExecutorId(), IdentityFrom(ctx).Subject,
		int(req.GetRunning()))
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
	extend := func(h store.Holder) error {
		return st.ExtendDeadline(ctx, h, roundUpSeconds(s.cfg.SilenceDeadline))
	}
	if err := s.retryPastAdoption(ctx, st, h, extend); err != nil {
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
	apply := func(h store.Holder) error {
		return outcome.Apply(ctx, st, h, req.GetOutcome(), row.DispatchedTo, s.backoffSeconds())
	}
	if err := s.retryPastAdoption(ctx, st, h, apply); err != nil {
		if errors.Is(err, gojob.ErrFenced) {
			// Before calling it fenced, ask whether the result LANDED.
			//
			// Recovery can adopt this very attempt and record the outcome it obtained from the
			// executor while this callback is between its history check and its write. The
			// work is safe either way — but answering ABORTED tells the executor its attempt
			// was superseded, when in fact its result was accepted, by someone else, moments
			// earlier. That contradicts the idempotency this call advertises, and an executor
			// that logs an aborted result as a lost one sends an operator looking for a
			// failure that did not happen.
			if _, aErr := st.Attempt(ctx, req.GetExecutionKey(), req.GetRunToken()); aErr == nil {
				return &gojobv1.ReportResultResponse{AlreadyRecorded: true}, nil
			}
			return nil, status.Error(codes.Aborted, "this attempt has been fenced")
		}
		return nil, status.Errorf(codes.Internal, "record result: %v", err)
	}
	return &gojobv1.ReportResultResponse{}, nil
}

// executionReader is the one thing retryPastAdoption needs from a store: the current row.
//
// Narrow on purpose. The rule it implements is a decision about a race between two writes,
// and a race is worth a test that reproduces it deterministically rather than one that runs
// the pieces in a fixed order and reports the race as covered.
type executionReader interface {
	ExecutionByKey(ctx context.Context, key string) (store.Stale, error)
}

// retryPastAdoption runs a fenced write once more when the fence was RECOVERY ADOPTING THIS
// VERY ATTEMPT, rather than anything superseding it.
//
// Both callbacks read the execution row and then write against the epoch they read. Between
// those two statements, recovery can adopt the execution: it finds an expired lease, asks the
// executor, is told the attempt is still running, and takes ownership — bumping fence_epoch
// while leaving run_token untouched, because it is the SAME attempt under new management.
// That is the handover working exactly as designed.
//
// Read naively, the write then affects zero rows and the executor is told to stop. So a
// handover — the mechanism whose entire purpose is to keep a running handler alive across the
// loss of its scheduler — would kill the handler it just rescued, and a result arriving in
// that window would be answered ABORTED and discarded.
//
// The distinguishing question is whether the RUN TOKEN still matches. A token names one
// attempt and is minted per dispatch, so a token that is still on the row means this caller is
// still the current attempt and the epoch change was a change of owner, not of attempt. Once,
// not in a loop: a second adoption inside one callback is not a case worth serving, and a
// retry loop against a row being repeatedly fenced is how a callback becomes a hot spin.
func (s *Server) retryPastAdoption(ctx context.Context, st executionReader, h store.Holder,
	write func(store.Holder) error) error {

	err := write(h)
	if !errors.Is(err, gojob.ErrFenced) {
		return err
	}
	row, readErr := st.ExecutionByKey(ctx, h.ExecutionKey)
	if readErr != nil || row.RunToken != h.RunToken || row.FenceEpoch == h.FenceEpoch {
		// Genuinely fenced: a different attempt owns the row, the row is gone, or the epoch
		// did not move and the refusal was about something else — a terminal status, or an
		// elapsed runtime cap.
		return err
	}
	h.FenceEpoch = row.FenceEpoch
	return write(h)
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
