// Package dispatch is the scheduler's side of the executor contract: connections to executor
// processes, the Run/Cancel/GetExecution calls, and the translation of gRPC status codes into
// the transitions doc/protocol.md §2 specifies.
//
// The translation is the part worth care. Every code an executor can return means something
// different for the retry budget, and getting one wrong is expensive in a way that does not
// show up in testing: charging an attempt for RESOURCE_EXHAUSTED marches a job to `dead`
// without a line of business code having run, and NOT charging one for a genuine failure
// makes max_attempts unbounded.
package dispatch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
)

// Answer is how an executor responded to a dispatch, reduced to the three outcomes the
// protocol distinguishes. Anything finer belongs in the log, not in the control flow.
type Answer int

const (
	// Accepted: the executor has the work and will report a result. The attempt is charged.
	Accepted Answer = iota

	// Refused: the executor provably did not take it — busy, shutting down, or it does not
	// have the handler. No attempt is charged, and the scheduler tries another instance.
	Refused

	// Unknown: the call failed in a way that does not say whether the executor took it.
	// The execution stays `dispatching` with its target recorded, and a bounded re-send to
	// the SAME executor resolves it; recovery resolves it if the re-send bound is exhausted.
	Unknown
)

func (a Answer) String() string {
	switch a {
	case Accepted:
		return "accepted"
	case Refused:
		return "refused"
	default:
		return "unknown"
	}
}

// Result of one dispatch attempt.
type Result struct {
	Answer Answer

	// HeldToken is set when the executor answered ALREADY_EXISTS, naming the token it
	// actually holds. It is the difference between two situations that look identical:
	// a re-send of THIS attempt after a lost reply, which is an acceptance, and a new
	// attempt colliding with an older one the scheduler already fenced, which is not —
	// the old handler is still running and this attempt never started.
	HeldToken string

	// Code is the gRPC code, kept for metrics and logs. Never branched on outside this
	// package; the Answer is the decision.
	Code codes.Code

	Err error
}

// Client dispatches to executors and reuses one connection per address.
//
// Connections are cached by address rather than by executor id: a restarted executor mints a
// new id but usually keeps its address, and a fresh connection per dispatch would pay a
// handshake on every job in a fleet where handshakes are the expensive part.
// Credentials are how the scheduler proves itself to an executor, and how it verifies the
// executor in return.
//
// Both directions matter. Without server verification the scheduler will hand a job's
// parameters to whatever answers at the recorded address; without a client credential the
// executor cannot tell a scheduler from anything else that can reach its port.
type Credentials struct {
	// Transport is the gRPC credential. nil means plaintext, which is a deliberate choice for
	// a deployment whose network genuinely is the boundary and is logged at startup.
	Transport credentials.TransportCredentials

	// BearerToken is sent as `authorization: Bearer <token>` on every call, for executors that
	// authenticate the scheduler by shared secret rather than by certificate.
	BearerToken string
}

type Client struct {
	dialTimeout time.Duration
	callTimeout time.Duration
	creds       Credentials

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn

	// dialer is overridable so tests can supply an in-process connection. Production passes
	// nil and gets grpc.NewClient.
	dialer func(target string) (*grpc.ClientConn, error)
}

// NewClient builds a dispatch client. callTimeout bounds Run, Cancel and Describe; recovery's
// GetExecution passes its own, shorter deadline, because that one is made while a job's
// recovery is waiting on it.
func NewClient(dialTimeout, callTimeout time.Duration, creds Credentials) *Client {
	return &Client{
		dialTimeout: dialTimeout,
		callTimeout: callTimeout,
		creds:       creds,
		conns:       make(map[string]*grpc.ClientConn),
	}
}

// SetDialer replaces connection establishment, for tests.
func (c *Client) SetDialer(d func(target string) (*grpc.ClientConn, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dialer = d
}

func (c *Client) conn(address string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok := c.conns[address]; ok {
		return cc, nil
	}
	dial := c.dialer
	if dial == nil {
		dial = func(target string) (*grpc.ClientConn, error) {
			transport := c.creds.Transport
			if transport == nil {
				transport = insecure.NewCredentials()
			}
			opts := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
			if c.creds.BearerToken != "" {
				opts = append(opts, grpc.WithUnaryInterceptor(bearerInterceptor(c.creds.BearerToken)))
			}
			return grpc.NewClient(target, opts...)
		}
	}
	cc, err := dial(address)
	if err != nil {
		return nil, fmt.Errorf("connect to executor at %s: %w", address, err)
	}
	c.conns[address] = cc
	return cc, nil
}

// Forget drops a cached connection, so an executor that was deregistered or reaped does not
// hold one open for the life of the process.
func (c *Client) Forget(address string) {
	c.mu.Lock()
	cc, ok := c.conns[address]
	delete(c.conns, address)
	c.mu.Unlock()
	if ok {
		_ = cc.Close()
	}
}

// Close releases every connection.
func (c *Client) Close() error {
	c.mu.Lock()
	conns := c.conns
	c.conns = make(map[string]*grpc.ClientConn)
	c.mu.Unlock()

	var firstErr error
	for _, cc := range conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// bearerInterceptor attaches the scheduler's credential to every outbound call.
func bearerInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// RunSpec is one dispatch.
type RunSpec struct {
	Address      string
	Tenant       string
	ExecutionKey string
	RunToken     string
	JobName      string
	HandlerKey   string
	Attempt      int
	ScheduledAt  time.Time

	// SilenceDeadline is how long the executor may say nothing before the scheduler treats it
	// as lost; RemainingTimeout is how long the work itself may take.
	//
	// Both are sent as DURATIONS rather than instants. An executor's clock is not the
	// scheduler's, and an instant would be interpreted against whatever the executor's host
	// believes the time is — which for a cap measured in seconds is the difference between a
	// budget and a coin flip.
	SilenceDeadline  time.Duration
	RemainingTimeout time.Duration

	Params map[string]any
}

// Run hands one execution to an executor and classifies the answer.
func (c *Client) Run(ctx context.Context, spec RunSpec) Result {
	cc, err := c.conn(spec.Address)
	if err != nil {
		// Never reaching the executor is the one connection failure that IS provably a
		// non-delivery: no request was sent.
		return Result{Answer: Refused, Code: codes.Unavailable, Err: err}
	}

	params, err := structpb.NewStruct(spec.Params)
	if err != nil {
		// The scheduler built these from the job's stored JSON, so this is a broken
		// definition rather than an executor problem, and retrying it would fail identically.
		return Result{Answer: Refused, Code: codes.InvalidArgument,
			Err: fmt.Errorf("encode params for %s: %w", spec.ExecutionKey, err)}
	}

	req := &gojobv1.RunRequest{
		ExecutionKey:            spec.ExecutionKey,
		RunToken:                spec.RunToken,
		Tenant:                  spec.Tenant,
		JobName:                 spec.JobName,
		HandlerKey:              spec.HandlerKey,
		Attempt:                 int32(spec.Attempt),
		ScheduledAt:             spec.ScheduledAt.Format(time.RFC3339),
		SilenceDeadlineSeconds:  int32(roundUpSeconds(spec.SilenceDeadline)),
		RemainingTimeoutSeconds: int32(roundUpSeconds(spec.RemainingTimeout)),
		Params:                  &gojobv1.JobParams{Values: params},
	}

	callCtx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	resp, err := gojobv1.NewJobExecutorClient(cc).Run(callCtx, req)
	return classifyRun(resp, err)
}

// classifyRun maps a Run reply onto the three outcomes.
//
// The rule is short because the sound version of it has to be: **only an OK response can be a
// refusal**, and only through RunResponse.refused. A gRPC status cannot separate an
// application's refusal from a transport failure that happened AFTER the request was
// delivered — UNAVAILABLE means "draining" when the executor sends it and "the connection
// broke" when the transport does, and the second can occur once the handler is already
// running. Reading that as a refusal releases the job and dispatches a successor beside a live
// handler, which is the one outcome this whole design exists to prevent.
//
// Everything else is therefore Unknown. Unknown never loses work: the execution keeps its
// lease and its recorded target, and either the bounded re-send or recovery resolves it. The
// cost is that an executor which signals draining with a status rather than the field pays one
// re-send cycle. That is the right direction to be wrong in.
func classifyRun(resp *gojobv1.RunResponse, err error) Result {
	if err == nil {
		if resp.GetRefused() {
			return Result{Answer: Refused, Code: codes.OK,
				Err: fmt.Errorf("executor declined: %s", resp.GetRefusalReason())}
		}
		return Result{Answer: Accepted, Code: codes.OK}
	}

	st, ok := status.FromError(err)
	if !ok {
		return Result{Answer: Unknown, Code: codes.Unknown, Err: err}
	}

	if st.Code() == codes.AlreadyExists {
		// The executor already holds this execution key. WHICH attempt it holds decides
		// whether this is an acceptance, and only the token detail says — so a reply without
		// one is Unknown, not Accepted. Recording an attempt as running while the executor is
		// in fact running a different, already-fenced one invents a start that never happened
		// and then waits for a result that belongs to someone else.
		tok := heldToken(st)
		if tok == "" {
			return Result{Answer: Unknown, Code: st.Code(),
				Err: fmt.Errorf("ALREADY_EXISTS carried no held token, so which attempt is "+
					"running cannot be established: %w", err)}
		}
		return Result{Answer: Accepted, Code: st.Code(), HeldToken: tok, Err: err}
	}

	return Result{Answer: Unknown, Code: st.Code(), Err: err}
}

// heldToken pulls the run token out of an ALREADY_EXISTS status detail.
func heldToken(st *status.Status) string {
	for _, d := range st.Details() {
		if held, ok := d.(*gojobv1.ExecutionHeld); ok {
			return held.GetHeldRunToken()
		}
	}
	return ""
}

// Cancel asks an executor to stop. Acknowledgement means the stop was SIGNALLED, not that
// work has ceased — the executor still reports a result when the work actually ends, and the
// execution keeps its lease until it does.
func (c *Client) Cancel(ctx context.Context, address, tenant, executionKey, runToken, reason string) error {
	cc, err := c.conn(address)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	_, err = gojobv1.NewJobExecutorClient(cc).Cancel(callCtx, &gojobv1.CancelRequest{
		Tenant:       tenant,
		ExecutionKey: executionKey,
		RunToken:     runToken,
		Reason:       reason,
	})
	if err != nil && status.Code(err) == codes.NotFound {
		// The executor does not have it. Nothing to stop, and not an error worth surfacing:
		// the row is fenced either way.
		return nil
	}
	return err
}

// Reconciliation is what recovery phase 2 learned.
type Reconciliation struct {
	// State is RUNNING, FINISHED, or unset when the executor could not answer.
	State gojobv1.ExecutionState

	// RunToken is the attempt the executor is describing. An answer about a DIFFERENT token
	// describes a different attempt and must be discarded.
	RunToken string

	Outcome *gojobv1.ExecutionOutcome
	Message string

	// Reachable is false when the deadline elapsed, the call failed, or the executor said
	// NOT_FOUND. All three mean the same thing to the protocol: the attempt is unknown, and
	// nobody can say whether the handler ran.
	Reachable bool
}

// GetExecution asks an executor what happened to work the scheduler lost track of.
//
// The deadline is explicit and short, and the call happens OUTSIDE any transaction. An RPC to
// a process that may be wedged must never be made while holding a row lock: a connection that
// stays open and never answers would pin job_state and job_execution indefinitely, blocking
// completion, cancellation and every other recovery for that job.
func (c *Client) GetExecution(ctx context.Context, address, tenant, executionKey string, deadline time.Duration) Reconciliation {
	cc, err := c.conn(address)
	if err != nil {
		return Reconciliation{}
	}
	callCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	resp, err := gojobv1.NewJobExecutorClient(cc).GetExecution(callCtx, &gojobv1.GetExecutionRequest{
		Tenant:       tenant,
		ExecutionKey: executionKey,
	})
	if err != nil {
		return Reconciliation{}
	}
	return Reconciliation{
		State:     resp.GetState(),
		RunToken:  resp.GetRunToken(),
		Outcome:   resp.GetOutcome(),
		Message:   resp.GetMessage(),
		Reachable: true,
	}
}

// ErrContractProbe means an executor failed the registration-time probe.
var ErrContractProbe = errors.New("gojob: executor failed the contract probe")

// Describe probes an executor at registration, before any work is routed to it.
//
// This is the third of the contract's four enforcement layers, and the only one that runs
// against the process actually deployed rather than against the code that was built. The
// distinction it exists to draw is between two failures that a hand-written HTTP executor
// cannot tell apart:
//
//   - UNIMPLEMENTED means the method is missing — the executor was generated from the
//     contract but did not implement it, so nothing it declares can be trusted;
//   - NOT_FOUND from a LATER call means the method exists and the thing asked about does not,
//     which is an ordinary answer.
//
// Registering an executor that fails this probe would put a process in the routing pool that
// cannot be reconciled with, which is precisely the state recovery has no answer for.
func (c *Client) Describe(ctx context.Context, address, tenant string) (*gojobv1.DescribeResponse, error) {
	cc, err := c.conn(address)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	resp, err := gojobv1.NewJobExecutorClient(cc).Describe(callCtx, &gojobv1.DescribeRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return nil, fmt.Errorf("%w: %s does not implement Describe", ErrContractProbe, address)
		}
		return nil, fmt.Errorf("%w: Describe at %s: %v", ErrContractProbe, address, err)
	}
	if resp.GetContractVersion() != ContractVersion {
		return nil, fmt.Errorf("%w: %s speaks contract version %q, this scheduler speaks %q",
			ErrContractProbe, address, resp.GetContractVersion(), ContractVersion)
	}
	if len(resp.GetHandlerKeys()) == 0 {
		return nil, fmt.Errorf("%w: %s declares no handlers", ErrContractProbe, address)
	}

	// Describe alone is not enough. It is the method an executor is most likely to have
	// implemented, and an executor that has ONLY it registers, receives work, and then fails
	// its first recovery with UNIMPLEMENTED — which is the state recovery has no answer for,
	// discovered in production, on a job that is already running.
	//
	// So GetExecution is probed too, with a key nothing can hold. NOT_FOUND is the right
	// answer and proves the method is there; UNIMPLEMENTED proves it is not. The two are
	// distinguishable, which is the whole reason this contract is gRPC rather than
	// hand-written HTTP.
	probeCtx, probeCancel := context.WithTimeout(ctx, c.callTimeout)
	defer probeCancel()

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("%w: generating a probe key: %v", ErrContractProbe, err)
	}
	// The tenant is carried, because execution identity is tenant-scoped everywhere else and a
	// conforming multi-tenant executor is entitled to require it. Omitting it would have such
	// an executor answer INVALID_ARGUMENT and be refused for implementing the contract
	// correctly.
	_, err = gojobv1.NewJobExecutorClient(cc).GetExecution(probeCtx, &gojobv1.GetExecutionRequest{
		Tenant:       tenant,
		ExecutionKey: "probe-" + hex.EncodeToString(nonce),
	})
	switch status.Code(err) {
	case codes.NotFound:
		// Implemented, and correctly says it has never heard of this. What we wanted.
	case codes.Unimplemented:
		return nil, fmt.Errorf("%w: %s does not implement GetExecution, so a lost execution "+
			"could never be reconciled with it", ErrContractProbe, address)
	case codes.OK:
		return nil, fmt.Errorf("%w: %s claims to be running an execution key that has never "+
			"existed", ErrContractProbe, address)
	default:
		return nil, fmt.Errorf("%w: probing GetExecution at %s: %v", ErrContractProbe, address, err)
	}

	return resp, nil
}

// ContractVersion is the executor contract this scheduler speaks. An executor reporting
// anything else is refused at registration rather than at its first incompatible call.
const ContractVersion = "1"

// roundUpSeconds never rounds a positive duration down to zero, because zero means "no budget"
// on the wire and would be read by the executor as an instruction to give up immediately.
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
