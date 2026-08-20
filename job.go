// Package gojob is a multi-tenant, database-durable job scheduling platform.
//
// The scheduler decides what runs, when, for which tenant, with exactly one owner at a
// time. Applications register as executors over the gRPC contract in
// proto/gojob/v1/executor.proto, in any language, and receive dispatches with parameters.
//
// This package holds the types shared across the scheduler; the engine lives under
// internal/. Executors do not import it — they generate from the .proto.
package gojob

import (
	"fmt"
	"time"
)

// ScheduleKind distinguishes the two shapes of scheduled work. They are modelled separately
// because they are different things: a cron job's identity is a fire instant, a poller's is
// a repeating pass. Forcing either into the other's model produces lost runs or millions of
// rows recording that nothing happened.
type ScheduleKind string

const (
	// ScheduleCron materializes one durable execution per fire instant.
	ScheduleCron ScheduleKind = "CRON"

	// ScheduleFixedDelay dispatches one pass at a time, the next scheduled a fixed delay
	// after the previous one COMPLETES — which is what stops a slow pass piling up on its
	// successor the way a cron job would.
	ScheduleFixedDelay ScheduleKind = "FIXED_DELAY"
)

// ConcurrencyPolicy decides what happens when a job is due while already running.
//
// There are exactly two. PARALLEL is absent by design: the job state row holds a single
// active holder, and heartbeat, completion, retry and recovery all guard on that row's token
// and epoch, so concurrent executions of one job would have no defined completion protocol.
type ConcurrencyPolicy string

const (
	// PolicyQueue leaves the execution ready and defers it by a bounded backoff. Default.
	PolicyQueue ConcurrencyPolicy = "QUEUE"

	// PolicyForbid marks the occurrence skipped. It never applies to a manual trigger:
	// silently discarding an operator's explicit request is the opposite of what pressing
	// the button means.
	PolicyForbid ConcurrencyPolicy = "FORBID"
)

// MisfirePolicy decides what happens to fire instants missed while nothing was running.
// Unbounded replay is not offered — an hour of downtime must not become an hour of catch-up
// executions arriving at once, which turns a recovery into a second incident.
type MisfirePolicy string

const (
	// MisfireSkip advances to the first future fire and records how many were missed.
	MisfireSkip MisfirePolicy = "SKIP"

	// MisfireFireOnce creates one catch-up execution for the latest missed fire.
	MisfireFireOnce MisfirePolicy = "FIRE_ONCE"
)

// TriggerType records why an execution exists.
type TriggerType string

const (
	TriggerCron   TriggerType = "cron"
	TriggerManual TriggerType = "manual"
	TriggerPoll   TriggerType = "poll"
)

// Status is an execution's position in the state machine.
type Status string

const (
	// StatusReady is available to claim at or after available_at. It is the single
	// available state for both a first attempt and a retry, so the claim predicate is an
	// equality rather than an IN — which lets one index satisfy the filter and the ordering
	// without a filesort.
	StatusReady Status = "ready"

	// StatusDispatching is claimed and handed to an executor, acceptance not yet known.
	// Claiming is not attempting: an executor may refuse for capacity, and charging a retry
	// budget for that would march a job to dead with no code having run.
	StatusDispatching Status = "dispatching"

	// StatusRunning is accepted by an executor and executing.
	StatusRunning Status = "running"

	// StatusCancelRequested has been asked to stop and still holds its lease. Releasing the
	// slot on the request rather than on cessation is how two handlers end up overlapping.
	StatusCancelRequested Status = "cancel_requested"

	StatusSuccess   Status = "success"
	StatusDead      Status = "dead"
	StatusCancelled Status = "cancelled"
	StatusSkipped   Status = "skipped"
)

// Terminal reports whether a status admits no further transitions.
func (s Status) Terminal() bool {
	switch s {
	case StatusSuccess, StatusDead, StatusCancelled, StatusSkipped:
		return true
	}
	return false
}

// TerminalReason says HOW a terminal state was reached. Status alone cannot: `cancelled`
// does not distinguish a handler that confirmed it stopped from an attempt that was merely
// fenced with its side effects unverified — and that distinction matters most for exactly
// the jobs where guessing is expensive.
type TerminalReason string

const (
	ReasonHandlerConfirmed TerminalReason = "handler_confirmed"
	ReasonFenced           TerminalReason = "fenced"
	ReasonTimeout          TerminalReason = "timeout"
	ReasonBudgetExhausted  TerminalReason = "budget_exhausted"
	ReasonPermanentFailure TerminalReason = "permanent_failure"
	ReasonRetired          TerminalReason = "retired"
	// ReasonConcurrencySkipped is an occurrence FORBID declined because the previous one was
	// still running.
	//
	// It replaces a ReasonOperator that this was the only writer of — "operator" reads as "a
	// human did this", the opposite of the truth, and whether a skipped run was policy or a
	// person is the only thing anyone looking at one wants to know. The old constant is gone
	// rather than left unused: a reason with no writer and a plausible name is how the next
	// terminal path gets mislabelled too.
	ReasonConcurrencySkipped TerminalReason = "concurrency_skipped"
)

// AttemptOutcome is one attempt's recorded result. `unknown` exists because it is the
// honest answer when an executor restarted and can no longer say what happened — recording
// it as failure would claim knowledge nobody has.
type AttemptOutcome string

const (
	AttemptSuccess AttemptOutcome = "success"
	AttemptFailed  AttemptOutcome = "failed"
	AttemptUnknown AttemptOutcome = "unknown"
	AttemptFenced  AttemptOutcome = "fenced"
)

// Definition is a job's configuration: the operator-editable half, stored in
// job_definition. It is created only through the admin API — the scheduler holds no handler
// code and so has no registry to generate it from.
type Definition struct {
	JobName    string
	HandlerKey string

	// ExecutorGroup empty means any group declaring the handler. Naming one distinguishes
	// two groups declaring the same handler: a partial rollout, or two configurations of
	// one service.
	ExecutorGroup string

	ScheduleKind ScheduleKind
	ScheduleExpr string

	Enabled bool
	Retired bool

	Concurrency ConcurrencyPolicy
	Misfire     MisfirePolicy

	MaxAttempts   int
	MaxRecoveries int
	Lease         time.Duration
	Timeout       time.Duration

	Params      []byte // JSON; merged with per-trigger overrides at execution creation
	Description string

	Version   int64 // optimistic CAS for edits
	UpdatedBy string
}

// Delay returns the configured pause between passes for a fixed-delay job.
//
// It returns an error rather than zero for a malformed expression. Zero is a legal-looking
// duration that means "no pause", so swallowing a parse failure would turn a corrupt row into
// a poller running flat out against its source table — a much worse outcome than a scheduler
// that refuses to schedule it and says why. The admin API validates the expression on write;
// this is the second line of defence for a row that reached the table another way.
func (d Definition) Delay() (time.Duration, error) {
	if d.ScheduleKind != ScheduleFixedDelay {
		return 0, nil
	}
	ms, err := parseInt(d.ScheduleExpr)
	if err != nil {
		return 0, fmt.Errorf("gojob: job %q has schedule_kind FIXED_DELAY but schedule_expr %q is not a number of milliseconds",
			d.JobName, d.ScheduleExpr)
	}
	if ms <= 0 {
		return 0, fmt.Errorf("gojob: job %q has a fixed delay of %d ms; a poller with no pause would run flat out",
			d.JobName, ms)
	}
	return time.Duration(ms) * time.Millisecond, nil
}
