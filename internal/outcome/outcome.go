// Package outcome turns an executor's reported disposition into a scheduler transition.
//
// It exists as its own package because TWO paths reach it: a result reported through the
// scheduler's gRPC service, and a result recovered from an executor that outlived the
// scheduler that dispatched it. Those two must agree exactly — the same disposition has to
// produce the same row — and the way two copies of this logic fail is that one of them gets a
// new case and the other does not, so a job behaves differently depending on whether its
// scheduler happened to survive.
package outcome

import (
	"context"
	"strings"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
	"github.com/abcdeqwer/go-job/internal/store"
)

// Decision is what a reported disposition means for the execution row.
type Decision struct {
	Status         gojob.Status
	TerminalReason gojob.TerminalReason
	Attempt        gojob.AttemptOutcome

	// Retryable sends the row back through Retry, which decides `ready` versus `dead` in SQL
	// against the budget the row actually holds at commit.
	Retryable bool
}

// Classify maps a disposition and failure kind onto a decision.
//
// DISPOSITION_UNSPECIFIED is a failure, not a success. An executor that does not say what
// happened has not said it worked, and the expensive mistake is the other one: recording
// success for a run whose outcome nobody stated.
func Classify(d gojobv1.Disposition, failureKind string) Decision {
	switch d {
	case gojobv1.Disposition_DISPOSITION_SUCCESS:
		return Decision{gojob.StatusSuccess, gojob.ReasonHandlerConfirmed, gojob.AttemptSuccess, false}

	case gojobv1.Disposition_DISPOSITION_STOPPED:
		// The handler confirmed it stopped. This is the only path to `cancelled` where side
		// effects were actually accounted for, as opposed to an attempt that was fenced with
		// its side effects unverified.
		return Decision{gojob.StatusCancelled, gojob.ReasonHandlerConfirmed, gojob.AttemptFailed, false}

	case gojobv1.Disposition_DISPOSITION_TIMED_OUT:
		// Deliberately not retryable: a job that exhausted its entire runtime budget will most
		// likely exhaust it again, and retrying is how one slow run becomes an afternoon of
		// them. An operator who disagrees can retry it explicitly, which is exactly the
		// judgement a human should make and the scheduler should not.
		return Decision{gojob.StatusDead, gojob.ReasonTimeout, gojob.AttemptFailed, false}

	default:
		if Permanent(failureKind) {
			return Decision{gojob.StatusDead, gojob.ReasonPermanentFailure, gojob.AttemptFailed, false}
		}
		return Decision{gojob.StatusDead, "", gojob.AttemptFailed, true}
	}
}

// Permanent decides whether a reported failure kind is worth retrying.
//
// The contract is a naming convention rather than an enum, because failure kinds are the
// executor's vocabulary and an enum here would mean every new kind of permanent failure needs
// a scheduler release. A kind is permanent when it is exactly "permanent" or namespaced under
// "permanent." — so `permanent.validation` and `permanent.not_found` need no change here.
//
// Retrying a validation failure is not merely wasteful: it burns the attempt budget on an
// input that cannot become valid, and the row then reaches `dead` with a budget-exhausted
// reason that hides the actual cause.
func Permanent(kind string) bool {
	return kind == "permanent" || strings.HasPrefix(kind, "permanent.")
}

// Apply writes a reported outcome, choosing completion or retry.
//
// backoffSeconds is only consulted on the retry path; the caller computes it because the
// backoff schedule is a scheduler configuration and not a property of the result.
func Apply(ctx context.Context, st *store.Store, h store.Holder, oc *gojobv1.ExecutionOutcome,
	executorID string, backoffSeconds int) error {
	d := Classify(oc.GetDisposition(), oc.GetFailureKind())

	o := store.Outcome{
		Status:         d.Status,
		TerminalReason: d.TerminalReason,
		AttemptOutcome: d.Attempt,
		FailureKind:    oc.GetFailureKind(),
		ErrorMessage:   oc.GetErrorDetail(),
		ResultSummary:  oc.GetSummary(),
		ExecutorID:     executorID,
	}
	if d.Retryable {
		return st.Retry(ctx, h, o, backoffSeconds)
	}
	return st.Complete(ctx, h, o)
}
