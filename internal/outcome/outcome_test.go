package outcome

import (
	"testing"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
)

// What an executor reports decides whether the row is terminal or comes back for another
// attempt, and two of the four dispositions are deliberately NOT retried. Getting either
// wrong is quiet: a non-retried failure looks like a scheduler that gave up, and a retried
// timeout turns one slow run into an afternoon of them.
func TestClassify(t *testing.T) {
	cases := []struct {
		name        string
		disposition gojobv1.Disposition
		failureKind string
		wantStatus  gojob.Status
		wantRetry   bool
	}{
		{"success", gojobv1.Disposition_DISPOSITION_SUCCESS, "", gojob.StatusSuccess, false},

		// The handler confirmed it stopped — the only path to `cancelled` with side effects
		// actually accounted for.
		{"stopped", gojobv1.Disposition_DISPOSITION_STOPPED, "", gojob.StatusCancelled, false},

		// A job that burned its whole runtime budget will most likely burn it again.
		{"timed out", gojobv1.Disposition_DISPOSITION_TIMED_OUT, "", gojob.StatusDead, false},

		{"plain failure retries", gojobv1.Disposition_DISPOSITION_FAILED, "io", gojob.StatusDead, true},
		{"failure with no kind retries", gojobv1.Disposition_DISPOSITION_FAILED, "", gojob.StatusDead, true},

		// An executor that does not say what happened has not said it worked.
		{"unspecified is a failure", gojobv1.Disposition_DISPOSITION_UNSPECIFIED, "", gojob.StatusDead, true},

		// Retrying a validation failure burns the budget on an input that cannot become valid,
		// so the row reaches dead with a budget-exhausted reason that hides the real cause.
		{"permanent", gojobv1.Disposition_DISPOSITION_FAILED, "permanent", gojob.StatusDead, false},
		{"namespaced permanent", gojobv1.Disposition_DISPOSITION_FAILED, "permanent.validation", gojob.StatusDead, false},

		// The convention is the exact word or a dotted namespace, not a prefix match: a kind
		// that merely starts with those letters must not be swept in.
		{"permanently_slow is not permanent", gojobv1.Disposition_DISPOSITION_FAILED, "permanently_slow", gojob.StatusDead, true},
		{"permanence is not permanent", gojobv1.Disposition_DISPOSITION_FAILED, "permanence", gojob.StatusDead, true},
	}

	for _, c := range cases {
		got := Classify(c.disposition, c.failureKind)
		if got.Status != c.wantStatus {
			t.Errorf("%s: status = %q, want %q", c.name, got.Status, c.wantStatus)
		}
		if got.Retryable != c.wantRetry {
			t.Errorf("%s: retryable = %v, want %v", c.name, got.Retryable, c.wantRetry)
		}
	}
}

// Success must never be retryable, whatever an executor puts in failure_kind. A conforming
// executor would not set one, and a non-conforming one must not be able to turn a completed
// run into a second run.
func TestSuccessIsNeverRetried(t *testing.T) {
	for _, kind := range []string{"", "io", "permanent", "permanent.validation"} {
		got := Classify(gojobv1.Disposition_DISPOSITION_SUCCESS, kind)
		if got.Retryable || got.Status != gojob.StatusSuccess {
			t.Errorf("failure_kind %q turned a success into %v/retry=%v", kind, got.Status, got.Retryable)
		}
	}
}

// Every non-retryable decision must name HOW it became terminal, because `cancelled` and
// `dead` alone cannot tell an operator whether side effects were verified — which is the
// question that matters most for exactly the jobs where guessing is expensive.
func TestTerminalDecisionsCarryAReason(t *testing.T) {
	for _, d := range []gojobv1.Disposition{
		gojobv1.Disposition_DISPOSITION_SUCCESS,
		gojobv1.Disposition_DISPOSITION_STOPPED,
		gojobv1.Disposition_DISPOSITION_TIMED_OUT,
	} {
		if got := Classify(d, ""); got.TerminalReason == "" {
			t.Errorf("%v produced a terminal decision with no reason", d)
		}
	}
	if got := Classify(gojobv1.Disposition_DISPOSITION_FAILED, "permanent"); got.TerminalReason == "" {
		t.Error("a permanent failure produced a terminal decision with no reason")
	}
}

// Every decision must record an attempt outcome. An attempt row with no outcome is a hole in
// the history a redelivered result is answered from.
func TestEveryDecisionRecordsAnAttemptOutcome(t *testing.T) {
	for _, d := range []gojobv1.Disposition{
		gojobv1.Disposition_DISPOSITION_UNSPECIFIED,
		gojobv1.Disposition_DISPOSITION_SUCCESS,
		gojobv1.Disposition_DISPOSITION_FAILED,
		gojobv1.Disposition_DISPOSITION_STOPPED,
		gojobv1.Disposition_DISPOSITION_TIMED_OUT,
	} {
		if got := Classify(d, ""); got.Attempt == "" {
			t.Errorf("%v produced no attempt outcome", d)
		}
	}
}
