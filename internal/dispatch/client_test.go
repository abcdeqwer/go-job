package dispatch

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
)

// The code table is the whole of this package's judgement, and every row of it decides
// whether an attempt is charged. Getting one wrong is invisible until a job either marches to
// `dead` without running or retries for ever.
func TestClassifyRun(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Answer
	}{
		{"nil is acceptance", nil, Accepted},
		{"explicit OK", status.Error(codes.OK, ""), Accepted},

		// Provable non-delivery. No attempt is charged and another instance is tried.
		{"at capacity", status.Error(codes.ResourceExhausted, "busy"), Refused},
		{"shutting down", status.Error(codes.Unavailable, "draining"), Refused},
		{"unknown handler", status.Error(codes.FailedPrecondition, "no such handler"), Refused},

		// The executor rejected the request itself. Retrying against the same fleet fails
		// identically, so it is refused rather than looped — but the caller must alert.
		{"bad request", status.Error(codes.InvalidArgument, "bad params"), Refused},
		{"unauthenticated", status.Error(codes.Unauthenticated, ""), Refused},
		{"permission denied", status.Error(codes.PermissionDenied, ""), Refused},
		{"method missing", status.Error(codes.Unimplemented, ""), Refused},

		// Outcome genuinely unknown: the request may or may not have arrived. Never Refused,
		// because releasing the job while an executor may be running it is the one mistake
		// this classification exists to avoid.
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, ""), Unknown},
		{"canceled", status.Error(codes.Canceled, ""), Unknown},
		{"internal", status.Error(codes.Internal, ""), Unknown},
		{"aborted", status.Error(codes.Aborted, ""), Unknown},
		{"unknown", status.Error(codes.Unknown, ""), Unknown},
		{"data loss", status.Error(codes.DataLoss, ""), Unknown},
		{"not a status error", errors.New("raw transport failure"), Unknown},

		// Already held is an acceptance at this layer; WHICH attempt it is belongs to the
		// caller, which is the only party that knows the token it sent.
		{"already exists", status.Error(codes.AlreadyExists, "held"), Accepted},
	}

	for _, c := range cases {
		got := classifyRun(c.err)
		if got.Answer != c.want {
			t.Errorf("%s: classifyRun = %v, want %v", c.name, got.Answer, c.want)
		}
	}
}

// Any gRPC code the table does not name must fall to Unknown, not to Refused. This is the
// property that keeps a future code — or one from a proxy in the middle — from silently
// releasing a job an executor is running.
func TestUnnamedCodesAreUnknown(t *testing.T) {
	named := map[codes.Code]bool{
		codes.OK: true, codes.AlreadyExists: true,
		codes.ResourceExhausted: true, codes.Unavailable: true, codes.FailedPrecondition: true,
		codes.InvalidArgument: true, codes.PermissionDenied: true,
		codes.Unauthenticated: true, codes.Unimplemented: true,
	}
	for c := codes.Code(0); c <= codes.Unauthenticated; c++ {
		if named[c] {
			continue
		}
		if got := classifyRun(status.Error(c, "")); got.Answer != Unknown {
			t.Errorf("code %v classified %v; unnamed codes must be Unknown so a job is never "+
				"released while an executor may be running it", c, got.Answer)
		}
	}
}

// The held token must survive classification: it is what tells a caller whether ALREADY_EXISTS
// is its own re-send being adopted or a collision with an attempt that was already fenced.
func TestAlreadyExistsCarriesTheHeldToken(t *testing.T) {
	st, err := status.New(codes.AlreadyExists, "held").
		WithDetails(&gojobv1.ExecutionHeld{HeldRunToken: "tok-A"})
	if err != nil {
		t.Fatalf("build status: %v", err)
	}

	got := classifyRun(st.Err())
	if got.Answer != Accepted {
		t.Fatalf("Answer = %v, want Accepted", got.Answer)
	}
	if got.HeldToken != "tok-A" {
		t.Fatalf("HeldToken = %q, want %q", got.HeldToken, "tok-A")
	}
}

// ALREADY_EXISTS without a detail is still an acceptance, with no token to compare. A caller
// that cannot tell which attempt is held must treat it conservatively; losing the detail must
// not turn into losing the classification.
func TestAlreadyExistsWithoutDetail(t *testing.T) {
	got := classifyRun(status.Error(codes.AlreadyExists, "held"))
	if got.Answer != Accepted || got.HeldToken != "" {
		t.Fatalf("got %v/%q, want Accepted with no token", got.Answer, got.HeldToken)
	}
}

// A budget must never round down to zero on the wire: zero means "no budget", which an
// executor reads as an instruction to give up before starting.
func TestRoundUpSeconds(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want int
	}{
		{0, 0},
		{-time.Second, 0},
		{time.Nanosecond, 1},
		{500 * time.Millisecond, 1},
		{time.Second, 1},
		{1500 * time.Millisecond, 2},
		{90 * time.Second, 90},
		{2 * time.Hour, 7200},
	}
	for _, c := range cases {
		if got := roundUpSeconds(c.in); got != c.want {
			t.Errorf("roundUpSeconds(%s) = %d, want %d", c.in, got, c.want)
		}
	}
}
