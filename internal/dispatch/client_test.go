package dispatch

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
)

// Only an OK response carrying refused=true may release a job. This is the whole of this
// package's judgement, and getting it wrong in the permissive direction runs a job twice.
func TestClassifyRun(t *testing.T) {
	accepted := &gojobv1.RunResponse{}
	declined := &gojobv1.RunResponse{Refused: true, RefusalReason: "at capacity"}

	cases := []struct {
		name string
		resp *gojobv1.RunResponse
		err  error
		want Answer
	}{
		{"plain OK is acceptance", accepted, nil, Accepted},
		{"OK with refused is the ONLY refusal", declined, nil, Refused},

		// A status cannot prove non-delivery: the executor may have taken the work and the
		// response been lost on the way back. Every one of these is Unknown, which keeps the
		// lease and lets the re-send or recovery resolve it.
		{"draining", nil, status.Error(codes.Unavailable, "draining"), Unknown},
		{"at capacity by status", nil, status.Error(codes.ResourceExhausted, "busy"), Unknown},
		{"unknown handler", nil, status.Error(codes.FailedPrecondition, "no handler"), Unknown},
		{"bad request", nil, status.Error(codes.InvalidArgument, "bad params"), Unknown},
		{"unauthenticated", nil, status.Error(codes.Unauthenticated, ""), Unknown},
		{"method missing", nil, status.Error(codes.Unimplemented, ""), Unknown},
		{"deadline exceeded", nil, status.Error(codes.DeadlineExceeded, ""), Unknown},
		{"canceled", nil, status.Error(codes.Canceled, ""), Unknown},
		{"internal", nil, status.Error(codes.Internal, ""), Unknown},
		{"aborted", nil, status.Error(codes.Aborted, ""), Unknown},
		{"data loss", nil, status.Error(codes.DataLoss, ""), Unknown},
		{"not a status error", nil, errors.New("raw transport failure"), Unknown},

		// Without the token detail, which attempt the executor holds is unestablished, so the
		// dispatch cannot be recorded as accepted.
		{"already exists, no token", nil, status.Error(codes.AlreadyExists, "held"), Unknown},
	}

	for _, c := range cases {
		got := classifyRun(c.resp, c.err)
		if got.Answer != c.want {
			t.Errorf("%s: classifyRun = %v, want %v", c.name, got.Answer, c.want)
		}
	}
}

// No status code may ever produce Refused. This is the property, checked across the whole
// code space rather than against a list that can drift.
func TestNoStatusCodeCanRelease(t *testing.T) {
	for c := codes.Code(1); c <= codes.Unauthenticated; c++ {
		if got := classifyRun(nil, status.Error(c, "")); got.Answer == Refused {
			t.Errorf("code %v classified Refused; only an OK response with refused=true may "+
				"release a job, because a status cannot prove the request was not delivered", c)
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

	got := classifyRun(nil, st.Err())
	if got.Answer != Accepted {
		t.Fatalf("Answer = %v, want Accepted", got.Answer)
	}
	if got.HeldToken != "tok-A" {
		t.Fatalf("HeldToken = %q, want %q", got.HeldToken, "tok-A")
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
