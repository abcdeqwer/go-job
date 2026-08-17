package server

import (
	"context"
	"errors"
	"testing"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/store"
)

// fakeReader returns one row, standing in for the execution as it is AFTER an adoption.
type fakeReader struct {
	row store.Stale
	err error
}

func (f fakeReader) ExecutionByKey(context.Context, string) (store.Stale, error) {
	return f.row, f.err
}

// A write fenced by an adoption of THIS attempt must be retried against the new epoch; a write
// fenced by anything else must not.
//
// The interleaving cannot be produced by calling the pieces in order — by the time a test
// bumps the epoch and then makes the call, the server reads the new epoch and nothing is
// stale. It has to be produced from inside the write, which is what the failing closure here
// does: it fails the first attempt exactly as the database would when recovery adopted the
// row between the read and the write.
func TestRetryPastAdoption(t *testing.T) {
	const key, token = "job:1", "tok-a"
	base := store.Holder{ExecutionKey: key, RunToken: token, FenceEpoch: 4, ExecutionID: 9}

	t.Run("adoption of this attempt is retried at the new epoch", func(t *testing.T) {
		var seen []int64
		write := func(h store.Holder) error {
			seen = append(seen, h.FenceEpoch)
			if h.FenceEpoch != 5 {
				return gojob.ErrFenced
			}
			return nil
		}
		// The row after adoption: same token, epoch moved.
		r := fakeReader{row: store.Stale{RunToken: token, FenceEpoch: 5}}

		if err := (&Server{}).retryPastAdoption(context.Background(), r, base, write); err != nil {
			t.Fatalf("a write fenced by an adoption of its own attempt was not retried: %v", err)
		}
		if len(seen) != 2 || seen[0] != 4 || seen[1] != 5 {
			t.Fatalf("epochs attempted = %v, want [4 5]", seen)
		}
	})

	t.Run("a different attempt is a real fence", func(t *testing.T) {
		calls := 0
		write := func(store.Holder) error { calls++; return gojob.ErrFenced }
		// A NEW attempt owns the row: different token. Retrying would apply this attempt's
		// result to somebody else's run.
		r := fakeReader{row: store.Stale{RunToken: "tok-b", FenceEpoch: 5}}

		if err := (&Server{}).retryPastAdoption(context.Background(), r, base, write); !errors.Is(err, gojob.ErrFenced) {
			t.Fatalf("err = %v, want ErrFenced", err)
		}
		if calls != 1 {
			t.Fatalf("write called %d times; a superseded attempt must not be retried", calls)
		}
	})

	t.Run("an unmoved epoch is a real fence", func(t *testing.T) {
		calls := 0
		write := func(store.Holder) error { calls++; return gojob.ErrFenced }
		// Same token, same epoch: the refusal was about something else — a terminal status,
		// or an elapsed runtime cap — and retrying it would spin, not recover.
		r := fakeReader{row: store.Stale{RunToken: token, FenceEpoch: 4}}

		if err := (&Server{}).retryPastAdoption(context.Background(), r, base, write); !errors.Is(err, gojob.ErrFenced) {
			t.Fatalf("err = %v, want ErrFenced", err)
		}
		if calls != 1 {
			t.Fatalf("write called %d times, want 1", calls)
		}
	})

	t.Run("a failing re-read leaves the original refusal standing", func(t *testing.T) {
		write := func(store.Holder) error { return gojob.ErrFenced }
		r := fakeReader{err: errors.New("database is gone")}

		if err := (&Server{}).retryPastAdoption(context.Background(), r, base, write); !errors.Is(err, gojob.ErrFenced) {
			t.Fatalf("err = %v, want the original ErrFenced rather than the read error", err)
		}
	})

	t.Run("a non-fence error is returned as is", func(t *testing.T) {
		boom := errors.New("boom")
		calls := 0
		write := func(store.Holder) error { calls++; return boom }
		r := fakeReader{row: store.Stale{RunToken: token, FenceEpoch: 5}}

		if err := (&Server{}).retryPastAdoption(context.Background(), r, base, write); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if calls != 1 {
			t.Fatalf("write called %d times; only a fence is retried", calls)
		}
	})
}

// A result that another writer already recorded is "already recorded", not "aborted".
//
// Recovery can adopt this very attempt and write the outcome it obtained from the executor
// while a callback is between its history check and its own write. The work is safe either
// way — but ABORTED tells the executor its attempt was superseded when its result was in fact
// accepted moments earlier by someone else, which contradicts the idempotency this call
// advertises and sends an operator looking for a failure that did not happen.
func TestRetryPastAdoptionDoesNotMaskARecordedResult(t *testing.T) {
	// The shape is checked here at the seam that decides it; the full path is exercised by
	// ReportResult, which needs a store.
	base := store.Holder{ExecutionKey: "job:1", RunToken: "tok-a", FenceEpoch: 4}

	// A fence that stays a fence: the row moved to a different attempt.
	calls := 0
	write := func(store.Holder) error { calls++; return gojob.ErrFenced }
	r := fakeReader{row: store.Stale{RunToken: "tok-b", FenceEpoch: 5}}
	if err := (&Server{}).retryPastAdoption(context.Background(), r, base, write); !errors.Is(err, gojob.ErrFenced) {
		t.Fatalf("err = %v, want ErrFenced so the caller can check attempt history", err)
	}
	if calls != 1 {
		t.Fatalf("write called %d times, want 1", calls)
	}
}
