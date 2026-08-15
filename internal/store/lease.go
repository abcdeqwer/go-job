package store

import (
	"context"
	"database/sql"
	"fmt"

	gojob "github.com/abcdeqwer/go-job"
)

// Holder identifies one attempt's ownership. Every ownership-bearing write carries it, which
// is what makes a revived zombie harmless: it holds an epoch that no longer matches, so every
// statement it attempts affects zero rows.
type Holder struct {
	JobName      string
	ExecutionID  int64
	ExecutionKey string
	Owner        string
	RunToken     string
	FenceEpoch   int64
}

// Renew extends both leases in ONE transaction, in the canonical order.
//
// Renewing the execution row first would take the two rows in the opposite order to
// completion and reproduce exactly the deadlock the canonical order exists to prevent.
// Renewing them in separate transactions would let a crash between the two leave one lease
// live and the other expired, with no rule saying which is authoritative.
//
// The lease is NOT a maximum execution time. It bounds how long ownership survives WITHOUT a
// heartbeat, not how long a handler may run: a twenty-hour handler keeps its job for twenty
// hours because this renews every lease/3 throughout. Recovery fires only when the heartbeat
// has actually stopped.
//
// Zero rows from either statement means ownership is lost. The caller must abandon the
// handler context, emit fence_lost, and make no further scheduler or business write from that
// execution.
func (s *Store) Renew(ctx context.Context, h Holder, leaseSeconds int) error {
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE job_state
			SET lease_until = TIMESTAMPADD(SECOND, ?, NOW()), heartbeat_at = NOW(), updated_at = ?
			WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?`,
			leaseSeconds, now, h.JobName, h.RunToken, h.FenceEpoch)
		if err != nil {
			return fmt.Errorf("renew job lock %q: %w", h.JobName, err)
		}
		n, err := affected(res, "renew: job lock")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: job %q lease not held under token %s epoch %d",
				gojob.ErrFenced, h.JobName, h.RunToken, h.FenceEpoch)
		}

		// The status set is deliberate. cancel_requested is in it because a cancelled-but-
		// not-yet-stopped handler must keep renewing: releasing its slot before it has
		// actually stopped is exactly the overlap this protocol exists to prevent.
		//
		// `lease_until >= NOW()` refuses to resurrect an already-expired lease. Once it has
		// lapsed the row belongs to recovery, and letting a slow scheduler renew its way back
		// in would give ownership transfer two implementations that can disagree.
		res, err = tx.ExecContext(ctx, `
			UPDATE job_execution
			SET lease_until = TIMESTAMPADD(SECOND, ?, NOW()), heartbeat_at = NOW(), updated_at = ?
			WHERE id = ?
			  AND status IN ('dispatching', 'running', 'cancel_requested')
			  AND owner_instance = ? AND run_token = ? AND fence_epoch = ?
			  AND lease_until >= NOW()`,
			leaseSeconds, now, h.ExecutionID, h.Owner, h.RunToken, h.FenceEpoch)
		if err != nil {
			return fmt.Errorf("renew execution %d: %w", h.ExecutionID, err)
		}
		n, err = affected(res, "renew: execution")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: execution %d lease not held under token %s epoch %d",
				gojob.ErrFenced, h.ExecutionID, h.RunToken, h.FenceEpoch)
		}
		return nil
	})
}

// ExtendDeadline pushes the silence budget forward on a progress report.
//
// deadline_at and timeout_at are two different bounds and only one of them moves here. The
// deadline bounds how long an executor may say NOTHING; the timeout bounds how long the work
// may take. A twenty-hour handler reporting progress every minute keeps extending the first
// and never touches the second, which is what lets a long job be both supervised and capped.
func (s *Store) ExtendDeadline(ctx context.Context, h Holder, silenceSeconds int) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE job_execution
		SET deadline_at = TIMESTAMPADD(SECOND, ?, NOW()), updated_at = ?
		WHERE id = ?
		  AND status IN ('dispatching', 'running', 'cancel_requested')
		  AND run_token = ? AND fence_epoch = ?
		  AND (timeout_at IS NULL OR timeout_at >= NOW())`,
		silenceSeconds, s.clock.Now(), h.ExecutionID, h.RunToken, h.FenceEpoch)
	if err != nil {
		return fmt.Errorf("extend deadline on execution %d: %w", h.ExecutionID, err)
	}
	n, err := affected(res, "extend deadline")
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: execution %d progress refused under token %s epoch %d",
			gojob.ErrFenced, h.ExecutionID, h.RunToken, h.FenceEpoch)
	}
	return nil
}
