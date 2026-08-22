package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ReapExecutions deletes one bounded batch of terminal execution history.
//
// Success has its own, shorter window because it is the ordinary high-volume outcome. Dead,
// cancelled and skipped rows share the longer audit window. Ready or owned work is absent
// from the predicate rather than excluded indirectly, so adding a new non-terminal status
// cannot accidentally make it eligible for deletion.
//
// Attempt history is deleted first in the same transaction. There is deliberately no foreign
// key between these tables: result redelivery reads attempts on a hot path, and retaining an
// execution must never depend on a cascading DDL choice. Locking the selected execution rows
// makes an operator retry of a dead execution serialize with cleanup rather than lose history.
func (s *Store) ReapExecutions(ctx context.Context, successRetention, otherRetention time.Duration,
	limit int) (int64, error) {
	if successRetention <= 0 {
		return 0, fmt.Errorf("success retention must be positive")
	}
	if otherRetention <= 0 {
		return 0, fmt.Errorf("other retention must be positive")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("retention batch size must be positive")
	}

	now := s.clock.Now()
	successBefore := now.Add(-successRetention)
	otherBefore := now.Add(-otherRetention)
	var deleted int64
	err := s.tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT id, execution_key
			FROM job_execution
			WHERE (status = 'success' AND finished_at < ?)
			   OR (status IN ('dead', 'cancelled', 'skipped') AND finished_at < ?)
			ORDER BY finished_at, id
			LIMIT ?
			FOR UPDATE SKIP LOCKED`, successBefore, otherBefore, limit)
		if err != nil {
			return fmt.Errorf("select execution retention batch: %w", err)
		}
		type candidate struct {
			id  int64
			key string
		}
		candidates := make([]candidate, 0, limit)
		for rows.Next() {
			var c candidate
			if err := rows.Scan(&c.id, &c.key); err != nil {
				rows.Close()
				return fmt.Errorf("scan execution retention batch: %w", err)
			}
			candidates = append(candidates, c)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close execution retention batch: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read execution retention batch: %w", err)
		}

		for _, c := range candidates {
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM job_execution_attempt WHERE execution_key = ?`, c.key); err != nil {
				return fmt.Errorf("delete attempts for execution %q: %w", c.key, err)
			}
			res, err := tx.ExecContext(ctx, `
				DELETE FROM job_execution
				WHERE id = ? AND execution_key = ?
				  AND status IN ('success', 'dead', 'cancelled', 'skipped')`, c.id, c.key)
			if err != nil {
				return fmt.Errorf("delete retained execution %q: %w", c.key, err)
			}
			n, err := affected(res, "delete retained execution")
			if err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("delete retained execution %q affected %d rows, expected 1", c.key, n)
			}
			deleted++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}
