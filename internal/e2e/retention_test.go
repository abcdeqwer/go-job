package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/store"
)

// Retention is exercised against MySQL because its safety depends on transaction locks,
// SKIP LOCKED, LIMIT and DATETIME cutoff semantics. A mock accepting the query proves none of
// those. The cases pin the status split, exact boundaries, manual success handling, batching,
// non-terminal safety and attempt-history cleanup together.
func TestExecutionRetention(t *testing.T) {
	h := setup(t)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock := gojob.NewFixedClock(now, time.UTC)
	st := store.New(h.db, clock)

	type execution struct {
		key      string
		status   string
		trigger  string
		finished time.Time
		deleted  bool
	}
	cases := []execution{
		{"success-expired", "success", "cron", now.Add(-15*24*time.Hour - time.Second), true},
		{"manual-success-expired", "success", "manual", now.Add(-16 * 24 * time.Hour), true},
		{"success-boundary", "success", "cron", now.Add(-15 * 24 * time.Hour), false},
		{"dead-expired", "dead", "cron", now.Add(-30*24*time.Hour - time.Second), true},
		{"cancelled-expired", "cancelled", "cron", now.Add(-31 * 24 * time.Hour), true},
		{"skipped-expired", "skipped", "cron", now.Add(-32 * 24 * time.Hour), true},
		{"dead-boundary", "dead", "cron", now.Add(-30 * 24 * time.Hour), false},
		// Even a nonsensical old finished_at cannot make live work eligible: status is the guard.
		{"ready-never-delete", "ready", "cron", now.Add(-365 * 24 * time.Hour), false},
	}

	for i, c := range cases {
		manualFirst := 0
		if c.trigger == "manual" {
			manualFirst = 1
		}
		_, err := h.db.Exec(`
			INSERT INTO job_execution
			    (execution_key, job_name, trigger_type, manual_first,
			     scheduled_at, available_at, status, max_attempts, max_recoveries,
			     finished_at, created_at, updated_at)
			VALUES (?, 'retention-job', ?, ?, ?, ?, ?, 3, 3, ?, ?, ?)`,
			c.key, c.trigger, manualFirst, c.finished, c.finished, c.status,
			c.finished, c.finished, c.finished)
		if err != nil {
			t.Fatalf("insert execution %s: %v", c.key, err)
		}
		_, err = h.db.Exec(`
			INSERT INTO job_execution_attempt
			    (execution_key, run_token, attempt_no, finished_at, outcome)
			VALUES (?, ?, 1, ?, 'success')`, c.key, fmt.Sprintf("%08d-0000-0000-0000-000000000000", i), c.finished)
		if err != nil {
			t.Fatalf("insert attempt %s: %v", c.key, err)
		}
	}

	ctx := context.Background()
	wantBatches := []int64{2, 2, 1, 0}
	for i, want := range wantBatches {
		got, err := st.ReapExecutions(ctx, 15*24*time.Hour, 30*24*time.Hour, 2)
		if err != nil {
			t.Fatalf("retention batch %d: %v", i+1, err)
		}
		if got != want {
			t.Fatalf("retention batch %d deleted %d rows, want %d", i+1, got, want)
		}
	}

	for _, c := range cases {
		var executions, attempts int
		if err := h.db.QueryRow(`SELECT COUNT(*) FROM job_execution WHERE execution_key = ?`, c.key).
			Scan(&executions); err != nil {
			t.Fatalf("count execution %s: %v", c.key, err)
		}
		if err := h.db.QueryRow(`SELECT COUNT(*) FROM job_execution_attempt WHERE execution_key = ?`, c.key).
			Scan(&attempts); err != nil {
			t.Fatalf("count attempts %s: %v", c.key, err)
		}
		want := 1
		if c.deleted {
			want = 0
		}
		if executions != want || attempts != want {
			t.Errorf("%s: execution=%d attempts=%d, want both %d", c.key, executions, attempts, want)
		}
	}
}

func TestExecutionRetentionRejectsUnsafeConfiguration(t *testing.T) {
	st := store.New(nil, gojob.NewFixedClock(time.Now(), time.UTC))
	for _, c := range []struct {
		name           string
		success, other time.Duration
		limit          int
	}{
		{"zero success window", 0, time.Hour, 1},
		{"zero other window", time.Hour, 0, 1},
		{"zero batch", time.Hour, time.Hour, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := st.ReapExecutions(context.Background(), c.success, c.other, c.limit); err == nil {
				t.Fatal("unsafe retention configuration was accepted")
			}
		})
	}
}
