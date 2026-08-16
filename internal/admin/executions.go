package admin

import (
	"net/http"
	"time"

	"github.com/abcdeqwer/go-job/internal/store"
)

func (a *API) listExecutions(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	from, err := parseTime(q.Get("from"))
	if err != nil {
		return err
	}
	to, err := parseTime(q.Get("to"))
	if err != nil {
		return err
	}

	// Filtering, ordering, the total and any export all come from the SAME query. Filtering a
	// page in memory would make the visible rows disagree with the count, and the disagreement
	// grows with the backlog — which is exactly when someone is looking at this screen.
	rows, total, err := st.Executions(r.Context(), store.ExecutionFilter{
		JobName: q.Get("job"),
		Status:  q.Get("status"),
		From:    from,
		To:      to,
		Limit:   atoiDefault(q.Get("limit"), 100),
		Offset:  atoiDefault(q.Get("offset"), 0),
	})
	if err != nil {
		return err
	}

	items := make([]map[string]any, 0, len(rows))
	for _, e := range rows {
		items = append(items, executionJSON(e))
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "items": items})
	return nil
}

func (a *API) getExecution(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	key := r.PathValue("key")

	row, err := st.ExecutionByKey(r.Context(), key)
	if err != nil {
		return err
	}
	attempts, err := st.Attempts(r.Context(), key)
	if err != nil {
		return err
	}

	items := make([]map[string]any, 0, len(attempts))
	for _, at := range attempts {
		items = append(items, map[string]any{
			"run_token":    at.RunToken,
			"attempt_no":   at.AttemptNo,
			"executor_id":  at.ExecutorID,
			"outcome":      at.Outcome,
			"failure_kind": at.FailureKind,
			"summary":      at.Summary,
			"finished_at":  nullTime(at.FinishedAt),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"execution_key":  row.ExecutionKey,
		"job_name":       row.JobName,
		"status":         row.Status,
		"attempt_no":     row.AttemptNo,
		"max_attempts":   row.MaxAttempts,
		"recovery_count": row.RecoveryCount,
		"max_recoveries": row.MaxRecoveries,
		"dispatched_to":  row.DispatchedTo,
		"attempts":       items,
	})
	return nil
}

// executionJSON renders one row, including the label that keeps `cancelled` honest.
func executionJSON(e store.ExecutionView) map[string]any {
	return map[string]any{
		"execution_key":   e.Key,
		"job_name":        e.JobName,
		"trigger_type":    e.TriggerType,
		"status":          e.Status,
		"status_label":    statusLabel(e),
		"attempt_no":      e.AttemptNo,
		"max_attempts":    e.MaxAttempts,
		"recovery_count":  e.RecoveryCount,
		"scheduled_at":    e.ScheduledAt.Format(time.RFC3339),
		"available_at":    e.AvailableAt.Format(time.RFC3339),
		"started_at":      nullTime(e.StartedAt),
		"finished_at":     nullTime(e.FinishedAt),
		"owner_instance":  e.OwnerInstance,
		"dispatched_to":   e.DispatchedTo,
		"failure_kind":    e.FailureKind,
		"terminal_reason": e.TerminalReason,
		"result_summary":  e.ResultSummary,
		"error_message":   e.ErrorMessage,
	}
}

// statusLabel is what the UI shows, and it exists because `cancelled` alone lies by omission.
//
// Reaching `cancelled` through a handler confirming it stopped, and reaching it through lease
// expiry, are different facts. Showing both as a plain "cancelled" invites an operator to
// assume nothing happened — which for a job with external effects is the most expensive
// available wrong assumption.
func statusLabel(e store.ExecutionView) string {
	switch e.Status {
	case "cancelled":
		switch e.TerminalReason {
		case "handler_confirmed":
			return "cancelled (handler confirmed stopped)"
		case "fenced":
			return "cancelled (fenced; side effects unverified)"
		default:
			return "cancelled"
		}
	case "dead":
		switch e.TerminalReason {
		case "timeout":
			return "dead (runtime cap elapsed; side effects unverified)"
		case "budget_exhausted":
			return "dead (attempt budget exhausted)"
		case "permanent_failure":
			return "dead (permanent failure; not retried)"
		default:
			return "dead"
		}
	case "cancel_requested":
		return "stopping (asked; still holds its slot)"
	case "skipped":
		return "skipped (FORBID: the previous run was still going)"
	default:
		return string(e.Status)
	}
}

// retryExecution returns a dead execution to ready and raises its budget.
//
// Raising the budget is not optional. The claim guard is `attempt_no < max_attempts`, so a
// retry that only flipped the status would be dead again on its next claim and the button
// would appear to do nothing.
func (a *API) retryExecution(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	var body action
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	if err := st.AuthorizedRetry(r.Context(), r.PathValue("key"),
		ActorFrom(r.Context()), body.Reason); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]bool{"queued": true})
	return nil
}

// cancelExecution asks a running execution to stop.
//
// It moves to `cancel_requested`, which KEEPS the lease and the job lock. Marking it cancelled
// and releasing the slot immediately would let the next execution start while the previous
// handler is still writing — "we asked it to stop" and "it stopped" are two states, and the
// UI shows them as two.
func (a *API) cancelExecution(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	var body action
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	row, err := st.ExecutionByKey(r.Context(), r.PathValue("key"))
	if err != nil {
		return err
	}
	if err := st.RequestCancel(r.Context(), row.ID, ActorFrom(r.Context())); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "cancel_requested",
		"note": "the handler has been asked to stop; it keeps its slot until it confirms " +
			"or is fenced",
	})
	return nil
}

func (a *API) listExecutors(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	xs, err := st.AllExecutors(r.Context())
	if err != nil {
		return err
	}
	cutoff := a.cfg.Clock.Now().Add(-a.cfg.ExecutorLiveness)

	out := make([]map[string]any, 0, len(xs))
	for _, x := range xs {
		out = append(out, map[string]any{
			"executor_id":      x.ExecutorID,
			"group":            x.Group,
			"address":          x.Address,
			"contract_version": x.ContractVersion,
			"revision":         x.Revision,
			"capacity":         x.Capacity,
			"running":          x.Running,
			"handlers":         x.Handlers,
			"started_at":       x.StartedAt.Format(time.RFC3339),
			"heartbeat_at":     x.HeartbeatAt.Format(time.RFC3339),
			// Shown rather than filtered out: an executor that stopped heartbeating is the
			// thing an operator is looking for, and removing it from the list turns "it died"
			// into "it was never there".
			"live": x.HeartbeatAt.After(cutoff),
		})
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (a *API) listOrphans(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	os, err := st.Orphans(r.Context(), a.cfg.ExecutorLiveness)
	if err != nil {
		return err
	}
	out := make([]map[string]any, 0, len(os))
	for _, o := range os {
		out = append(out, map[string]any{
			"job_name":       o.JobName,
			"handler_key":    o.HandlerKey,
			"executor_group": o.Group,
		})
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (a *API) listAudit(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	rows, err := st.AuditLog(r.Context(), q.Get("job"), q.Get("actor"),
		atoiDefault(q.Get("limit"), 100))
	if err != nil {
		return err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, e := range rows {
		out = append(out, map[string]any{
			"id":         e.ID,
			"actor":      e.Actor,
			"action":     e.Action,
			"job_name":   e.JobName,
			"execution":  e.Execution,
			"detail":     e.Detail,
			"created_at": e.CreatedAt.Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}
