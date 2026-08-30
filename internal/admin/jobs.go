package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/store"
)

// jobBody is the wire shape of a job, in both directions.
//
// Durations are seconds rather than Go duration strings, because this is consumed by a
// browser and by whatever curl an operator writes at 3am, and "300" needs no parser.
type jobBody struct {
	JobName       string          `json:"job_name"`
	HandlerKey    string          `json:"handler_key"`
	ExecutorGroup string          `json:"executor_group"`
	ScheduleKind  string          `json:"schedule_kind"`
	ScheduleExpr  string          `json:"schedule_expr"`
	Enabled       bool            `json:"enabled"`
	Concurrency   string          `json:"concurrency_policy"`
	Misfire       string          `json:"misfire_policy"`
	MaxAttempts   int             `json:"max_attempts"`
	MaxRecoveries int             `json:"max_recoveries"`
	LeaseSeconds  int             `json:"lease_seconds"`
	TimeoutSecond int             `json:"timeout_seconds"`
	Params        json.RawMessage `json:"params"`
	Description   string          `json:"description"`
	Reason        string          `json:"reason"`
}

// toDefinition validates a job body and fills in the defaults that are decisions.
//
// Defaulting here rather than in the database means an operator creating a job through the
// API and one created through a migration get the same policy, and that the reason for each
// default lives with the reason for every other one.
func (b jobBody) toDefinition() (gojob.Definition, error) {
	d := gojob.Definition{
		JobName:       b.JobName,
		HandlerKey:    b.HandlerKey,
		ExecutorGroup: b.ExecutorGroup,
		ScheduleKind:  gojob.ScheduleKind(b.ScheduleKind),
		ScheduleExpr:  b.ScheduleExpr,
		Enabled:       b.Enabled,
		Concurrency:   gojob.ConcurrencyPolicy(b.Concurrency),
		Misfire:       gojob.MisfirePolicy(b.Misfire),
		MaxAttempts:   b.MaxAttempts,
		MaxRecoveries: b.MaxRecoveries,
		Lease:         time.Duration(b.LeaseSeconds) * time.Second,
		Timeout:       time.Duration(b.TimeoutSecond) * time.Second,
		Params:        b.Params,
		Description:   b.Description,
	}

	if d.JobName == "" || d.HandlerKey == "" {
		return d, badRequest("job_name and handler_key are required")
	}
	switch d.ScheduleKind {
	case gojob.ScheduleCron, gojob.ScheduleFixedDelay:
	default:
		return d, badRequest("schedule_kind must be CRON or FIXED_DELAY")
	}
	if d.ScheduleExpr == "" {
		return d, badRequest("schedule_expr is required")
	}

	if d.Concurrency == "" {
		d.Concurrency = gojob.DefaultConcurrencyPolicy
	}
	if d.Misfire == "" {
		d.Misfire = gojob.DefaultMisfirePolicy
	}
	switch d.Concurrency {
	case gojob.PolicyQueue, gojob.PolicyForbid:
	default:
		return d, badRequest("concurrency_policy must be QUEUE or FORBID")
	}
	switch d.Misfire {
	case gojob.MisfireSkip, gojob.MisfireFireOnce:
	default:
		return d, badRequest("misfire_policy must be SKIP or FIRE_ONCE")
	}

	if d.MaxAttempts == 0 {
		d.MaxAttempts = 3
	}
	if d.MaxRecoveries == 0 {
		d.MaxRecoveries = 3
	}
	if d.Lease == 0 {
		d.Lease = 60 * time.Second
	}
	if d.Timeout == 0 {
		d.Timeout = 15 * time.Minute
	}

	// The same bounds the schema's CHECK constraints hold, applied here so a bad value is a
	// readable 400 rather than a driver error the UI shows as "something went wrong".
	if d.MaxAttempts < 1 || d.MaxAttempts > 100 {
		return d, badRequest("max_attempts must be between 1 and 100")
	}
	if d.MaxRecoveries < 1 || d.MaxRecoveries > 100 {
		return d, badRequest("max_recoveries must be between 1 and 100")
	}
	if d.Lease < 10*time.Second {
		return d, badRequest("lease_seconds must be at least 10")
	}
	if d.Timeout < time.Second || d.Timeout > 7*24*time.Hour {
		return d, badRequest("timeout_seconds must be between 1 and 604800")
	}
	if d.ScheduleKind == gojob.ScheduleFixedDelay {
		if _, err := d.Delay(); err != nil {
			return d, badRequest("%v", err)
		}
	}
	if d.ScheduleKind == gojob.ScheduleFixedDelay {
		delay, err := d.Delay()
		if err != nil {
			return d, badRequest("%v", err)
		}
		// A poller whose pause exceeds a day is a cron job with extra steps, and one that long
		// hides a mistyped unit — 86400000 meant as seconds is eleven days.
		if delay > 24*time.Hour {
			return d, badRequest("a fixed delay of %s is longer than a day; use a CRON schedule", delay)
		}
	}

	// The silence budget is the lease, so a lease longer than the timeout means an execution
	// can outlive its own runtime cap without ever looking silent.
	if d.Lease > d.Timeout {
		return d, badRequest("lease_seconds (%d) must not exceed timeout_seconds (%d)",
			int(d.Lease/time.Second), int(d.Timeout/time.Second))
	}

	if err := checkIdentifier("job_name", d.JobName, 128); err != nil {
		return d, err
	}
	if err := checkIdentifier("handler_key", d.HandlerKey, 128); err != nil {
		return d, err
	}
	if d.ExecutorGroup != "" {
		if err := checkIdentifier("executor_group", d.ExecutorGroup, 64); err != nil {
			return d, err
		}
	}
	if len(d.Description) > 512 {
		return d, badRequest("description must be at most 512 characters")
	}

	if len(d.Params) > 0 {
		// A cap, because these are copied onto every execution row and sent on every dispatch.
		// A megabyte of parameters is a megabyte per run, for ever.
		if len(d.Params) > store.MaxParamsBytes {
			return d, badRequest("params must be at most 64 KiB; they are copied onto every " +
				"execution and sent on every dispatch")
		}
		var probe map[string]any
		if err := json.Unmarshal(d.Params, &probe); err != nil {
			return d, badRequest("params must be a JSON object")
		}
	}
	return d, nil
}

// checkIdentifier bounds a name and restricts it to characters that survive a log line, a URL
// path and an execution key without escaping.
//
// The length bound matters beyond tidiness: job names go into execution keys, and a name at
// the column's limit is what pushed keys past theirs.
func checkIdentifier(field, value string, max int) error {
	if value == "" {
		return badRequest("%s is required", field)
	}
	if len(value) > max {
		return badRequest("%s must be at most %d characters", field, max)
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		// ':' is EXCLUDED, because it separates the fields of an execution key. Allowing it
		// makes the key ambiguous: job `a:b` with request `c` and job `a` with request `b:c`
		// both render `m:a:b:c`, and the second insert is then read as an ordinary idempotency
		// duplicate — so the API reports acceptance while creating nothing.
		case r == '.', r == '-', r == '_':
		default:
			return badRequest("%s may contain only letters, digits and . - _ :  (found %q)",
				field, string(r))
		}
	}
	return nil
}

func jobJSON(v store.JobView) map[string]any {
	return map[string]any{
		"job_name":           v.JobName,
		"handler_key":        v.HandlerKey,
		"executor_group":     v.ExecutorGroup,
		"schedule_kind":      v.ScheduleKind,
		"schedule_expr":      v.ScheduleExpr,
		"enabled":            v.Enabled,
		"retired":            v.Retired,
		"paused":             v.OpsPaused,
		"concurrency_policy": v.Concurrency,
		"misfire_policy":     v.Misfire,
		"max_attempts":       v.MaxAttempts,
		"max_recoveries":     v.MaxRecoveries,
		"lease_seconds":      int(v.Lease / time.Second),
		"timeout_seconds":    int(v.Timeout / time.Second),
		"params":             rawOrNull(v.Params),
		"description":        v.Description,
		"version":            v.Version,
		"updated_by":         v.UpdatedBy,
		"next_fire_at":       nullTime(v.NextFireAt),
		"next_poll_at":       nullTime(v.NextPollAt),
		"active_execution":   v.ActiveExec,
		"active_owner":       v.ActiveOwner,
		"last_success_at":    nullTime(v.LastSuccessAt),
		"last_failure_at":    nullTime(v.LastFailureAt),
		"config_version":     v.ConfigVersion,
	}
}

func rawOrNull(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

func (a *API) listJobs(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	jobs, err := st.Jobs(r.Context())
	if err != nil {
		return err
	}
	out := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobJSON(j))
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	j, err := st.Job(r.Context(), r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, jobJSON(j))
	return nil
}

// createJob is how jobs come into existence — there is no other way.
//
// The scheduler holds no handler code, so creating a job means naming a handler_key plus a
// schedule, parameters and policy. An unrecognised handler is ACCEPTED: a handler whose
// executor is down or not yet deployed must still be nameable, or a job could never be created
// before its executor ships. It shows as an orphan until an executor declares it.
func (a *API) createJob(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	var body jobBody
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	def, err := body.toDefinition()
	if err != nil {
		return err
	}
	next, err := parseCron(def.ScheduleKind, def.ScheduleExpr, a.cfg.Clock.Now())
	if err != nil {
		return err
	}
	if err := st.CreateJob(r.Context(), def, next, ActorFrom(r.Context()), body.Reason); err != nil {
		return err
	}

	j, err := st.Job(r.Context(), def.JobName)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, jobJSON(j))
	return nil
}

type copyAllJobsBody struct {
	Targets []string `json:"targets"`
	Reason  string   `json:"reason"`
}

// previewAllJobSync compares every non-retired source definition with selected target tenants.
// It is read-only; SyncJobs repeats the comparison under locks before applying it.
func (a *API) previewAllJobSync(w http.ResponseWriter, r *http.Request) error {
	sourceStore, source, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	targets := r.URL.Query()["target"]
	targetStores, err := a.copyTargetStores(source, targets)
	if err != nil {
		return err
	}
	seeds, err := a.activeJobSeeds(r.Context(), sourceStore)
	if err != nil {
		return err
	}
	results := make([]map[string]any, 0, len(targets))
	for _, target := range targets {
		plan, planErr := targetStores[target].PlanJobSync(r.Context(), seeds)
		result := map[string]any{"tenant": target, "plan": plan}
		if planErr != nil {
			result["error"] = planErr.Error()
		}
		results = append(results, result)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source": source, "available": len(seeds), "results": results,
	})
	return nil
}

// copyAllJobs synchronises every non-retired definition to one or more admitted tenants. Each
// target is atomic: missing names are created and changed names are updated. Tenant runtime
// state remains local, and a retired target name blocks that target rather than being revived.
func (a *API) copyAllJobs(w http.ResponseWriter, r *http.Request) error {
	sourceStore, source, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	var body copyAllJobsBody
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	targetStores, err := a.copyTargetStores(source, body.Targets)
	if err != nil {
		return err
	}
	seeds, err := a.activeJobSeeds(r.Context(), sourceStore)
	if err != nil {
		return err
	}

	actor := ActorFrom(r.Context())
	results := make([]map[string]any, 0, len(body.Targets))
	for _, target := range body.Targets {
		plan, copyErr := targetStores[target].SyncJobs(
			r.Context(), seeds, source, actor, body.Reason)
		result := map[string]any{"tenant": target, "plan": plan}
		if copyErr != nil {
			result["error"] = copyErr.Error()
			a.log.Warn("sync all jobs to tenant failed", "source", source, "target", target,
				"actor", actor, "error", copyErr)
		}
		results = append(results, result)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source": source, "available": len(seeds), "results": results,
	})
	return nil
}

func (a *API) copyTargetStores(source string, targets []string) (map[string]*store.Store, error) {
	if len(targets) == 0 || len(targets) > 100 {
		return nil, badRequest("targets must contain between 1 and 100 tenants")
	}
	targetStores := make(map[string]*store.Store, len(targets))
	seen := map[string]bool{}
	for _, raw := range targets {
		target := strings.TrimSpace(raw)
		if target == "" || target != raw {
			return nil, badRequest("target tenant names must be non-empty and contain no surrounding whitespace")
		}
		if target == source {
			return nil, badRequest("source tenant %q cannot also be a target", source)
		}
		if seen[target] {
			return nil, badRequest("target tenant %q is repeated", target)
		}
		seen[target] = true
		st, ok := a.tenants.Store(target)
		if !ok {
			return nil, badRequest("target tenant %q is not admitted", target)
		}
		targetStores[target] = st
	}
	return targetStores, nil
}

func (a *API) activeJobSeeds(ctx context.Context, sourceStore *store.Store) ([]store.JobSeed, error) {
	jobs, err := sourceStore.Jobs(ctx)
	if err != nil {
		return nil, err
	}
	now := a.cfg.Clock.Now()
	seeds := make([]store.JobSeed, 0, len(jobs))
	for _, job := range jobs {
		if job.Retired {
			continue
		}
		def := job.Definition
		def.Retired = false
		def.Version = 0
		def.UpdatedBy = ""
		next, err := parseCron(def.ScheduleKind, def.ScheduleExpr, now)
		if err != nil {
			return nil, fmt.Errorf("source job %q has an invalid schedule: %w", def.JobName, err)
		}
		seeds = append(seeds, store.JobSeed{Definition: def, NextFire: next})
	}
	return seeds, nil
}

func (a *API) liveHandlerDescriptions(ctx context.Context, st *store.Store) (map[string]string, error) {
	metadata, err := st.DeclaredHandlerMetadata(ctx, a.cfg.ExecutorLiveness)
	if err != nil {
		return nil, err
	}
	descriptions := make(map[string]string, len(metadata))
	for _, handler := range metadata {
		if handler.Description != "" {
			descriptions[handler.Key] = handler.Description
		}
	}
	return descriptions, nil
}

// previewJobDescriptionSync is deliberately read-only. The operator sees every replacement and
// every job without live metadata before the write-shaped endpoint becomes relevant.
func (a *API) previewJobDescriptionSync(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	descriptions, err := a.liveHandlerDescriptions(r.Context(), st)
	if err != nil {
		return err
	}
	result, err := st.PlanJobDescriptionSync(r.Context(), descriptions)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, result)
	return nil
}

type syncJobDescriptionsBody struct {
	Reason string `json:"reason"`
}

func (a *API) syncJobDescriptions(w http.ResponseWriter, r *http.Request) error {
	st, tenant, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	var body syncJobDescriptionsBody
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	descriptions, err := a.liveHandlerDescriptions(r.Context(), st)
	if err != nil {
		return err
	}
	actor := ActorFrom(r.Context())
	result, err := st.SyncJobDescriptions(r.Context(), descriptions, actor, body.Reason)
	if err != nil {
		return err
	}
	a.log.Info("job descriptions synced", "tenant", tenant, "actor", actor,
		"changed", len(result.Changes), "missing", len(result.Missing), "unchanged", result.Unchanged)
	writeJSON(w, http.StatusOK, result)
	return nil
}

// patchJob edits a job under If-Match against its version.
//
// A stale version is a 409, never a silent overwrite: two operators editing from two tabs is
// ordinary, and the loser discovering five minutes later that their change vanished is not.
func (a *API) patchJob(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	name := r.PathValue("name")

	ifMatch := r.Header.Get("If-Match")
	if ifMatch == "" {
		return badRequest("If-Match with the job's current version is required")
	}
	version := int64(atoiDefault(ifMatch, -1))
	if version < 0 {
		return badRequest("If-Match must be the job's version number")
	}

	var body jobBody
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	body.JobName = name
	def, err := body.toDefinition()
	if err != nil {
		return err
	}
	if _, err := parseCron(def.ScheduleKind, def.ScheduleExpr, a.cfg.Clock.Now()); err != nil {
		return err
	}

	if err := st.UpdateJob(r.Context(), def, version, ActorFrom(r.Context()), body.Reason); err != nil {
		return err
	}
	// next_fire_at is NOT recomputed here. The version bump makes the row drifted, and the
	// drift scan recomputes it inside the same locked transaction shape materialization uses —
	// so there is one recomputation path rather than two that can disagree.
	j, err := st.Job(r.Context(), name)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, jobJSON(j))
	return nil
}

func (a *API) pauseJob(w http.ResponseWriter, r *http.Request) error { return a.setPaused(w, r, true) }
func (a *API) resumeJob(w http.ResponseWriter, r *http.Request) error {
	return a.setPaused(w, r, false)
}

func (a *API) setPaused(w http.ResponseWriter, r *http.Request, paused bool) error {
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
	if err := st.SetPaused(r.Context(), r.PathValue("name"), paused,
		ActorFrom(r.Context()), body.Reason); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]bool{"paused": paused})
	return nil
}

func (a *API) retireJob(w http.ResponseWriter, r *http.Request) error {
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
	if err := st.Retire(r.Context(), r.PathValue("name"), ActorFrom(r.Context()), body.Reason); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]bool{"retired": true})
	return nil
}

// triggerJob queues a manual run, idempotently on request_id.
//
// The request_id is required rather than generated server-side, because the repeat this
// guards against is the CLIENT repeating — a double-clicked button, or a retry after a
// timeout — and a server-generated id is different on every one of those.
func (a *API) triggerJob(w http.ResponseWriter, r *http.Request) error {
	st, _, err := a.tenantStore(r)
	if err != nil {
		return err
	}
	var body struct {
		Reason    string          `json:"reason"`
		RequestID string          `json:"request_id"`
		Params    json.RawMessage `json:"params"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	if body.RequestID == "" {
		return badRequest("request_id is required so a repeated click cannot run the job twice")
	}
	if len(body.Params) > 0 {
		if len(body.Params) > store.MaxParamsBytes {
			return badRequest("params must be at most %d bytes", store.MaxParamsBytes)
		}
		var probe map[string]any
		if err := json.Unmarshal(body.Params, &probe); err != nil {
			return badRequest("params must be a JSON object")
		}
		if probe == nil {
			return badRequest("params is JSON null; omit it to use the job's defaults")
		}
	}

	key, err := st.Trigger(r.Context(), r.PathValue("name"), body.RequestID,
		ActorFrom(r.Context()), body.Reason, body.Params)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"execution_key": key})
	return nil
}
