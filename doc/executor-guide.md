# Writing an executor

This document is written to be handed to an agent, with one job: build an executor that go-job
can schedule, and register its jobs correctly. It states the rules that are **enforced** — the
ones where getting it wrong produces a running system that is silently wrong — and separates
them from the ones that are merely advice.

Read it once through before writing code. The mistakes it warns about are not stylistic; each
one has a failure that appears in production and not in your tests.

---

## 0. What you are building

go-job is a scheduler. It holds no handler code. Your executor is a **separate process** that:

1. **serves** four gRPC methods (`gojob.v1.JobExecutor`), so the scheduler can hand it work,
   cancel work, and ask what it is doing;
2. **calls** four gRPC methods (`gojob.v1.JobScheduler`), to register itself, heartbeat, report
   progress and report results.

Both service definitions are in `proto/gojob/v1/executor.proto`. Generate a client and server
for your language from it. In Go, the generated code is committed under `gen/gojob/v1` and the
scheduler is built with `require_unimplemented_servers=false`, so **a new RPC breaks your build
rather than surfacing at runtime** — do not defeat that by embedding an `Unimplemented…` stub.

`internal/testexec/executor.go` is a complete, conforming reference executor in ~450 lines. Read
it. It is the shortest correct answer to most of the questions below.

---

## 1. The rules that are enforced

Break one of these and the system does not fail loudly — it produces double execution, lost
results, or work that is never recovered.

### 1.1 A restart MUST mint a fresh `executor_id`

`executor_id` identifies **one process**, not one deployment or one host. Generate a new one
every time the process starts — a UUID is fine.

The scheduler **refuses** a registration for an id that is already live at a different address.
That refusal is deliberate: the registration carries the address recovery uses to ask "do you
still have this work?", so a second process taking over a live id makes recovery reach the wrong
process, be told `NOT_FOUND`, and dispatch the work a second time while the first is still
running.

An id whose heartbeat has lapsed (past `-executor-liveness`, 30s by default) is reusable — that
is what a legitimate restart finds.

### 1.2 One `run_token` is at most one execution of the handler

Every dispatch carries `execution_key` and `run_token`. The key names the logical occurrence;
the **token names the attempt**. Your executor must guarantee: for a given `run_token`, the
handler body runs **at most once**, however many times `Run` is delivered.

Deduplicate on `(tenant, execution_key, run_token)`, not on the key alone — a retry is a new
token for the same key and must be allowed to run.

If `Run` arrives for a key you are already running under a **different** token, answer
`ALREADY_EXISTS` with the token you hold, in the `ExecutionHeld` error detail. Do not start a
second handler and do not silently accept.

### 1.3 Refusal is a FIELD, never a status code

To decline work — you are at capacity, you are draining, you do not have that handler — return
**OK** with `RunResponse.refused = true` and a reason.

Never signal refusal with `UNAVAILABLE` or any other non-OK status. A gRPC status cannot
distinguish "I declined" from "the connection broke after I accepted", and the scheduler must
treat the second as unknown. An OK response with `refused = true` is the *only* thing that
releases the job for immediate re-routing; everything else leaves the execution held and lets
recovery resolve it.

Echo `execution_key` and `run_token` in `RunResponse` if you set them at all. A **non-empty**
value that disagrees with what was sent is treated as an answer about other work — the response
is discarded as unknown. Sending neither is fine.

### 1.4 `GetExecution` must answer about the attempt it was asked about

When the scheduler has lost track of an attempt it asks `GetExecution(execution_key)`. Your
answer decides whether work is re-run.

- `run_token` is **REQUIRED** whenever state is `RUNNING` or `FINISHED`. An answer that names no
  attempt proves nothing and is treated as unknown — costing a recovery.
- `outcome` is **REQUIRED** when state is `FINISHED`, with a real disposition.
  `DISPOSITION_UNSPECIFIED` is refused; "finished" without saying how is not an outcome, and the
  scheduler will not invent one.
- Answer `NOT_FOUND` for a key you have never seen or no longer remember. That is a useful,
  honest answer — it tells recovery nothing is running and the work can be re-dispatched.
- Keep finished attempts for a while (the reference executor keeps them in memory). A result the
  scheduler could not record is recoverable **only** through this call.

Answering `RUNNING` about a key holds it: the scheduler will not release an execution key while
a handler for it is running, whichever attempt holds it.

### 1.5 Report a result exactly once, and retry until it lands

`ReportResult` is the authoritative outcome. Call it once per attempt and retry on transport
failure — it is idempotent on `(tenant, execution_key, run_token)`.

- `already_recorded = true` means it landed (possibly via recovery). Stop retrying. **Success.**
- `ABORTED` means this attempt was genuinely superseded. Discard it; do not retry.
- Any other error: retry with backoff.

`outcome.disposition` is required. Map honestly:

| Disposition | Meaning |
| --- | --- |
| `SUCCESS` | the handler completed |
| `FAILED` | the handler failed; retryable unless `failure_kind` says otherwise |
| `STOPPED` | the handler stopped because it was cancelled — **and confirmed it stopped** |
| `TIMED_OUT` | the handler hit its own limit |

Set `failure_kind` to a short, stable, groupable token (`upstream_5xx`, `validation`,
`timeout`). Prefix it `permanent` or `permanent.something` to say a retry cannot help — a
validation failure retried three times just burns the budget and reaches `dead` with a reason
that hides the real cause.

`summary`, `error_detail` and `failure_kind` are **truncated** by the scheduler (512, 512 and 48
characters). Do not put a stack trace in `summary` and expect it back.

### 1.6 Progress is how a long handler stays alive — and how a cancel reaches it

`ReportProgress` extends the **silence** budget, which is separate from the runtime cap. A
twenty-hour handler reporting every interval never touches the cap; one that stops reporting is
treated as lost.

Report on the interval the scheduler gave you at registration (`progress_interval_seconds`,
a third of the silence budget by default, so you may lose two reports in a row). The reference
executor reports on a timer from the *executor*, not from the handler — **the handler is not
required to call anything.** Do the same; a handler that forgets to report should not be killed.

`ReportProgress` answers `proceed`. **`proceed = false` means stop.** It is how an operator's
cancel reaches a handler that is not watching its context. Honour it.

### 1.7 Cancellation is cooperative, and "acknowledged" is not "stopped"

`Cancel` asks you to stop. Signal the handler and answer whether you knew the execution.

Then actually stop, and report `STOPPED` when you have. The scheduler treats a delivered cancel
as a request, not as proof: it will not release the job lock while `GetExecution` still answers
`RUNNING`. A handler that ignores cancellation therefore blocks its tenant from being retired —
which is correct, and is why cooperation matters.

### 1.8 Heartbeat, or lose your registration

Call `Heartbeat` on the interval given at registration. `known = false` means your registration
lapsed and you must call `Register` again — **with the same executor id**, since it is the same
process. Do not silently ignore it: a reaped registration has no handlers declared, so the
scheduler will never route to you again.

---

## 2. Registering, in order

```
Register(executor_id, group, tenant, address, contract_version, revision, capacity, handlers)
   → heartbeat_interval_seconds, progress_interval_seconds, silence_deadline_seconds
```

- `address` is where the scheduler will reach **you**. It must be resolvable from the
  scheduler's network, not from yours. This is the single most common deployment mistake.
- `handlers` are the `handler_key` strings you serve. A job's `handler_key` must match one of
  them exactly, or the job is never dispatched to you.
- `DescribeResponse.handlers` may repeat those keys with an operator-facing description of the
  compiled code. Keep populating `handler_keys` for rolling compatibility; descriptions are
  optional metadata and never routing authority.
- `contract_version` is `"1"`.
- `capacity` is advisory. Your refusal is what is authoritative.
- Register **once per tenant** you serve. The scheduler probes back to your address during
  registration, so be listening before you call.

Registration is refused if the credential you present is not listed in `executor_identity`
(unless the deployment set `-allow-unlisted-executors`), or if it is scoped to a different
`group` than the one you claim.

---

## 3. Creating the jobs

Jobs are rows the scheduler owns; your executor does not create them. Use the admin API
(`doc/admin.md`) or the UI. `POST /api/tenants/{tenant}/jobs`:

```json
{
  "job_name": "nightly-report",
  "handler_key": "report.nightly",
  "schedule_kind": "CRON",
  "schedule_expr": "0 0 3 * * *",
  "enabled": true,
  "reason": "why this job exists"
}
```

Six-field cron, seconds first, evaluated in the deployment's business location.

Defaults worth setting deliberately:

| Field | Default | Set it when |
| --- | --- | --- |
| `timeout_seconds` | 900 (15 min) | the job legitimately runs longer — this is the **per-attempt** runtime cap, and passing it is terminal |
| `lease_seconds` | 60 | rarely; minimum 10, must not exceed the timeout |
| `max_attempts` | 3 | a failure is worth retrying more or fewer times |
| `concurrency_policy` | `QUEUE` | `FORBID` if a second occurrence must be skipped rather than queued |
| `misfire_policy` | `FIRE_ONCE` | `SKIP` if a missed run is worthless after its instant — it will then genuinely never run |
| `params` | none | the handler needs configuration; an operator's manual override is MERGED over these, not substituted |

`params` carry **no secrets**. They are stored in the tenant schema, shown in the UI, and sent
over the wire to an executor.

---

## 4. Migrating an existing scheduled job

The order matters, because the failure mode of getting it wrong is running the job twice.

1. **Write the handler in the executor**, calling the same code the old scheduler called. Do not
   re-implement the logic; call it.
2. **Deploy the executor** and confirm it registers: `GET /api/tenants/{tenant}/handlers` should
   list your `handler_key`.
3. **Create the job enabled, on a schedule that cannot fire before you are finished.** A cron of
   `0 0 3 29 2 *` — 03:00 on 29 February — is years away and unambiguous.

   Not `"enabled": false`, and not paused. Both are runnability conditions: a disabled or paused
   job is never claimed, and that includes a manual trigger, so a trigger against one sits in
   `ready` for ever and tells you nothing. What makes this step safe is the schedule, not a
   flag.
4. **Trigger it manually** (`POST /api/tenants/{tenant}/jobs/{name}/trigger` with a
   `request_id` and a `reason`) and check the execution history: dispatched, ran, reported a
   result, correct outcome.
5. **Turn OFF the old scheduler's copy.** This is the cutover. Both enabled at once is a double
   run, and nothing in go-job can prevent it — the old scheduler is not a participant in its
   protocol.
6. **Set the real schedule** — `PATCH …/jobs/{name}` with an `If-Match: <version>` header.

   `PATCH` here **replaces the whole definition**; it is not a partial edit. Send every field,
   including `job_name` and `handler_key`, or it answers `400 job_name and handler_key are
   required`. The safe way is to `GET` the job, change what you meant to change, and send it
   back with its `version` in `If-Match`.

   The response still shows the OLD `next_fire_at`. That is expected: the new instant is
   recomputed by the drift scan, inside the same locked transaction shape materialization uses,
   and appears within a scan interval. Re-read the job to see it before concluding anything.
7. Watch the first natural fire.

Migrate in small batches and let each batch run through at least one natural fire before
starting the next. A `request_id` for a manual trigger is an idempotency key **bound to that
job**: re-using one against a different job is refused, not silently answered with the first
job's execution.

`"enabled": false` still has a use — staging a definition an operator will turn on later, or
stopping a job without losing its configuration. It is just not a step in this procedure.

---

## 5. Verifying an executor before you trust it

The reference executor passes all of these; check yours does too.

- **Duplicate delivery**: send the same `Run` twice with the same token. The handler body runs
  once, and both calls answer the same way.
- **Different token, same key**: the second is answered `ALREADY_EXISTS` with the held token,
  and no second handler starts.
- **Refusal**: at capacity, `Run` returns OK with `refused = true`, never a status code.
- **Restart**: kill the process mid-execution and restart it. It registers with a NEW id, and
  `GetExecution` for the interrupted key answers `NOT_FOUND` rather than inventing a state.
- **Cancel**: `Cancel` reaches the handler, the handler stops, and `STOPPED` is reported.
  `GetExecution` stops answering `RUNNING`.
- **Result retry**: block `ReportResult`, let the attempt finish, unblock. The result lands
  exactly once and `already_recorded` is handled as success.
- **Reconciliation after a scheduler restart**: while an execution is running, restart the
  scheduler. `GetExecution` answers `RUNNING` with the correct token, and the execution is
  adopted rather than re-run.

---

## 6. Things that are advice, not rules

- Prefer one executor process per deployable unit of work, not one per job. Routing is by
  `handler_key`, so one process can serve many.
- `executor_group` is for partial rollouts: a canary registers in its own group and only jobs
  assigned to that group reach it.
- Keep handlers idempotent anyway. The protocol bounds duplicate execution tightly; it does not
  make it impossible, and a handler that tolerates being run twice is cheaper than an
  investigation into why it was.
