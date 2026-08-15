# Executor contract

This is the wire contract between the scheduler and an executor. It is plain HTTP and
JSON, in both directions, so an executor can be written in any language.

An executor implements four endpoints and calls four. Nothing else is required of it, and
in particular **an executor never touches the scheduler's database and never implements any
part of the ownership protocol**. Deciding who owns an execution is subtle enough that it
should exist in exactly one place; that place is the scheduler.

---

## 1. Shape

```text
  executor                                scheduler
     |                                        |
     |-- register ------------------------->  |   who I am, what I can run
     |-- heartbeat ------------------------>  |   still alive, current load
     |                                        |
     |  <---------------------- run --------  |   run this execution
     |-- accepted (202) ------------------->  |
     |                                        |
     |-- progress ------------------------->  |   still working; may I continue?
     |-- result --------------------------->  |   done, here is what happened
     |                                        |
     |  <-------------------- cancel -------  |   stop if you can
     |  <-------------------- status -------  |   what happened to X? (reconciliation)
```

Dispatch is **asynchronous**: the scheduler hands over an execution, the executor accepts
it and reports later. A synchronous call that stayed open for the duration would tie an
HTTP connection to a job that may run for hours and would turn every proxy timeout into a
lost result.

---

## 2. Identity

| Field | Meaning |
| --- | --- |
| `executor_id` | unique per **process**: `<group>-<host>-<boot-nonce>`. A restart produces a new one, so a restarted executor never inherits its predecessor's registration. |
| `group` | the logical executor a job routes to, e.g. `orders-worker`. Several instances share a group. |
| `tenant` | which tenant's work this executor serves. |
| `handler_key` | the unit of routing. A job names a handler; a group declares which handlers it implements. |
| `execution_key` | the scheduler's durable identity for one logical run. **This is the idempotency key.** |
| `run_token` | identifies one *attempt* of that execution. It fences results: a result carrying a stale token is refused. |

---

## 3. Executor → scheduler

### 3.1 Register

```http
POST /api/v1/executors/register
Content-Type: application/json

{
  "executor_id": "orders-worker-a1b2c3",
  "group":       "orders-worker",
  "tenant":      "np",
  "address":     "http://10.0.1.5:9100",
  "handlers":    ["orders.rollup", "orders.reconcile"],
  "revision":    "8f21c4e",
  "capacity":    8
}
```

```json
200 OK
{ "heartbeat_interval_seconds": 10, "registration_ttl_seconds": 45 }
```

`address` is where the scheduler will call. It must be reachable from the scheduler — see
§7. `capacity` is how many concurrent executions this instance is willing to hold; the
scheduler will not exceed it.

Registration is idempotent: re-registering the same `executor_id` updates the record.

### 3.2 Heartbeat

```http
POST /api/v1/executors/heartbeat
{ "executor_id": "orders-worker-a1b2c3", "running": 3 }
```

```json
200 OK
{ "known": true }
```

`known: false` means the scheduler has forgotten this executor — its TTL lapsed. The
executor must re-register rather than continue heartbeating, which is what makes recovery
after a scheduler restart automatic.

An executor that misses `registration_ttl_seconds` is removed from routing. It is not
killed and its running executions are not abandoned; §6 covers what happens to them.

### 3.3 Progress

For anything long enough that the scheduler would otherwise time it out.

```http
POST /api/v1/executions/{execution_key}/progress
{ "run_token": "9c2f…", "message": "3000/10000 rows" }
```

```json
200 OK
{ "continue": true, "deadline": "2026-08-15T01:38:00+08:00" }
```

`continue: false` means **stop now**: the execution was cancelled, or this attempt was
fenced because the scheduler already gave the work to someone else. An executor that keeps
working after `continue: false` is outside the contract, and its results will be refused.

Progress extends the deadline. An executor that stops reporting progress will have its
execution treated as lost once the deadline passes.

### 3.4 Result

```http
POST /api/v1/executions/{execution_key}/result
{
  "run_token":    "9c2f…",
  "status":       "success",
  "summary":      "rows=10000",
  "failure_kind": null
}
```

`status` is `success` or `failed`. `failure_kind` is a short stable string for grouping and
alerting — `timeout`, `upstream_5xx`, `validation` — not a free-text message. `summary` is
the one line an operator reads in history.

```json
200 OK      accepted
409 Conflict {"error":"stale_run_token"}   this attempt was already fenced; discard
404 Not Found                              unknown execution; discard
```

**A 409 is not a retry condition.** It means the scheduler has moved on and another attempt
owns the work. The executor must stop and must not resend.

Result delivery is retried by the executor on 5xx and network failure, with backoff. Result
posts are idempotent on `(execution_key, run_token)`.

---

## 4. Scheduler → executor

### 4.1 Run

```http
POST {address}/jobs/run
Idempotency-Key: cron:orders.rollup:2026-08-15T01:30:00

{
  "execution_key": "cron:orders.rollup:2026-08-15T01:30:00",
  "job_name":      "orders-rollup",
  "handler_key":   "orders.rollup",
  "tenant":        "np",
  "run_token":     "9c2f…",
  "attempt":       1,
  "scheduled_at":  "2026-08-15T01:30:00+08:00",
  "deadline":      "2026-08-15T01:38:00+08:00",
  "params":        {}
}
```

The executor answers immediately — it does not run the job first:

```json
202 Accepted   { "accepted": true }
409 Conflict   { "accepted": false, "reason": "already_running" }
429 Too Many   { "accepted": false, "reason": "at_capacity" }
503            { "accepted": false, "reason": "shutting_down" }
```

**202 is a promise.** It means the executor has durably taken responsibility and will
eventually post a result. If it cannot promise that, it must not answer 202.

**409 must be answered when the executor already holds that `execution_key`**, and it is
the executor's half of at-most-once dispatch. When a dispatch times out, the scheduler does
not know whether the executor received it, so it re-sends with the same `Idempotency-Key`.
An executor that starts a second run instead of answering 409 turns one network hiccup into
a duplicate run.

Deduplication is on `execution_key`, and it must survive the length of the run — an
in-memory set of currently-held keys is sufficient, since a restarted executor has lost the
work anyway and the scheduler will notice through §6.

`deadline` is in business time and is authoritative. An executor should stop work of its own
accord when it passes, rather than relying on being told.

### Parameters

`params` is a JSON object the handler receives verbatim. It comes from two places, merged
at dispatch:

1. **the job's configured default parameters**, edited and audited in the admin UI like any
   other job setting;
2. **per-trigger overrides**, supplied when an operator triggers the job manually.

The merged result is **snapshotted onto the execution row** when the execution is created,
not resolved at dispatch time. History therefore shows what each run actually ran with,
including runs whose configuration has since changed — otherwise "why did last Tuesday's
run behave differently" is unanswerable.

Three rules:

- `params` is **data, never instructions.** No command lines, no scripts, no URLs to fetch,
  no handler names. The executor resolves `handler_key` against its own compiled registry;
  a scheduler that could name arbitrary code would make its admin UI a remote shell;
- size is bounded and the bound is enforced at the API, not discovered when a row
  overflows;
- **secrets do not travel here.** Parameters are stored in the database, shown in the UI and
  written to history. An executor needing a credential reads it from its own configuration.

Values a handler could derive should be derived rather than passed: `scheduled_at` is in
every dispatch, so a daily job computes its own business date instead of being told one that
can drift from the fire instant it belongs to.

### 4.2 Cancel

```http
POST {address}/jobs/cancel
{ "execution_key": "…", "run_token": "9c2f…" }
```

```json
200 OK { "acknowledged": true }
```

Acknowledgement means "I have signalled the work to stop", not "it has stopped".
Cancellation is cooperative everywhere, and this contract does not pretend otherwise: the
executor still posts a result when the work actually ends, and until it does the scheduler
holds the execution in `cancel_requested`.

### 4.3 Status — reconciliation

```http
GET {address}/jobs/{execution_key}
```

```json
200 OK { "state": "running", "started_at": "…", "message": "3000/10000" }
200 OK { "state": "finished", "status": "success", "summary": "rows=10000" }
404 Not Found                       never seen, or already forgotten
```

The scheduler calls this when an execution has gone quiet — deadline passed, no progress,
no result. It is what turns "I don't know" into a fact before any decision is made about
retrying.

---

## 5. Routing

A job names a `handler_key`. The scheduler dispatches to a **live instance of a group that
declares that handler for that tenant**.

Instance selection is **round-robin among healthy instances that are below capacity, with
failover to the next instance on a dispatch failure**. That is the whole policy.

There is deliberately no routing-strategy family — no consistent hash, no least-frequently-
used, no sticky broadcast. Each additional strategy is a mode that must be understood
during an incident, and none of them addresses a problem that round-robin plus capacity
limits does not.

If no instance accepts, the execution stays `ready` and is retried on the next dispatch
pass with a bounded backoff. It is never marked failed for lack of an executor: nothing was
attempted, and an execution that reports a business failure when no business code ran is a
lie in the history.

An enabled job whose handler **no live group declares** is an *orphan*. It is surfaced in
the UI and alerted on, because nothing will ever run it (`admin.md` §4).

---

## 6. Liveness: is a long execution alive or dead?

A twenty-hour execution is not an edge case to be discouraged; it is a thing that must have
a defined answer. The answer rests on **two independent signals**, and conflating them is
the classic way to get this wrong.

| Signal | Question it answers | Interval |
| --- | --- | --- |
| **registration heartbeat** (§3.2) | is the executor *process* alive? | ~10s, per process |
| **execution progress** (§3.3) | is *this execution* still going? | ~30s, per execution |

Neither substitutes for the other, because the interesting failures separate them:

| Heartbeat | Progress | Conclusion |
| --- | --- | --- |
| fresh | fresh | healthy — **keep waiting, however long it takes** |
| fresh | stale past deadline | the process is fine but this execution is wedged: deadlocked, spinning, or blocked on a socket. Query §4.3, then decide |
| stale / gone | any | the executor is lost; every execution it held is lost with it |
| fresh, `known:false` | — | the scheduler forgot it; the executor re-registers and its in-flight work is reconciled through §4.3 |

An executor that is alive but whose handler is stuck looks perfectly healthy to a
process-level heartbeat. That is exactly the case the per-execution signal exists to catch,
and it is why "the process is up" is never accepted as evidence that work is progressing.

### The deadline is not a maximum duration

`deadline` bounds **silence**, not runtime. Every progress call pushes it forward, so an
execution that keeps reporting is never reclaimed — twenty hours, or two hundred.

Three separate bounds exist and they are deliberately not merged:

| Bound | Meaning | Typical |
| --- | --- | --- |
| `progress_interval` | how often the executor must speak | 30s |
| `deadline` | silence tolerated before the execution is investigated | 3 × `progress_interval` |
| `timeout_seconds` | **hard cap on total runtime**, declared by the job | 24h for a long job |

A long job therefore configures a long `timeout_seconds` and an ordinary
`progress_interval`. Raising the deadline to cover a job's full runtime would be the wrong
fix: it converts "I will notice a stuck job in 90 seconds" into "I will notice it in 20
hours".

### Progress must not depend on handler cooperation

The executor framework emits progress **automatically on a timer while a handler is
running**. It does not require the handler to call anything.

This matters because the handlers most likely to run for hours are exactly the ones least
able to report: a single long-running query, a bulk load, a third-party call that blocks.
If liveness depended on the handler remembering to check in, those jobs would be
periodically declared dead while working perfectly.

Handler-supplied progress messages (`"3000/10000 rows"`) remain available and are worth
using — but they are for **observability**, so an operator can see movement, not for
liveness. The two concerns are separated on purpose.

The residual case — an executor whose handler is hung while the framework cheerfully
reports progress — is why `timeout_seconds` exists as an independent hard cap, and why the
UI shows time-since-last-*handler*-message alongside time-since-last-progress. A job whose
framework is reporting but whose handler has said nothing for six hours is visible as such.

---

## 7. Failure handling

The hard cases, and what the scheduler does about each.

| Situation | Scheduler behaviour |
| --- | --- |
| dispatch returns 202, result arrives | ordinary path |
| dispatch times out, unknown whether received | re-dispatch with the same `Idempotency-Key`; the executor answers 409 if it already has it |
| dispatch refused (409 / 429 / 503) | try the next instance; if none accept, leave `ready` with backoff |
| accepted, then silence past the deadline | call `GET /jobs/{key}`; the answer decides |
| status says `running` | extend and keep waiting; a slow job is not a failed one |
| status says `finished` | adopt that result as if it had been posted |
| status 404, or the executor is unreachable | the attempt is **lost**: fence the `run_token`, then retry per policy |
| executor registration expires while holding work | the same lost-attempt path; registration TTL is not itself proof the work stopped |

### Fencing across a network

The scheduler cannot stop a process on another machine. What it can do — and what
`run_token` is for — is **refuse everything that attempt produces afterwards**. Once an
attempt is fenced, its progress calls receive `continue: false` and its result posts
receive `409`.

That bounds the damage to the scheduler's own state. It does not undo whatever the
executor already did to the outside world, and the contract says so plainly rather than
implying a guarantee it cannot deliver. This is why **an executor must be able to run the
same `execution_key` twice without harm** — the ultimate protection is idempotent business
logic, not the fence.

---

## 8. Network and security

Push requires the scheduler to reach executors, which is a real operational requirement and
the main cost of this model:

- executors listen on an address reachable **from the scheduler**, not from the public
  internet. A private network, a service mesh, or an internal load balancer are all fine;
- if several instances sit behind one address, the scheduler cannot choose between them and
  its capacity accounting becomes an estimate. Prefer registering instances individually;
- both directions are authenticated with a shared secret or mTLS. The dispatch endpoint
  executes work on request, so an unauthenticated one is remote code execution by
  configuration;
- `params` are treated as data. Handlers are named and resolved by the executor from its
  own registry; the scheduler never sends code, a command line, a script or a URL to fetch.

---

## 9. What an executor must guarantee

Short enough to fit on one page, which is the point of separating executors from the
scheduler at all:

1. **Answer 409 for an `execution_key` you already hold.** This is at-most-once dispatch.
2. **Only answer 202 if you will really post a result.**
3. **Post a result exactly once per attempt**, retrying on 5xx and network failure, stopping
   permanently on 409.
4. **Stop when told** — `continue: false`, a cancel request, or a passed deadline.
5. **Be idempotent per `execution_key`.** The scheduler will occasionally deliver the same
   logical work twice: after a lost attempt, after an operator retry, after a network
   partition it cannot see through. Being run twice must be harmless.
6. **Re-register when heartbeat says `known: false`.**

Everything else — leases, fences, claim ordering, recovery, misfire, retry budgets — is the
scheduler's problem and stays inside it.
