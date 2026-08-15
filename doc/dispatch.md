# Executor contract

`proto/gojob/v1/executor.proto` is the contract. This document explains it and states the
behaviour the types cannot express.

Two services, in opposite directions:

| Service | Hosted by | Called by |
| --- | --- | --- |
| `JobExecutor` | the executor | the scheduler |
| `JobScheduler` | the scheduler | the executor |

An executor **never touches the scheduler's database and never implements any part of the
ownership protocol**. Deciding who owns an execution is subtle enough to exist in exactly
one place, and that place is the scheduler. What an executor does is narrow: accept work,
run it, say what happened.

---

## 1. Why gRPC, and what it actually buys

The goal is to fail at **build time**, not at registration time. Discovering a missing
endpoint when an executor first tries to register — then another when the first is fixed —
is the failure mode this contract is shaped to avoid.

Generated stubs move a whole class of problems earlier:

| Problem | Hand-written HTTP | Generated stubs |
| --- | --- | --- |
| a method is not implemented | found when first called | **Go: compile error.** Other languages: `UNIMPLEMENTED` at startup probe |
| wrong field type or name | runtime parse failure | **compile error, every language** |
| hand-rolled serialization bug | a recurring source of defects | does not exist — the code is generated |
| a new field is added to the contract | silently ignored | appears in generated code; visible in review |
| probing "did you implement this?" | `404` is ambiguous — missing route, or missing key? | `UNIMPLEMENTED` and `NOT_FOUND` are different codes. Unambiguous |

That last row is why registration-time verification actually works here (§6).

### Compile-time enforcement per language

- **Go** — generated servers carry `mustEmbedUnimplementedJobExecutorServer`. **An executor
  must not embed `UnimplementedJobExecutorServer`.** Without it, failing to implement any
  method is a compile error, and adding a method to the contract breaks the build of every
  executor immediately — which is the point.
- **Java** — `JobExecutorGrpc.JobExecutorImplBase` supplies defaults returning
  `UNIMPLEMENTED`, so the compiler will not catch a missing method. The registration probe
  does, at startup, before any work is routed.
- **Python, Node, PHP** — dynamic; the probe is the enforcement.

The contract is therefore strongest in Go and still fails early elsewhere. It never fails
*silently* anywhere.

### What no type system can enforce

Semantics. That a duplicate `execution_key` returns `ALREADY_EXISTS`, that an accepted
execution eventually reports a result, that running the same work twice is harmless — none
of these are expressible in a signature. They are covered by the conformance suite (§7).

### The four layers

Each catches what the previous cannot. The point is that **each layer reports everything it
finds at once**, rather than one problem per attempt:

| Layer | Catches | When |
| --- | --- | --- |
| build | missing methods, wrong types, serialization | compile |
| conformance suite | semantics: dedupe, fencing, capacity, result delivery | executor's CI |
| registration probe | declared-but-not-implemented, contract version mismatch | executor startup |
| runtime detection | "accepted but never reported", duplicate token reports | continuously |

---

## 2. Parameters

`JobParams` carries a JSON-shaped structure to the handler. This is a first-class part of
the contract, not an afterthought: a scheduler that can only say "run X" and not "run X for
2026-08-14 with batch size 500" pushes that configuration into code, where operators cannot
reach it.

Parameters come from two places and are merged at execution creation:

1. the job's **configured defaults**, edited and audited in the admin UI like any other
   setting;
2. **per-trigger overrides**, supplied when an operator triggers the job manually.

The merged value is **snapshotted onto the execution row when the execution is created**,
not resolved at dispatch. History therefore shows what each run actually ran with, including
runs whose configuration has since changed — otherwise "why did last Tuesday's run behave
differently" has no answer.

Three rules, enforced by the scheduler:

- **data, never instructions.** No command lines, scripts, URLs to fetch, or handler names.
  The executor resolves `handler_key` against its own registry — the scheduler holds no
  handler code at all. A scheduler able to name arbitrary code would be a remote shell with
  an admin UI;
- **bounded size**, rejected at the API rather than discovered when a row overflows;
- **no secrets.** Parameters are stored, displayed in the UI and written to history. An
  executor reads credentials from its own configuration.

Values a handler can derive should be derived rather than passed. `scheduled_at` accompanies
every dispatch, so a daily job computes its own business date instead of being handed one
that can drift from the fire instant it belongs to.

---

## 3. Dispatch

`Run` returns immediately. The executor does **not** run the job before answering.

```text
scheduler                         executor
   |-- Run(execution_key, ...) ------>|
   |<---------------- OK -------------|   a promise: a result will follow
   |                                  |   ... work happens ...
   |<-- ReportProgress ---------------|   every progress_interval
   |--- proceed=true, +silence budget >|
   |<-- ReportResult -----------------|
```

An OK response to `Run` is **a promise**: the executor has durably taken responsibility and
will eventually call `ReportResult`. An executor that cannot promise that must not answer
OK.

Refusals are error codes rather than a boolean in the response body, so no caller can ignore
one by forgetting to read a field:

| Code | Meaning | Scheduler does |
| --- | --- | --- |
| `ALREADY_EXISTS` | this `(tenant, execution_key)` is already held — the error names the token held | **depends on the token**, see below |
| `RESOURCE_EXHAUSTED` | at capacity | try the next instance |
| `UNAVAILABLE` | shutting down | try the next instance |
| `FAILED_PRECONDITION` | unknown `handler_key` | log, alert; routing is wrong |

**`ALREADY_EXISTS` is the executor's half of at-most-once dispatch.** When a dispatch times
out, the scheduler does not know whether it arrived, so it re-sends. An executor that starts
a second run instead of refusing turns one network hiccup into a duplicate run.

Two things must be exact about it:

**Deduplicate on `(tenant, execution_key)`, never on the key alone.** Keys are unique only
within a tenant — two tenants legitimately hold the same job name and therefore the same
deterministic key — so a multi-tenant executor deduplicating on the key would refuse tenant
B's work because tenant A's is running, and would let a `Cancel` or `GetExecution` for one
tenant reach the other's execution.

**The refusal names the token held**, because "already held" is two different situations:

| Held token | Meaning | Scheduler does |
| --- | --- | --- |
| same as the one just sent | a re-send of this attempt landed | **treat as acceptance** — `dispatching` → `running`, `attempt_no` +1, exactly as an OK reply |
| a **different** token | an older attempt the scheduler already fenced is still running there | **not** acceptance — `dispatching` → `ready` with a backoff, `attempt_no` unchanged, `dispatched_to` cleared. The old attempt is resolved by its own recovery; dispatching this one elsewhere now would put two handlers on the same work |

Without the token, the second case is indistinguishable from the first, and the scheduler
would mark an attempt `running` that no handler ever started — then adopt the *old* attempt's
progress and result as if they belonged to it.

Deduplication must last as long as the executor holds the work. An in-memory map is
sufficient: a restarted executor has lost the work anyway, and §5 covers that case.

---

## 4. Liveness: is a long execution alive or dead?

A twenty-hour execution is not an edge case to discourage; it needs a defined answer. That
answer rests on **two independent signals**, and conflating them is the classic way to get
this wrong.

| Signal | Question | Interval |
| --- | --- | --- |
| `Heartbeat` | is the executor **process** alive? | ~10s, per process |
| `ReportProgress` | is **this execution** still going? | ~30s, per execution |

Neither substitutes for the other, because the interesting failures separate them:

| Heartbeat | Progress | Conclusion |
| --- | --- | --- |
| fresh | fresh | healthy — **keep waiting, however long it takes** |
| fresh | stale past deadline | the process is fine, this execution is wedged: deadlocked, spinning, blocked on a socket. Investigate via `GetExecution` |
| stale / gone | **stale too** | the executor is lost, and every execution it held is lost with it |
| stale / gone | **fresh** | **progress wins.** Keep waiting, and treat the registration as lapsed only for *routing* |
| `known=false` | — | the registration lapsed; the executor re-registers and declares its in-flight work (§5) |

The fourth row is a precedence rule, and getting it backwards fences work that is visibly
fine. A registration heartbeat and a progress call are separate loops in the executor, so one
can fail while the other succeeds — a wedged registration goroutine, a rejected credential,
a partition affecting one path. Fresh progress is **positive evidence about this execution**;
an expired registration is only absence of evidence about the process. Positive evidence
wins.

The lapse still costs the executor its place in routing — it receives no new dispatches until
it re-registers — but work already in flight and demonstrably progressing is not reclaimed.

An executor that is alive while its handler is stuck looks perfectly healthy to a
process-level heartbeat. That is precisely what the per-execution signal exists to catch,
and why "the process is up" is never accepted as evidence that work is progressing.

### The deadline bounds silence, not runtime

Every `ReportProgress` pushes the deadline forward, so an execution that keeps reporting is
never reclaimed — twenty hours, or two hundred.

Three bounds exist and are deliberately not merged:

| Bound | Meaning | Typical |
| --- | --- | --- |
| `progress_interval_seconds` | how often the executor must speak | 30s |
| `silence_deadline_seconds` | silence tolerated before investigation | 3 × progress interval |
| `remaining_timeout_seconds` | **what is left of the hard runtime cap** | per job; 24h for a long one |

The scheduler sends what **remains** of the cap, not the job's configured value. Its own
clock started when the execution was claimed, and dispatch takes time — a bounded
unknown-outcome re-send is bounded, not instant. Sending the full cap would hand the executor
a later deadline than the scheduler is enforcing, so the scheduler would fence a run the
executor still believed it was entitled to perform, and a successor could overlap it. A
dispatch whose remaining budget has already elapsed is not sent.

The cap is enforced **on both sides, because neither alone suffices**:

- the **executor** abandons the handler at the cap and reports failure with
  `failure_kind = "timeout"`;
- the **scheduler** independently fences the attempt when the cap passes with no result,
  sends `Cancel`, and resolves the execution with `terminal_reason = "timeout"`. It does not
  wait for the executor to notice, because the case the cap exists for is a handler that has
  stopped noticing anything.

A handler that ignores cancellation cannot be stopped by either. The fence bounds what it
can still affect in the scheduler, the job's slot is released, and the residue is reported
rather than silently tolerated.

A long job configures a long `timeout_seconds` and an ordinary progress interval. Raising the
deadline to cover a job's whole runtime would convert "I notice a stuck job in 90 seconds"
into "I notice it in twenty hours".

### Progress must not depend on handler cooperation

The executor's framework calls `ReportProgress` **automatically on a timer while a handler
runs**. The handler is not required to call anything.

This matters because the handlers most likely to run for hours are the least able to report:
a single long query, a bulk load, a blocking third-party call. If liveness depended on the
handler remembering to check in, exactly those jobs would be declared dead while working
perfectly.

Handler-supplied messages (`"3000/10000 rows"`) remain available and are worth using, but
they are **observability**, not liveness. The residual case — a hung handler inside a
cheerfully reporting framework — is what `timeout_seconds` exists for, and the UI shows
time-since-last-*handler*-message next to time-since-last-progress so it is visible.

---

## 5. Reconciliation: `in_flight` at registration

The hardest question in a distributed scheduler is "did that actually run?" after either
side restarts. This contract answers it by **having the executor declare what it is holding
whenever it registers**, rather than by having the scheduler ask.

```text
executor restarts / reconnects
  -> Register(in_flight: [{execution_key, run_token}, ...])
  -> scheduler adopts what it still recognises,
     and returns `fenced` for what it does not
```

That is more reliable than querying, because only the executor knows what it is really doing,
and it costs one field on a message that is sent anyway.

### `GetExecution` and the limits of asking

`GetExecution` remains, for the case where the scheduler lost track while the executor kept
running. It is reliable for exactly one question — **"are you running this right now?"** —
and its `NOT_FOUND` must be read as **UNKNOWN, never as "it did not run"**:

| Executor state | Answer | Means |
| --- | --- | --- |
| currently running | `RUNNING` | reliable |
| just finished, still remembered | `FINISHED` + outcome | reliable |
| restarted since | `NOT_FOUND` | **unknown** — it may have run partially, or fully, before dying |

Making `NOT_FOUND` mean "did not run" would require the executor to persist execution state
durably, which is real work to build a worse answer than one that already exists: **the
handler's own idempotency key in business data.** A settlement that inserts
`settlement_done(day)` in the same transaction as its output answers "did this run?"
definitively, survives every restart, and costs one unique index.

So the division is:

| Question | Answered by |
| --- | --- |
| are you running it right now? | `GetExecution` / `in_flight` |
| did this work ever actually happen? | the handler's idempotency key, in business data |

This is why §8's first requirement is idempotency and not any of the RPCs.

---

## 6. Registration and probing

Registration is a handshake, not an announcement. The scheduler verifies before routing:

```text
executor            Register(...)            scheduler
        ------------------------------------>
                                             calls back:
        <---- Describe() -------------------  contract version, handlers, capacity
        <---- GetExecution(random nonce) ---  must answer NOT_FOUND
        ------------------------------------>
                 registration active
```

The second probe is the important one: it asks for a key the executor **cannot** hold. A
correct executor answers `NOT_FOUND`; one that never implemented the method answers
`UNIMPLEMENTED`. Those are different codes, so "declared but not implemented" is caught at
startup rather than during a failover months later.

An incompatible `contract_version` is refused outright. A refused registration is a startup
failure with a logged reason and an alert — never a silent partial capability.

### Optional capabilities degrade visibly

`Capability` lets an executor declare what it supports beyond the mandatory set. Absence is
not an error; it changes what the scheduler offers:

| Missing | Effect |
| --- | --- |
| `CAPABILITY_CANCEL` | the admin UI disables cancel for jobs routed here, rather than offering a button that does nothing |
| `CAPABILITY_PROGRESS_DETAIL` | history shows framework keepalives only; no handler messages |

---

## 7. Routing

A job names a `handler_key`. The scheduler dispatches to a live instance of a group that
declares that handler for that tenant.

Selection is **round-robin among healthy instances below capacity, failing over to the next
on a dispatch refusal**. That is the whole policy.

**Capacity is advisory, and the refusal is authoritative.** Two scheduler instances can read
`running=0, capacity=1` at the same moment and both choose the same executor; and a process
registered for several tenants advertises capacity separately in each isolated schema, so
the schedulers together can offer it more than it declared in any one. Neither is fixable
with a counter, because the counters live in different databases by design.

So the executor's own `RESOURCE_EXHAUSTED` is what actually bounds concurrency, and it must
be returned against what the process can really run **across all tenants it serves** — not
against the per-registration number. The scheduler's capacity accounting exists to spread
load, not to enforce a limit, and a refusal is an ordinary outcome rather than an error. There is deliberately no routing-strategy
family — no consistent hash, no least-frequently-used, no sticky broadcast — because each one
is a mode that must be understood during an incident and none solves a problem that
round-robin plus capacity does not.

If no instance accepts, the execution stays `ready` and is retried with bounded backoff. It
is **never marked failed for lack of an executor**: nothing was attempted, and recording a
business failure when no business code ran is a lie in the history.

An enabled job whose handler no live group declares is an **orphan**, surfaced in the UI and
alerted on, because nothing will ever run it.

---

## 8. Failure handling

| Situation | Scheduler behaviour |
| --- | --- |
| `Run` OK, result arrives | ordinary path |
| `Run` times out; unknown whether received | re-send with the same `execution_key`; the executor answers `ALREADY_EXISTS` if it has it |
| `Run` refused | try the next instance; if none accept, stay `ready` with backoff |
| accepted, then silence past the deadline | `GetExecution`; the answer decides |
| `RUNNING` | extend and keep waiting — a slow job is not a failed one |
| `FINISHED` | adopt the reported outcome |
| `NOT_FOUND`, or unreachable | the attempt is **unknown**: fence the `run_token`, then retry per policy — which is safe only because handlers are idempotent (§9) |
| registration expires while holding work, **no progress** | the unknown-attempt path; a lapsed TTL is not proof the work stopped, so reconcile before deciding |
| registration expires while holding work, **progress still fresh** | keep waiting; remove from routing only. See §4 |

### Fencing across a network

The scheduler cannot stop a process on another machine. What `run_token` gives it is the
ability to **refuse everything that attempt produces afterwards**: once fenced, its
`ReportProgress` receives `proceed=false` and its `ReportResult` receives `ABORTED`.

That bounds the damage to the scheduler's own state. It does not undo what the executor
already did to the outside world, and this contract says so rather than implying a guarantee
it cannot deliver.

---

## 9. What an executor must guarantee

Short enough to fit on one page, which is the point of separating executors from the
scheduler at all.

1. **Be idempotent per `execution_key`.** This is first because everything else depends on
   it. The scheduler will occasionally deliver the same logical work twice — after an
   unknown attempt, after an operator retry, after a partition it cannot see through. Being
   run twice must be harmless, and that property lives in business data, not in this
   protocol.
2. **Answer `ALREADY_EXISTS` for an `execution_key` you already hold.**
3. **Only answer OK to `Run` if you will really report a result.**
4. **Report a result exactly once per attempt**, retrying on transient errors and stopping
   permanently on `ABORTED`.
5. **Stop when told** — `proceed=false`, a `Cancel`, or a passed deadline.
6. **Declare `in_flight` when you register**, and re-register when `Heartbeat` says
   `known=false`.

Everything else — leases, fences, claim ordering, recovery, misfire, retry budgets — is the
scheduler's problem and stays inside it.

---

## 10. Conformance suite

The suite is the enforcement layer for everything §1 says types cannot express. It is a
black-box gRPC client run against an executor in its own CI:

```sh
gojob-conformance --target executor:9100 --tenant test
```

It must cover, at minimum:

- `Run` twice with the same `execution_key` → second returns `ALREADY_EXISTS`, and the work
  runs **once**;
- `ReportResult` with a stale `run_token` → `ABORTED`, and the executor stops rather than
  retrying;
- `ReportProgress` returning `proceed=false` → the executor actually stops;
- accepting a `Run` → a result eventually arrives;
- dispatch beyond `capacity` → `RESOURCE_EXHAUSTED`, not silent queueing;
- `GetExecution` for an unheld key → `NOT_FOUND`, not `UNIMPLEMENTED` and not an error;
- `Cancel` for an unheld key → `acknowledged=false`, not an error;
- an unknown `handler_key` → `FAILED_PRECONDITION`;
- a result with `DISPOSITION_UNSPECIFIED` → rejected; the field is required;
- a cancelled handler that stops → `DISPOSITION_STOPPED`, never `DISPOSITION_FAILED`, so the
  scheduler does not retry work an operator stopped;
- `Run` for the same `execution_key` in **two different tenants** → both accepted and run
  independently; deduplication is per `(tenant, execution_key)`;
- `Run` for a held `execution_key` with a **different** `run_token` → `ALREADY_EXISTS`
  naming the token actually held;
- a handler exceeding `timeout_seconds` → the executor abandons it and reports
  `failure_kind = "timeout"` rather than running on;
- `Cancel` and `GetExecution` for a key held under another tenant → `NOT_FOUND`.

Passing the suite is what makes something a conforming executor. "We read the document" is
not.

---

## 11. Non-gRPC executors

A gateway can transcode this contract to HTTP/JSON for teams that cannot run gRPC. It is
supported and second-class, and the trade is explicit: **the build-time guarantees of §1 do
not apply on that path.** Such an executor is verified by the registration probe and the
conformance suite only, and its first missing method appears at startup rather than at
compile time.

Choosing that path is a decision to move enforcement later. It should be a decision, not an
accident.
