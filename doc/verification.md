# Verification

The tests an implementation must pass before it can be trusted with anything that matters.

Two rules govern this list:

1. **Every item names the mechanism it exercises.** An assertion with no mechanism behind
   it is how a specification comes to require behaviour its own design cannot produce.
2. **Mocks are insufficient for anything in sections 1–4.** Row locks, `SKIP LOCKED`,
   isolation semantics and timestamp behaviour are the properties under test; a fake that
   implements them correctly is a second implementation of the thing being verified.

Real MySQL, real concurrency, real process kills. Plus Go unit tests, race detector and
static analysis for everything else.

---

## 1. Claim, lease and fencing

1. two scheduler instances racing one execution produce exactly one owner;
2. a claimant skips a state row locked by another and does not block;
3. concurrent claim, completion and recovery under sustained contention produce no
   deadlock, and every transaction locks state before execution;
4. a crashed owner's expired lease returns the execution to `ready` with `fence_epoch` and
   `recovery_count` incremented and `attempt_no` **unchanged**, and the next claim
   increments `attempt_no` exactly once;
5. a handler that kills its executor on every attempt consumes one `attempt_no` per real
   handler start, so `max_attempts = 3` yields three starts — not two;
6. an old token or fence epoch cannot heartbeat, checkpoint, succeed, fail or release the
   job lock;
7. **a claim never acquires a state row whose lease has expired but whose `active_kind` is
   still set.** The claim is refused, recovery clears the row, and only the next claim
   succeeds — proving exactly one path transfers ownership;
8. after a crash, the previous execution never remains permanently `running`: recovery
   resolves it with the old token before any new claim can overwrite the state row;
9. claim and job-lock acquisition are atomic; a **missing** state row fails closed rather
   than being reported as ordinary contention;
10. `QUEUE` and `FORBID` behave as documented, and `QUEUE` contention does not spin;
    `PARALLEL` is not an accepted `concurrency_policy` value at all;
11. retry backoff, `max_attempts` and the `dead` transition are decided in SQL and are
    deterministic given a fixed jitter seed;
12. an authorized cancel moves `running -> cancel_requested` while **keeping** the lease and
    job lock; the slot is released and the row reaches `cancelled` only after handler
    confirmation or fenced lease expiry, and no second execution starts in between;
13. the first heartbeat after a cancel still affects one row — the guard accepts
    `cancel_requested` — so ownership is not lost and the slot is not released early;
14. a `cancel_requested` row whose lease expires is recovered to `cancelled`, never to
    `ready`;
15. graceful shutdown stops claiming and does not release a live lease early.

---

## 2. Cron materialization

16. the state-row scan materializes a due execution **with the timer process disabled
    outright**, and the run happens within one scan interval;
17. two replicas scanning the same due job produce exactly one execution row and one
    `next_fire_at` advance: the loser skips the locked state row rather than reaching the
    insert, and on its next pass observes an already-advanced `next_fire_at`;
18. process restart requires no separate heap-rebuild step — the scan alone recovers all due
    work without duplicate execution rows;
19. duplicate schedule creation is rejected by `execution_key`;
20. `SKIP` and `FIRE_ONCE` produce exactly the documented number of catch-up executions
    after a long outage, and neither replays unbounded;
21. every cron expression in the supported dialect yields the fire instants its
    specification requires, over a boundary corpus and a multi-year horizon, evaluated in
    the configured location.

---

## 3. Fixed-delay passes

22. exactly one pass of a job is in flight at any moment, across two scheduler instances and
    two executors;
23. the next pass is dispatched `delay` after the previous **result**, not after its
    dispatch — verified with a pass that runs longer than the delay, which must not overlap
    its successor;
24. **liveness, not just safety**: with a poller running continuously, a manual trigger for
    that job **executes within a bounded time** and reaches a terminal state.

    An assertion that merely proves the manual run does not overlap the poll would pass
    against a design in which it never runs at all. That is the specific defect this test
    exists to catch;
25. a pass reporting `did_work=false` is retained briefly and then swept, so a redelivered
    result can still be answered `already_recorded=true`; one reporting `did_work=true`
    persists for the ordinary retention window; a **failed** pass persists regardless of
    `did_work`;
26. an executor that dies mid-pass leaves an expired execution that recovery reconciles and
    resolves, after which the next pass can be dispatched;
27. pausing a job stops further passes being dispatched, and the in-flight one is allowed to
    finish rather than being abandoned.

---

## 4. Time

28. ownership decisions use database time only and are unaffected by a shifted business
    clock;
28a. a claim is not dispatched on once its lease may have elapsed, measured in MONOTONIC time
    since ownership was last proved — a frozen process must not hand an executor work another
    instance has already recovered, which is the one failure fencing cannot undo;
28b. the control-plane fence expires on elapsed monotonic time, so a backward host-clock step
    cannot make an old registry read look fresh and let a written-off instance keep claiming;
28c. the ownership bound is carried INTO the dispatch call's context, not only checked before
    it, so a process suspended between the check and the send wakes with an expired context
    rather than a satisfied check;
28d. a reconciliation answer that does not name the attempt it describes, or reports FINISHED
    without a disposition, is treated as unknown — spending a recovery rather than an attempt,
    and never adopting a state that belongs to another run;
28e. a drain stops CLAIMING and keeps RENEWING, so an in-flight lease cannot expire inside the
    drain window and leave work only a stopped recovery pass could clear;
28f. retirement settles expired work owned by a crashed instance as well as its own, without
    adopting anything — a running handler is left held, which is quiescence reporting
    correctly rather than failing to clean up;
28g. executor-supplied strings are bounded before they reach a column, so a long message
    cannot make a terminal write fail identically on every retry and turn a success into a
    recorded timeout;
28h. an OK dispatch acknowledgement naming a different execution or attempt is unknown, not
    accepted;
28i. retiring a generation records an observation for it, so the acknowledgement half of the
    cutover gate is satisfied by an answer rather than by a liveness timeout;
28j. an execution key with a live handler is never released, whichever attempt holds it: a
    reconciliation reporting RUNNING under a DIFFERENT token holds the row rather than
    resolving it, and the runtime cap remains the bound;
28k. an instance that is still ADMITTING blocks a cutover, and the gate is re-read inside the
    transaction that moves the DSN — a snapshot taken before the new schema was verified is
    not evidence at the moment of the write;
28l. an ownership proof is dated to BEFORE the call that established it, so a process
    suspended between the commit and its bookkeeping cannot make an old fact read as new;
28m. retirement asks the executor before ending its own in-flight work: a RUNNING answer
    leaves the row held and the tenant un-quiet, because a delivered cancel is not a stopped
    handler and quiescence must not be a forced database transition presented as proof;
28n. the blocker read holds the range it read for the duration of the cutover transaction,
    and admission confirms the generation against the REGISTRY — not against the poll that
    started it — as its last act before publishing an engine;
28o. a manual trigger's request_id identifies one job: reused against another it is a
    conflict, and the loser of a race reads back the row that was actually created;
28p. an admission's generation proof expires: past the staleness limit it is abandoned rather
    than acted on, and a superseded engine stops CLAIMING before the pass that refreshes the
    right to operate;
28q. retirement DEFERS while a handler is still running — the engine, its routing and its pool
    stay alive so that handler can still report and something is left to recover it — and the
    next reconciliation retries;
28r. retirement applies a confirmed FINISHED outcome rather than recording `dead`/unknown over
    a success the executor was still holding;
28s. a result another writer recorded concurrently is answered already_recorded, not ABORTED;
28t. the right to operate is refreshed only by control knowledge that is FRESH — a pass slower
    than the staleness limit does not refresh — and COMPLETE: an instance whose observation
    did not land loses the fence rather than keeping it, because a cutover cannot see what it
    was not told;
28u. an admission whose pre-admission observation fails does not start;
28v. every HOLDING pass — heartbeat, timeout, silence, cancel — runs through a drain, which is
    what makes the runtime cap the bound on a deferred retirement;
28w. retirement reports work it could not settle rather than reporting success: a failed
    listing, a page it could not progress, an exhausted page budget and an unsettled recovery
    sweep all defer;
28x. disabling or demoting an admin account takes effect on the next request, not at session
    expiry;
29. business timestamps match the configured `Clock`, stay whole-second, and are never
    compared against the database clock — including when it is wrapped, so
    `available_at <= TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP())` fails the check too;
30. admission fails when the driver would parse timestamps in a location other than the
    configured `Location`, or when this host's UTC clock and the database's disagree by more
    than a minute. It does NOT constrain the session time zone: ownership is written and
    compared with `UTC_TIMESTAMP()`, business columns are values this process computes, and
    no column carries a `CURRENT_TIMESTAMP` default, so the session clock participates in
    nothing;
31. a schedule edit to a job whose `next_fire_at` is far in the future takes effect within
    one drift-scan interval — tested on a weekly job re-pointed to fire within the hour —
    rather than when the stale fire instant eventually arrives;
32. changing the business clock recomputes every cron `next_fire_at` for that tenant under
    state-row locks before the call returns, and acquiring the `CONTROL_PLANE` role
    recomputes them once at startup, so a shift applied while the process was down is still
    honoured.

---

## 5. Multi-tenancy and isolation

33. two tenant schemas hold identical job names and identical row ids without interference;
34. one tenant's database failure cannot route work to another tenant, and does not prevent
    other tenants' runtimes from serving;
35. admission is fail-closed **per tenant**: a missing table, an unreachable database, an
    unresolved credential, an invalid duration or an unsupported schema version prevents
    that tenant from being admitted, records `last_error`, and is retried on a backoff;
36. **a broken tenant does not take down healthy ones.** Add a tenant with a malformed DSN
    to a scheduler already serving several; every existing tenant keeps running while the
    new one is visibly failed. This is the specific regression hot add would otherwise
    introduce;
37. a tenant added to `tenant_registry` is admitted within one registry-poll interval with
    no restart; disabling it drains in-flight work and releases the pool;
38. DSNs are stored encrypted and never returned in plaintext by the API — reads are masked;
39. no query issued against a tenant omits the tenant boundary, because no query carries a
    tenant predicate to omit — verified by asserting every statement runs on a tenant-bound
    connection.

---

## 6. Jobs, executors and deployment

40. a job can be created only through the API with a `handler_key`; the scheduler holds no
    handler code and materializes no job from any registry of its own;
41. an orphaned job — no live executor anywhere declares its handler — stays `ready`, raises
    an alert, is visible in the UI, and is **never** marked failed, because nothing was
    attempted;
42. an orphan never prevents a scheduler from starting; refusing to start because an executor
    is down turns one outage into two;
43. an executor restart produces a new `executor_id` and never inherits or has to clean up
    its predecessor's registration row;
44. an executor declaring a handler no job uses is harmless and visible;
45. a job whose `handler_key` matches no declared handler can still be created — an executor
    may simply be down — but is flagged as an orphan until one appears.

---

## 7. Control plane and API

43. every mutating endpoint rejects a missing or empty `reason`, and rejects an
    unattributable actor rather than defaulting one;
44. an edit with a stale `If-Match` version returns `409` and changes nothing;
45. pausing a job races a claim deterministically: the claim either commits before the pause
    or is refused by it, never producing one extra run after the pause is acknowledged;
46. the job list names **every** failed runnability condition, not the first one;
47. `cancelled` executions are distinguishable in the API and UI between handler-confirmed
    and fenced-with-unverified-effects;
48. a `VIEWER` cannot perform any action in `admin.md` §5.

---

## 8. Retention and growth

49. terminal-row retention is bounded, indexed, batched and interruptible, and **never
    deletes a non-terminal row**;
50. executor registration rows age out on heartbeat expiry;
51. audit rows are retained for the configured window and are never silently truncated;
52. a scheduler running continuously for the retention window does not grow without bound in
    any table it owns — asserted by row counts, not by inspection.

---

## 9. Failure injection

The properties above are asserted under normal operation. These are asserted while things
are breaking:

53. `SIGKILL` of an executor mid-handler: the execution is recovered, budgets are consumed
    correctly, and no second executor starts before recovery completes;
54. database connection loss during a handler: ownership is lost cleanly, the fence prevents
    further writes, and the execution is later recovered;
55. a handler that ignores context cancellation and runs past its lease: its **scheduler-state
    writes** — progress, result, lock release — are all rejected by the fence, and the fact
    that it cannot be stopped is reported rather than silently tolerated. Its business writes
    are explicitly **not** covered: nothing in this system sits between a handler and its own
    database, and a test asserting otherwise could never pass;
56. clock skew between hosts does not affect ownership, because ownership never reads a host
    clock;
57. a slow database making heartbeats late: ownership loss is detected and the handler
    stops, rather than two owners proceeding;
58. **a handler running far longer than its lease keeps its job**: with a handler held
    running for many multiples of `lease_seconds` and the heartbeat healthy, the lease never
    expires, recovery never fires, and no second executor claims it. The lease bounds heartbeat
    absence, not execution time, and this test is what pins that down;
59. a handler that exhausts the connection pool does **not** starve the heartbeat: renewal
    uses a reserved connection and continues to succeed while every handler connection is
    checked out;
60. a process paused (`SIGSTOP`) for longer than its lease loses ownership, and on resume
    every scheduler write it attempts affects zero rows — the fence holds against a
    resurrected owner.

---

## 10. Dispatch contract

Asserted against a real executor process, not a mock, because the properties under test are
what a real executor gets wrong. These duplicate the conformance suite of `dispatch.md` §10
deliberately: that suite proves an *executor* conforms, these prove the *scheduler* behaves
correctly when one does not.

61. a dispatch that times out is re-sent with the same `execution_key`, the executor answers
    `ALREADY_EXISTS`, and the work runs **once**;
62. an executor answering `RESOURCE_EXHAUSTED` or `UNAVAILABLE` causes failover to the next
    instance; when no instance accepts, the execution stays `ready` with backoff and is
    **never** marked failed — nothing was attempted;
63. an accepted execution that goes silent past its deadline triggers `GetExecution`, and
    each of `RUNNING`, `FINISHED` and `NOT_FOUND` produces the documented outcome;
64. `NOT_FOUND` is treated as **unknown**, not as "did not run": the attempt is fenced and
    retried under the stated idempotency assumption, and the log says so;
65. a fenced attempt's `ReportProgress` receives `proceed=false` and its `ReportResult`
    receives `ABORTED`; neither mutates scheduler state;
66. an executor re-registering with `in_flight` has its still-recognised executions adopted
    and its unrecognised ones returned in `fenced`;
67. parameters reach the executor exactly as configured, merged with any trigger override,
    and the execution row records the merged value — a later edit to the job's defaults does
    not change what history says that run used;
68. a scheduler instance killed while an execution is running does **not** cause a
    re-dispatch: the instance that takes over reconciles with the executor first, adopts the
    run with the same `run_token` and a new fence epoch, and lets it finish.

---

## 11. Contract-hardening cases from adversarial review

Each of these corresponds to a defect found before implementation, and exists so the fix
cannot regress silently.

69. a callback (`Heartbeat`, `ReportProgress`, `ReportResult`) carrying a tenant the caller's
    identity is not authorized for is refused; a `Register` naming another tenant or group is
    refused and alerted;
70. two tenants holding the same job name and therefore the same `execution_key` are routed
    correctly, with callbacks arriving at an arbitrary scheduler instance;
71. repeated `RESOURCE_EXHAUSTED` refusals never consume `attempt_no` and never drive a job
    to `dead` without a handler having started;
72. a `dispatching` row whose scheduler dies is recovered like a `running` one, reconciling
    with `dispatched_to` before deciding;
73. **entering recovery never re-dispatches a run the executor is still performing**: with an
    executor deliberately held mid-run, kill the owning scheduler and assert the successor
    adopts with the same `run_token` and a new fence epoch;
74. a fixed-delay pass survives scheduler failover: the pass's execution row, parameters and
    budgets are readable by the instance that takes over, and a `did_work=true` result
    reported to a *different* instance is recorded correctly;
75. a successful `ReportResult` arriving after `cancel_requested` records `success`, not
    `cancelled`, and the cancel request remains in the audit trail;
76. redelivering a result already recorded returns `already_recorded=true`, never `ABORTED`;
77. a manual trigger is selected ahead of a due poll of the same job, repeatedly, under
    sustained polling — mutual exclusion is not fairness;
78. heartbeat renews both rows in one transaction in the canonical order, and sustained
    heartbeat/completion contention produces no deadlock;
79. retiring a job stops new materialization, moves `ready` executions to `cancelled` and
    running ones to `cancel_requested`; a running handler that succeeds anyway is recorded as
    `success`, not overwritten as cancelled; every row reaches a terminal state within the
    job's own lease and timeout bounds, with no extra mechanism;
80. disabling a tenant completes within `drain_timeout`, fencing whatever is still
    outstanding rather than waiting indefinitely; **changing `coordination_dsn` on an enabled
    tenant is rejected**, so no interval exists in which two schedulers work one tenant across
    two schemas;
81. the cron cases of `scheduling.md` §1 — day-of-month/day-of-week OR semantics, rejected
    `L`/`W`/`#`, rejected impossible dates, spring-forward and fall-back — each produce the
    single documented answer;
82. every validation bound in `data-model.md` §0.4 is rejected at the API, not discovered
    later;
83. `Cancel` and `GetExecution` are tenant-scoped: a key held for tenant A is invisible to a
    request naming tenant B;
84. a `Run` refused with `ALREADY_EXISTS` naming a **different** token does not mark the new
    attempt `running`, and the scheduler never adopts the older attempt's progress or result
    as the new attempt's;
85. a scheduler killed between sending `Run` and recording acceptance does not cause a second
    dispatch: `dispatched_to` was written before the send, so recovery reconciles;
86. `timeout_seconds` is enforced by the executor **and** independently by the scheduler; a
    handler that ignores cancellation still yields a terminal execution with
    `terminal_reason = 'timeout'` and a released slot;
87. an operator retry of a `dead` execution raises `max_attempts` and continues the attempt
    numbering; the attempt-history insert never collides;
88. a manual trigger is never `skipped` by `FORBID`; it queues and eventually runs;
89. sustained manual load does not prevent cron and poll executions from being discovered,
    and sustained poll load does not prevent manual ones — each class has a bounded share;
90. a job naming a required `executor_group` is dispatched only to that group, even when
    another group declares the same `handler_key`;
91. recovery holds no database lock while calling `GetExecution`, and a wedged executor that
    never answers does not block completion, cancellation or another recovery for that job;
92. a result arriving while the execution is still `dispatching` — a handler faster than the
    acceptance write — is accepted, and applies the acceptance effects and the outcome
    together;
93. an executor reporting `DISPOSITION_STOPPED` moves the execution to `cancelled` with
    `terminal_reason = 'handler_confirmed'`, and is never treated as a retryable failure that
    restarts the work an operator cancelled;
94. materializing a fixed-delay pass clears `next_poll_at` in the same transaction, so a
    second scanner cannot create a second pass; a retryable pass failure leaves it clear, so
    the retry is the next pass rather than a second one;
95. `timeout_at` is durable and survives failover: an execution adopted by another scheduler
    instance is still capped at the original instant, neither granted a fresh cap nor left
    uncapped;
96. a DSN change is refused until every live scheduler instance reports the disable
    generation quiesced, and the refusal names which instance is blocking;
97. the attempt-history row is written in the same transaction as the terminal or retry
    transition, so a result redelivery is always answerable;
98. a job bound to an executor group is reported unrunnable when the only executors declaring
    its handler are in another group — not reported runnable and then never dispatched;
99. a repeated manual trigger carrying the same `request_id` returns the first execution and
    creates no second run;
100. a `Register` from an identity with no `executor_identity` row for the claimed tenant and
    group is refused;
101. **a scheduler partitioned from the control database stops claiming, materializing AND
    renewing within `control_staleness_limit`**, so the work it held becomes recoverable by
    healthy instances and the old schema can actually reach zero;
101a. a DSN change is accepted only when a **scan of the old coordination schema** shows no
    held `job_state` row and no non-terminal execution — not when the reachable instances
    happen to have acknowledged. Run it with one instance partitioned and still holding work:
    the change must be refused;
102. `timeout_at` is written in the claim transaction: a scheduler killed after the executor
    accepted but before recording acceptance leaves a successor with the **original** cap,
    neither refreshed nor absent;
103. orphaned and otherwise unrunnable executions have `available_at` pushed forward on
    rejection, so a page of permanently unrunnable rows cannot hide every newer runnable one;
104. an executor that heartbeats but never answers `Run` causes a bounded number of re-sends,
    after which the row is left to recovery and the executor is deprioritised in routing — it
    never sits `dispatching` indefinitely with its lease renewed;
105. while a manual execution is `ready` for a job, no new cron instant or poll pass is
    materialized for that job, so the manual run acquires the job at the next release;
106. an executor whose registration lapses while its per-execution progress stays fresh keeps
    its work — it is removed from routing only, and positive evidence about an execution
    outranks absence of evidence about its process;
107. recovery restores `next_poll_at` only when it ends the pass; a recovered pass returned to
    `ready` leaves it `NULL`, so no second pass is materialized alongside it;
108. a repeated trigger with the same `request_id` is rejected by the unique key and returns
    the original execution;
109. an attempt whose acceptance reply was lost, and whose executor restarted before the
    re-send, is recorded in attempt history as `unknown` **without** consuming `attempt_no`
    and **without** colliding with the earlier attempt that shares that ordinal — the case
    that proves attempt identity is the token, not the number;
110. **scheduler and executor never disagree about the runtime cap**: with a dispatch delayed
    close to the re-send bound, the executor's budget and the scheduler's `timeout_at` expire
    within tolerance of each other, and the scheduler never fences a run the executor still
    considers live. A dispatch whose remaining budget has elapsed is not sent at all;
111. a result carrying `DISPOSITION_UNSPECIFIED` is rejected, and a cancelled handler
    reporting `DISPOSITION_STOPPED` is never retried;
112. admission refuses a tenant whose coordination schema presents no `schema_identity` row,
    names a different tenant, or carries a different `schema_uuid` than the registry expects —
    tested against an empty schema, another tenant's schema, and a restored snapshot;
113. a DSN change is refused unless the old schema is quiescent **and** the new one presents
    the expected identity; neither check alone permits the cutover;
114. `POST /jobs` writes `job_definition` and `job_state` in one transaction, and the new job
    actually runs: a cron job at its first instant at or after creation, a poller promptly
    rather than after one delay, and neither fires for instants that passed before creation;
115. a result arriving after `timeout_at` is refused: the cap wins regardless of which writer
    reaches the database first, and the execution records `dead` with
    `terminal_reason = 'timeout'` whether the executor noticed or the scheduler did;
116. `DISPOSITION_TIMED_OUT` resolves to `dead` and is not retried;
117. `tenant_observation` and `control_audit` are bounded: a cluster restarting repeatedly,
    each process minting a new `instance_id`, does not grow either table without limit.

---

## 12. What this list does not cover

Verification of **handler logic** is the host's responsibility, not this library's. This
list proves the scheduler runs a handler exactly once per accepted execution and reports
honestly what happened; it says nothing about whether the handler computes the right
answer.

Hosts migrating existing scheduled work from another implementation need their own
equivalence strategy — differential replay against the previous implementation, coverage of
the handler's decision points, and property tests for anything computing money. That work
belongs in the host's repository, against the host's data, and is out of scope here.
