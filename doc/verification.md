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

1. two workers racing one execution produce exactly one owner;
2. a claimant skips a state row locked by another and does not block;
3. concurrent claim, completion and recovery under sustained contention produce no
   deadlock, and every transaction locks state before execution;
4. a crashed owner's expired lease returns the execution to `ready` with `fence_epoch` and
   `recovery_count` incremented and `attempt_no` **unchanged**, and the next claim
   increments `attempt_no` exactly once;
5. a handler that kills its worker on every attempt consumes one `attempt_no` per real
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

## 3. Fixed-delay loops

22. a singleton loop runs exactly one instance across two worker processes;
23. **liveness, not just safety**: with a healthy loop running continuously and never
    releasing its lease, a manual trigger for that job **executes within a bounded time**
    and its execution row reaches a terminal state.

    An assertion that merely proves the manual run does not overlap the loop passes against
    a design in which it never runs at all. That is the specific defect this test exists to
    catch;
24. a manual trigger issued while no loop holds the lease stays `ready` and is executed by
    the next loop that acquires it — never skipped, never lost;
25. a loop whose process dies leaves an expired state-row lease that recovery clears, after
    which a new loop can acquire it;
26. an empty poll produces metrics but no execution or history row;
27. pausing a job stops loop-lease renewal, so the loop exits within one pass and its
    absence is readable from the state row.

---

## 4. Time

28. ownership decisions use database time only and are unaffected by a shifted business
    clock;
29. business timestamps match the configured `Clock`, stay whole-second, and are never
    compared against `NOW()`;
30. admission fails when the database session time zone differs from the configured
    `Location`;
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
35. admission is all-or-nothing per tenant and fail-closed: a missing table, an unreachable
    database, an unresolved credential, an invalid duration or an unsupported schema version
    prevents readiness;
36. no query issued by the library omits the tenant boundary, because no query carries a
    tenant predicate to omit — verified by inspecting that every statement runs on a
    tenant-bound connection.

---

## 6. Registry, roles and deployment

37. a job whose handler is outside this deployment's assignment is not claimed by it and is
    **not** an admission failure; a handler inside the assignment but absent from the build
    registry **is**;
38. an orphaned job — no live worker anywhere serves its handler — raises an alert and is
    visible in the UI, but never prevents a worker from starting. Refusing to start because
    another deployment is down turns one missing lane into two;
39. registry reconciliation inserts missing jobs with declared defaults and **never**
    overwrites an operator edit;
40. reconciliation is idempotent under concurrency: running it twice, or from two processes,
    inserts no duplicates and loses no edit;
41. a control-plane deployment with an empty execution assignment reconciles and serves the
    API while claiming no work;
42. a worker restart produces a new `worker_id` and never inherits or has to clean up its
    predecessor's registration row.

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
50. worker registration rows age out on heartbeat expiry;
51. audit rows are retained for the configured window and are never silently truncated;
52. a scheduler running continuously for the retention window does not grow without bound in
    any table it owns — asserted by row counts, not by inspection.

---

## 9. Failure injection

The properties above are asserted under normal operation. These are asserted while things
are breaking:

53. `SIGKILL` of a worker mid-handler: the execution is recovered, budgets are consumed
    correctly, and no second worker starts before recovery completes;
54. database connection loss during a handler: ownership is lost cleanly, the fence prevents
    further writes, and the execution is later recovered;
55. a handler that ignores context cancellation and runs past its lease: its writes are
    rejected by the fence, and the fact that it cannot be stopped is visible rather than
    silently tolerated;
56. clock skew between worker hosts does not affect ownership, because ownership never reads
    a host clock;
57. a slow database making heartbeats late: ownership loss is detected and the handler
    stops, rather than two owners proceeding;
58. **a handler running far longer than its lease keeps its job**: with a handler held
    running for many multiples of `lease_seconds` and the heartbeat healthy, the lease never
    expires, recovery never fires, and no second worker claims it. The lease bounds heartbeat
    absence, not execution time, and this test is what pins that down;
59. a handler that exhausts the connection pool does **not** starve the heartbeat: renewal
    uses a reserved connection and continues to succeed while every handler connection is
    checked out;
60. a process paused (`SIGSTOP`) for longer than its lease loses ownership, and on resume
    every scheduler write it attempts affects zero rows — the fence holds against a
    resurrected owner.

---

## 10. What this list does not cover

Verification of **handler logic** is the host's responsibility, not this library's. This
list proves the scheduler runs a handler exactly once per accepted execution and reports
honestly what happened; it says nothing about whether the handler computes the right
answer.

Hosts migrating existing scheduled work from another implementation need their own
equivalence strategy — differential replay against the previous implementation, coverage of
the handler's decision points, and property tests for anything computing money. That work
belongs in the host's repository, against the host's data, and is out of scope here.
