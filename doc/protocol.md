# Execution protocol

This document specifies how exactly one owner is established, kept, proved and reclaimed.
It is the part of the system that must be right; everything else is a convenience on top of
it.

## 0. Two layers of ownership

Since the scheduler runs as a cluster and executors are separate processes, "who owns this
work" has two answers at once, and keeping them distinct is what keeps the design tractable.

| Layer | Question | Mechanism | Specified in |
| --- | --- | --- | --- |
| **1 — internal** | which *scheduler instance* owns materializing, dispatching and tracking this execution? | MySQL: row lock, guarded CAS, lease, fence epoch | this document |
| **2 — external** | which *executor instance* is running it, and is it still alive? | HTTP: dispatch, registration heartbeat, per-execution progress | `dispatch.md` |

Layer 1 is what the rest of this document specifies. Every scheduler instance is
**identical and equal** — there is no leader, no designated node and no configured control
plane. Instances coordinate entirely through the tables, so the cluster can be scaled,
rolled and lost a node at a time without any of them being special.

The two layers fail independently, and section 10 covers the interesting case: a scheduler
instance dying while an executor is happily running the work it dispatched.

Throughout this document, "owner" means the **scheduler instance** that holds an execution.
The process actually running business code is the **executor**, and it owns nothing — it is
told what to run and reports what happened.

---

MySQL is the only mechanism for layer 1. There is no coordination service, no consensus
protocol and no lock server. Four ingredients do the whole job:

| Ingredient | Prevents |
| --- | --- |
| a row lock on `job_state` | two scheduler instances deciding simultaneously |
| a guarded CAS (`active_kind IS NULL`) | a decision based on a stale read |
| a lease with heartbeats | a crashed owner holding the job forever |
| a fence epoch on every ownership write | a revived owner overwriting its successor |

Remove any one and the protocol is unsound. Together they are sufficient, and the number of
scheduler instances is irrelevant to that — exclusion comes from the single state row,
not from knowing how many instances exist.

---

## 1. Canonical lock order

Every transaction that touches more than one row acquires locks in exactly this order:

```text
job_state (job_name)
  -> job_execution (id)
```

Claim, completion, retry, recovery and administrative actions all follow it. This is not a
style rule. If a claim took execution rows first and completion took the state row first,
a claim holding an execution row and waiting on the state row deadlocks against a
completion holding the state row and waiting on the execution row. Any new transaction that
seems to need a different order is a design change, not an implementation detail.

`job_definition` is **never locked in the hot path**. It is read without a lock and
reconciled through `config_version`, so an operator's edit never contends with a running
job.

Because the state row must be locked first, **candidate discovery is a non-locking read**
whose result the claim transaction re-verifies:

```sql
SELECT id, job_name
FROM job_execution
WHERE status = 'ready'
  AND available_at <= ?          -- business time, supplied by the scheduler
ORDER BY manual_first DESC, available_at, id
LIMIT ?;
```

`manual_first` is a stored column set at creation — `1` for `trigger_type = 'manual'`, `0`
otherwise — so operator-initiated work is selected ahead of scheduled work of the same job.

Without it, exclusion alone does not give **fairness**: a fast poller releases the job lock
and immediately becomes due again, and nothing stops it winning the next claim
indefinitely while an operator's manual run waits. Mutual exclusion is not liveness, and a
trigger button that works only when the queue happens to be quiet is not a working trigger
button.

---

## 2. Claim

Per candidate, one short transaction:

1. `SELECT ... FROM job_state WHERE job_name = ? FOR UPDATE SKIP LOCKED`.
   A skip means another scheduler instance is already deciding about this job; abandon this candidate
   and move on without blocking.
2. Read `job_definition` and evaluate runnability (section 3). A failure here is a logged
   rejection, not contention.
3. Acquire the job lock with the guarded update below.
4. `SELECT ... FROM job_execution WHERE id = ? FOR UPDATE`. Under the canonical order
   nobody else can hold this row, so a failure here is an assertion failure, not a skip.
5. Guarded claim update.

```sql
UPDATE job_state
SET active_kind      = 'EXECUTION',
    active_execution = ?,
    active_owner = ?,
    active_run_token = ?,
    fence_epoch      = fence_epoch + 1,
    lease_until      = TIMESTAMPADD(SECOND, ?, NOW()),
    heartbeat_at     = NOW(),
    updated_at       = ?
WHERE job_name   = ?
  AND ops_paused = 0
  AND active_kind IS NULL;       -- NOT "or the lease expired" — see section 6
```

```sql
UPDATE job_execution
SET status         = 'dispatching',
    owner_instance = ?,
    run_token      = ?,
    fence_epoch    = ?,
    lease_until    = TIMESTAMPADD(SECOND, ?, NOW()),
    heartbeat_at   = NOW(),
    updated_at     = ?
WHERE id = ?
  AND status = 'ready'
  AND attempt_no < max_attempts;
```

Every statement's affected-row count is asserted; any mismatch rolls the transaction back.
**Dispatch happens only after commit.**

### Claiming is not attempting

The claim commits `dispatching`, **not** `running`, and does not touch `attempt_no`. An
attempt is a *handler start*, and at this point nothing has started: the chosen executor may
answer `RESOURCE_EXHAUSTED` because it is busy, or `UNAVAILABLE` because it is shutting
down. Consuming a retry budget for an executor's capacity is a defect — a job whose
executors are saturated would march to `dead` without one line of business code running.

So dispatch has two outcomes, each a short fenced transaction:

| Executor answers | Transition | `attempt_no` | `dispatched_to` |
| --- | --- | --- | --- |
| OK / `ALREADY_EXISTS` | `dispatching` → `running` | **+1** | set |
| `RESOURCE_EXHAUSTED`, `UNAVAILABLE` | `dispatching` → `ready`, release the job lock, try the next instance; when none accept, apply a bounded backoff | unchanged | unset |
| `FAILED_PRECONDITION` (unknown handler) | `dispatching` → `ready`, alert: routing is wrong | unchanged | unset |
| transport error, outcome unknown | stay `dispatching`; re-send to the **same** executor with the same `execution_key`, which answers `ALREADY_EXISTS` if it has it | +1 on eventual acceptance | set |

```sql
-- acceptance
UPDATE job_execution
SET status        = 'running',
    dispatched_to = ?,
    attempt_no    = attempt_no + 1,
    started_at    = COALESCE(started_at, ?),
    deadline_at   = TIMESTAMPADD(SECOND, ?, NOW()),
    updated_at    = ?
WHERE id = ? AND status = 'dispatching'
  AND run_token = ? AND fence_epoch = ?;
```

`dispatched_to` is written on acceptance, before any result can arrive, so a takeover always
knows which executor to reconcile with (section 10).

A `dispatching` row whose lease expires is recovered exactly like a `running` one, and for
the same reason: the scheduler that was mid-dispatch may have died after the executor
accepted.

`attempt_no` is incremented on acceptance and nowhere else. Recovery never touches it
(section 7).

---

## 3. Runnability

Evaluated inside the claim transaction, in this order, with every failed condition recorded
rather than collapsed into one boolean:

1. `job_definition.enabled = 1` and the job is not retired;
2. `job_state.ops_paused = 0`;
3. **at least one live executor declares this job's `handler_key`** for this tenant;
4. the schema version matches what this scheduler requires.

Condition 3 replaces what an earlier revision called "resolves in this binary's registry" —
a leftover from a design in which handlers were compiled into the scheduler. **The scheduler
holds no handler code.** It knows a `handler_key` only as a string that executors declare and
operators select, so the only meaningful question is whether anything alive can run it.

A job failing condition 3 is an **orphan**: it stays `ready`, is never dispatched, and is
alerted on. It is never marked failed, because nothing was attempted.

---

## 4. Concurrency policies

Two, and no more in the first delivery:

- **`QUEUE`** — leave the execution `ready` and push `available_at` forward by a bounded
  contention backoff, so a blocked claim does not spin against a busy job. Default.
- **`FORBID`** — mark this occurrence `skipped`, recording which execution holds the job.

A policy is evaluated **only after** the state-row `SELECT ... FOR UPDATE` has proved the
row exists and is genuinely held. Any other zero-row outcome — a missing state row, a
paused job, a schema mismatch — is an operational failure and must never be reported as
ordinary contention. Conflating "someone else is running it" with "the row is missing"
turns a broken installation into a silently idle one.

There is no `PARALLEL`. `job_state` holds a single active holder, and heartbeat,
completion, retry and recovery all guard on that row's token and epoch; concurrent
executions of one job would have no defined completion protocol. If a job genuinely needs
concurrency, it needs a per-execution lease table — a schema change with its own review,
not a configuration value.

`REPLACE` is out of scope: it requires a cancellation protocol strong enough to prove the
displaced run stopped, which section 8 explicitly declines to promise.

---

## 5. Lease, heartbeat and fencing

Ownership decisions use **database time only**. A scheduler never compares a lease against its
own host clock, because that would make ownership depend on clock skew between machines.

```text
heartbeat interval  <=  lease / 3
handler timeout         per job
shutdown grace      <   remaining lease, where practical
```

Both rows are renewed in **one transaction, in the canonical order** — `job_state` first,
then `job_execution`. Renewing the execution first would take the two rows in the opposite
order to completion and reproduce exactly the deadlock section 1 exists to prevent; renewing
them in separate transactions would let a crash between the two leave one lease live and the
other expired, with no rule saying which is authoritative.

```sql
-- 1. state row first
UPDATE job_state
SET lease_until = TIMESTAMPADD(SECOND, ?, NOW()), heartbeat_at = NOW(), updated_at = ?
WHERE job_name = ?
  AND active_run_token = ? AND fence_epoch = ?;

-- 2. then the execution row, same transaction
UPDATE job_execution
SET lease_until = TIMESTAMPADD(SECOND, ?, NOW()), heartbeat_at = NOW(), updated_at = ?
WHERE id = ?
  AND status IN ('dispatching', 'running', 'cancel_requested')
  AND owner_instance = ? AND run_token = ? AND fence_epoch = ?
  AND lease_until >= NOW();
```

`status IN ('running', 'cancel_requested')` is deliberate: a cancelled-but-not-yet-stopped
handler must keep renewing, because releasing its slot before it has actually stopped is
exactly the overlap this protocol exists to prevent.

If either guarded update affects zero rows, ownership is lost. The scheduler abandons the
handler context, emits `fence_lost`, and **prohibits all further scheduler and business
writes from that execution**.

Every ownership-bearing write — progress, retry, success, failure, release — carries
`run_token` and `fence_epoch`. This is what makes a revived zombie harmless: it holds an
epoch that no longer matches, so every statement it attempts affects zero rows.

### Long-running handlers

**The lease is not a maximum execution time.** It bounds how long ownership survives
*without a heartbeat*, not how long a handler may run. A handler that runs for twenty hours
keeps its job for twenty hours, because the heartbeat goroutine renews `lease_until` every
`lease/3` throughout. Recovery fires only when the heartbeat has actually stopped — which
means the process died, lost the database, or is wedged badly enough that it cannot run a
goroutine.

So the common worry — "will a long job be reclaimed mid-run and started by someone else?" —
has the answer *no, while the process is healthy*. The failure modes that do exist are
worth naming precisely, because two of them look like health:

**Connection-pool starvation.** A handler that consumes the entire pool leaves the
heartbeat unable to acquire a connection. Renewal fails, the lease expires, recovery hands
the job to another executor — while the original process is alive and still working. This is
the most realistic way to lose a long job, and the mitigation is structural: **the
heartbeat uses a dedicated reserved connection**, never one shared with handler work.

**Stop-the-world pauses.** A GC pause or paging storm longer than the whole lease expires
it. The heartbeat interval of `lease/3` tolerates two consecutive misses, so the lease
should be sized against the worst tolerable pause — not against the handler's runtime.
Sizing a lease to "how long the job takes" is the wrong axis entirely.

**Ownership lost without the handler noticing.** If ownership is genuinely fenced away, the
handler does not stop by itself. It finds out only when it calls `Fence()`. A handler that
runs for hours without checking will keep writing after it has lost the job.

Hence the requirement: **long handlers work in bounded chunks and re-check the fence before
each chunk.** A single uninterruptible operation longer than the lease is not acceptable
without a job-specific safety argument, because there is no way to stop it and no way to
prove it stopped.

### What the fence does and does not protect

The fence protects **scheduler state** completely. An execution that has lost ownership
cannot mark itself successful, cannot release the job lock, cannot record a retry — every
one of those statements carries a stale token or epoch and affects zero rows. The new owner
cannot be corrupted by the old one.

The fence does **not** protect the handler's own **business writes**. Nothing in this
library sits between a handler and its database. If a handler keeps writing after losing
ownership, those writes land.

That is the boundary this design draws, and it is why `Fence()` exists and why business
idempotency remains mandatory rather than optional. A scheduler can guarantee one owner at
a time; only the handler can guarantee that being run twice is harmless.

### A twenty-hour job is a design signal

It can be supported, and the rules above are what supporting it requires. But the better
shape is usually a **cursor-driven short job**: run every few minutes, process a bounded
batch, commit a durable checkpoint, exit. A crash then costs one batch instead of twenty
hours, restart resumes instead of restarting, and the lease returns to an ordinary size.
Prefer that refactor over tuning a lease upward.

---

## 6. Completion, retry and the single reclaim path

### Completion

One transaction, canonical order: release the job lock guarded by the current token and
epoch, then perform the terminal execution CAS. Both statements assert exactly one affected
row.

```sql
UPDATE job_state
SET active_kind = NULL, active_execution = NULL,
    active_owner = NULL, active_run_token = NULL,
    lease_until = NULL, last_success_at = ?, updated_at = ?
WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?;
```

```sql
UPDATE job_execution
SET status = 'success', finished_at = ?, lease_until = NULL, updated_at = ?
WHERE id = ? AND status = 'running' AND run_token = ? AND fence_epoch = ?;
```

### Retry

Same order, same guards, and terminality decided **in SQL** rather than by the handler:

```sql
UPDATE job_execution
SET status = IF(attempt_no >= max_attempts, 'dead', 'ready'),
    available_at = IF(attempt_no >= max_attempts,
                      available_at, TIMESTAMPADD(SECOND, ?, ?)),
    owner_instance = NULL, dispatched_to = NULL, run_token = NULL, lease_until = NULL,
    failure_kind = ?, error_message = ?, updated_at = ?
WHERE id = ? AND status = 'running' AND run_token = ? AND fence_epoch = ?;
```

Backoff is bounded, configurable and deterministic; jitter is applied after computing the
base delay so the sequence stays testable. Validation and permanent failures are classified
non-retryable and go straight to `dead` with a stable `failure_kind`.

### Reclaiming an expired holder

**A claim never steals an expired lease.** The state-row guard is `active_kind IS NULL`,
not "or the lease has expired".

The reason is concrete. If a fresh claim could overwrite an expired holder directly, the
previous execution would still be `running` with the old token, and when recovery later
tried to release the state row guarded by that old ownership it would affect zero rows and
never resolve — a permanently stuck `running` row that no path can clean up.

So exactly one path clears an expired holder, and ordering is strict:

```text
holder's lease expires
  -> recovery fences the old holder, resolves its execution, clears the state row
  -> only then can the next claim acquire it
```

The cost is one recovery interval of latency after a crash — which is the SLA the lease
duration already implies. The benefit is that ownership transfer has one implementation
instead of two that can disagree.

---

## 7. Recovery

Recovery is the only path that clears an expired holder. It finds work with a non-locking
read over `job_execution` rows in `dispatching`, `running` or `cancel_requested` whose lease
has expired. Every holder of a job's state row has an execution row — including a fixed-delay
pass (`scheduling.md` §2) — so there is one scan and one algorithm.

**Recovery must reconcile before it decides.** This is the whole of the difficulty: an
expired lease means the *scheduler* that owned the execution stopped renewing, which says
nothing about the *executor*, which may be running the work perfectly well. An earlier
revision had recovery move `running` straight to `ready`, which would start a second
executor while the first was still writing.

So, in one transaction following the canonical order:

1. lock `job_state`, then the execution row;
2. verify the old token, fence epoch and expired lease;
3. if `dispatched_to` is set, **call `GetExecution` on that executor** and act on the answer:

   | Executor says | Action |
   | --- | --- |
   | `RUNNING` | **adopt**: take the lease with a new `fence_epoch` and the **same** `run_token`, and resume tracking. The work was never interrupted, and rotating the token would fence a healthy run |
   | `FINISHED` | adopt the reported outcome and complete the execution normally |
   | `NOT_FOUND`, or unreachable | the attempt is **unknown**; continue to step 4 |

4. release the state row guarded by the old ownership, clearing `active_kind`;
5. increment `fence_epoch` and `recovery_count`;
6. resolve by prior status:
   - `dispatching` or `running` → `ready` with bounded recovery backoff, or `dead` when
     `attempt_no >= max_attempts` or `recovery_count >= max_recoveries`;
   - `cancel_requested` → **`cancelled`**, never `ready`. Someone asked this run to stop;
     its executor dying is not a reason to start it again.

`dispatched_to` unset means the dispatch never reached an executor, so there is nothing to
reconcile with and step 3 is skipped.

The normal claim path then assigns capacity and a new owner, so a recovery scan can never
end up owning more work than a tenant's quota permits.

**Recovery does not touch `attempt_no`.** It was incremented at claim, and the re-claim that
follows recovery increments it again — which is exactly the budget a crash should cost.
Incrementing in both places would exhaust a budget of three in two real handler starts. A
handler that reliably kills its executor still terminates: claim, crash, recover, claim,
crash, recover, claim → the limit is reached and the row goes `dead`.

### Graceful shutdown

Stop claiming and drop readiness first, then let in-flight work finish. If a handler has not
proved it stopped, **let its lease expire rather than releasing it**. Lease expiry is not
proof a handler stopped — but releasing early is a guarantee that a second executor may start
while the first is still writing.

---

## 8. State machine

```text
ready --claim--> dispatching --accepted (attempt_no+1)--> running --success--> success
                     |                                      |
                     +--refused--> ready (no attempt)        +--retryable--> ready
                     +--stale lease--> recovery              +--exhausted--> dead
                                                             +--cancel--> cancel_requested
                                                             +--stale lease--> recovery

cancel_requested --result arrives: success--> success     (the work finished; see below)
cancel_requested --result arrives: failed---> dead | ready
cancel_requested --handler confirms stopped-> cancelled
cancel_requested --stale lease, fenced------> cancelled   (never back to ready)

ready --FORBID contention--> skipped
dead  --authorized retry--> ready (attempt_no reset, audited)
any non-terminal --job retired--> cancelled (audited)
```

### A cancel that loses the race does not rewrite history

An operator can commit `running -> cancel_requested` a moment after the handler already
finished successfully, and the success result then arrives against a `cancel_requested` row.

**The real outcome wins.** The execution becomes `success`, not `cancelled`: the work
happened, and recording it as cancelled would tell an operator the opposite of the truth
about a job that may have moved money. The audit trail keeps the cancel request, so "we
asked, but it had already finished" remains visible.

`cancelled` is therefore reached only when the handler confirms it stopped, or when the
attempt is fenced without ever reporting.

Every transition has guarded SQL carrying the expected prior status and, where an owner
exists, the token and fence epoch. No handler updates status with a bare `WHERE id = ?`.

`terminal_reason` on the execution row records *how* a terminal state was reached —
`handler_confirmed`, `fenced`, `budget_exhausted`, `permanent_failure`, `retired`,
`operator` — because `cancelled` alone cannot tell an operator whether side effects were
verified (section 8, and `admin.md` §5).

### Cancellation is two steps

Cancelling a handler's context is a cooperative signal. It does not prove a SQL statement
finished, a file write completed, or an outbound request returned. Marking an execution
`cancelled` and releasing the job lock immediately would let the next execution start while
the previous handler is still writing.

An authorized cancel therefore moves the execution to `cancel_requested`, which **keeps the
lease and the job lock and keeps heartbeating**, and is shown in the UI as a state of its
own — because "we asked it to stop" and "it stopped" are different facts an operator needs
to distinguish.

### What `cancelled` means

It is a statement about scheduler ownership, and the UI must present it as one:

- it means the scheduler will accept no further results from that execution, and the job's
  concurrency slot is free;
- it does **not** mean an in-flight request was withdrawn or a partial write was rolled
  back. Reaching `cancelled` through lease expiry proves only that the old executor lost
  ownership;
- the two paths are labelled differently — "cancelled (handler confirmed stopped)" versus
  "cancelled (fenced; side effects unverified)". Showing both as a plain "cancelled" invites
  an operator to assume nothing happened, which for a job with external effects is the most
  expensive available wrong assumption.

---

## 9. Scheduler clustering

Every scheduler instance runs the same loops: materialize due jobs, dispatch claimed
executions, track them, recover stale ones, serve the API. None of them is a leader.

### What makes that safe

- **materialization** takes the state row `FOR UPDATE SKIP LOCKED`, so exactly one instance
  creates a given fire instant's execution and advances `next_fire_at`; the others skip that
  job on that pass (`scheduling.md` §1);
- **dispatch** requires the claim of section 2, so exactly one instance holds an execution
  and is responsible for handing it to an executor;
- **result callbacks are instance-agnostic.** An executor posts to whichever scheduler the
  load balancer picks, and that instance writes the result to MySQL guarded by `run_token`.
  There is no session affinity to preserve and no requirement that the result return to the
  instance that dispatched;
- **the executor registry lives in MySQL, not in scheduler memory.** This is not an
  optimization — an in-memory registry would give each instance a different view of which
  executors exist, and routing decisions would depend on which instance happened to make
  them;
- **the admin API is stateless.** Any instance can serve any request; all state is in the
  tables.

### Periodic singleton work

A few background activities should run once per interval across the cluster rather than
once per instance: retention sweeps, orphan scanning, expired-registration cleanup. They
are not correctness-critical — running them twice is harmless — but running them on every
instance every minute is waste.

These take a **named lease** in a small table, acquired with the same guarded CAS used
everywhere else. A lease, not an election: whoever holds it does the work this interval,
and if that instance dies the lease expires and another picks it up. No instance is
promoted, and nothing waits for consensus.

### What clustering does not change

Nothing about correctness depends on the number of scheduler instances, exactly as nothing
depends on the number of executors. One instance and five instances run the same code paths;
five is simply five times as likely to skip a locked row. There is no single-instance mode
and no cluster mode to configure.

---

## 10. Scheduler failover while an execution is running

The case the two-layer split creates. The naive response is a duplicate run:

```text
scheduler A claims execution E, dispatches to executor X, holds the lease
scheduler A dies
E's lease expires
scheduler B's recovery loop picks up E
```

**B must not re-dispatch.** Executor X may still be running E; A's death says nothing about
X. Re-dispatching on the assumption that a dead scheduler means dead work is how a
fifteen-minute settlement job runs twice.

There is no separate failover algorithm: this *is* recovery, and step 3 of section 7 is
where it is handled. `dispatched_to` records which executor to ask, `GetExecution` supplies
the answer, and adoption keeps the same `run_token` because the executor still holds it.

`dispatched_to` must therefore be durable, written when the dispatch is accepted and before
any result can arrive. An execution whose dispatch target is not recorded cannot be
reconciled, and recovery would have to choose between re-dispatching blindly and giving up —
both wrong.

---

## 11. Fairness and quotas

Polling is round-robin across admitted tenants; a tenant cannot monopolise dispatch capacity merely by
having the oldest rows.

Within a process, per-tenant concurrency is bounded by a pool and a queue, with a
process-wide budget validated at startup so misconfiguration fails loudly rather than
exhausting memory under load.

**In-process semaphores are not a fleet-wide quota.** Before more than one replica serves the
same tenant, an aggregate limit requires lease-backed slot rows claimed in the same
transaction as the execution. Until then, per-tenant limits are per-process limits, and the
documentation says so rather than implying a guarantee that does not hold.

---

## 12. Executor registration

Executors call `Register` and `Heartbeat` (`dispatch.md` §6); the scheduler stores the
result in `job_executor` and `job_executor_handler`.

```text
Register   -> probe (Describe, plus a GetExecution for a key it cannot hold)
           -> INSERT job_executor, job_executor_handler x N
           -> adopt or fence the declared in_flight set
Heartbeat  -> UPDATE heartbeat_at, running
lapse      -> the row ages out; the executor re-registers on known=false
```

`executor_id` is unique per process, so a restarted executor never inherits its
predecessor's row and never has to clean one up.

Registration is **not** part of the exclusion mechanism — nothing in the claim protocol
consults it, and ownership is decided entirely by `job_state`. It serves four purposes:

- **routing**: dispatch goes to a live instance of a group declaring the job's
  `handler_key` and below capacity (`dispatch.md` §7);
- **orphan detection**: an enabled job whose handler no live executor declares will never
  run. That is a condition to alert on, not a mystery to investigate a week later;
- **reconciliation**: the `in_flight` set declared at registration is how a scheduler learns
  what survived a restart on either side, without having to ask;
- **operations**: the admin UI shows which processes are live, at which build, running what.

**The registry is in the database, not in scheduler memory.** With a scheduler cluster, an
in-memory registry would give each instance a different view of which executors exist, and
routing would depend on which instance happened to decide.

Liveness is a fresh heartbeat evaluated in database time. Rows aged past the bound are
deleted by retention.

---

## 13. Observability contract

Structured fields on every execution event:

```text
tenant, job_name, execution_id, execution_key, trigger_type,
owner_instance, executor_id, run_token_hash, attempt_no, recovery_count,
scheduled_at, claimed_at, duration_ms, status, failure_kind
```

Raw run tokens are never logged; the hash is enough to correlate and useless to forge with.

Metrics:

- backlog and oldest ready age, by tenant and job;
- dispatch lag (`claimed_at - scheduled_at`);
- terminal-state counters, plus fence losses, lease-renewal failures and stale recoveries;
- execution duration and timeout totals;
- per-tenant active concurrency and queue depth;
- loop liveness, empty-poll rate and last successful poll for fixed-delay jobs;
- **time since last success per job**, which is the metric that catches a job that silently
  stopped — the failure mode nobody notices, because nothing is erroring;
- live executors by group and build revision, and **enabled jobs whose handler no live
  executor serves**;
- expired `running` executions whose handler has no live executor, reported separately from
  the ready backlog, because nothing will recover them;
- connection pool usage and waits.

Readiness goes false when admission fails or degrades. A degraded tenant is visible without
its work being silently routed elsewhere.
