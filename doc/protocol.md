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
| a row lock on `job_state` | two workers deciding simultaneously |
| a guarded CAS (`active_kind IS NULL`) | a decision based on a stale read |
| a lease with heartbeats | a crashed owner holding the job forever |
| a fence epoch on every ownership write | a revived owner overwriting its successor |

Remove any one and the protocol is unsound. Together they are sufficient, and the number of
worker processes is irrelevant to that — exclusion comes from the single state row, not
from knowing how many workers exist.

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
  AND available_at <= ?          -- business time, supplied by the worker
ORDER BY available_at, id
LIMIT ?;
```

---

## 2. Claim

Per candidate, one short transaction:

1. `SELECT ... FROM job_state WHERE job_name = ? FOR UPDATE SKIP LOCKED`.
   A skip means another worker is already deciding about this job; abandon this candidate
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
    active_worker_id = ?,
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
SET status       = 'running',
    worker_id    = ?,
    run_token    = ?,
    fence_epoch  = ?,
    lease_until  = TIMESTAMPADD(SECOND, ?, NOW()),
    heartbeat_at = NOW(),
    attempt_no   = attempt_no + 1,
    started_at   = COALESCE(started_at, ?),
    updated_at   = ?
WHERE id = ?
  AND status = 'ready'
  AND attempt_no < max_attempts;
```

Every statement's affected-row count is asserted; any mismatch rolls the transaction back.

**Dispatch happens only after commit**, and `dispatched_to` is written in the same
transaction so a takeover can reconcile with the right executor (section 10). No row lock is
held while an executor runs the work — leases, not locks, carry ownership across an
execution's lifetime, which may be hours.

`attempt_no` is incremented here and nowhere else. Recovery does not touch it (section 7).

---

## 3. Runnability

Evaluated inside the claim transaction, in this order, with every failed condition recorded
rather than collapsed into one boolean:

1. `handler_key` resolves in this binary's registry;
2. `handler_key` is within this deployment's execution assignment;
3. `job_definition.enabled = 1`;
4. `job_state.ops_paused = 0`;
5. the schema version matches what this library requires.

Condition 2 is why a deployment serving only some handlers is normal rather than broken:
work outside its assignment is simply not its work.

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

Ownership decisions use **database time only**. A worker never compares a lease against its
own host clock, because that would make ownership depend on clock skew between machines.

```text
heartbeat interval  <=  lease / 3
handler timeout         per job
shutdown grace      <   remaining lease, where practical
```

Both rows are renewed in the same heartbeat cycle:

```sql
UPDATE job_execution
SET lease_until = TIMESTAMPADD(SECOND, ?, NOW()), heartbeat_at = NOW(), updated_at = ?
WHERE id = ?
  AND status IN ('running', 'cancel_requested')
  AND worker_id = ? AND run_token = ? AND fence_epoch = ?
  AND lease_until >= NOW();
```

```sql
UPDATE job_state
SET lease_until = TIMESTAMPADD(SECOND, ?, NOW()), heartbeat_at = NOW(), updated_at = ?
WHERE job_name = ?
  AND active_run_token = ? AND fence_epoch = ?;
```

`status IN ('running', 'cancel_requested')` is deliberate: a cancelled-but-not-yet-stopped
handler must keep renewing, because releasing its slot before it has actually stopped is
exactly the overlap this protocol exists to prevent.

If either guarded update affects zero rows, ownership is lost. The worker cancels the
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
the job to another worker — while the original process is alive and still working. This is
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
    active_worker_id = NULL, active_run_token = NULL,
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
    worker_id = NULL, run_token = NULL, lease_until = NULL,
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

Recovery is the only path that clears an expired holder, for every `active_kind`. It finds
work two ways, both non-locking reads:

- expired `job_state` rows — `lease_until < NOW()` with a non-null `active_kind` — which is
  what catches a dead polling loop, since a loop has no execution row to scan;
- expired `job_execution` rows in `running` or `cancel_requested`.

Each is recovered in a transaction following the canonical order. **Recovery never becomes
the executor:**

1. lock `job_state`, then the execution row if there is one;
2. verify the old token, fence epoch and expired lease;
3. release the state row guarded by that old ownership, clearing `active_kind`;
4. increment `fence_epoch` and `recovery_count`;
5. resolve the execution by its prior status:
   - `running` → `ready` with bounded recovery backoff, or `dead` when
     `attempt_no >= max_attempts` or `recovery_count >= max_recoveries`;
   - `cancel_requested` → **`cancelled`**, never `ready`. Someone asked this run to stop;
     its worker dying is not a reason to start it again;
   - `active_kind = 'LOOP'` → nothing to resolve; step 3 is the whole recovery.

The normal claim path then assigns capacity and a new owner, so a recovery scan can never
end up owning more work than a tenant's quota permits.

**Recovery does not touch `attempt_no`.** It was incremented at claim, and the re-claim that
follows recovery increments it again — which is exactly the budget a crash should cost.
Incrementing in both places would exhaust a budget of three in two real handler starts. A
handler that reliably kills its worker still terminates: claim, crash, recover, claim,
crash, recover, claim → the limit is reached and the row goes `dead`.

### Graceful shutdown

Stop claiming and drop readiness first, then let in-flight work finish. If a handler has not
proved it stopped, **let its lease expire rather than releasing it**. Lease expiry is not
proof a handler stopped — but releasing early is a guarantee that a second worker may start
while the first is still writing.

---

## 8. State machine

```text
ready --claim (attempt_no+1)--> running --success--> success
                                   |
                                   +--retryable, budget left--> ready (future avail_at)
                                   +--budget exhausted / permanent--> dead
                                   +--authorized cancel--> cancel_requested
                                   +--stale lease--> ready (fence+1, recovery+1) | dead

cancel_requested --handler confirms exit--> cancelled
cancel_requested --stale lease, fenced----> cancelled       (never back to ready)

ready --FORBID contention--> skipped
dead  --authorized retry--> ready (attempt_no reset, audited)
```

Every transition has guarded SQL carrying the expected prior status and, where an owner
exists, the token and fence epoch. No handler updates status with a bare `WHERE id = ?`.

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
  back. Reaching `cancelled` through lease expiry proves only that the old worker lost
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

This is the case the two-layer split creates, and it has a specific answer because the naive
one is a duplicate run.

```text
scheduler A claims execution E, dispatches to executor X, holds the lease
scheduler A dies
E's lease expires
scheduler B's recovery loop picks up E
```

**B must not re-dispatch.** Executor X may still be running E perfectly well; A's death says
nothing about X. Re-dispatching on the assumption that a dead scheduler means dead work is
how a fifteen-minute settlement job gets run twice.

So recovery of an execution in `running` **reconciles before it decides**:

1. read `dispatched_to` from the execution row — the executor instance A handed it to;
2. call `GET {address}/jobs/{execution_key}` (`dispatch.md` §4.3);
3. act on the answer:

| Executor says | B does |
| --- | --- |
| `running` | adopt tracking: take the lease with a **new** fence epoch, keep the same `run_token`, and continue waiting. The work was never interrupted. |
| `finished` | adopt the reported result and complete the execution normally |
| `404`, or unreachable | the attempt is genuinely lost: fence the `run_token` and retry per policy |

Keeping the same `run_token` in the adopt case matters: the executor is still holding it and
will present it with its progress and result calls. Rotating it would fence a healthy
running execution — the mistake of invalidating the current generation while it is still
working.

The fence epoch does advance, because scheduler-side ownership genuinely moved and any write
attempted by a resurrected A must still be refused.

`dispatched_to` therefore has to be durable, written in the same transaction as the claim.
An execution whose dispatch target is not recorded cannot be reconciled, and would leave
recovery guessing.

---

## 11. Fairness and quotas

Polling is round-robin across admitted tenants; a tenant cannot monopolise workers merely by
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

Registration is an `INSERT` and a heartbeat, not a protocol. There is no service registry,
no broadcast and no inbound endpoint on workers.

```text
start:  INSERT job_worker(worker_id, role, build_revision, started_at, heartbeat_at)
        INSERT job_worker_handler(worker_id, job_name) x N
run:    UPDATE job_worker SET heartbeat_at = NOW() every interval
stop:   DELETE own rows on graceful exit; otherwise heartbeat ages out
```

`worker_id` is `<hostname>:<pid>:<boot-nonce>` — unique per process, so a restarted worker
never inherits its predecessor's row and never has to clean one up.

Registration is **not** part of the exclusion mechanism, and nothing in the claim protocol
consults it. It exists for three operational purposes:

- the admin UI can show which processes are live, what they are running and at which build;
- **orphan detection** — a job that is enabled but whose handler no live worker registers
  will never be claimed by anyone. That is a condition to alert on, not a mystery to
  investigate a week later;
- routing evidence when a workload is split across deployments: the registered handler set
  is already the fact that says which deployment serves what.

Liveness is a fresh heartbeat evaluated in database time. Rows aged past the bound are
deleted by retention.

---

## 13. Observability contract

Structured fields on every execution event:

```text
tenant, job_name, execution_id, execution_key, trigger_type,
worker_id, run_token_hash, attempt_no, recovery_count,
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
- live workers by role and build revision, and **enabled jobs whose handler no live worker
  serves**;
- expired `running` executions whose handler has no live worker, reported separately from
  the ready backlog, because nothing will recover them;
- connection pool usage and waits.

Readiness goes false when admission fails or degrades. A degraded tenant is visible without
its work being silently routed elsewhere.
