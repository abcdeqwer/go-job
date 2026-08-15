# Data model

One small **control** database holds the tenant registry and everything that is genuinely
global. Everything else lives in **one coordination schema per tenant**, where there is no
`tenant_id` column anywhere: the connection *is* the tenant boundary. Two tenants may hold
identical job names and identical row ids without any possibility of interference, and a
query that forgets a tenant predicate cannot leak across tenants because no such predicate
exists.

Adding a site is a row in the registry, not a redeploy.

Table names carry a configurable prefix, shown here as `job_`.

---

## 0. Two kinds of database

| Database | How many | Holds |
| --- | --- | --- |
| **control** | exactly one | the tenant registry, admin accounts, control-plane leases and control audit |
| **coordination** | one per tenant | everything else in this document — jobs, state, executions, executor registrations |

The control database is the only place in this system that knows more than one tenant
exists. It holds no business data, no execution state and no job configuration; a tenant's
scheduling lives entirely in its own schema, and the isolation property of section 0.2 is
unaffected.

### 0.1 The tenant registry

Sites are added over time, so the tenant list is **data, not deployment configuration**. A
new site is a row, and schedulers pick it up without a restart.

```sql
CREATE TABLE tenant_registry (
    tenant           VARCHAR(64)     NOT NULL,
    coordination_dsn VARBINARY(2048) NOT NULL,   -- encrypted
    enabled          TINYINT(1)      NOT NULL DEFAULT 1,
    generation       BIGINT          NOT NULL DEFAULT 1,  -- bumped by every enable/disable
    schema_version   VARCHAR(16)     NULL,       -- last version verified on this tenant
    admitted_at      DATETIME        NULL,
    last_error       VARCHAR(512)    NULL,
    created_at       DATETIME        NOT NULL,
    updated_at       DATETIME        NOT NULL,
    PRIMARY KEY (tenant)
);
```

**DSNs are encrypted at rest** with AES-GCM under a key the scheduler holds from its
environment or a KMS. They contain database passwords, and this table is reachable from an
admin UI. The API accepts a plaintext DSN over TLS, encrypts before storing, and **never
returns one** — reads are masked to `user@host/schema`. An operator changing a DSN replaces
it; there is no "show me the current password" affordance, because there is no legitimate
use for one that outweighs the risk.

### 0.2 Hot add, and why admission stops being all-or-nothing

Schedulers poll `tenant_registry` on a short interval:

| Change | Effect |
| --- | --- |
| a new `enabled` row appears | open a pool, verify the schema version, admit, start loops |
| a new `enabled` row appears | open a pool, verify the schema version, admit, start loops |
| a row is disabled | stop claiming, drain, release the pool |

**Re-pointing a DSN is not a hot change.** Changing `coordination_dsn` on an enabled tenant
is rejected. The reason is a split brain that no amount of draining fixes: schedulers poll
the registry independently, so instance A can adopt the new DSN while instance B is still
working the old schema — two `job_state` rows for one tenant, in two databases, each
correctly excluding only itself, dispatching the same job twice.

Re-pointing is therefore three audited steps — **disable, confirm quiescence, then change the
DSN and re-enable** — and the middle one is the part that has to be mechanical rather than a
pause. "Wait a bit after disabling" proves nothing: a replica partitioned from the control
database, or simply slow to poll, has not seen the disable and is still claiming against the
old schema when the new one appears.

So the registry carries a `generation`, bumped by every enable or disable, and each scheduler
records the generation it has observed:

```sql
CREATE TABLE tenant_observation (
    tenant          VARCHAR(64)  NOT NULL,
    instance_id     VARCHAR(128) NOT NULL,
    generation      BIGINT       NOT NULL,   -- highest this instance has applied
    quiesced        TINYINT(1)   NOT NULL,   -- it holds nothing for this tenant
    observed_at     DATETIME     NOT NULL,
    PRIMARY KEY (tenant, instance_id),
    KEY idx_tenant_observation_gen (tenant, generation)
);
```

A DSN change is accepted only when **every live scheduler instance** reports the disable
generation with `quiesced = 1`. Live means a fresh `observed_at`; the API names which
instance is blocking rather than making the operator guess.

That alone would still be unsound, and the hole is worth naming: an instance **partitioned
from the control database** stops reporting, drops out of the "live" set, and the change
proceeds — while it happily keeps claiming against the old schema, because its connection to
the *tenant* database is fine. Absence of an acknowledgement is not evidence of quiescence.

So the control database is also a **lease on the right to operate**:

> A scheduler instance that has not successfully read `tenant_registry` within
> `control_staleness_limit` (default 30s, and always well under the liveness bound the API
> uses) **stops claiming, stops materializing and drops readiness** for every tenant. It
> keeps renewing leases for work already in flight so nothing is stranded, and resumes when
> the control database returns.

A partitioned instance therefore fences *itself* before the API could ever conclude it is
gone. The proof is not "everyone acknowledged" but "everyone either acknowledged or has
stopped by construction", which is a property a partition cannot break.

Nothing here is a consensus protocol — an acknowledgement table with a freshness bound, plus
a self-fencing rule with a shorter one.

**Draining is bounded.** "Let in-flight work finish" is not a terminating condition: an
execution can hang, and a scheduler that waits for it forever means a disable or a DSN change
that never takes effect. So a drain waits at most `drain_timeout`, then fences everything
still outstanding — those executions become `dead` with `terminal_reason = 'fenced'`, and the
pool is released. Which is correct rather than merely convenient: after the DSN changes, the
old connection no longer points at the schema those rows live in, so holding the pool open
would not help them and closing it silently would strand them.

This forces a design change worth stating explicitly. Admission was previously
all-or-nothing across tenants: any tenant failing prevented readiness. **With hot add that
becomes wrong** — a newly added tenant with a typo in its DSN would take down a scheduler
that is happily serving twenty healthy tenants.

So admission is **per tenant**:

- a tenant that fails to admit records `last_error`, is surfaced in the UI, and is retried
  on a backoff. Its failure is loud;
- **other tenants are unaffected** and keep running;
- process readiness reflects the control database and the tenants that were healthy at
  startup — not every tenant that has ever been added.

The startup case keeps its stricter behaviour: a tenant that was admitted when the process
started and cannot be re-admitted after a restart is a regression, not a new site, and is
alerted as such.

### 0.3 The rest of the control database

```sql
-- Named leases for periodic work that should run once per cluster per interval:
-- retention sweeps, orphan scans, expired-registration cleanup.
-- A lease, not an election — see protocol.md §9.
CREATE TABLE control_lease (
    lease_name   VARCHAR(64)  NOT NULL,
    holder_id    VARCHAR(128) NOT NULL,
    run_token    CHAR(36)     NOT NULL,
    lease_until  DATETIME     NOT NULL,
    heartbeat_at DATETIME     NOT NULL,
    PRIMARY KEY (lease_name)
);

-- Admin accounts are global: an operator logs in once and works across tenants.
-- Present only when built-in authentication is used.
CREATE TABLE admin_user (
    username      VARCHAR(64)  NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role          VARCHAR(16)  NOT NULL,   -- VIEWER | OPERATOR
    disabled      TINYINT(1)   NOT NULL DEFAULT 0,
    created_at    DATETIME     NOT NULL,
    updated_at    DATETIME     NOT NULL,
    PRIMARY KEY (username)
);

-- Which authenticated identity may register as what. Without this table the authorization
-- rule in dispatch.md has no source of truth, and "bound to (tenant, group)" is a sentence
-- rather than a check.
CREATE TABLE executor_identity (
    identity       VARCHAR(255) NOT NULL,   -- mTLS subject, or credential id
    tenant         VARCHAR(64)  NOT NULL,
    executor_group VARCHAR(64)  NOT NULL,
    disabled       TINYINT(1)   NOT NULL DEFAULT 0,
    created_at     DATETIME     NOT NULL,
    PRIMARY KEY (identity, tenant, executor_group)
);

-- Actions on the registry itself: adding, disabling or re-pointing a tenant.
-- Per-job actions are audited in that tenant's own schema.
CREATE TABLE control_audit (
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    actor      VARCHAR(64)  NOT NULL,
    action     VARCHAR(48)  NOT NULL,
    tenant     VARCHAR(64)  NULL,
    detail     VARCHAR(1024) NULL,
    created_at DATETIME     NOT NULL,
    PRIMARY KEY (id),
    KEY idx_control_audit_time (created_at)
);
```

Adding a site is therefore: create its coordination schema, apply the scheduler DDL, add one
audited row. No redeploy, no restart, no code change.

### 0.4 Validation bounds

"Bounded" is only meaningful with a number. These are validated at the API and rejected
there, so two implementations cannot invent different limits:

| Value | Bound |
| --- | --- |
| `params_json` | ≤ 64 KiB serialized |
| fixed delay | ≥ 1s, ≤ 24h — a zero delay is a spin, not a schedule |
| `timeout_seconds` | ≥ 1s, ≤ 7 days |
| `lease_seconds` | ≥ 10s, ≤ `timeout_seconds` |
| `max_attempts` | 1 – 100 |
| `max_recoveries` | 1 – 100 |
| retry backoff | ≥ 1s, ≤ 6h, and monotonic |
| `drain_timeout` | ≥ 30s, ≤ 1h |
| `job_name`, `handler_key` | ≤ 128 chars, `[A-Za-z0-9._-]+` |

---

### 0.5 Where the per-tenant tables live

The scheduler needs **one** MySQL schema per tenant, for the coordination tables in this
document. Whether that schema also holds business data is not its concern and it has no way
to tell: it stores one DSN per tenant and uses it.

An earlier revision carried a second `business_dsn` "for handlers". That was a leftover of
the embedded-library design and is removed. In this architecture the scheduler holds no
handler code, `RunRequest` carries no DSN, and parameters must contain no secrets — so a
business DSN in the scheduler's control database would have no consumer, while adding a
production credential to a table that nothing reads. **Executors reach their own databases
with their own configuration.**

Adopters may still put the coordination tables in a business schema or a dedicated one. That
choice is invisible to the scheduler and purely operational:

| | Dedicated coordination schema | Shared with business |
| --- | --- | --- |
| Backup, tuning, access control | independent | coupled |
| Blast radius of scheduler churn | isolated | scheduler write load sits in the business schema |
| Schemas to provision and migrate | one more per tenant | none extra |

**Dedicated is the better default.** Shared is a reasonable simplification for a small
installation that would rather manage one schema.

### How executors are discovered

There is no discovery protocol: executors do not broadcast, do not appear in a service
registry, and are not listed in any configuration. An executor **registers itself** at a
scheduler endpoint it is configured with, and the scheduler records it in that tenant's
`job_executor` table (§5). Two processes that can reach each other and authenticate can
work together; two that cannot, cannot. That is the whole model.

---

## 1. Why configuration and hot state are separate tables

`job_definition` is written by operators, rarely, through the admin UI.
`job_state` is written by the scheduler, constantly, on every claim, heartbeat and
completion.

Keeping them in one table would be simpler to read and worse to run: an operator editing a
cron expression would take a row lock that every claim of that job contends on, and a busy
job's hot-path writes would sit behind a human's edit transaction. Splitting them means the
hot path never locks the row humans touch, and the admin UI never waits on a running job.

The link between them is `job_state.config_version`, which records the definition version
that produced the current `next_fire_at`. When it falls behind, the schedule is recomputed
(see `scheduling.md`).

---

## 2. `job_definition` — operator-editable configuration

```sql
CREATE TABLE job_definition (
    job_name           VARCHAR(128) NOT NULL,
    handler_key        VARCHAR(128) NOT NULL,
    executor_group     VARCHAR(64)  NULL,      -- NULL = any group declaring the handler
    schedule_kind      VARCHAR(16)  NOT NULL,  -- CRON | FIXED_DELAY
    schedule_expr      VARCHAR(128) NOT NULL,  -- cron expression, or delay in ms
    enabled            TINYINT(1)   NOT NULL DEFAULT 1,
    retired            TINYINT(1)   NOT NULL DEFAULT 0,
    concurrency_policy VARCHAR(16)  NOT NULL,  -- QUEUE | FORBID
    misfire_policy     VARCHAR(16)  NOT NULL,  -- SKIP | FIRE_ONCE
    max_attempts       INT          NOT NULL,
    max_recoveries     INT          NOT NULL,
    lease_seconds      INT          NOT NULL,
    timeout_seconds    INT          NOT NULL,
    params_json        JSON         NULL,      -- default parameters
    description        VARCHAR(512) NULL,
    version            BIGINT       NOT NULL DEFAULT 1,
    updated_by         VARCHAR(64)  NULL,
    created_at         DATETIME     NOT NULL,
    updated_at         DATETIME     NOT NULL,
    PRIMARY KEY (job_name)
);
```

Schedule, enablement, concurrency, retry budget and timeouts are **data**. They are edited
in the admin UI under an optimistic `version` check and recorded in `job_audit`.

`handler_key` is the one field that is not data: it must resolve in the running binary's
registry, because it names compiled code. It is stored rather than derived so that renaming
a Go symbol does not silently repoint a job.

### Where these rows come from

**Operators create them, through the admin UI or API.** The scheduler holds no handler code
and therefore no registry to materialize from; an earlier revision described reconciliation
from a code registry, which was a leftover of a design where handlers were compiled into the
scheduler.

Creating a job means choosing a `handler_key` from those that live executors declare, plus a
schedule, parameters and policy. The UI offers the declared handlers as a list, so a typo
becomes an unselectable option rather than an orphan discovered later.

A `handler_key` that no live executor declares is not rejected — an executor may simply be
down — but the job is flagged as an orphan until one appears.

### Retiring

`retired` is a state of the definition, not a deletion:

| | `enabled = 0` | `retired = 1` |
| --- | --- | --- |
| meaning | temporarily off | permanently done with |
| new executions materialized | no | no |
| existing non-terminal executions | left alone | **`cancel_requested`**, audited — see below |
| row and history | kept | kept |

Retiring resolves outstanding work rather than stranding it. Without that rule a delayed
retry sits `ready` forever against a job nothing will ever claim, while retention refuses to
delete non-terminal rows — an unbounded table by construction.

But it **requests** the stop; it does not declare the outcome. A `ready` execution goes
straight to `cancelled` because nothing is running. A *running* one becomes
`cancel_requested` and resolves through the ordinary path (`protocol.md` §8): if its handler
was already finishing successfully, the result still wins and the row records `success`.
Retirement that forced every row to `cancelled` would write "cancelled by retirement" over a
run that had in fact completed — and for a job that moves money, a history that says the
opposite of what happened is worse than no history.

Termination is therefore bounded by the same lease and `timeout_seconds` machinery as any
other cancellation, not by retirement inventing its own.

---

## 3. `job_state` — scheduler-owned hot state

```sql
CREATE TABLE job_state (
    job_name          VARCHAR(128) NOT NULL,
    next_fire_at      DATETIME     NULL,
    next_poll_at      DATETIME     NULL,
    ops_paused        TINYINT(1)   NOT NULL DEFAULT 0,
    active_kind       VARCHAR(16)  NULL,      -- EXECUTION
    active_execution  VARCHAR(160) NULL,
    active_owner      VARCHAR(128) NULL,      -- scheduler instance holding it
    active_run_token  CHAR(36)     NULL,
    dispatched_to     VARCHAR(128) NULL,      -- executor_id it was handed to
    fence_epoch       BIGINT       NOT NULL DEFAULT 0,
    lease_until       DATETIME     NULL,
    heartbeat_at      DATETIME     NULL,
    last_success_at   DATETIME     NULL,
    last_failure_at   DATETIME     NULL,
    config_version    BIGINT       NOT NULL DEFAULT 0,
    updated_at        DATETIME     NOT NULL,
    PRIMARY KEY (job_name),
    KEY idx_job_state_due (next_fire_at),
    KEY idx_job_state_poll (next_poll_at),
    KEY idx_job_state_lease (lease_until)
);
```

### Two "who" fields, because there are two layers

`active_owner` is the **scheduler instance** that owns this job's current work.
`dispatched_to` is the **executor instance** actually running it. They are separate columns
because they fail independently: a scheduler instance can die while its executor runs
happily on, and recovery has to reconcile with the executor rather than assume the work
stopped (`protocol.md` §10). A single conflated "worker id" cannot express that.

### One active holder

This row *is* the job's lock. `active_kind` says what holds it:

| `active_kind` | Holder | Released by |
| --- | --- | --- |
| `EXECUTION` | a claimed execution — cron, manual or fixed-delay pass | result, retry or recovery |
| `NULL` | nobody | — |

There is one holder kind because there is one execution model. A fixed-delay pass creates an
ordinary execution row before dispatch and deletes it afterwards if it found nothing
(`scheduling.md` §2); it is not a special state-row-only holder, because such a holder
cannot be reconciled with its executor after a scheduler dies.

A single-valued holder is a deliberate constraint, and it is why there is no `PARALLEL`
concurrency policy: heartbeat, result and recovery all guard on `active_run_token` and
`fence_epoch` from this row, so two concurrent executions of one job would have no way to
each own, renew and release it.

It is also what makes a manual trigger serialize against a running poll without a second
mechanism.

`next_poll_at` is the fixed-delay counterpart of `next_fire_at`: set to *result time +
delay* when a pass completes, which is what makes the delay measured from completion rather
than from start.

### `ops_paused`

An operational pause, distinct from `enabled`. `enabled` answers "should this job exist in
the schedule at all"; `ops_paused` answers "stop it right now, temporarily". They have
different actors and different lifecycles, and the admin UI shows both rather than
collapsing them into one boolean that means neither.

`ops_paused` lives here, not in `job_definition`, precisely so a pause is instant and takes
the same lock the claim does — a pause that races a claim resolves deterministically
instead of leaking one more run.

---

## 4. `job_execution` — execution instances

```sql
CREATE TABLE job_execution (
    id             BIGINT       NOT NULL AUTO_INCREMENT,
    execution_key  VARCHAR(160) NOT NULL,
    job_name       VARCHAR(128) NOT NULL,
    trigger_type   VARCHAR(16)  NOT NULL,  -- cron | manual | poll
    manual_first   TINYINT(1)   NOT NULL DEFAULT 0,  -- 1 when trigger_type = 'manual'
    request_id     VARCHAR(64)  NULL,       -- manual only; idempotency of the API call
    scheduled_at   DATETIME     NOT NULL,
    available_at   DATETIME     NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    attempt_no     INT          NOT NULL DEFAULT 0,
    recovery_count INT          NOT NULL DEFAULT 0,
    max_attempts   INT          NOT NULL,
    max_recoveries INT          NOT NULL,
    params_json    JSON         NULL,       -- snapshot; see below
    owner_instance VARCHAR(128) NULL,       -- scheduler instance holding it
    dispatched_to  VARCHAR(128) NULL,       -- executor_id it was handed to
    run_token      CHAR(36)     NULL,
    fence_epoch    BIGINT       NOT NULL DEFAULT 0,
    lease_until    DATETIME     NULL,
    heartbeat_at   DATETIME     NULL,
    deadline_at    DATETIME     NULL,       -- silence deadline; extended by progress
    timeout_at     DATETIME     NULL,       -- hard runtime cap; never extended
    started_at     DATETIME     NULL,
    finished_at    DATETIME     NULL,
    failure_kind   VARCHAR(48)  NULL,
    terminal_reason VARCHAR(24) NULL,       -- how a terminal state was reached
    result_summary VARCHAR(512) NULL,
    error_message  VARCHAR(512) NULL,
    created_at     DATETIME     NOT NULL,
    updated_at     DATETIME     NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_execution_key (execution_key),
    UNIQUE KEY uk_job_execution_request (request_id),
    KEY idx_job_execution_claim (status, manual_first, available_at, id),
    KEY idx_job_execution_recovery (status, lease_until, id),
    KEY idx_job_execution_history (job_name, scheduled_at, id)
);
```

### `id` is `AUTO_INCREMENT`, not a snowflake

Uniqueness is only ever required within one tenant schema, and this table lives in one.
Ordering only needs to be monotonic for the claim tiebreak, which `AUTO_INCREMENT`
guarantees more strongly than any clock-derived scheme. Deduplication is carried entirely
by `execution_key`, not by the surrogate key.

A distributed id generator would add node-identity allocation — and therefore a
coordination dependency — to buy nothing this table needs.

### `execution_key` is the idempotency key

Deterministic and whole-second:

```text
cron:nightly-rollup:2026-08-15T01:30:00
poll:sync-orders:01J9Z6QWERTY
manual:01J9Z6QWERTZ
```

A cron key is derived from the fire instant, which is what makes duplicate materialization a
unique-key violation rather than a second run. Manual and poll keys carry a fresh
monotonic identifier instead: neither has a fire instant to be derived from, and a
timestamp-derived poll key would collide with a retained pass after a business-clock shift,
while a reusable per-job key would make every pass after the first a duplicate.

The unique index is what makes concurrent materialization safe: two schedulers that both
decide a fire instant is due produce one row and one error, never two runs.

### Statuses

`ready` is the single available state for both a first attempt and a retry, so the claim
predicate is an equality rather than an `IN` — which is what lets
`idx_job_execution_claim (status, available_at, id)` satisfy the filter *and* the ordering
without a filesort.

| Status | Meaning |
| --- | --- |
| `ready` | available to claim, at or after `available_at` |
| `dispatching` | claimed, handed to an executor, acceptance not yet known |
| `running` | accepted by an executor and executing |
| `cancel_requested` | asked to stop; still owned, still leased |
| `success` | terminal |
| `dead` | terminal; retry budget exhausted or permanently failed |
| `cancelled` | terminal |
| `skipped` | terminal; `FORBID` contention |

### Empty fixed-delay passes are retained briefly, then swept

A pass reporting `did_work = false` is marked terminal like any other execution and left in
place; retention removes it after a short window (default 15 minutes), not the terminal
transaction.

Deleting it immediately would break result idempotency: the executor is required to redeliver
a result whose response was lost, and a scheduler with no row and no tombstone could only
answer `NOT_FOUND` for a result it had in fact accepted moments earlier. Since `NOT_FOUND`
tells an executor to discard, that is survivable — but it also teaches executor authors that
the terminal codes are unreliable, which is exactly the lesson that makes `ABORTED` get
ignored somewhere it matters.

A window slightly longer than the executor's result-retry budget costs a few minutes of rows
and keeps every terminal answer truthful.

### `terminal_reason`

`status` says where an execution ended; `terminal_reason` says how it got there, and the two
are not interchangeable. `cancelled` alone cannot tell an operator whether a handler
confirmed it stopped or whether the attempt was merely fenced with its side effects
unverified — a distinction that matters most for exactly the jobs where it is expensive to
guess.

Values: `handler_confirmed`, `fenced`, `timeout`, `budget_exhausted`, `permanent_failure`,
`retired`, `operator`.

### `request_id`

The durable half of manual-trigger idempotency. A retried `POST .../trigger` — a
double-clicked button, a client resending after a timeout — carries the same `request_id`,
the unique key rejects the second insert, and the API returns the execution the first call
created. Without a stored mapping the idempotency contract in `admin.md` §6 would be a
promise with nothing behind it, on the one operation an operator is most likely to repeat
under stress.

### `manual_first`

A stored `1`/`0`, set at creation, ordered ahead of `available_at` in the claim index so an
operator's manual run is selected before scheduled work of the same job. Exclusion alone
gives no fairness: a fast poller becomes due again the moment it releases the lock, and
without this column it can win indefinitely while a manual trigger waits.

### An operator retry grants budget; it does not reset the counter

`attempt_no` is monotonic for the life of an execution, because it is half the primary key of
`job_execution_attempt`. Resetting it on an authorized retry would make the next attempt
number 1 again and collide with history already written.

So a retry of a `dead` execution **raises `max_attempts`** by the configured grant and
returns the row to `ready`. Attempt numbers keep climbing, history stays append-only, and the
audit records who granted the extra budget and why.

### Two budgets

`attempt_no` bounds real handler starts and is incremented **only on dispatch acceptance**.
`recovery_count` bounds crash-and-reclaim cycles.

They are separate because they answer different questions. A job that fails cleanly three
times and a job that kills its executor three times both stop, but an operator needs to tell
them apart, and only the second is a reason to look at memory limits.

`attempt_no` is deliberately *not* incremented by claiming, nor by recovery. A claim that is
refused by a busy executor started nothing, and a recovery is charged to `recovery_count`
instead — so a budget of three buys three real handler starts, which is what an operator
setting it expects.

### `params_json` is a snapshot, not a reference

The definition's `params_json` holds the job's configured defaults. The execution's holds
the **merged, resolved** value — defaults plus any per-trigger override — frozen when the
execution was created.

It is copied rather than joined on purpose. History has to answer "what did last Tuesday's
run actually use", and a join would answer "what would it use if it ran now" — a different
question, and the wrong one whenever someone has since edited the configuration.

### `dispatched_to` must be durable **before the send**

Written in the claim transaction, naming the executor already selected — never after the
reply. A scheduler that dies between sending `Run` and recording where it sent it would leave
recovery with an unset target, and recovery would conclude the dispatch never landed and
dispatch the same work elsewhere while the first executor runs it. See `protocol.md` §2.

### Truncation

`result_summary` and `error_message` are `VARCHAR(512)` and writers truncate explicitly
rather than relying on the database. Silent truncation of an error message removes exactly
the tail that explains the failure.

---

## 4a. `job_execution_attempt` — attempt history

The execution row holds the **current** attempt; each retry overwrites its fields. Without a
separate log, "attempt 1 failed with `upstream_5xx`, attempt 2's executor died, attempt 3
succeeded" is unreconstructable, and the admin API cannot honour the attempt history it
offers.

```sql
CREATE TABLE job_execution_attempt (
    execution_key  VARCHAR(160) NOT NULL,
    attempt_no     INT          NOT NULL,
    executor_id    VARCHAR(128) NULL,
    run_token      CHAR(36)     NOT NULL,
    started_at     DATETIME     NULL,
    finished_at    DATETIME     NULL,
    outcome        VARCHAR(20)  NOT NULL,   -- success | failed | unknown | fenced
    failure_kind   VARCHAR(48)  NULL,
    summary        VARCHAR(512) NULL,
    PRIMARY KEY (execution_key, attempt_no),
    KEY idx_job_attempt_time (finished_at)
);
```

One row is appended per attempt when it reaches a terminal state, including `unknown` for an
attempt whose executor could never be reconciled.

**The append is part of the same transaction as the execution's terminal or retry
transition**, guarded identically. Writing it separately would leave the two disagreeing
whenever a crash landed between them — and since redelivery of a result is answered *from*
this table (`dispatch.md` §3), a missing row turns an accepted result into a spurious
`ABORTED`.

Retention deletes attempts with their execution.

---

## 5. `job_executor` and `job_executor_handler` — registration

Written by `JobScheduler.Register` and `Heartbeat` (`dispatch.md` §6). One row per executor
**process per tenant**: a process serving several tenants registers once in each tenant's
schema, so one tenant's routing is never decided on another tenant's evidence.

```sql
CREATE TABLE job_executor (
    executor_id      VARCHAR(128) NOT NULL,
    executor_group   VARCHAR(64)  NOT NULL,   -- jobs route to a group
    address          VARCHAR(255) NOT NULL,   -- where the scheduler calls
    contract_version VARCHAR(16)  NOT NULL,
    revision         VARCHAR(64)  NOT NULL,
    capacity         INT          NOT NULL,
    running          INT          NOT NULL DEFAULT 0,
    capabilities     VARCHAR(255) NULL,       -- declared optional features
    started_at       DATETIME     NOT NULL,
    heartbeat_at     DATETIME     NOT NULL,
    PRIMARY KEY (executor_id),
    KEY idx_job_executor_alive (heartbeat_at),
    KEY idx_job_executor_group (executor_group, heartbeat_at)
);

CREATE TABLE job_executor_handler (
    executor_id VARCHAR(128) NOT NULL,
    handler_key VARCHAR(128) NOT NULL,
    PRIMARY KEY (executor_id, handler_key),
    KEY idx_job_executor_handler_key (handler_key)
);
```

`executor_id` is unique per process. A restart produces a new one, so a restarted executor
never inherits its predecessor's row and never has to clean one up — the old row simply ages
out.

The handler set is normalized rather than stored as a delimited string, because the query
that matters is an indexed join:

```sql
-- Orphans: enabled jobs no live executor can run.
SELECT d.job_name
FROM job_definition d
LEFT JOIN job_executor_handler h ON h.handler_key = d.handler_key
LEFT JOIN job_executor e ON e.executor_id = h.executor_id
                        AND e.heartbeat_at > ?        -- liveness bound
WHERE d.enabled = 1
GROUP BY d.job_name
HAVING COUNT(e.executor_id) = 0;
```

That query is the difference between noticing within a minute that a job has no executor
and noticing next week that it stopped running. It is also a cutover precondition and a
dispatch-time check, not just an alert.

**The registry lives in the database, not in scheduler memory.** With a scheduler cluster,
an in-memory registry would give each instance a different view of which executors exist,
and routing would depend on which instance happened to decide.

Rows whose heartbeat has aged past the bound are deleted by the retention sweep. Liveness is
a fresh heartbeat evaluated in database time — never against a scheduler's host clock.

---

## 6. `job_audit` — operator actions

```sql
CREATE TABLE job_audit (
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    actor      VARCHAR(64)  NOT NULL,
    action     VARCHAR(48)  NOT NULL,
    job_name   VARCHAR(128) NULL,
    execution  VARCHAR(160) NULL,
    detail     VARCHAR(1024) NULL,
    created_at DATETIME     NOT NULL,
    PRIMARY KEY (id),
    KEY idx_job_audit_job (job_name, id),
    KEY idx_job_audit_time (created_at)
);
```

Every mutating action — trigger, pause, resume, edit, retry, cancel, retire — records who
did it, to what, and what changed. `actor` is never defaulted to a placeholder when
identity is unavailable: an action that cannot be attributed is rejected instead.

---

## 7. Admin accounts live in the control database

Not here. An operator logs in once and works across every tenant, so per-tenant accounts
would mean one password per site and a login that breaks the moment a site is added. See
`admin_user` in §0.3.

Two roles, deliberately: one that can look, one that can act. Finer-grained authorization is
the adopter's business, and installations that need it put the UI behind their own access
controls.

---

## 8. Time in this schema

Two clocks, and every column belongs to exactly one of them.

| Clock | Columns | Source |
| --- | --- | --- |
| **business** | `next_fire_at`, `next_poll_at`, `scheduled_at`, `available_at`, `started_at`, `finished_at`, `created_at`, `updated_at` | the configured `Location` |
| **ownership** | `lease_until`, `heartbeat_at`, `deadline_at`, `timeout_at` | the database's `NOW()` |

`timeout_at` is set once, **in the claim transaction** — alongside `dispatched_to`, before
the `Run` call — to `NOW() + timeout_seconds`, and is **never extended**.

Setting it on acceptance instead would leave the same crash window `dispatched_to` closes: a
scheduler that dies after the executor accepted but before recording acceptance leaves a
successor with no cap at all, which then grants a fresh one or none. Starting the clock at
claim rather than acceptance charges the dispatch round trip to the job's budget — a second
or two against a cap measured in minutes or hours, in exchange for a cap that survives. It has to be a durable ownership instant rather than a timer in the
dispatching scheduler's memory: an execution can outlive the instance that started it, and a
successor that inherited only a progress-extended silence deadline would have no way to know
how much of the original cap had already elapsed — it would grant a fresh one, or none.
`started_at` cannot serve, because it is a business-clock column and ownership logic never
compares those against `NOW()`.

`deadline_at` is an **ownership** column for the same reason, because it answers an ownership
question — has this attempt gone silent long enough to be treated as lost? Writing it from a
business clock that a test environment can shift would make a healthy execution appear stale
the moment someone fast-forwarded the clock. The wire carries a *duration*
(`silence_deadline_seconds`), never an instant, so an executor never has to share a clock
with anything.

Ownership columns are the only ones ever compared against `NOW()`. This is not stylistic:
if availability were written in business time and compared against database time, any
divergence between the two — a mismatched session time zone, a deliberately shifted test
clock — would make every execution either permanently invisible or immediately due.

**The database session time zone must equal the configured `Location`.** Admission asserts
this and fails closed on a mismatch, rather than discovering it as an eight-hour scheduling
error at 2am.

All business timestamps are truncated to whole seconds. Execution keys derive from them, so
sub-second precision would let two callers produce two keys for one logical fire.

---

## 9. Retention

Every table this library owns is bounded. Growth without a cleanup path is not a smaller
problem than a bug; it is a bug with a delay.

| Table | Policy |
| --- | --- |
| `job_execution` | terminal rows only. `success` and `skipped` on a short window; `dead`, `cancelled` and manual runs kept for the longer audit window. **Non-terminal rows are never deleted.** |
| `job_executor`, `job_executor_handler` | rows whose heartbeat is older than the liveness bound |
| `job_audit` | the approved audit window; never truncated silently |

Cleanup is an **internal scheduler task**, not a job: it holds a named control lease
(`protocol.md` §9), runs inside the scheduler, and needs no executor. It could not be an
ordinary job — a job requires a handler, and no executor has access to the scheduler's own
tables, by design.

It is bounded like one, though: a batch size, a per-run row cap and a business-time cutoff,
with its runs visible in the UI alongside real jobs.

---

## 10. Schema versioning

`schema/mysql` holds the DDL as embedded, versioned files. This library **never executes
DDL**: the host applies it with whatever migration tool it already runs.

`schema.Version` declares the version the running library requires. Admission compares it
with what the database carries and **fails closed on a mismatch** — no silent degradation,
no partial feature set, no writing to a column that may not exist.

The consequence is a real compatibility contract: a library upgrade that needs new columns
is a schema migration the host must apply first, and the release notes must say so. That is
the cost of not running DDL at runtime, and it is the right trade for a component that
holds a lock on someone else's production database.
