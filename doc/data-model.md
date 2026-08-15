# Data model

Every table lives in one MySQL schema per tenant. There is no `tenant_id` column anywhere:
the connection *is* the tenant boundary. Two tenants may hold identical job names and
identical row ids without any possibility of interference, and a query that forgets a
tenant predicate cannot leak across tenants because no such predicate exists.

Table names carry a configurable prefix, shown here as `job_`.

---

## 0. Where these tables live

The library requires **a** MySQL schema per tenant for its coordination tables. Whether
that is the tenant's business schema or a dedicated one is chosen **by the adopter, in
configuration** — it is not a build-time option and there is no code path in this library
that differs between the two.

Each tenant supplies up to two DSNs:

```go
Tenants: map[string]gojob.Tenant{
    // dedicated coordination schema
    "acme": {Coordination: "…/acme_scheduler", Business: "…/acme"},

    // shared: the same schema holds both
    "beta": {Coordination: "…/beta"},
},
```

`Coordination` is required and is where the tables in this document live. `Business` is
optional and defaults to `Coordination`; it is what `ctx.DB()` hands to handlers. That is
the entire selection mechanism, and it exists in exactly one place.

### The difference is operational, not behavioural

In this version the two topologies are **identical in correctness**. Nothing about
ownership, durability or recovery changes, and no guarantee is stronger in one than the
other.

What differs is operations:

| | Dedicated coordination schema | Shared with business |
| --- | --- | --- |
| Backup, tuning, access control | independent of business data | coupled |
| Blast radius of scheduler churn | isolated | scheduler write load sits in the business schema |
| Locks held inside the business database | none | the state row lives there |
| Schemas to provision and migrate | one more per tenant | none extra |
| Connection budget | two pools per tenant | one |

**Dedicated is the better default.** Shared is a reasonable simplification for small
installations that would rather manage one schema.

### Why there is no atomicity argument here

A tempting claim is that sharing a schema lets a handler commit its own progress in the
**same transaction** as the scheduler's completion write, making checkpoint patterns
airtight. That would be a real advantage — but it is not one this library provides, because
it exposes no API through which a handler can join the completion transaction. A handler
always opens its own transaction on `ctx.DB()`, and whether the coordination tables happen
to live in the same schema is irrelevant to it.

Such an API is deliberately absent rather than merely unbuilt:

- it would bind handler code to the scheduler's transaction lifecycle;
- it would work in only one topology, so code written against it breaks the day someone
  moves the coordination schema — a trap that fires long after the decision that set it;
- it closes exactly one of several crash points. A handler still has to survive crashing
  before its own write, or after the completion write and before the next run observes it,
  so idempotency is required regardless and the API buys less than it appears to.

If a future version offers it, it will be an opt-in capability that fails loudly when the
topology cannot support it — not a silent difference in guarantees between two
configurations.

### What is not offered

One shared coordination schema for all tenants, with a `tenant_id` column. That trades the
isolation property this whole design rests on for a saving in schema count, and it puts
every coordination query one forgotten predicate away from crossing a tenant boundary.

### How this affects discovery

There is no discovery protocol, and this is the point: workers do not broadcast, do not
register with a service registry, and do not expose an endpoint for a controller to call.
A worker that is configured with a coordination DSN **is** discovered, because it writes
itself into `job_worker` in that schema and the admin UI reads the same table.

Registration therefore requires no shared business database, no network path from the
control plane to workers, and no configuration listing which workers exist. Two processes
that can reach the same coordination schema can coordinate; two that cannot, cannot. That
is the whole model.

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
    schedule_kind      VARCHAR(16)  NOT NULL,  -- CRON | FIXED_DELAY
    schedule_expr      VARCHAR(128) NOT NULL,  -- cron expression, or delay in ms
    enabled            TINYINT(1)   NOT NULL DEFAULT 1,
    concurrency_policy VARCHAR(16)  NOT NULL,  -- QUEUE | FORBID
    misfire_policy     VARCHAR(16)  NOT NULL,  -- SKIP | FIRE_ONCE
    max_attempts       INT          NOT NULL,
    max_recoveries     INT          NOT NULL,
    lease_seconds      INT          NOT NULL,
    timeout_seconds    INT          NOT NULL,
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

### Reconciliation

The `CONTROL_PLANE` role materializes rows from the code registry: insert what is missing
with the registration's declared defaults, **never overwrite what exists**. An existing row
may carry an operator's edits, and a deployment must not undo them by restarting.

Reconciliation is therefore idempotent, which is what makes a misconfigured second control
plane cost duplicated effort rather than damage.

Deleting a job from the registry does not delete its row. Retiring a job is an explicit
audited action, so that history and any pending executions stay inspectable.

---

## 3. `job_state` — scheduler-owned hot state

```sql
CREATE TABLE job_state (
    job_name          VARCHAR(128) NOT NULL,
    next_fire_at      DATETIME     NULL,
    ops_paused        TINYINT(1)   NOT NULL DEFAULT 0,
    active_kind       VARCHAR(16)  NULL,      -- EXECUTION | LOOP
    active_execution  VARCHAR(160) NULL,
    active_worker_id  VARCHAR(128) NULL,
    active_run_token  CHAR(36)     NULL,
    fence_epoch       BIGINT       NOT NULL DEFAULT 0,
    lease_until       DATETIME     NULL,
    heartbeat_at      DATETIME     NULL,
    last_success_at   DATETIME     NULL,
    last_failure_at   DATETIME     NULL,
    config_version    BIGINT       NOT NULL DEFAULT 0,
    updated_at        DATETIME     NOT NULL,
    PRIMARY KEY (job_name),
    KEY idx_job_state_due (next_fire_at),
    KEY idx_job_state_lease (lease_until)
);
```

### One active holder, and what may hold it

This row *is* the job's lock. `active_kind` says what holds it:

| `active_kind` | Holder | Released by |
| --- | --- | --- |
| `EXECUTION` | a claimed cron or manual execution | completion, retry or recovery |
| `LOOP` | a fixed-delay polling loop | clean loop exit, or recovery |
| `NULL` | nobody | — |

A single-valued holder is a deliberate constraint, and it is why there is no `PARALLEL`
concurrency policy: heartbeat, completion, retry and recovery all guard on
`active_run_token` and `fence_epoch` from this row, so two concurrent executions of one job
would have no way to each own, renew and release it. A policy declaring "no job-level lock"
would leave every one of those statements operating on a row that does not describe the
writer.

Unifying loops and executions behind the same holder is what makes a manual trigger
serialize against a running poller without a second mechanism.

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
    trigger_type   VARCHAR(16)  NOT NULL,  -- cron | manual
    scheduled_at   DATETIME     NOT NULL,
    available_at   DATETIME     NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    attempt_no     INT          NOT NULL DEFAULT 0,
    recovery_count INT          NOT NULL DEFAULT 0,
    max_attempts   INT          NOT NULL,
    max_recoveries INT          NOT NULL,
    worker_id      VARCHAR(128) NULL,
    run_token      CHAR(36)     NULL,
    fence_epoch    BIGINT       NOT NULL DEFAULT 0,
    lease_until    DATETIME     NULL,
    heartbeat_at   DATETIME     NULL,
    started_at     DATETIME     NULL,
    finished_at    DATETIME     NULL,
    failure_kind   VARCHAR(48)  NULL,
    result_summary VARCHAR(512) NULL,
    error_message  VARCHAR(512) NULL,
    created_at     DATETIME     NOT NULL,
    updated_at     DATETIME     NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_execution_key (execution_key),
    KEY idx_job_execution_claim (status, available_at, id),
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
manual:01J9Z6QWERTY
```

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
| `running` | owned and executing |
| `cancel_requested` | asked to stop; still owned, still leased |
| `success` | terminal |
| `dead` | terminal; retry budget exhausted or permanently failed |
| `cancelled` | terminal |
| `skipped` | terminal; `FORBID` contention |

### Two budgets

`attempt_no` bounds real handler starts and is incremented **only at claim**.
`recovery_count` bounds crash-and-reclaim cycles.

They are separate because they answer different questions. A job that fails cleanly three
times and a job that kills its worker three times both stop, but an operator needs to tell
them apart, and only the second is a reason to look at memory limits.

`attempt_no` is deliberately *not* incremented by recovery: the re-claim that follows a
crash increments it, so incrementing in both places would exhaust a budget of three in two
real starts.

### Truncation

`result_summary` and `error_message` are `VARCHAR(512)` and writers truncate explicitly
rather than relying on the database. Silent truncation of an error message removes exactly
the tail that explains the failure.

---

## 5. `job_worker` and `job_worker_handler` — registration

```sql
CREATE TABLE job_worker (
    worker_id      VARCHAR(128) NOT NULL,
    role           VARCHAR(16)  NOT NULL,   -- WORKER | CONTROL_PLANE | BOTH
    build_revision VARCHAR(64)  NOT NULL,
    handler_count  INT          NOT NULL,
    started_at     DATETIME     NOT NULL,
    heartbeat_at   DATETIME     NOT NULL,
    PRIMARY KEY (worker_id),
    KEY idx_job_worker_alive (heartbeat_at)
);

CREATE TABLE job_worker_handler (
    worker_id VARCHAR(128) NOT NULL,
    job_name  VARCHAR(128) NOT NULL,
    PRIMARY KEY (worker_id, job_name),
    KEY idx_job_worker_handler_job (job_name)
);
```

`worker_id` is `<hostname>:<pid>:<boot-nonce>`, unique per process, so a restarted process
never inherits its predecessor's row and never has to clean one up.

The handler set is normalized rather than stored as a delimited string, because the query
that matters — "is any live worker serving this job?" — must be an indexed join. That
query drives the orphan alert, which is the difference between noticing within a minute
that a job has no executor and noticing next week that it stopped running.

Rows whose heartbeat has aged past the retention bound are deleted; liveness is a fresh
heartbeat evaluated in database time.

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

## 7. `job_admin_user` — optional

Present only when the built-in authentication is used. Hosts fronting the UI with their own
SSO disable built-in login and this table stays empty.

```sql
CREATE TABLE job_admin_user (
    username      VARCHAR(64)  NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role          VARCHAR(16)  NOT NULL,   -- VIEWER | OPERATOR
    disabled      TINYINT(1)   NOT NULL DEFAULT 0,
    created_at    DATETIME     NOT NULL,
    updated_at    DATETIME     NOT NULL,
    PRIMARY KEY (username)
);
```

Two roles, deliberately: one that can look, one that can act. A permission model with more
gradations is the host's business, and hosts that need it put the UI behind their own
authorization.

---

## 8. Time in this schema

Two clocks, and every column belongs to exactly one of them.

| Clock | Columns | Source |
| --- | --- | --- |
| **business** | `next_fire_at`, `scheduled_at`, `available_at`, `started_at`, `finished_at`, `created_at`, `updated_at` | the configured `Location` |
| **ownership** | `lease_until`, `heartbeat_at` | the database's `NOW()` |

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
| `job_worker`, `job_worker_handler` | rows whose heartbeat is older than the liveness bound |
| `job_audit` | the approved audit window; never truncated silently |

Cleanup runs as an ordinary job of this scheduler, with a batch size, a per-run row cap and
a business-time cutoff, so it is visible, bounded and interruptible like any other job.

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
