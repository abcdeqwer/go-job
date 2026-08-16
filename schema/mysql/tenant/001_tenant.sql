-- go-job tenant coordination schema, schema version 1.
--
-- One of these per tenant. There is no tenant_id column anywhere: the connection IS the
-- tenant boundary, so two tenants may hold identical job names and identical row ids
-- without any possibility of interference, and a query that forgets a tenant predicate
-- cannot leak across tenants because no such predicate exists.
--
-- go-job never executes DDL. Apply this with whatever migration tool you already run,
-- before adding the tenant to the control database's registry.
--
-- Requires MySQL 8.0 or later. The session time zone of every connection to this schema
-- must equal the scheduler's configured business Location; admission asserts it.

SET NAMES utf8mb4;

-- ---------------------------------------------------------------------------
-- What this schema is.
--
-- Admission refuses a tenant whose schema does not name that tenant with the schema_uuid
-- the registry expects. Without this, isolation would rest on a DSN string being typed
-- correctly, and three ordinary mistakes would all be undetectable: pointing at another
-- tenant's schema (cross-tenant execution), at an empty schema (the tenant's jobs silently
-- vanish), or at a restored snapshot (stale configuration and executions replay).
--
-- schema_uuid is assigned once at provisioning and never changes. Re-pointing a tenant at a
-- genuinely new schema is an explicit re-provision, not a connection-string edit.
-- ---------------------------------------------------------------------------
CREATE TABLE schema_identity (
    lock_row       TINYINT      NOT NULL DEFAULT 1,
    tenant         VARCHAR(64)  NOT NULL,
    schema_uuid    CHAR(36)     NOT NULL,
    schema_version VARCHAR(16)  NOT NULL,
    created_at     DATETIME     NOT NULL,
    PRIMARY KEY (lock_row),
    CONSTRAINT ck_schema_identity_single CHECK (lock_row = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Job configuration. Operator-editable, rarely written.
--
-- Separate from job_state so an operator's edit never takes a lock the hot path contends
-- on, and a busy job's writes never queue behind a human's transaction.
--
-- Rows are created only through the admin API. The scheduler holds no handler code and has
-- no registry to materialize them from; handler_key is a string executors declare and
-- operators select.
-- ---------------------------------------------------------------------------
CREATE TABLE job_definition (
    job_name           VARCHAR(128) NOT NULL,
    handler_key        VARCHAR(128) NOT NULL,

    -- NULL = any group declaring the handler. Naming one distinguishes two groups that
    -- declare the same handler — a partial rollout, or two configurations of one service.
    executor_group     VARCHAR(64)  NULL,

    schedule_kind      VARCHAR(16)  NOT NULL,  -- CRON | FIXED_DELAY
    schedule_expr      VARCHAR(128) NOT NULL,  -- six-field cron, or delay in milliseconds

    enabled            TINYINT(1)   NOT NULL DEFAULT 1,
    retired            TINYINT(1)   NOT NULL DEFAULT 0,

    -- Defaults are decisions, recorded in the library's defaults.go with their reasons.
    -- FIRE_ONCE rather than SKIP: SKIP advances to the first FUTURE fire and runs nothing from
    -- the past, so a five-minute outage costs a daily job its whole day. That is the right
    -- answer for some jobs, but it is a surprising default, and the surprise arrives during an
    -- incident. A job that genuinely must not catch up says so explicitly.
    concurrency_policy VARCHAR(16)  NOT NULL DEFAULT 'QUEUE',      -- QUEUE | FORBID
    misfire_policy     VARCHAR(16)  NOT NULL DEFAULT 'FIRE_ONCE',  -- SKIP | FIRE_ONCE

    max_attempts       INT          NOT NULL,
    max_recoveries     INT          NOT NULL,
    lease_seconds      INT          NOT NULL,
    timeout_seconds    INT          NOT NULL,

    params_json        JSON         NULL,      -- defaults; merged with trigger overrides
    description        VARCHAR(512) NULL,

    version            BIGINT       NOT NULL DEFAULT 1,   -- optimistic CAS for edits
    updated_by         VARCHAR(64)  NULL,
    created_at         DATETIME     NOT NULL,
    updated_at         DATETIME     NOT NULL,
    PRIMARY KEY (job_name),
    KEY idx_job_definition_handler (handler_key),
    CONSTRAINT ck_job_definition_kind        CHECK (schedule_kind IN ('CRON', 'FIXED_DELAY')),
    CONSTRAINT ck_job_definition_concurrency CHECK (concurrency_policy IN ('QUEUE', 'FORBID')),
    CONSTRAINT ck_job_definition_misfire     CHECK (misfire_policy IN ('SKIP', 'FIRE_ONCE')),
    CONSTRAINT ck_job_definition_attempts    CHECK (max_attempts   BETWEEN 1 AND 100),
    CONSTRAINT ck_job_definition_recoveries  CHECK (max_recoveries BETWEEN 1 AND 100),
    CONSTRAINT ck_job_definition_lease       CHECK (lease_seconds  >= 10),
    CONSTRAINT ck_job_definition_timeout     CHECK (timeout_seconds BETWEEN 1 AND 604800)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Hot state. This row IS the job's lock.
--
-- Created in the same transaction as its definition; a definition without one is inert,
-- because both scheduling scans read this table and a claim fails closed without it.
--
-- Two "who" columns, because there are two layers: active_owner is the scheduler instance
-- that owns the work, dispatched_to is the executor actually running it. They fail
-- independently — a scheduler can die while its executor runs on — and recovery has to
-- reconcile with the executor rather than assume the work stopped.
-- ---------------------------------------------------------------------------
CREATE TABLE job_state (
    job_name         VARCHAR(128) NOT NULL,

    -- Business time. NULL on the kind that does not apply. next_poll_at is additionally
    -- NULL while a pass is outstanding, which is what reserves the loop.
    next_fire_at     DATETIME     NULL,
    next_poll_at     DATETIME     NULL,

    ops_paused       TINYINT(1)   NOT NULL DEFAULT 0,

    active_kind      VARCHAR(16)  NULL,      -- EXECUTION, or NULL
    active_execution VARCHAR(160) NULL,
    active_owner     VARCHAR(128) NULL,      -- scheduler instance
    active_run_token CHAR(36)     NULL,
    dispatched_to    VARCHAR(128) NULL,      -- executor_id
    fence_epoch      BIGINT       NOT NULL DEFAULT 0,

    -- Ownership clock: written and compared with UTC_TIMESTAMP(), never against business
    -- time and never against NOW() — see internal/store/store.go for why the session clock is
    -- not good enough.
    lease_until      DATETIME     NULL,
    heartbeat_at     DATETIME     NULL,

    last_success_at  DATETIME     NULL,
    last_failure_at  DATETIME     NULL,

    -- The definition version that produced the current next_fire_at. When it lags, a drift
    -- scan recomputes — this is how an edit to a weekly job takes effect in seconds instead
    -- of when its stale instant eventually arrives.
    config_version   BIGINT       NOT NULL DEFAULT 0,

    -- Incremented by every guarded write. See job_execution.write_seq.
    write_seq        BIGINT       NOT NULL DEFAULT 0,
    updated_at       DATETIME     NOT NULL,
    PRIMARY KEY (job_name),
    KEY idx_job_state_due   (next_fire_at),
    KEY idx_job_state_poll  (next_poll_at),
    KEY idx_job_state_lease (lease_until),
    CONSTRAINT fk_job_state_definition
        FOREIGN KEY (job_name) REFERENCES job_definition (job_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Execution instances.
--
-- id is AUTO_INCREMENT rather than a distributed id: uniqueness is only ever needed within
-- this schema, ordering only needs to be monotonic for the claim tiebreak, and deduplication
-- is carried entirely by execution_key.
-- ---------------------------------------------------------------------------
CREATE TABLE job_execution (
    id              BIGINT       NOT NULL AUTO_INCREMENT,

    -- Deterministic for cron (derived from the fire instant, so duplicate materialization is
    -- a unique-key violation rather than a second run); a fresh monotonic id for manual and
    -- poll, which have no instant to derive from.
    execution_key   VARCHAR(160) NOT NULL,
    job_name        VARCHAR(128) NOT NULL,

    trigger_type    VARCHAR(16)  NOT NULL,  -- cron | manual | poll
    manual_first    TINYINT(1)   NOT NULL DEFAULT 0,
    request_id      VARCHAR(64)  NULL,      -- manual only; API idempotency

    -- Business clock.
    scheduled_at    DATETIME     NOT NULL,
    available_at    DATETIME     NOT NULL,

    status          VARCHAR(20)  NOT NULL,

    attempt_no      INT          NOT NULL DEFAULT 0,   -- handler starts; +1 on acceptance
    recovery_count  INT          NOT NULL DEFAULT 0,   -- crash-and-reclaim cycles
    max_attempts    INT          NOT NULL,
    max_recoveries  INT          NOT NULL,

    params_json     JSON         NULL,      -- snapshot, so history shows what a run used

    owner_instance  VARCHAR(128) NULL,      -- scheduler instance
    dispatched_to   VARCHAR(128) NULL,      -- executor; written BEFORE the Run call
    run_token       CHAR(36)     NULL,
    fence_epoch     BIGINT       NOT NULL DEFAULT 0,

    -- Ownership clock throughout.
    lease_until     DATETIME     NULL,
    heartbeat_at    DATETIME     NULL,
    deadline_at     DATETIME     NULL,      -- silence budget; extended by progress
    timeout_at      DATETIME     NULL,      -- per-attempt runtime cap; set at claim, never
                                            -- extended; cleared only by retry and recovery

    -- Business clock.
    started_at      DATETIME     NULL,
    finished_at     DATETIME     NULL,

    failure_kind    VARCHAR(48)  NULL,      -- stable, groupable
    terminal_reason VARCHAR(24)  NULL,      -- how a terminal state was reached
    result_summary  VARCHAR(512) NULL,
    error_message   VARCHAR(512) NULL,

    -- Incremented by every guarded write, and by nothing else.
    --
    -- It exists because MySQL reports rows CHANGED, not rows matched, and every other column
    -- a guarded write touches can legitimately be assigned the value it already holds:
    -- DATETIME columns are whole-second, so a heartbeat or a progress report redelivered
    -- inside the same database second writes lease_until, deadline_at and updated_at back
    -- unchanged. Without a column that always moves, zero changed rows would be ambiguous
    -- between "the ownership guard failed" and "this write was an exact repeat" — and the
    -- protocol reads the first as fencing, which would abort a healthy twenty-hour handler
    -- because one response packet was lost.
    --
    -- With it, zero changed rows means the guard failed, everywhere, unconditionally.
    write_seq       BIGINT       NOT NULL DEFAULT 0,

    created_at      DATETIME     NOT NULL,
    updated_at      DATETIME     NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_execution_key     (execution_key),
    UNIQUE KEY uk_job_execution_request (request_id),

    -- Claim: equality on status, then the manual/scheduled split, then age. Each class is
    -- read with its own bounded query so neither starves the other.
    KEY idx_job_execution_claim     (status, manual_first, available_at, id),
    KEY idx_job_execution_recovery  (status, lease_until, id),
    KEY idx_job_execution_timeout   (status, timeout_at),
    KEY idx_job_execution_history   (job_name, scheduled_at, id),

    CONSTRAINT ck_job_execution_trigger CHECK (trigger_type IN ('cron', 'manual', 'poll')),
    CONSTRAINT ck_job_execution_status  CHECK (status IN (
        'ready', 'dispatching', 'running', 'cancel_requested',
        'success', 'dead', 'cancelled', 'skipped'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Attempt history.
--
-- Keyed by run_token, NOT by attempt_no. A token identifies an attempt; attempt_no counts
-- budget, and the two differ in a case that occurs in normal operation: an attempt accepted
-- whose reply is lost, whose executor then restarts, is classified unknown and correctly
-- does not consume the ordinal — so two attempts legitimately share one.
--
-- Appended in the same transaction as the execution's terminal or retry transition, because
-- a redelivered result is answered from this table.
-- ---------------------------------------------------------------------------
CREATE TABLE job_execution_attempt (
    execution_key VARCHAR(160) NOT NULL,
    run_token     CHAR(36)     NOT NULL,
    attempt_no    INT          NOT NULL,   -- budget ordinal; not unique
    executor_id   VARCHAR(128) NULL,
    started_at    DATETIME     NULL,
    finished_at   DATETIME     NULL,
    outcome       VARCHAR(20)  NOT NULL,   -- success | failed | unknown | fenced
    failure_kind  VARCHAR(48)  NULL,
    summary       VARCHAR(512) NULL,
    PRIMARY KEY (execution_key, run_token),
    KEY idx_job_attempt_ordinal (execution_key, attempt_no),
    KEY idx_job_attempt_time    (finished_at),
    CONSTRAINT ck_job_attempt_outcome
        CHECK (outcome IN ('success', 'failed', 'unknown', 'fenced'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Executor registration. One row per executor PROCESS per tenant.
--
-- In the database rather than scheduler memory: with a scheduler cluster, an in-memory
-- registry would give each instance a different view of which executors exist, and routing
-- would depend on which instance happened to decide.
-- ---------------------------------------------------------------------------
CREATE TABLE job_executor (
    executor_id      VARCHAR(128) NOT NULL,   -- unique per process; a restart mints a new one
    executor_group   VARCHAR(64)  NOT NULL,
    address          VARCHAR(255) NOT NULL,
    contract_version VARCHAR(16)  NOT NULL,
    revision         VARCHAR(64)  NOT NULL,
    capacity         INT          NOT NULL,   -- advisory; the executor's refusal is authoritative
    running          INT          NOT NULL DEFAULT 0,
    capabilities     VARCHAR(255) NULL,

    -- The authenticated identity that registered this process.
    --
    -- Callbacks carry an execution or an executor id, not a group, so without this a
    -- group-restricted identity could heartbeat any executor id it knew — keeping a dead
    -- process's address routable indefinitely, which produces unknown dispatches and consumes
    -- recovery budget on jobs that were never going to run.
    identity         VARCHAR(255) NOT NULL DEFAULT '',

    started_at       DATETIME     NOT NULL,
    heartbeat_at     DATETIME     NOT NULL,

    -- Same purpose as job_execution.write_seq, for the same reason. A heartbeat repeated
    -- inside one database second, from an executor whose running count has not changed,
    -- rewrites every column identically and MySQL reports zero rows changed. The heartbeat
    -- reads zero as "your registration has lapsed, call Register again", so without a column
    -- that always moves, a healthy idle executor would be told to re-register once a second.
    write_seq        BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (executor_id),
    KEY idx_job_executor_alive (heartbeat_at),
    KEY idx_job_executor_group (executor_group, heartbeat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Normalized rather than a delimited string, because the query that matters — "is any live
-- executor serving this job?" — must be an indexed join. That query drives the orphan
-- alert, which is the difference between noticing in a minute that a job has no executor
-- and noticing next week that it stopped running.
CREATE TABLE job_executor_handler (
    executor_id VARCHAR(128) NOT NULL,
    handler_key VARCHAR(128) NOT NULL,
    PRIMARY KEY (executor_id, handler_key),
    KEY idx_job_executor_handler_key (handler_key),
    CONSTRAINT fk_job_executor_handler_executor
        FOREIGN KEY (executor_id) REFERENCES job_executor (executor_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Operator actions on this tenant's jobs. Registry actions are audited in the control
-- database instead.
-- ---------------------------------------------------------------------------
CREATE TABLE job_audit (
    id         BIGINT        NOT NULL AUTO_INCREMENT,
    actor      VARCHAR(64)   NOT NULL,   -- never defaulted; unattributable actions are refused
    action     VARCHAR(48)   NOT NULL,
    job_name   VARCHAR(128)  NULL,
    execution  VARCHAR(160)  NULL,
    detail     VARCHAR(1024) NULL,
    created_at DATETIME      NOT NULL,
    PRIMARY KEY (id),
    KEY idx_job_audit_job  (job_name, id),
    KEY idx_job_audit_time (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
