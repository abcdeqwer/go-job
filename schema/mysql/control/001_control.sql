-- go-job control database, schema version 1.
--
-- Exactly one of these exists per installation. It is the only place in the system that
-- knows more than one tenant exists; it holds no job configuration, no execution state and
-- no business data.
--
-- go-job never executes DDL. Apply this with whatever migration tool you already run, and
-- apply it before starting a scheduler.
--
-- Requires MySQL 8.0 or later.

SET NAMES utf8mb4;

-- ---------------------------------------------------------------------------
-- Which tenants exist, and how to reach them.
--
-- Adding a site is one row here: schedulers poll this table and admit new tenants without a
-- restart. Admission is per tenant, so a row with a bad DSN records last_error and is
-- retried while every other tenant keeps running.
-- ---------------------------------------------------------------------------
CREATE TABLE tenant_registry (
    tenant           VARCHAR(64)     NOT NULL,

    -- AES-GCM ciphertext. Contains a database password and this table is reachable from an
    -- admin UI, so it is never stored or returned in clear.
    coordination_dsn VARBINARY(2048) NOT NULL,

    enabled          TINYINT(1)      NOT NULL DEFAULT 1,

    -- Bumped by every enable and disable. Instances acknowledge the generation they have
    -- applied in tenant_observation, which is how an operator sees who is lagging.
    generation       BIGINT          NOT NULL DEFAULT 1,

    -- The identity the coordination schema must present. Admission refuses a schema naming a
    -- different tenant or carrying a different uuid, which is what stops a mistyped DSN from
    -- silently adopting another tenant's schema, an empty one, or a restored snapshot.
    schema_uuid      CHAR(36)        NOT NULL,

    schema_version   VARCHAR(16)     NULL,
    admitted_at      DATETIME        NULL,
    last_error       VARCHAR(512)    NULL,
    created_at       DATETIME        NOT NULL,
    updated_at       DATETIME        NOT NULL,
    PRIMARY KEY (tenant)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- What each scheduler instance has seen.
--
-- Diagnostic, not proof. Quiescence before a DSN change is established by scanning the
-- tenant's own schema; this table answers the operator's next question, which is *which
-- instance* is still working.
-- ---------------------------------------------------------------------------
CREATE TABLE tenant_observation (
    tenant      VARCHAR(64)  NOT NULL,
    instance_id VARCHAR(128) NOT NULL,
    generation  BIGINT       NOT NULL,
    quiesced    TINYINT(1)   NOT NULL,
    observed_at DATETIME     NOT NULL,
    PRIMARY KEY (tenant, instance_id),
    KEY idx_tenant_observation_gen (tenant, generation),
    -- Retention sweeps by this; every restart mints a new instance_id, so without it the
    -- table grows once per process per tenant forever.
    KEY idx_tenant_observation_seen (observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Named leases for cluster-wide periodic work: retention sweeps, orphan scans.
-- A lease, not an election — no instance is promoted and nothing waits for consensus.
-- ---------------------------------------------------------------------------
CREATE TABLE control_lease (
    lease_name   VARCHAR(64)  NOT NULL,
    holder_id    VARCHAR(128) NOT NULL,
    run_token    CHAR(36)     NOT NULL,
    lease_until  DATETIME     NOT NULL,
    heartbeat_at DATETIME     NOT NULL,
    PRIMARY KEY (lease_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Which authenticated identity may register as what.
--
-- Without this the authorization rule is a sentence rather than a check, and any process
-- that can reach the scheduler could register for any tenant, declare a valuable handler,
-- and be handed that tenant's work by ordinary routing.
-- ---------------------------------------------------------------------------
CREATE TABLE executor_identity (
    identity       VARCHAR(255) NOT NULL,   -- mTLS subject, or credential id
    tenant         VARCHAR(64)  NOT NULL,
    executor_group VARCHAR(64)  NOT NULL,
    disabled       TINYINT(1)   NOT NULL DEFAULT 0,
    created_at     DATETIME     NOT NULL,
    PRIMARY KEY (identity, tenant, executor_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Admin accounts are global: an operator logs in once and works across tenants.
-- Present only when built-in authentication is used; installations fronting the UI with
-- their own SSO leave it empty.
-- ---------------------------------------------------------------------------
CREATE TABLE admin_user (
    username      VARCHAR(64)  NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role          VARCHAR(16)  NOT NULL,   -- VIEWER | OPERATOR
    disabled      TINYINT(1)   NOT NULL DEFAULT 0,
    created_at    DATETIME     NOT NULL,
    updated_at    DATETIME     NOT NULL,
    PRIMARY KEY (username),
    CONSTRAINT ck_admin_user_role CHECK (role IN ('VIEWER', 'OPERATOR'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Actions on the registry itself. Per-job actions are audited in that tenant's own schema.
-- ---------------------------------------------------------------------------
CREATE TABLE control_audit (
    id         BIGINT        NOT NULL AUTO_INCREMENT,
    actor      VARCHAR(64)   NOT NULL,
    action     VARCHAR(48)   NOT NULL,
    tenant     VARCHAR(64)   NULL,
    detail     VARCHAR(1024) NULL,
    created_at DATETIME      NOT NULL,
    PRIMARY KEY (id),
    KEY idx_control_audit_time (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
