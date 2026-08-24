# go-job

A multi-tenant, database-durable **job scheduling platform**.

`go-job` decides what runs, when, and for which tenant, with exactly one owner at a time. Your applications
run the work: they register as **executors** over a gRPC contract, in any language, and
receive dispatches with parameters. MySQL is the only infrastructure dependency, and the
admin UI ships with the scheduler.

```text
        ┌──────────────────────┐        registers, reports
        │      go-job          │◄───────────────────────────┐
        │  scheduler cluster   │                            │
        │  + admin UI + API    │──────dispatch + params────►│
        └──────────┬───────────┘                    ┌───────┴────────┐
                   │                                │   executors    │
        control DB │ per-tenant coordination DBs    │  (any language)│
                   ▼                                └────────────────┘
```

> **Status: implemented.** The scheduler runs. `doc/` is the specification and the code
> follows it; where they disagreed during review, the document won unless it was itself
> incoherent, and the commit says which.
>
> Built: the cron engine, the full execution protocol against MySQL, materialization and
> misfire, the executor registry, the gRPC contract in both directions, the scheduler loops,
> the tenant registry with hot add, the operator API and UI, and a runnable binary. Ten
> end-to-end scenarios run the whole thing against a real MySQL over real gRPC.
>
> Not built: metrics exposition, the differential-replay harness `doc/verification.md`
> describes, and executor SDKs for languages other than Go — the contract is the SDK, and
> `internal/testexec` is a complete Go executor to read.

---

## 1. Design goals

**Durable before prompt.** A scheduler that occasionally forgets a run is worse than one
that runs a few seconds late. The database is the authority for what is due and who owns
it; in-process timers only reduce latency. Losing a timer, a process or a host costs
latency, never a run.

**Exactly one owner, provably.** Not "usually one" — a single lock order, leases with
heartbeats, and a fence epoch on every ownership-bearing write, so a process that was
paused, partitioned or resurrected cannot overwrite the state of the owner that replaced
it. There is exactly one code path that reclaims an expired holder.

**Multi-tenant by physical isolation.** One coordination schema per tenant. No `tenant_id`
column threaded through every table, no default tenant, no cross-tenant fallback. A
tenant's jobs, configuration, history and failures are its own. Adding a site is a row in
the registry — not a redeploy.

**Executors are dumb on purpose.** They accept work, run it and report. Ownership, leases,
fencing, recovery, retry budgets and misfire policy all live in the scheduler, so a new
executor in a new language cannot get any of it subtly wrong.

**Operable by people who did not write it.** Schedule, enablement, concurrency policy,
retry budget and timeout are data you can edit and audit at runtime, not constants
recompiled into a binary. A job that silently stopped running is a detectable condition,
not something you notice a week later.

**Honest about its guarantees.** Owning a job prevents two executors from running it
concurrently. It does not make an outbound HTTP call idempotent, and it does not prove that
a cancelled handler's in-flight request was withdrawn. This platform states which guarantee
it provides and refuses to imply the other.

**One dependency.** MySQL. No Redis, no message broker, no ZooKeeper, no etcd, no
sidecar, no separate scheduler service to operate.

---

## 2. Features

### Scheduling

- **Cron jobs** — six-field expressions with seconds, steps, ranges, lists and named
  weekdays and months. Each fire instant becomes one durable execution with a
  deterministic key, so duplicate creation is rejected by the database, not by luck.
- **Fixed-delay pollers** — one pass at a time per tenant and job, the next dispatched a
  configured delay after the previous **completes**. A pass that finds nothing writes no
  execution row, so an idle three-second poller does not produce 28,800 rows a day saying
  nothing happened.
- **Parameters** — every job carries configured defaults, overridable per manual trigger,
  snapshotted onto each execution so history shows what a run actually used.
- **Misfire policies** — after an outage, either skip to the next future fire or run one
  catch-up. Unbounded replay is not offered: an hour of downtime must not become an hour
  of catch-up executions.
- **Runtime schedule changes** — edit a cron expression and it takes effect within
  seconds, including for jobs whose next fire is days away.

### Execution

- leases, heartbeats and fence epochs, with a single canonical lock order so claim,
  completion, retry and recovery cannot deadlock against each other;
- automatic recovery of work whose owner died, bounded separately from ordinary retries so
  a handler that kills its process still terminates instead of cycling forever;
- bounded, deterministic retry backoff, with terminality decided in SQL rather than by the
  handler;
- concurrency policy per job — queue the next occurrence, or skip it;
- cooperative cancellation with an explicit intermediate state, so a job that was asked to
  stop is distinguishable from one that has actually stopped;
- graceful shutdown that stops claiming before it stops working.

### Operations

- **built-in admin UI** — tenants, job list with effective state, execution history, live
  executors, manual trigger with parameter overrides, pause and resume, schedule and policy
  editing, retry and cancel;
- **executor registration** — every executor registers its handler set and heartbeats, so
  "no live executor serves this job" is an alert rather than a mystery;
- **audit trail** — every operator action recorded with actor, target and reason;
- **Prometheus metrics** — backlog, dispatch lag, durations, terminal-state counters,
  fence losses, stale recoveries, pool usage;
- **bounded retention** — every table go-job owns has a cleanup policy that ships with it,
  so history does not grow without limit.

### Not included, deliberately

Each of these is a refusal rather than a gap. They are the features that turn a scheduler
into a distributed-systems project:

- job DAGs, sub-tasks, fan-out/fan-in;
- sharded broadcast execution;
- script or shell jobs, and dynamically registered handlers — handlers are compiled in and
  typed, so a scheduler compromise is not remote code execution;
- routing-strategy families — a job runs on a deployment whose assignment includes its
  handler;
- exactly-once external side effects;
- arbitrary/operator-supplied DDL at runtime; the binary only applies its own ordered,
  embedded additive tenant migrations during admission.

---

## 3. What you need to run it

| Component | Requirement | Why |
| --- | --- | --- |
| **MySQL** | 8.0 or later, one coordination schema per tenant | durability and coordination authority; `SELECT ... FOR UPDATE SKIP LOCKED` is required |
| **Go** | 1.26 or later — for the scheduler only | executors may be written in any language with gRPC support |
| A migration tool | optional — Flyway, golang-migrate, plain scripts, your own | useful for control-schema setup or manually managed tenant upgrades |

That is the complete list. There is no broker, no coordination service and no separate
scheduler daemon.

Optional:

- a reverse proxy in front of the admin UI if you want your own SSO instead of the
  built-in authentication.

Not yet available: there is no metrics endpoint. Operational visibility today is the admin UI,
the execution history it reads, and structured logs.

---

## 4. Getting it running

From nothing to a scheduler running a job. Every command here has been executed as written.

### 1. Initialise MySQL

go-job has one control database per installation and one coordination database per tenant.
Use the naming convention `gojob_<tenant>` for tenant databases, for example `gojob_cp`,
`gojob_bp` and `gojob_app`.

The first complete tenant schema is version 1. A tenant can be provisioned explicitly from the
UI, which applies the complete embedded migration stream to an empty database. On later starts,
admission verifies the tenant and schema UUID, then applies any missing embedded additive tenant
migrations before starting that tenant's engine. Therefore every tenant DSN account needs the
DDL privileges required by those migrations; the control account also needs DDL privileges if
operators use UI provisioning beside the control database.

#### Simple single-account setup

Run the following as a MySQL DBA. Replace the password placeholder before executing it.
The escaped underscore in `` `gojob\_%` `` is intentional: it grants access to current and
future databases whose names start with `gojob_`, without granting access to unrelated
databases.

```sql
CREATE USER IF NOT EXISTS 'gojob'@'%'
  IDENTIFIED BY '<CHANGE_ME_STRONG_PASSWORD>';

CREATE DATABASE IF NOT EXISTS `gojob_control`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

-- Control database: runtime DML plus one-time schema installation.
GRANT SELECT, INSERT, UPDATE, DELETE,
      CREATE, ALTER, INDEX, REFERENCES
ON `gojob_control`.*
TO 'gojob'@'%';

-- Tenant databases: also covers future gojob_cp, gojob_bp, gojob_app, ... databases.
-- CREATE permits CREATE DATABASE gojob_<tenant>; REFERENCES is required by the foreign keys
-- in schema/mysql/tenant/001_tenant.sql. DROP is deliberately not granted.
GRANT SELECT, INSERT, UPDATE, DELETE,
      CREATE, ALTER, INDEX, REFERENCES
ON `gojob\_%`.*
TO 'gojob'@'%';
```

`REFERENCES` is required: without it, tenant schema installation fails while creating
`job_state` with `REFERENCES command denied ... for table job_definition`.

Install the control schema once:

```sh
mysql -h <mysql-host> -u gojob -p gojob_control \
  < schema/mysql/control/001_control.sql
```

The process DSN is then:

```text
gojob:<PASSWORD>@tcp(<mysql-host>:3306)/gojob_control
```

Do not commit the real password or DSN to this repository.

#### Create a tenant database

When adding a tenant in the UI with only a database name, go-job can create and initialise the
empty database with the control account above. The equivalent manual preparation for tenant
`cp` is:

```sql
CREATE DATABASE IF NOT EXISTS `gojob_cp`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;
```

```sh
mysql -h <mysql-host> -u gojob -p gojob_cp \
  < schema/mysql/tenant/001_tenant.sql
mysql -h <mysql-host> -u gojob -p gojob_cp \
  < schema/mysql/tenant/002_execution_retention.sql
mysql -h <mysql-host> -u gojob -p gojob_cp \
  < schema/mysql/tenant/003_handler_descriptions.sql
```

Then claim the schema for exactly one tenant. Save the returned `schema_uuid`; tenant admission
checks it so that a mistyped DSN cannot silently attach another tenant's database.

```sql
INSERT INTO `gojob_cp`.schema_identity
  (lock_row, tenant, schema_uuid, schema_version, created_at)
VALUES
  (1, 'cp', UUID(), '3', NOW());

SELECT tenant, schema_uuid, schema_version, created_at
FROM `gojob_cp`.schema_identity
WHERE lock_row = 1;
```

For another tenant, replace both `gojob_cp` and `cp`. A database must contain exactly one
`schema_identity` row and can belong to only one tenant. Do not insert `tenant_registry`
manually: add the tenant in the UI/API so its DSN is encoded consistently and admission can
validate the database identity.

#### Optional scripted bootstrap records

The preferred first-admin flow is the setup page shown when `admin_user` is empty. For a
scripted bootstrap, generate the bcrypt hash and insert it into the control database:

```sh
gojob -hash-password '<AT_LEAST_12_CHARACTERS>'
```

```sql
INSERT INTO `gojob_control`.admin_user
  (username, password_hash, role, disabled, created_at, updated_at)
VALUES
  ('admin', '<BCRYPT_HASH_FROM_THE_COMMAND>', 'OPERATOR', 0, NOW(), NOW());
```

Executor credentials are normally created in the UI. For a shared-token executor, hash the
token first and store only its SHA-256 value:

```sh
gojob -hash-token '<EXECUTOR_TOKEN>'
```

```sql
INSERT INTO `gojob_control`.executor_identity
  (identity, tenant, executor_group, token_sha256, disabled, created_at)
VALUES
  ('job-worker-go', 'cp', '', '<SHA256_FROM_THE_COMMAND>', 0, NOW());
```

#### Verify the installation

```sql
SHOW GRANTS FOR 'gojob'@'%';

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'gojob_control'
ORDER BY table_name;

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'gojob_cp'
ORDER BY table_name;

SELECT tenant, schema_uuid, schema_version
FROM `gojob_cp`.schema_identity
WHERE lock_row = 1;
```

This setup intentionally omits `DROP`, so the account cannot directly run `DROP DATABASE` or
`DROP TABLE`. It is not a read-only safety boundary: the account can still change rows and
schema objects through its granted DML and `ALTER` privileges. If operations require a stricter
split, apply both schema files with a separate migration account and leave the scheduler account
only `SELECT, INSERT, UPDATE, DELETE` on `gojob_control` and `gojob\_%`.

### 2. Start it

```sh
gojob -control-dsn 'gojob:PASSWORD@tcp(mysql:3306)/gojob_control'
```

**That is the only required flag.** Everything else has a working default; `README §6` lists
them all. The ones most deployments set:

```sh
gojob \
  -control-dsn 'gojob:PASSWORD@tcp(mysql:3306)/gojob_control' \
  -location    'Asia/Manila' \       # cron expressions are evaluated in this zone
  -admin-addr  ':8080' \             # operator UI and API
  -grpc-addr   ':9090' \             # executors connect here
  -tls-cert /certs/server.crt -tls-key /certs/server.key \
  -tls-client-ca /certs/executor-ca.crt
```

Each flag also reads a `GOJOB_`-prefixed environment variable — `GOJOB_CONTROL_DSN` and so on
— which is usually the better way to carry them in a container.

**Read the startup log once.** Four warnings mean you are running something you may not have
intended, and each names what it costs. Two appear by default — TLS and DSN encryption are both
opt-in — and two only if you asked for them:

```
the executor gRPC service is PLAINTEXT            ← unless -tls-cert/-tls-key
tenant DSNs are stored WITHOUT encryption         ← unless -dsn-key
executor calls are accepted WITHOUT a credential  ← only with -allow-unauthenticated-executors
authenticated executors are accepted for tenants
  they are NOT listed for                         ← only with -allow-unlisted-executors
```

In production the log should have none of them.

### 3. Everything else is in the browser

Open `http://host:8080`. There is nothing more to configure on the command line.

| Step | Where |
| --- | --- |
| **First administrator** | The page offers it when no account exists yet. Twelve characters minimum. It refuses once an account exists, so this is not an open door. |
| **Add a tenant** | 租户 → 添加租户. Enter host, database, user, password; **test the connection**; if the database is empty it offers to create the tables. Then save. |
| **Authorise an executor** | 凭证 → 授权执行器. mTLS by certificate subject, or a generated token shown **once**. Nothing registers without a row here; revoke before permanently deleting one. |
| **Create jobs** | jobs → new job. Every field carries an explanation, and the schedule shows the next five fire instants as you type. |

The tenant appears in the picker within one registry poll (10s by default) — no restart.

### 4. Connect an executor

An executor is **your** process, in any language, implementing the four `JobExecutor` RPCs from
`proto/gojob/v1/executor.proto`. `doc/executor-guide.md` is written to be handed to whoever —
or whatever — writes it: the rules that are enforced, what breaks when each is ignored, and the
order to migrate an existing scheduled job in.

The short version: mint a fresh executor id every start, run a handler at most once per
`run_token`, refuse with `refused=true` rather than a status code, answer `GetExecution` about
the attempt you were asked about, and report the result until it lands.

### 5. Check it works

Create a job with the handler key your executor declares, press **run** on it, and watch the
executions tab. A dispatched execution that stays `ready` means no executor declares that
handler key — the jobs list flags it `no executor`.

---

## 5. Admin UI

Served on `-admin-addr`, from the binary itself. Nothing to deploy separately, nothing to
build, and no assets fetched from the internet — a strict CSP forbids it, which is what keeps it
working on an isolated network.

- **Jobs** — every job with its schedule, owner deployment, and effective state. When a
  job will not run, the UI names every failed condition rather than showing one misleading
  boolean.
- **Executions** — ready, running, retry-delayed, dead, skipped and cancelled, with owner,
  attempt number, lease and heartbeat age, failure kind and result summary.
- **Executors** — live processes, their build revision, uptime and handler sets.
- **Tenants** — admission state, generation, and the last error for one that will not start.
- **Credentials** — who may register as an executor, for which tenant and group.
- **Actions** — manual trigger, pause and resume, edit schedule and policy, retry a dead
  execution, cancel a running one. Each is audited with actor and reason.

Authentication is built in and minimal by design: local accounts with roles for viewing
and for acting. If you already run SSO, put the UI behind your proxy and disable the
built-in login — the library does not attempt to be an identity provider.

---

## 6. Deployment

**`doc/deployment.md` is the step-by-step runbook.** This section is the reference it points
back to: what every setting means, and where things live.

You deploy **one binary**, `cmd/gojob`, plus a MySQL. It holds no handler code: handlers live
in your executors, which are separate processes reached over gRPC.

An earlier revision of this section described a library you linked into your own binary, with
a `SCHEDULER_ROLE`, a `WORKER_HANDLERS` assignment and a handler registry compiled in. None of
that survived the decision to make executors separate projects in any language — the settings
it named do not exist, and following it would have wasted an afternoon.

### What a deployment needs

| Thing | How |
| --- | --- |
| control database | `mysql gojob_control < schema/mysql/control/001_control.sql`, once per installation |
| one schema per tenant | provision an empty database once through the UI (it applies every embedded tenant migration), or import every `schema/mysql/tenant/*.sql` file in filename order and add its `schema_identity` row manually — see §4.1 |
| a DSN encryption key | 32 bytes of hex; identical on every replica and across restarts, or the stored tenant DSNs become unreadable |
| the first admin account | `gojob -hash-password '…'` prints a bcrypt hash; INSERT it into `admin_user` |
| executor credentials | `gojob -hash-token '…'` prints the SHA-256 for `executor_identity.token_sha256` |

For an existing tenant, go-job validates identity first and then applies missing embedded
additive migrations during admission. An already-current schema performs no DDL, and a schema
newer than the binary is refused rather than downgraded. The explicit UI provisioning action
remains the path for creating an empty schema. You may instead apply the schema files with the
migration tool you already use.

### Configuration

Fourteen of the twenty-six flags also read a `GOJOB_`-prefixed environment variable; the
timing ones are flag-only. Each table below says which.

Required — the process refuses to start without it:

| Flag | Env | Meaning |
| --- | --- | --- |
| `-control-dsn` | `GOJOB_CONTROL_DSN` | the control database |

That is the only one. Everything else has a working default.

| Flag | Env | Meaning |
| --- | --- | --- |
| `-dsn-key` | `GOJOB_DSN_KEY` | **optional.** A hex key (16, 24 or 32 bytes) encrypting tenant DSNs at rest. Without it they are stored as typed, and startup says so. |

It is optional because of what it does and does not protect. It does **not** protect against
someone who can read this process's configuration — the key sits beside the control DSN, so
whoever has one has both. What it protects against is disclosure of the control **database**: a
backup file, a read replica, an engineer with SELECT, where the ciphertext travels and the key
does not. Set it if that is a threat you have; skip it if the key you would then have to never
lose is the bigger risk.

Turning it on later works: a keyed process reads rows written without a key. Turning it off
does not — the encrypted rows stay encrypted, and those tenants report that they need the key
rather than failing obscurely.


Identity and addresses:

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `-location` | `GOJOB_LOCATION` | `UTC` | business time zone — cron expressions are evaluated in it |
| `-instance-id` | `GOJOB_INSTANCE_ID` | host + random | must be unique per replica |
| `-grpc-addr` | `GOJOB_GRPC_ADDR` | `:9090` | executor-facing gRPC |
| `-admin-addr` | `GOJOB_ADMIN_ADDR` | `:8080` | operator API and UI |

Timing — **flag-only, no environment variable** — all with working defaults:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-scan-interval` | 5s | how often ready work is discovered |
| `-recover-interval` | 15s | expired leases, runtime caps, silence, cancels |
| `-reap-interval` | 1m | how often execution retention, executor cleanup and orphan detection run |
| `-registry-poll` | 10s | how often the tenant registry is re-read |
| `-control-staleness` | 30s | how long an instance may act on an unrefreshed registry read before fencing itself |
| `-executor-liveness` | 30s | registration TTL, and the silence budget; the progress interval is a third of it |
| `-executor-retention` | 1h | how long a dead registration is kept for diagnosis |
| `-execution-success-retention` | 360h (15d) | how long successful execution history is kept |
| `-execution-other-retention` | 720h (30d) | how long dead, cancelled and skipped execution history is kept |
| `-session-ttl` | 12h | admin session lifetime |

The tenant drain bound is **not configurable**: it is 15 seconds, in `cmd/gojob/main.go`. If a
deployment needs a different one, that is a flag worth adding rather than a value worth
editing.

Security — a plaintext, uncredentialed gRPC port is refused unless you say so explicitly:

| Flag | Meaning |
| --- | --- |
| `-tls-cert`, `-tls-key` | serve the gRPC port over TLS |
| `-tls-client-ca` | require and verify executor client certificates (mTLS) |
| `-executor-token` | bearer token this scheduler presents when calling executors |
| `-executor-ca` | CA for verifying executor certificates on outbound calls |
| `-cookie-secure` (`GOJOB_COOKIE_SECURE`) | mark the admin session cookie Secure; set it whenever the UI is behind TLS |
| `-allow-unauthenticated-executors` | flag-only. Development only: anything reaching the port can register for any tenant |
| `-allow-unlisted-executors` | flag-only. Accept identities with no `executor_identity` row |

`-tls-cert`, `-tls-key`, `-tls-client-ca`, `-executor-ca` and `-executor-token` each read the
matching `GOJOB_`-prefixed variable.

If an SSO proxy calls the admin API, `-trusted-user-header` and `-trusted-role-header`
(`GOJOB_TRUSTED_USER_HEADER`, `GOJOB_TRUSTED_ROLE_HEADER`) let it supply identity. Requests
without an identity header use built-in login. Only enable trusted headers when every caller
that can reach the listener is trusted not to forge them.

### Not process configuration

Three things are configured in the control database rather than on the command line, because
they change without a restart and outlive the process:

| What | Where |
| --- | --- |
| admin accounts | `admin_user` — provision the first with `gojob -hash-password` |
| executor credentials | `executor_identity` — hash with `gojob -hash-token` |
| tenants | `tenant_registry`, through `POST /api/tenants`; never edit the DSN column by hand, it is encrypted |

Jobs themselves — schedule, timeout, attempts, params — are rows created through the admin API.
See `doc/executor-guide.md` §3.

### Adding a tenant

Sign in, then `POST /api/tenants` with the coordination DSN and the `schema_uuid` that schema
presents. Admission verifies identity, version and the clock contract before the tenant is
scheduled at all, and a DSN is never returned in plaintext afterwards.

### Endpoints

- `GET /healthz` — liveness
- `GET /readyz` — readiness
- `GET /` — admin UI
- `/api/...` — the same operations the UI performs, documented in `doc/admin.md`

There is **no `/metrics` endpoint yet**. An earlier version of this section promised Prometheus
exposition; nothing implements it.

### Scaling

Run several replicas against the same control database, each with its own `-instance-id`.
Leases and fencing are what make that safe, and `doc/protocol.md` states the argument.

### Shutdown

Graceful shutdown stops claiming and drops readiness first, then lets in-flight work finish. A
lease whose handler has not proved it stopped is allowed to expire rather than be released
early — releasing early is how two executors end up overlapping.

---

## 7. Working on this repository

### What you need

| | |
| --- | --- |
| Go | 1.26 (see `go.mod`) |
| MySQL 8.x | for the tests — see below |
| protoc + plugins | **only** to change the gRPC contract; `make proto` installs the plugins |

Nothing else. The generated protobuf code is committed, so building and testing needs no
protoc.

### Running the tests, and the trap in not doing it properly

```sh
docker run -d --name gojob-mysql -p 33307:3306 \
  -e MYSQL_ROOT_PASSWORD=gojobtest mysql:8.4

GOJOB_TEST_DSN="root:gojobtest@tcp(127.0.0.1:33307)/" make check
```

`make check` is `gofmt -l .`, `go vet ./...` and `go test -race ./...`. The race detector is
part of it rather than something to reach for after a mystery: this is several loops per
tenant, several tenants per process, and a gRPC server on top.

**Without `GOJOB_TEST_DSN`, `go test ./...` prints `ok` for every package while silently
skipping 29 tests — the entire end-to-end suite among them.** A test that needs a database
skips rather than fails, because failing would make a clean checkout look broken; the cost is
that a green run means nothing until you have set it. Check for `SKIP` before believing a pass:

```sh
go test ./... -v 2>&1 | grep -c -- "--- SKIP"     # want 0
```

The DSN needs no database name. Each test creates and drops its own schema.

### Where things are

| Path | Contents |
| --- | --- |
| `clock.go`, `job.go`, `errors.go`, `defaults.go` | the vocabulary: Clock, Definition, Status, the sentinel errors, the defaults and why each is what it is |
| `cmd/gojob` | the binary — flags, wiring, TLS, the `-hash-*` helpers |
| `internal/store` | every statement that touches a tenant schema, and the static tests that police them |
| `internal/engine` | the scheduler loops: materialize, dispatch, heartbeat, recover, timeout, silence, cancel |
| `internal/dispatch` | the outbound gRPC client, and the accepted/refused/unknown classification |
| `internal/server` | the inbound gRPC service executors call |
| `internal/control` | the control database: tenant registry, admission, the operate fence |
| `internal/runtime` | tenant lifecycle: admission, retirement, draining, DSN cutover |
| `internal/testexec` | a conforming reference executor, ~450 lines |
| `internal/e2e` | the database-backed tests |
| `proto/`, `gen/` | the contract, and its committed generated code |
| `schema/` | versioned SQL embedded for tenant admission and exported for manual application |

### Two conventions that are enforced by tests

Both live in `internal/store/store_test.go`, and both exist because the failures they prevent
are invisible in ordinary testing.

- **Every SQL statement must be a plain, readable literal.** Not concatenated, not built, not
  prepared, not reached through a method value or an aliased transaction handle. The fence,
  clock, `write_seq` and lock-order audits read statements out of the source, and anything that
  hides the text from them hides it from all of them at once.
- **A transaction handle is called `tx`.** The lock-order walker recognises a transactional
  statement by its receiver's spelling, because deciding it properly needs type information the
  parser does not carry.

`doc/verification.md` lists what an implementation has to satisfy to be trusted; the tests are
written against it.

### Changing the contract

`make proto` regenerates from `proto/gojob/v1/executor.proto`. The generated code is committed
and reviewed like any other diff — a contract change nobody reads is how an executor discovers
at runtime that a field moved. `require_unimplemented_servers=false` is deliberate: a new RPC
becomes a **compile error** in every Go executor rather than an `Unimplemented` at runtime.

---

## 8. Documentation

| Document | Contents |
| --- | --- |
| `doc/protocol.md` | lock order, claim, lease, heartbeat, fencing, recovery, retry, state machine |
| `doc/scheduling.md` | cron materialization, misfire, fixed-delay loops, configuration drift |
| `doc/data-model.md` | the tables, their indexes, and why configuration and hot state are separate |
| `doc/admin.md` | the admin UI and API surface |
| `doc/verification.md` | the tests an implementation must pass to be trusted |
| `doc/deployment.md` | the deployment runbook, in order, with the production steps |
| `doc/executor-guide.md` | how to write an executor and register its jobs — written to be handed to an agent |

---

## 9. License

Not yet chosen. Do not distribute externally until it is.
