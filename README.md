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
- DDL at runtime.

---

## 3. What you need to run it

| Component | Requirement | Why |
| --- | --- | --- |
| **MySQL** | 8.0 or later, one coordination schema per tenant | durability and coordination authority; `SELECT ... FOR UPDATE SKIP LOCKED` is required |
| **Go** | 1.26 or later — for the scheduler only | executors may be written in any language with gRPC support |
| A migration tool | any — Flyway, golang-migrate, plain scripts, your own | go-job exports versioned DDL and never executes it |

That is the complete list. There is no broker, no coordination service and no separate
scheduler daemon.

Optional:

- a reverse proxy in front of the admin UI if you want your own SSO instead of the
  built-in authentication.

Not yet available: there is no metrics endpoint. Operational visibility today is the admin UI,
the execution history it reads, and structured logs.

---

## 4. How to use it

Three things happen once, in this order: run the scheduler, add a tenant, connect an
executor.

### Run the scheduler

```sh
go-job \
  --control-dsn  "user:pass@tcp(mysql:3306)/gojob_control" \
  --location     "Asia/Manila" \
  --listen       ":8090"          # admin UI, gRPC, health, metrics
```

Everything else — which tenants exist, which jobs they run, on what schedule — is data.
The binary takes no job list and no tenant list.

### Add a tenant

One audited row in the control database, through the UI or the API:

```text
tenant   coordination_dsn
np       …/np_scheduler
np2      …/np2
```

Schedulers pick it up within one poll — **no restart**. Adding another site later is the same
one row. DSNs are encrypted at rest and never read back in plaintext.

Only the scheduler's own coordination schema is named here. Executors reach their business
databases with their own configuration; the scheduler never holds a business credential.

### Connect an executor

An executor implements the four `JobExecutor` RPCs from
`proto/gojob/v1/executor.proto`, registers, and starts receiving work. In Go, with the
generated stubs and **without** embedding the `Unimplemented` struct, a missing method is a
compile error:

```go
func (e *MyExecutor) Run(ctx context.Context, r *gojobv1.RunRequest) (*gojobv1.RunResponse, error) {
    if !e.claim(r.ExecutionKey) {
        return nil, status.Error(codes.AlreadyExists, "already running")
    }
    go e.run(r)                       // returns immediately; reports later
    return &gojobv1.RunResponse{ExecutionKey: r.ExecutionKey}, nil
}
```

The handler reads its parameters from `RunRequest.Params` and its business date from
`RunRequest.ScheduledAt` — business time, not the executor's wall clock:

```go
func (e *MyExecutor) run(r *gojobv1.RunRequest) {
    day := parseDay(r.ScheduledAt).AddDate(0, 0, -1)
    size := int(r.Params.Values.Fields["batch_size"].GetNumberValue())

    n, err := rebuild(day, size)
    e.report(r, ok(err), fmt.Sprintf("rows=%d", n), n > 0)   // last arg: did_work
}
```

While it runs, the executor's framework calls `ReportProgress` on a timer — the handler
does not have to remember to. A `proceed=false` response means ownership was lost or the
run was cancelled, and the handler must stop without further writes.

Executors in other languages implement the same four methods from the same `.proto`.
`doc/dispatch.md` §9 is the complete list of what any of them must guarantee — six items.

### Apply the schema

`schema/mysql` holds the required DDL, embedded and versioned. Copy or wrap those files
into whatever migration sequence you already use, and apply them to every schema the
scheduler will use before first start. `schema.Version` declares the schema version the
library requires; a mismatch is a startup error, never a silent degradation.

---

## 5. Admin UI

The library serves its own operations surface on `Listen`. There is nothing to deploy
separately and nothing to build.

- **Jobs** — every job with its schedule, owner deployment, and effective state. When a
  job will not run, the UI names every failed condition rather than showing one misleading
  boolean.
- **Executions** — ready, running, retry-delayed, dead, skipped and cancelled, with owner,
  attempt number, lease and heartbeat age, failure kind and result summary.
- **Workers** — live processes, their build revision, uptime and handler sets.
- **Actions** — manual trigger, pause and resume, edit schedule and policy, retry a dead
  execution, cancel a running one. Each is audited with actor and reason.

Authentication is built in and minimal by design: local accounts with roles for viewing
and for acting. If you already run SSO, put the UI behind your proxy and disable the
built-in login — the library does not attempt to be an identity provider.

---

## 6. Deployment

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
| one schema per tenant | `mysql np_scheduler < schema/mysql/tenant/001_tenant.sql`, plus its `schema_identity` row — see `schema/README.md` |
| a DSN encryption key | 32 bytes of hex; identical on every replica and across restarts, or the stored tenant DSNs become unreadable |
| the first admin account | `gojob -hash-password '…'` prints a bcrypt hash; INSERT it into `admin_user` |
| executor credentials | `gojob -hash-token '…'` prints the SHA-256 for `executor_identity.token_sha256` |

go-job never runs DDL. Apply the schemas with whatever migration tool you already use.

### Configuration

Every flag has a `GOJOB_`-prefixed environment variable. The ones a deployment must set:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-control-dsn` | — | the control database; **required** |
| `-dsn-key` | — | hex key encrypting tenant DSNs at rest; **required** |
| `-location` | `UTC` | business time zone — cron expressions are evaluated in it |
| `-grpc-addr` | `:9090` | executor-facing gRPC |
| `-admin-addr` | `:8080` | operator API and UI |
| `-instance-id` | derived | must be unique per replica |

Timing, all with working defaults:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-scan-interval` | 5s | how often ready work is discovered |
| `-recover-interval` | 15s | expired leases, runtime caps, silence, cancels |
| `-registry-poll` | 10s | how often the tenant registry is re-read |
| `-control-staleness` | 30s | how long an instance may act on an unrefreshed registry read before fencing itself |
| `-executor-liveness` | 30s | registration TTL, and the silence budget; the progress interval is a third of it |
| `-drain-timeout` | 15s | how long retiring a tenant waits for in-flight work |

Security — a plaintext, uncredentialed gRPC port is refused unless you say so explicitly:

| Flag | Meaning |
| --- | --- |
| `-tls-cert`, `-tls-key` | serve the gRPC port over TLS |
| `-tls-client-ca` | require and verify executor client certificates (mTLS) |
| `-executor-token` | bearer token this scheduler presents when calling executors |
| `-executor-ca` | CA for verifying executor certificates on outbound calls |
| `-allow-unauthenticated-executors` | development only; anything reaching the port can register for any tenant |
| `-allow-unlisted-executors` | accept identities with no `executor_identity` row |

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

## 7. Documentation

| Document | Contents |
| --- | --- |
| `doc/protocol.md` | lock order, claim, lease, heartbeat, fencing, recovery, retry, state machine |
| `doc/scheduling.md` | cron materialization, misfire, fixed-delay loops, configuration drift |
| `doc/data-model.md` | the tables, their indexes, and why configuration and hot state are separate |
| `doc/admin.md` | the admin UI and API surface |
| `doc/verification.md` | the tests an implementation must pass to be trusted |
| `doc/executor-guide.md` | how to write an executor and register its jobs — written to be handed to an agent |

---

## 8. License

Not yet chosen. Do not distribute externally until it is.
