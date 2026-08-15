# go-job

A multi-tenant, database-durable **job scheduling platform**.

`go-job` decides what runs, when, for which tenant, and exactly once. Your applications
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

> **Status: design.** This repository contains the design, the schema and the gRPC
> contract. No engine code is written yet. `doc/` is the specification implementation
> follows.

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

- a Prometheus scrape of `/metrics`;
- a reverse proxy in front of the admin UI if you want your own SSO instead of the
  built-in authentication.

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
tenant   coordination_dsn                business_dsn
np       …/np_scheduler                  …/np
np2      …/np2                           (null: one schema for both)
```

Schedulers pick it up within one poll — **no restart**. Adding another site later is the
same one row. DSNs are encrypted at rest and never read back in plaintext.

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

What you deploy is **your own binary**. There is no `go-job` process to install.

### Roles

One process can carry either role or both. A single-process installation carries both and
this section is trivial; the separation matters only when a workload is later split.

| Role | Responsibility |
| --- | --- |
| `WORKER` | claims executions and dispatches them to executors |
| `CONTROL_PLANE` | reconciles job configuration, recomputes schedules after a clock change, serves the admin UI and API |

The `CONTROL_PLANE` role is **assigned by configuration, not elected.** A designated
deployment carries the complete handler registry and reconciles every job for every
tenant, including jobs it does not itself run — so a partial deployment can never leave
another deployment's jobs unmaterialized. It may run with an empty execution assignment:
reconciling and serving the UI while claiming no work.

Reconciliation is idempotent — insert what is missing, never overwrite what exists — so a
misconfiguration that starts two control planes costs duplicated effort, not damage.

### Configuration

| Setting | Meaning |
| --- | --- |
| `SCHEDULER_ROLE` | `WORKER`, `CONTROL_PLANE`, or both |
| `WORKER_HANDLERS` | this deployment's execution assignment; defaults to the whole registry |
| `LISTEN_ADDRESS` | admin UI, health, readiness and metrics |
| tenant list | per tenant, a coordination DSN and an optional business DSN — see `doc/data-model.md` §0 |

A handler named in configuration but missing from the binary is a fatal startup error — a
packaging mistake, caught loudly. A handler outside this deployment's assignment is simply
not its work: neither an error nor a reason to refuse to start. Refusing to start because
another deployment is down turns one missing lane into two.

### Startup

Admission is fail-closed and all-or-nothing. A missing table, an unreachable database, an
unresolved credential, an invalid duration or an unsupported schema version prevents
readiness. A failure affecting one tenant is reported for that tenant and never causes
another tenant's runtime to borrow its configuration or connection.

### Endpoints

- `GET /healthz` — liveness
- `GET /readyz` — readiness; false while admission is incomplete or degraded
- `GET /metrics` — Prometheus exposition
- `GET /` — admin UI
- `/api/...` — the same operations the UI performs

### Scaling

Start with one deployment carrying both roles. Split only on evidence of interference —
a heavy job starving a latency-sensitive one — and split by **handler assignment**, giving
each deployment its own resources, health endpoint and rollout. Registration data already
carries the routing fact, so splitting is a deployment change rather than a schema change.

Running several replicas of the same assignment is what leases and fencing exist for, with
two preconditions stated in `doc/protocol.md`: aggregate per-tenant concurrency needs
lease-backed slots rather than in-process semaphores, and the `CONTROL_PLANE` role needs
all-or-none acquisition across tenants.

### Shutdown

Graceful shutdown stops claiming and drops readiness first, then lets in-flight work
finish. A lease whose handler has not proved it stopped is allowed to expire rather than be
released early — releasing early is how two executors end up overlapping.

---

## 7. Documentation

| Document | Contents |
| --- | --- |
| `doc/protocol.md` | lock order, claim, lease, heartbeat, fencing, recovery, retry, state machine |
| `doc/scheduling.md` | cron materialization, misfire, fixed-delay loops, configuration drift |
| `doc/data-model.md` | the tables, their indexes, and why configuration and hot state are separate |
| `doc/admin.md` | the admin UI and API surface |
| `doc/verification.md` | the tests an implementation must pass to be trusted |

---

## 8. License

Not yet chosen. Do not distribute externally until it is.
