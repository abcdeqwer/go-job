# Programme overview

A single document describing what is being built, why, in which repository, and what is
already decided. It exists so a reviewer can judge the whole shape without reading six
specifications, and so decisions that were argued through do not have to be re-argued.

---

## 1. The programme

The parent system is a Java monolith-ish estate (Spring Boot services plus a Vue admin UI)
moving to Go. Scheduled work is the first thing to move, in three steps:

1. remove **every** scheduled job from the admin API service, leaving it read-mostly;
2. remove **every** scheduled job from the gateway service, leaving it a pure callback edge;
3. replace the consumer-facing API service with Go — a separate project sharing nothing with
   scheduling.

The work is split into two independent parts, in two repositories:

| Part | Repository | Contents |
| --- | --- | --- |
| **1 — infrastructure** | `go-job` (this one) | a standalone, general-purpose job scheduling platform. No knowledge of the parent system exists here. |
| **2 — migration** | the parent repository | how the parent system's 55 jobs move onto that platform: verification, cutover, phasing. Not discussed in this repository. |

This document, and everything else in `go-job`, is **part 1 only**.

---

## 2. What go-job is

A multi-tenant, database-durable **job scheduling platform**.

- it decides what runs, when, and for which tenant, with exactly one owner at a time;
- applications register as **executors** over a gRPC contract, in any language, and receive
  dispatches with parameters;
- MySQL is the only infrastructure dependency;
- the admin UI ships with the scheduler.

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

### Design goals

1. **Durable before prompt.** The database is the authority for what is due and who owns it.
   Timers only reduce latency; losing one costs latency, never a run.
2. **Exactly one owner, provably** — one lock order, leases with heartbeats, a fence epoch on
   every ownership-bearing write, and exactly one code path that reclaims an expired holder.
3. **Multi-tenant by physical isolation** — one coordination schema per tenant, no
   `tenant_id` column, no default tenant, no cross-tenant fallback.
4. **Executors are dumb on purpose** — they accept, run and report. Everything subtle stays
   in the scheduler so a new executor in a new language cannot get it wrong.
5. **Operable by people who did not write it** — schedule, enablement, concurrency, retry
   budget and timeout are editable, audited data; a silently stopped job is detectable.
6. **Honest about guarantees.** The delivery guarantee is **at-least-once with exactly one
   owner at a time**, not exactly-once. When an attempt's fate cannot be established — an
   executor restarted and answers `NOT_FOUND` — the scheduler fences and retries, which can
   run the logical work twice. Handler idempotency is what makes that harmless, and it is
   required rather than recommended. Ownership is not idempotency, and a fenced cancellation
   is not proof that an in-flight request was withdrawn.
7. **One dependency.** MySQL. No Redis, broker, ZooKeeper or etcd.

### Explicit non-goals

Refusals, not gaps: job DAGs or sub-tasks; sharded broadcast; script/shell jobs and
dynamically registered handlers; a routing-strategy family; exactly-once external effects;
DDL at runtime.

---

## 3. Architecture

### Two layers of ownership

| Layer | Question | Mechanism |
| --- | --- | --- |
| 1 — internal | which **scheduler instance** owns materializing, dispatching and tracking this execution? | MySQL row lock, guarded CAS, lease, fence epoch |
| 2 — external | which **executor instance** is running it, and is it alive? | gRPC dispatch, registration heartbeat, per-execution progress |

They fail independently. The interesting case — a scheduler instance dying while its
executor runs on — is handled by reconciling with the executor before deciding, never by
re-dispatching.

### Scheduler clustering

All instances are equal. No leader, no designated node. Materialization uses
`FOR UPDATE SKIP LOCKED` on the job state row; dispatch requires a claim; result callbacks
are instance-agnostic; the executor registry lives in MySQL, not memory. Periodic
cluster-wide singleton work (retention, orphan scans) takes a **named lease**, not an
election.

### Databases

| Database | How many | Holds |
| --- | --- | --- |
| control | exactly one | tenant registry, global admin accounts, control leases, control audit |
| coordination | one per tenant | job definitions, hot state, executions, executor registrations, per-tenant audit |

Adding a site is one audited row in the registry — no restart, no redeploy. DSNs are
encrypted at rest and never returned in plaintext.

---

## 4. Decisions already taken

Each was argued and settled; a reviewer should treat re-opening one as needing new evidence.

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | Separate repository, not a module in the parent | it is a general-purpose platform; the parent must not leak into it |
| 2 | Scheduling platform + separate executors, not an embedded library | executors are separate projects, some not written in Go |
| 3 | gRPC contract, not hand-written HTTP | moves missing methods and wrong types to build time; `UNIMPLEMENTED` vs `NOT_FOUND` makes registration probing unambiguous |
| 4 | Push dispatch (scheduler calls executor) | keeps the ownership protocol in one place; executors implement four RPCs and nothing subtle |
| 5 | Executors register with `in_flight`; the scheduler adopts or fences | more reliable than asking, and free on a message already sent |
| 6 | `GetExecution` `NOT_FOUND` means **unknown**, never "did not run" | durable proof belongs in the handler's idempotency key, not the protocol |
| 6a | Job definitions are created by operators through the API/UI only | the scheduler holds no handler code, so it has no registry to generate them from; executors declare `handler_key`s, operators select one |
| 6b | Claiming commits `dispatching`, and `attempt_no` is consumed only on acceptance | a busy executor refusing work is not an attempt; otherwise saturated capacity marches a job to `dead` with no code having run |
| 6c | A fixed-delay pass is an ordinary execution, deleted when it found nothing | a state-row-only holder cannot be reconciled with its executor after a scheduler dies |
| 6d | Both directions authenticated; identity bound to `(tenant, group)` | an unauthenticated `Register` hands a stranger a tenant's work by ordinary routing |
| 7 | `AUTO_INCREMENT` execution ids, not snowflake | uniqueness is only needed per tenant schema; a distributed generator would add a coordination dependency |
| 8 | Two concurrency policies only: `QUEUE`, `FORBID` | `PARALLEL` has no defined completion protocol against a single-holder state row |
| 9 | Fixed-delay = one dispatched pass at a time, `next_poll_at` from the **result** | delay measured from completion; also removes the manual-trigger starvation an executor-side loop lease would create |
| 10 | Empty passes (`did_work=false`) leave no execution row | a three-second poller would otherwise write 28,800 rows a day per tenant |
| 11 | Claims never steal an expired lease; recovery is the only reclaim path | two reclaim paths can disagree and strand a `running` row forever |
| 12 | `attempt_no` incremented **on dispatch acceptance** only — not at claim, not by recovery | it counts handler starts; a refused or unanswered dispatch started nothing, and incrementing in two places exhausts a budget of three in two real starts |
| 12a | `dispatched_to` is written in the claim transaction, before the `Run` call | recording the intended target only after the reply loses work when the scheduler dies between send and record |
| 12b | Executors deduplicate on `(tenant, execution_key)`, and `ALREADY_EXISTS` names the token held | keys are unique only within a tenant, and "already held" by *my* attempt and by a *fenced older* attempt need opposite responses |
| 12c | The reconciling `GetExecution` happens outside any transaction, with a deadline | an RPC under two row locks can pin a job indefinitely against a wedged executor |
| 12d | Manual and scheduled work each get a bounded share of each candidate pass | a single priority ordering swaps manual starvation for scheduled starvation |
| 12e | Re-pointing a tenant DSN requires disable → **prove the old schema quiescent by scanning it** → change → enable | schedulers poll the registry independently, so a hot re-point splits one tenant across two schemas. Counting acknowledgements cannot prove quiescence — a partitioned instance stops replying while still holding work — so the proof is a direct scan, and a control-database staleness limit that stops a cut-off instance renewing is what makes zero reachable |
| 12h | Attempt history is keyed by `(execution_key, run_token)`, not by attempt ordinal | a token identifies an attempt; `attempt_no` counts budget. An acceptance whose reply was lost is recorded as unknown without consuming budget, so two attempts legitimately share an ordinal |
| 12i | The dispatch carries the **remaining** runtime budget, not the configured cap | the scheduler's clock starts at claim and dispatch is not instant; sending the full cap makes the two sides enforce different deadlines |
| 12j | `disposition` is required; there is no boolean success to fall back to | a boolean cannot express "stopped because asked", and every mapping of it loses either genuine failures or cancelled runs |
| 12k | Every coordination schema carries a `schema_identity` row, and the registry records the `schema_uuid` it expects | otherwise isolation rests on a DSN string being typed correctly, and pointing at another tenant's schema, an empty one, or a restored snapshot is undetectable |
| 12f | An operator retry raises `max_attempts`; it never resets `attempt_no` | attempt numbers are half the primary key of the attempt history |
| 12g | No `business_dsn` in the control database | the scheduler holds no handler code and dispatches no DSN, so it would be a production credential with no consumer |
| 13 | Cancellation is two-step (`cancel_requested` → `cancelled`) | cancelling a context is cooperative; releasing the slot early permits overlap |
| 14 | Tenant list is a control-database registry, hot-added | sites are added over time; a redeploy per site is the wrong cost |
| 15 | Admission is per tenant, not all-or-nothing | with hot add, one bad new tenant must not stop a scheduler serving twenty good ones |
| 16 | Business time and ownership time are separate clocks | availability written in one and compared against the other is either permanently invisible or immediately due |
| 17 | Cron dialect fixed at six fields, seconds first | two dialects means two sets of untested edge cases |
| 18 | Built-in admin UI and minimal two-role auth | it is a platform, not a library needing a host's UI; SSO users put it behind a proxy |

---

## 5. Repository contents

| Path | Contents |
| --- | --- |
| `README.md` | purpose, features, required components, usage, admin UI, deployment |
| `proto/gojob/v1/executor.proto` | **the contract**: `JobExecutor` (4 RPCs, executor-hosted), `JobScheduler` (4 RPCs, scheduler-hosted) |
| `doc/protocol.md` | lock order, claim, lease/heartbeat/fence, completion, recovery, state machine, clustering, failover, registration, observability |
| `doc/dispatch.md` | the gRPC contract explained; build-time enforcement; parameters; liveness; reconciliation; routing; failure handling; conformance suite |
| `doc/scheduling.md` | cron materialization by state-row scan, misfire, fixed-delay passes, config and clock drift |
| `doc/data-model.md` | control database, per-tenant tables, two-clock rule, retention, schema versioning |
| `doc/admin.md` | UI, API surface, auth |
| `doc/verification.md` | 68 tests an implementation must pass, each naming its mechanism |
| `schema/mysql/` | the DDL, exported and versioned: `control/001_control.sql` (one per installation), `tenant/001_tenant.sql` (one per tenant) |
| `internal/cron/` | the six-field engine: parsing, `Next`, `Latest`, `CountBetween` |
| `internal/store/` | every statement that touches a tenant's schema, plus static checks that read this package's own source |
| `internal/dispatch/` | the outbound half of the contract, and the status-code classification |
| `internal/server/` | the inbound half: `Register`, `Heartbeat`, `ReportProgress`, `ReportResult` |
| `internal/outcome/` | one classification of an executor's disposition, shared by the reporting and recovery paths |
| `internal/engine/` | the seven loops, per tenant |
| `internal/control/` | tenant registry, admission, and the operate-lease fence |
| `internal/admin/` | operator API and the embedded UI |
| `internal/runtime/` | registry polling, hot add, per-tenant engine lifecycle |
| `internal/testexec/` | a complete conforming executor — the shortest readable description of what implementing the contract means |
| `internal/e2e/` | ten scenarios against a real MySQL over real gRPC |
| `cmd/gojob/` | the binary |

Status: **implemented.** The scheduler runs, and the end-to-end tests exercise it against a
real database. Still missing: metrics exposition, the differential-replay harness described
in `doc/verification.md`, and executor SDKs for languages other than Go.

---

## 6. Key mechanisms, in brief

**Exclusion** rests on MySQL alone: a row lock on `job_state`, a guarded CAS
(`active_kind IS NULL`), a lease with heartbeats, and a fence epoch on every
ownership-bearing write. The number of scheduler instances and executors is irrelevant to
it — exclusion comes from the single state row.

**Cron** materializes from a scan of `job_state.next_fire_at`, inside one locked
transaction that also advances `next_fire_at`. The timer heap is a latency optimization; a
lost timer costs one scan interval. An earlier design had the timer create the row and
polling executions as the fallback, which cannot work — nothing was written to poll for.

**Fixed-delay** dispatches one pass at a time, holding the state row as `active_kind='POLL'`
while the pass is in flight, and sets `next_poll_at` from the result.

**Liveness for long executions** uses two independent signals: a process-level registration
heartbeat (~10s) and a per-execution progress call (~30s). The deadline bounds *silence*,
not runtime, so a twenty-hour execution that keeps reporting is never reclaimed. Progress is
emitted by the executor's framework on a timer, so liveness never depends on a handler
remembering to check in. `timeout_seconds` is a separate hard cap.

**Parameters** are merged from job defaults and per-trigger overrides, snapshotted onto the
execution at creation so history shows what a run actually used. Data never instructions;
bounded; no secrets.

**Enforcement of the executor contract** is layered: build (missing methods, wrong types) →
conformance suite in the executor's CI (semantics) → registration probe (declared but not
implemented, version mismatch) → runtime detection (accepted but never reported).

---

## 7. Known gaps

Stated rather than hidden:

1. No conformance suite exists yet, though `dispatch.md` §10 specifies what it must cover.
3. No reference executor SDK for any language.
4. Multi-replica per-tenant concurrency quotas need lease-backed slot rows; in-process
   limits are per-process and documented as such.
5. Non-gRPC executors would need an HTTP transcoding gateway, and would lose the build-time
   guarantees; supported but second-class, and not yet specified.
6. No license chosen.
7. The conformance suite must also cover the scheduler side (that it tolerates a
   non-conforming executor), which `verification.md` §10 specifies but nothing yet runs.
