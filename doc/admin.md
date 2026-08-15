# Admin UI and API

The library serves its own operations surface. There is nothing to deploy separately and
nothing to build — the UI is embedded in the binary and mounted on the configured listen
address by the process carrying the `CONTROL_PLANE` role.

Everything the UI does, it does through the same HTTP API documented here. There is no
private path, so anything achievable by clicking is achievable by script.

---

## 1. What an operator needs to answer

The design target is not "expose the tables". It is these five questions, which are what
people actually have at 3am:

1. **Is this job going to run?** — and if not, *why* not.
2. **Did it run, and what happened?**
3. **Is anything running right now, and who owns it?**
4. **Is there an executor alive that can run this at all?**
5. **What changed, and who changed it?**

Everything below exists to answer one of those.

---

## 2. Jobs

The job list shows, per job: schedule, next fire time, last success, last failure, and
**effective runnable state**.

That last column is the one that matters. When a job will not run, the UI names **every**
failed condition rather than showing one boolean:

```text
nightly-rollup          will not run
  ✗ ops_paused = 1              (paused by alice, 2h ago, "waiting on upstream fix")
  ✓ enabled
  ✓ handler declared by 2 live executors
  ✓ schema version matches
```

A single "enabled: false" is how an operator spends twenty minutes re-enabling something
that was never the problem. The conditions, in evaluation order, are those in
`protocol.md` §3 plus `ops_paused`.

### Job detail

- current configuration, with `updated_by` and `version` for its last edit;
- recent executions;
- which live executors declare this handler;
- the audit trail for this job.

---

## 3. Executions

Filterable by job, status and time. Every non-terminal state is visible, not just failures:

| Column | Why it is there |
| --- | --- |
| status | including `ready`, `retry`-delayed, `cancel_requested` and `skipped` |
| owner | the scheduler instance holding it, and the executor it was dispatched to |
| attempt / recovery | the two budgets, separately — see `data-model.md` §4 |
| lease age, heartbeat age | a running job whose heartbeat is aging is the shape of a hung handler |
| failure kind | stable enum, so failures group instead of being unique strings |
| summary | the handler's own one-line result |

`ready` executions with a future `available_at` show that time explicitly. "Nothing is
running and nothing is wrong" and "a retry is scheduled in six minutes" look identical
otherwise.

---

## 4. Executors

Live executors: `executor_id`, group, address, build revision, contract version, uptime,
heartbeat age, capacity and current load, and the handler set each one declares.

Two derived views matter more than the list:

- **orphaned jobs** — enabled, but no live executor declares the handler. Nothing will ever
  claim them. This is the single most valuable screen in the UI, because it catches the
  failure mode that produces no errors at all;
- **stuck executions** — expired `running` rows whose handler has no live executor. Nothing
  will recover them either, and that is a different problem from a backlog.

---

## 5. Actions

Each is audited with actor, target and reason. Each requires the `OPERATOR` role.

| Action | Effect | Guard |
| --- | --- | --- |
| **Create** | a new job: handler, group, schedule, parameters, policy | the UI *offers* the handlers live executors declare; an unknown one is a **warning**, not a rejection |
| **Trigger** | a manual execution, with optional parameter overrides | competes for the same job lock as a scheduled run, so it cannot overlap one — and is selected ahead of it, so it cannot starve |
| **Pause / resume** | sets `ops_paused` | takes the state-row lock, so it cannot race a claim into one extra run |
| **Edit** | schedule, concurrency, retry budget, timeouts | optimistic `version` CAS; rejected if the row changed underneath |
| **Retry** | `dead` → `ready`, attempt budget reset | audited with a reason; never automatic |
| **Cancel** | `running` → `cancel_requested` | see below |
| **Retire** | ends a job permanently | **cancels its outstanding executions**, audited; the row and history are kept |

### Cancel is presented honestly

A cancel request shows as `cancel_requested` until the handler confirms it stopped. The UI
does not claim the job is cancelled while it may still be writing.

When an execution reaches a terminal state, the UI reads `terminal_reason` and says which of
these happened:

- *cancelled (handler confirmed stopped)* — the handler returned after the signal;
- *cancelled (fenced; side effects unverified)* — the lease expired and ownership was fenced.
  The scheduler will accept no further results, but nothing here proves an in-flight request
  was withdrawn or a partial write rolled back;
- *cancelled (job retired)* — resolved because its job was retired;
- **and a cancel that arrived too late shows as `success`, not `cancelled`** — with the cancel
  request visible in the audit trail. The work finished; recording it as cancelled would tell
  an operator the opposite of the truth about a job that may have moved money.

Collapsing both into "cancelled" invites an operator to assume nothing happened. For a job
with external effects that is the most expensive available wrong assumption, so the UI
refuses to make it.

---

## 6. API

The UI is a client of this API; the API is the contract.

**Every job, execution and executor route is under an explicit tenant prefix.** A job name is
unique only within a tenant, so a path without one is ambiguous and two implementations would
resolve it differently — or worse, consistently but not as the operator expected.

```text
GET    /api/tenants                                    registry, admission state, last_error
POST   /api/tenants                                    add a site; body includes DSNs
PATCH  /api/tenants/{tenant}                           enable/disable, re-point DSN
GET    /api/tenants/{tenant}/handlers                  handler_keys live executors declare

GET    /api/tenants/{tenant}/jobs                      list with effective state
POST   /api/tenants/{tenant}/jobs                      create: handler_key, schedule, params
GET    /api/tenants/{tenant}/jobs/{name}               detail, configuration, conditions
PATCH  /api/tenants/{tenant}/jobs/{name}               edit; requires If-Match: <version>
POST   /api/tenants/{tenant}/jobs/{name}/pause         body: {reason}
POST   /api/tenants/{tenant}/jobs/{name}/resume        body: {reason}
POST   /api/tenants/{tenant}/jobs/{name}/trigger       body: {reason, params?}
POST   /api/tenants/{tenant}/jobs/{name}/retire        body: {reason}

GET    /api/tenants/{tenant}/executions                filter: job, status, from, to
GET    /api/tenants/{tenant}/executions/{key}          detail plus attempt history
POST   /api/tenants/{tenant}/executions/{key}/retry    dead -> ready; body: {reason}
POST   /api/tenants/{tenant}/executions/{key}/cancel   body: {reason}

GET    /api/tenants/{tenant}/executors                 live executors and handler sets
GET    /api/tenants/{tenant}/orphans                   enabled jobs no live executor serves
GET    /api/tenants/{tenant}/audit                     filter: job, actor, from, to

GET    /healthz                                        liveness
GET    /readyz                                         readiness
GET    /metrics                                        Prometheus exposition
```

`POST /jobs` is how jobs come into existence — there is no other way. The scheduler holds no
handler code, so creating a job means naming a `handler_key`, plus a schedule, parameters and
policy.

`GET /handlers` lists what live executors currently declare, and the UI offers it as a
picker. **It is a convenience, not a constraint**: a handler whose executor happens to be
down or not yet deployed must still be nameable, or a job could never be created before its
executor ships. An unrecognised handler is accepted with a warning and the job shows as an
orphan until an executor declares it.

A job may also name a required executor **group**, which is what distinguishes two groups
declaring the same handler — a partial rollout, or two configurations of one service. A job
naming no group accepts any group declaring its handler.

Attempt history at `GET /executions/{key}` is served from `job_execution_attempt`.

Conventions:

- every mutating call requires a `reason`, and rejects an empty one. A reason recorded at
  the moment of action is worth more than a reconstruction attempted later;
- edits use `If-Match` against `job_definition.version`; a stale version is a `409`, never
  a silent overwrite;
- **actor identity is never defaulted.** An action that cannot be attributed to someone is
  rejected, not attributed to a placeholder;
- responses distinguish "refused" from "failed" — a paused job rejecting a trigger is not
  an error condition, and the API says which it is.

### Multi-tenancy in the API

Every path is scoped to one tenant, selected by an explicit parameter. There is no
"all tenants" endpoint that fans out, because a cross-tenant read is the one operation this
design has otherwise made impossible, and reintroducing it in the API would undo the
guarantee.

---

## 7. Authentication

Built in and minimal by design: local accounts with two roles.

| Role | May |
| --- | --- |
| `VIEWER` | read everything |
| `OPERATOR` | read, plus every action in section 5 |

Two roles, deliberately. Finer-grained authorization is a property of the organization
running the scheduler, not of the scheduler, and every gradation added here is one that
does not match what some host already has.

Hosts that already run SSO put the UI behind their proxy, disable built-in login, and pass
an identity header the library trusts. **The library does not attempt to be an identity
provider** and will not grow OIDC, LDAP or SAML support.

The UI is not intended to be exposed to the public internet. It performs privileged
operations on production data and should sit on an internal network or behind the host's
existing access controls.
