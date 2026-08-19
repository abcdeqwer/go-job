# Deploying go-job

An ordered runbook, from nothing to a scheduler running a job. `README.md` §6 is the reference
for what every flag means; this is the sequence, and the things that only bite in production.

Every command here was executed against a real deployment. Where something is untested, it says
so.

---

## What you are deploying

One binary and one MySQL. That is the whole installation.

```
                     ┌──────────────┐
   operator ─────────│  :8080  UI   │
                     │              │──── control DB   (tenants, admins, audit)
   executors ────────│  :9090 gRPC  │──── tenant DB    (jobs, executions)  ×N
                     └──────────────┘     tenant DB    …
```

Executors are **your** processes, in any language, reached over gRPC. The scheduler holds no
handler code — see `doc/executor-guide.md`.

---

## 1. Databases

The **control** database you apply yourself, once — the process cannot serve a page without it,
so there is nowhere for a UI to do it from.

**Tenant schemas you can create from the UI.** Add-tenant asks for host, database, user and
password, tests the connection, and — if the database is empty — offers to create the tables
and mint the identity row. That is the only automatic schema management this has: one operator,
one button, on a database they just named. Nothing runs DDL at startup, because MySQL DDL does
not roll back and several replicas starting together would race to apply it.

The SQL below is the same thing if you would rather run it yourself.

```sh
# once per installation
mysql gojob_control < schema/mysql/control/001_control.sql

# once per tenant
mysql np_scheduler < schema/mysql/tenant/001_tenant.sql
mysql -e "INSERT INTO np_scheduler.schema_identity
          (tenant, schema_uuid, schema_version, created_at)
          VALUES ('np', UUID(), '1', NOW())"
mysql -N -e "SELECT schema_uuid FROM np_scheduler.schema_identity"   # keep this
```

That `schema_uuid` is what stops a mistyped DSN from adopting another tenant's schema, an empty
one, or a restored snapshot. You will be asked for it when registering the tenant, and admission
refuses a DSN whose schema does not present exactly it.

**The MySQL user needs no DDL rights at runtime** — only DML on its own schema. Grant DDL for
the migration, not for the process.

---

## 2. The DSN encryption key — optional

```sh
openssl rand -hex 32        # only if you want it
```

Tenant DSNs contain database passwords. With `-dsn-key` they are encrypted at rest; without it
they are stored as typed and startup warns.

Worth understanding before choosing. The key does **not** protect against someone who can read
this process's configuration: it sits beside the control DSN, so whoever has one has both. What
it protects against is disclosure of the control **database** — a backup file, a read replica,
an engineer with SELECT — where the ciphertext travels and the key does not.

Against that it costs a secret you can never lose: **identical on every replica and across every
restart**, because a key that changes makes every registered tenant unreadable.

Turning it on later works — a keyed process reads rows written without one. Turning it off does
not: the encrypted rows stay encrypted, and those tenants report that they need the key.

---

## 3. The first admin account

**Open the UI and create it there.** An installation with no administrator shows a setup form
instead of a login: name, password, done, and you are signed in. Nothing to run, nothing to
INSERT.

The endpoint behind it refuses the moment an administrator exists, and the check is part of the
insert rather than a query before it — so it is not an account-creation endpoint that happens
to be guarded, and a dozen simultaneous callers still produce exactly one account.

Passwords must be **at least 12 characters**.

If you would rather provision it from a script, `-hash-password` still prints a bcrypt hash to
put in `admin_user` yourself. Note that it prints its refusal to stderr, so an unquoted `$(…)`
around a short password yields an empty hash, an account nobody can sign in to, and a 401 that
looks like a problem somewhere else.

Roles are `OPERATOR` (can change things) and `VIEWER` (can look). Disabling or demoting an
account takes effect on the **next request**, not at session expiry — `UPDATE admin_user SET
disabled = 1` is an immediate revocation.

---

## 4. Executor credentials

Skip this only if executors reach the gRPC port over a network you fully control **and** you
accept that anything reaching that port can register for any tenant.

**Do this in the UI** — the 凭证 tab issues and revokes credentials, and for a token it
generates the value, shows it once, and stores only its SHA-256. The SQL below is the same
thing if you would rather script it.

Two mechanisms. Prefer the first.

### mTLS (preferred)

Issue each executor a client certificate from a CA you control. The identity is the
certificate's **CommonName** (falling back to the first DNS SAN, then the full subject). Only
verified chains count — an unverified certificate is not an identity.

```sh
mysql -e "INSERT INTO gojob_control.executor_identity
          (identity, tenant, executor_group, created_at)
          VALUES ('report-worker', 'np', '', NOW())"
```

Start the scheduler with `-tls-cert`, `-tls-key` and `-tls-client-ca`.

### Shared token

For deployments that cannot run mTLS. The executor sends `authorization: Bearer <token>`.

```sh
gojob -hash-token 'the-token-you-issued'        # prints SHA-256, exits

mysql -e "INSERT INTO gojob_control.executor_identity
          (identity, tenant, executor_group, token_sha256, created_at)
          VALUES ('report-worker', 'np', '', '<sha256>', NOW())"
```

The token itself is never stored. Lost it? Issue a new one and update the hash.

### About the columns

- `tenant` — an identity is authorised **per tenant**. One row per tenant it may serve.
- `executor_group` — empty means any group. Naming one is what stops a canary in a partial
  rollout registering as `main` and silently taking production traffic.
- `disabled` — flip to 1 to revoke.

An identity with no row is refused. There is deliberately no "empty table means allow
everything" mode: in an mTLS installation that would let any certificate the CA ever signed
register as an arbitrary production tenant. Bootstrapping is one INSERT, and it is visible in
the audit trail.

---

## 5. Running it

```sh
gojob \
  -control-dsn 'gojob:PASSWORD@tcp(mysql:3306)/gojob_control' \
  -dsn-key "$GOJOB_DSN_KEY" \
  -location 'Asia/Manila' \
  -admin-addr :8080 \
  -grpc-addr :9090 \
  -instance-id "$HOSTNAME" \
  -tls-cert /certs/server.crt \
  -tls-key  /certs/server.key \
  -tls-client-ca /certs/executor-ca.crt
```

`-control-dsn`, `-dsn-key`, `-location`, `-instance-id`, the two addresses and the four TLS
settings all read `GOJOB_`-prefixed environment variables instead, if you prefer. **The timing
flags do not** — `-scan-interval` and its neighbours are flag-only, so a compose deployment that
needs to change one must change the `command:`, not the `.env`.

`-location` is the business time zone: cron expressions are evaluated in it. Changing it later
recomputes every cron job's next fire instant, under lock, for every tenant.

### Check the startup log

Three warnings mean you are running something you probably did not intend:

```
the executor gRPC service is PLAINTEXT; job parameters and results cross the network unencrypted
executor calls are accepted WITHOUT a credential; anything that can reach the gRPC port can register for any tenant
authenticated executors are accepted for tenants they are NOT listed for
```

They exist so nobody discovers `-allow-unauthenticated-executors` by reading the source months
later. In production the log should have none of them.

### Health

- `GET /healthz` — the process is alive
- `GET /readyz` — it has admitted its tenants and holds a fresh control-plane lease

Point your load balancer at `/readyz`. It goes false when the instance loses the control
database, which is also when the instance stops claiming — that is the fence working, not a
fault to route around.

There is **no metrics endpoint**. Visibility today is the admin UI, the execution history it
reads, and structured JSON logs.

---

## 6. Registering a tenant

Sign in at `:8080`, then:

```sh
curl -b cookies -X POST http://localhost:8080/api/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"np",
       "dsn":"gojob:PASSWORD@tcp(mysql:3306)/np_scheduler",
       "schema_uuid":"<the uuid from step 1>",
       "reason":"why this tenant exists"}'
```

Admission then verifies, before the tenant is scheduled at all: the schema presents that uuid,
its version matches this build, the driver parses timestamps in the business location, and this
host's UTC clock agrees with the database's within a minute. Any of those failing leaves the
tenant unadmitted with the reason recorded, and does not affect the others.

The DSN is never returned in plaintext afterwards; `GET /api/tenants` shows it masked.

---

## 7. Several replicas

Run as many as you like against the same control database.

- **Same `-dsn-key`.** Different keys means each replica can read a different subset of tenants.
- **Unique `-instance-id`.** It is the owner recorded on every lease. Two replicas sharing one
  id is two schedulers that believe they own each other's work.
- Same `-location`, same flags otherwise.

Leases and fencing are what make this safe; `doc/protocol.md` states the argument. There is no
leader election and nothing to configure for it.

---

## 8. Upgrading

1. Read the release's schema requirement. If it needs a migration, **apply it first** — the
   scheme is fail-closed: a build that requires schema version N refuses to admit a tenant still
   at N-1, rather than writing to a column that may not exist.
2. Roll replicas one at a time. Shutdown stops claiming and drops readiness first, then lets
   in-flight work finish; a lease whose handler has not proved it stopped is left to expire
   rather than released early.

Mixed versions during a roll are fine **only** when the schema is unchanged. When it is not,
apply the migration and accept that older replicas stop admitting until they are replaced.

---

## 9. Moving a tenant to another database

The procedure exists because two schemas serving one tenant would each correctly exclude only
themselves, and dispatch the same job twice.

1. **Disable** the tenant — `PATCH /api/tenants/np` with `{"enabled": false, "reason": "…"}`.
   Every replica stops claiming and drains what it holds.
2. **Watch quiescence** — `GET /api/tenants/np/quiescence`:

   ```json
   {"generation": 2, "schema_quiescent": true,
    "held": 0, "in_flight": 0, "queued_and_would_be_abandoned": 0,
    "blockers": [{"InstanceID": "…", "Generation": 1, "Quiesced": true, "ObservedAt": "…"}]}
   ```

   `held` and `in_flight` must be zero — those drain by themselves. `blockers` lists replicas
   that have not yet acknowledged the CURRENT generation; a replica still showing the previous
   one has not polled the disable yet, so wait a poll interval rather than forcing anything.
   `queued_and_would_be_abandoned` is the count step 4 makes you accept explicitly.
3. **Copy the data** by whatever means you use, and mint a `schema_identity` row in the new
   schema.
4. **Re-point** — `PUT /api/tenants/np/dsn` with the new DSN, its `schema_uuid`, and a reason.
   Refused until the old schema is quiet and every live replica has acknowledged. If queued
   `ready` work would be left behind, the response says how much and you must re-send with
   `"abandon_queued": true` to accept losing it.
5. **Re-enable.**

An instance partitioned from the control database fences **itself** within
`-control-staleness` — that is what makes step 2 reachable rather than a guess.

---

## 10. Backups

- **Control database** — the tenant registry, admin accounts, executor identities and the audit.
  Small, and the thing you cannot reconstruct. Back it up, and back up the DSN key with it.
- **Tenant schemas** — job definitions are configuration and worth backing up. Execution history
  is a log; keep what your retention needs.

Restoring a tenant schema into a different database and pointing a tenant at it is exactly what
`schema_uuid` refuses. That is deliberate: mint a new identity row when you intend the move, and
the refusal stays in place for when you did not.

---

## What this document has not proven

The end-to-end rehearsal behind it covered: schemas applied, first admin provisioned, the
process started, health endpoints answering, a tenant registered and admitted through the API,
and a job created with its next fire instant materialized.

It also covered the tenant lifecycle in §9: disable through the API, and a quiescence response
showing the schema quiet with the acknowledging replica listed — the JSON above is that
response, not an invention.

**Not rehearsed:** TLS and mTLS wiring (the flags exist and are covered by tests, but no
certificate has been through this path end to end), a multi-replica installation, a DSN cutover
onto a second database with real data, and `docker build` — which stalls fetching the
`golang:1.26-alpine` image on the machine this was written on, a network problem rather than a
Dockerfile one.

Treat those five as steps to walk through once in a staging environment before trusting them.
