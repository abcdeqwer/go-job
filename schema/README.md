# Schema

go-job embeds the tenant files and applies missing additive versions during tenant admission,
after verifying the tenant name and schema UUID. The files remain exported so you can inspect
or apply them with Flyway, golang-migrate, plain scripts, or your own tooling.

```text
control/001_control.sql   one per installation
tenant/*.sql              one ordered migration stream per tenant
```

## Applying

**Control database**, once:

```sh
mysql gojob_control < control/001_control.sql
```

**Each tenant**, before adding it to the registry, apply every migration in filename order:

```sh
mysql np_scheduler < tenant/001_tenant.sql
mysql np_scheduler < tenant/002_execution_retention.sql
mysql np_scheduler < tenant/003_handler_descriptions.sql

# then give the schema its identity — the registry will refuse a DSN whose schema
# does not present exactly this
INSERT INTO np_scheduler.schema_identity
    (lock_row, tenant, schema_uuid, schema_version, created_at)
VALUES (1, 'np', UUID(), '3', NOW());
```

For an existing version 1 or 2 tenant, starting this binary applies every missing ordered
migration through `003_handler_descriptions.sql`, verifies version 3, and then admits the tenant.
If additive DDL committed but the process stopped before the version row advanced, the next
admission verifies the exact index/column definition and resumes. Applying the files manually
remains supported.

Record that `schema_uuid` in the tenant's `tenant_registry` row. Admission checks identity
before version, which is what stops a mistyped DSN from adopting another tenant's schema, an
empty one, or a restored snapshot.

## Version compatibility

`schema.Version` in the library states the version it requires. Admission upgrades an older
recognized version through the ordered embedded stream, then **fails closed** unless the exact
required version is present — no silent degradation, no partial feature set, no downgrade.

Tenant migrations must be additive and restart-safe because MySQL DDL is not transactional.
The runtime database user needs the DDL privileges required by each new migration.

## Requirements

- **MySQL 8.0 or later.** `SELECT ... FOR UPDATE SKIP LOCKED` is load-bearing, not an
  optimization.
- **The session time zone of every connection to a tenant schema must equal the scheduler's
  configured business `Location`.** Admission asserts it and refuses on mismatch, rather than
  letting it surface as an eight-hour scheduling error at 2am.
- `utf8mb4` throughout.

## Naming

Table names are shown unprefixed. If your installation needs a prefix, apply it consistently
across every table in a schema — the library takes one prefix for all of them, not per table.
