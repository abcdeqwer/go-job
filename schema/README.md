# Schema

go-job **never executes DDL**. These files are exported for you to apply with whatever
migration tool you already run — Flyway, golang-migrate, plain scripts, anything.

```text
control/001_control.sql   one per installation
tenant/001_tenant.sql     one per tenant
```

## Applying

**Control database**, once:

```sh
mysql gojob_control < control/001_control.sql
```

**Each tenant**, before adding it to the registry:

```sh
mysql np_scheduler < tenant/001_tenant.sql

# then give the schema its identity — the registry will refuse a DSN whose schema
# does not present exactly this
INSERT INTO np_scheduler.schema_identity (tenant, schema_uuid, schema_version, created_at)
VALUES ('np', UUID(), '1', NOW());
```

Record that `schema_uuid` in the tenant's `tenant_registry` row. Admission checks identity
before version, which is what stops a mistyped DSN from adopting another tenant's schema, an
empty one, or a restored snapshot.

## Version compatibility

`schema.Version` in the library states the version it requires, and admission **fails closed**
on a mismatch — no silent degradation, no partial feature set, no writing to a column that
may not exist.

The consequence is a real contract: an upgrade needing new columns is a migration you apply
first, and the release notes will say so. That is the price of not running DDL at runtime,
and it is the right price for a component holding a lock in someone else's production
database.

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
