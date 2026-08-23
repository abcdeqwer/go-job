package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/abcdeqwer/go-job/internal/admin"
	"github.com/abcdeqwer/go-job/internal/control"
	"github.com/abcdeqwer/go-job/internal/tenantmigration"
)

func TestExistingTenantSchemaMigratesFromVersionOne(t *testing.T) {
	db, migrations := setupVersionOneSchema(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := tenantmigration.Upgrade(ctx, db, migrations, "1", control.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	assertTenantSchemaVersionAndRetentionIndex(t, db)

	// A later replica in the rolling restart reads version 2 and performs no DDL.
	if err := tenantmigration.Upgrade(ctx, db, migrations,
		control.SchemaVersion, control.SchemaVersion); err != nil {
		t.Fatalf("repeat upgrade: %v", err)
	}
}

func TestExistingTenantSchemaMigrationResumesAfterIndexDDLCommitted(t *testing.T) {
	db, migrations := setupVersionOneSchema(t)
	if _, err := db.Exec(`ALTER TABLE job_execution
		ADD KEY idx_job_execution_retention (status, finished_at, id)`); err != nil {
		t.Fatalf("simulate committed index DDL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := tenantmigration.Upgrade(ctx, db, migrations, "1", control.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	assertTenantSchemaVersionAndRetentionIndex(t, db)
}

func setupVersionOneSchema(t *testing.T) (*sql.DB, []tenantmigration.Migration) {
	t.Helper()
	base := dsn(t)

	serverDB, err := sql.Open("mysql", base+"?parseTime=true&loc=UTC&multiStatements=true")
	if err != nil {
		t.Fatalf("open mysql server: %v", err)
	}
	defer serverDB.Close()

	schema := fmt.Sprintf("gojob_migration_t%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := serverDB.Exec("CREATE DATABASE " + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupDB, err := sql.Open("mysql", base)
		if err == nil {
			_, _ = cleanupDB.Exec("DROP DATABASE IF EXISTS " + schema)
			_ = cleanupDB.Close()
		}
	})

	db, err := sql.Open("mysql", base+schema+"?parseTime=true&loc=UTC&multiStatements=true")
	if err != nil {
		t.Fatalf("open tenant schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	migrations, err := admin.TenantMigrations()
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	if migrations[0].Version != tenantmigration.InitialVersion {
		t.Fatalf("first embedded migration is version %d, want %d",
			migrations[0].Version, tenantmigration.InitialVersion)
	}
	for _, statement := range tenantmigration.SplitDDL(migrations[0].DDL) {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("apply initial schema at %q: %v", firstLine(statement), err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO schema_identity (lock_row, tenant, schema_uuid, schema_version, created_at)
		VALUES (1, 'cp', UUID(), '1', NOW())`); err != nil {
		t.Fatalf("mint version-one identity: %v", err)
	}
	return db, migrations
}

func assertTenantSchemaVersionAndRetentionIndex(t *testing.T, db *sql.DB) {
	t.Helper()
	var version string
	if err := db.QueryRow(`SELECT schema_version FROM schema_identity WHERE lock_row = 1`).
		Scan(&version); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != control.SchemaVersion {
		t.Fatalf("schema version = %q, want %q", version, control.SchemaVersion)
	}

	var columns sql.NullString
	if err := db.QueryRow(`
		SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',')
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		  AND table_name = 'job_execution'
		  AND index_name = 'idx_job_execution_retention'`).Scan(&columns); err != nil {
		t.Fatalf("read retention index: %v", err)
	}
	if !columns.Valid || columns.String != "status,finished_at,id" {
		t.Fatalf("retention index columns = %q, want %q",
			columns.String, "status,finished_at,id")
	}
}
