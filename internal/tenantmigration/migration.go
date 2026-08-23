// Package tenantmigration upgrades existing tenant coordination schemas before admission.
package tenantmigration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// InitialVersion is the first complete tenant coordination schema.
const InitialVersion = 1

// Migration is one ordered tenant-schema change embedded in the go-job binary.
type Migration struct {
	Version int
	Name    string
	DDL     string
}

// Validate rejects a migration stream with a missing or reordered version.
func Validate(migrations []Migration) error {
	if len(migrations) == 0 {
		return fmt.Errorf("no embedded tenant migrations")
	}
	for i, migration := range migrations {
		want := InitialVersion + i
		if migration.Version != want {
			return fmt.Errorf("tenant migration %q is version %d, want contiguous version %d",
				migration.Name, migration.Version, want)
		}
		if strings.TrimSpace(migration.Name) == "" {
			return fmt.Errorf("tenant migration version %d has no name", migration.Version)
		}
	}
	return nil
}

// Pending selects the migrations after current through required. A newer schema is a
// downgrade attempt and fails closed.
func Pending(migrations []Migration, current, required string) ([]Migration, error) {
	currentVersion, err := parseVersion("current", current)
	if err != nil {
		return nil, err
	}
	requiredVersion, err := parseVersion("required", required)
	if err != nil {
		return nil, err
	}
	if currentVersion < InitialVersion {
		return nil, fmt.Errorf("current tenant schema version %d predates initial version %d",
			currentVersion, InitialVersion)
	}
	if currentVersion > requiredVersion {
		return nil, fmt.Errorf("tenant schema version %d is newer than this binary requires (%d); downgrade refused",
			currentVersion, requiredVersion)
	}
	if currentVersion == requiredVersion {
		return nil, nil
	}
	if err := Validate(migrations); err != nil {
		return nil, err
	}
	if requiredVersion > migrations[len(migrations)-1].Version {
		return nil, fmt.Errorf("binary requires tenant schema version %d but embedded migrations end at %d",
			requiredVersion, migrations[len(migrations)-1].Version)
	}

	pending := make([]Migration, 0, requiredVersion-currentVersion)
	for _, migration := range migrations {
		if migration.Version > currentVersion && migration.Version <= requiredVersion {
			pending = append(pending, migration)
		}
	}
	if len(pending) != requiredVersion-currentVersion {
		return nil, fmt.Errorf("embedded tenant migrations do not cover versions %d through %d",
			currentVersion+1, requiredVersion)
	}
	return pending, nil
}

// Upgrade applies every missing additive migration on one pinned MySQL connection. The caller
// has already verified the tenant and schema UUID; this function changes only schema version.
func Upgrade(ctx context.Context, db *sql.DB, migrations []Migration, current, required string) error {
	pending, err := Pending(migrations, current, required)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open tenant migration connection: %w", err)
	}
	defer conn.Close()

	for _, migration := range pending {
		for _, statement := range SplitDDL(migration.DDL) {
			skip, err := alreadyApplied(ctx, conn, migration.Version, statement)
			if err != nil {
				return fmt.Errorf("inspect tenant migration %s: %w", migration.Name, err)
			}
			if skip {
				continue
			}
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply tenant migration %s at %q: %w",
					migration.Name, firstLine(statement), err)
			}
		}
	}

	var got string
	if err := conn.QueryRowContext(ctx,
		`SELECT schema_version FROM schema_identity WHERE lock_row = 1`).Scan(&got); err != nil {
		return fmt.Errorf("verify tenant schema version after migration: %w", err)
	}
	if got != required {
		return fmt.Errorf("tenant migrations completed but schema version is %q, want %q", got, required)
	}
	return nil
}

// alreadyApplied makes the version-2 index addition restartable if MySQL committed the ALTER
// TABLE but the process stopped before schema_identity advanced.
func alreadyApplied(ctx context.Context, conn *sql.Conn, version int, statement string) (bool, error) {
	if version != 2 || !strings.Contains(statement, "idx_job_execution_retention") {
		return false, nil
	}
	var columns sql.NullString
	err := conn.QueryRowContext(ctx, `
		SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',')
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		  AND table_name = 'job_execution'
		  AND index_name = 'idx_job_execution_retention'`).Scan(&columns)
	if err != nil {
		return false, err
	}
	if !columns.Valid || columns.String == "" {
		return false, nil
	}
	if columns.String != "status,finished_at,id" {
		return false, fmt.Errorf("existing idx_job_execution_retention has columns %q, want %q",
			columns.String, "status,finished_at,id")
	}
	return true, nil
}

// SplitDDL removes line comments before splitting an embedded migration into statements.
func SplitDDL(ddl string) []string {
	var kept []string
	for _, line := range strings.Split(ddl, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		if strings.TrimSpace(line) != "" {
			kept = append(kept, line)
		}
	}
	var statements []string
	for _, raw := range strings.Split(strings.Join(kept, "\n"), ";") {
		if statement := strings.TrimSpace(raw); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

func parseVersion(label, value string) (int, error) {
	version, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s tenant schema version %q is not numeric", label, value)
	}
	return version, nil
}

func firstLine(statement string) string {
	if i := strings.IndexByte(statement, '\n'); i >= 0 {
		statement = statement[:i]
	}
	statement = strings.TrimSpace(statement)
	if len(statement) > 80 {
		statement = statement[:80]
	}
	return statement
}
