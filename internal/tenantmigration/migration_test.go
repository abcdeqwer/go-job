package tenantmigration

import (
	"testing"
)

func TestEmbeddedMigrationsStartAtVersionOneAndRemainContiguous(t *testing.T) {
	migrations := []Migration{
		{Version: 1, Name: "001_tenant.sql"},
		{Version: 2, Name: "002_execution_retention.sql"},
	}
	if err := Validate(migrations); err != nil {
		t.Fatal(err)
	}
	if migrations[0].Version != InitialVersion {
		t.Fatalf("first tenant migration is version %d, want initial version %d",
			migrations[0].Version, InitialVersion)
	}
	for i, migration := range migrations {
		want := InitialVersion + i
		if migration.Version != want {
			t.Fatalf("tenant migration %q is version %d, want contiguous version %d",
				migration.Name, migration.Version, want)
		}
	}
}

func TestPendingRejectsDowngradesAndSelectsOnlyMissingVersions(t *testing.T) {
	migrations := []Migration{
		{Version: 1, Name: "001_tenant.sql"},
		{Version: 2, Name: "002_execution_retention.sql"},
		{Version: 3, Name: "003_more_state.sql"},
	}

	pending, err := Pending(migrations, "1", "3")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].Version != 2 || pending[1].Version != 3 {
		t.Fatalf("pending migrations = %#v, want versions 2 and 3", pending)
	}

	pending, err = Pending(migrations, "3", "3")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("current schema unexpectedly has pending migrations: %#v", pending)
	}

	if _, err := Pending(migrations, "3", "2"); err == nil {
		t.Fatal("a newer schema was accepted by an older binary")
	}
}
