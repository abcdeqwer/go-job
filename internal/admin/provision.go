package admin

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"strings"

	gojob "github.com/abcdeqwer/go-job"
	"github.com/abcdeqwer/go-job/internal/control"
)

// tenantDDL is the schema this build requires, embedded so what the UI applies is exactly what
// ships — not a file on disk that may be a different version than the binary reading it.
//
//go:embed schema/001_tenant.sql
var tenantDDL string

// probeTenant reports what is actually in a database before anything is written to it.
//
// The alternative is what an operator does today: paste a DSN, get "admission failed", and
// work out from a log line whether the host is wrong, the credentials are wrong, the schema is
// empty, or the schema belongs to a different tenant. Those have four different fixes and one
// error message.
func (a *API) probeTenant(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		DSN string `json:"dsn"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if strings.TrimSpace(body.DSN) == "" {
		return badRequest("dsn is required")
	}

	db, err := a.cfg.OpenDB(body.DSN)
	if err != nil {
		return badRequest("cannot open that DSN: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(r.Context()); err != nil {
		return badRequest("cannot reach that database: %v", err)
	}

	// Is our schema there at all? schema_identity is the marker: it exists only because this
	// schema was provisioned for go-job.
	var (
		tenant, uuid, version string
		found                 bool
	)
	err = db.QueryRowContext(r.Context(),
		`SELECT tenant, schema_uuid, schema_version FROM schema_identity WHERE lock_row = 1`).
		Scan(&tenant, &uuid, &version)
	switch {
	case err == nil:
		found = true
	case errors.Is(err, sql.ErrNoRows):
		// Tables exist but nothing claims them. Provisioning can finish the job.
	default:
		// Most likely "table doesn't exist", which is the empty-schema case and the one worth
		// offering to fix. Anything else surfaces as the same offer and fails loudly on apply.
	}

	// Distinguish "no tables" from "tables but no identity", because only the first is a
	// clean provision.
	var tables int
	if err := db.QueryRowContext(r.Context(), `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN
		      ('schema_identity','job_definition','job_state','job_execution')`).Scan(&tables); err != nil {
		return badRequest("cannot inspect that database: %v", err)
	}

	out := map[string]any{"reachable": true, "tables": tables, "provisioned": found}
	if found {
		out["tenant"] = tenant
		out["schema_uuid"] = uuid
		out["schema_version"] = version
		out["version_ok"] = version == control.SchemaVersion
	}
	out["expects_version"] = control.SchemaVersion
	writeJSON(w, http.StatusOK, out)
	return nil
}

// provisionTenant applies the schema and mints the identity row, once, on an EMPTY database.
//
// Deliberately not something that happens at startup. MySQL DDL does not roll back, so a
// migration interrupted half way leaves a schema that is neither the old one nor the new one;
// and several replicas starting together would race to apply it. Both problems disappear when
// it is one operator pressing one button on a database they just named — which is the only
// form of automatic schema management this will ever have.
//
// It refuses a database that already holds any of our tables. Recovering a half-provisioned
// schema is a job for whoever can see it, not for a retry loop.
func (a *API) provisionTenant(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		DSN    string `json:"dsn"`
		Tenant string `json:"tenant"`
		Reason string `json:"reason"`
	}
	if err := decode(r, &body); err != nil {
		return err
	}
	if err := requireReason(body.Reason); err != nil {
		return err
	}
	if err := checkIdentifier("tenant", body.Tenant, 64); err != nil {
		return err
	}

	db, err := a.cfg.OpenDB(body.DSN)
	if err != nil {
		return badRequest("cannot open that DSN: %v", err)
	}
	defer db.Close()

	var tables int
	if err := db.QueryRowContext(r.Context(), `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN
		      ('schema_identity','job_definition','job_state','job_execution')`).Scan(&tables); err != nil {
		return badRequest("cannot inspect that database: %v", err)
	}
	if tables > 0 {
		return fmt.Errorf("%w: that database already holds %d of go-job's tables; provisioning "+
			"is for an empty schema, and finishing a partial one by hand is safer than guessing",
			gojob.ErrProtocol, tables)
	}

	for _, stmt := range splitDDL(tenantDDL) {
		if _, err := db.ExecContext(r.Context(), stmt); err != nil {
			return fmt.Errorf("%w: applying the schema failed at %q: %v",
				gojob.ErrProtocol, firstLineOf(stmt), err)
		}
	}

	// The identity row LAST, so a schema that failed half way is not claimed by anybody — a
	// partial schema with an identity row would be admitted and then fail on a missing column.
	var uuid string
	if err := db.QueryRowContext(r.Context(), `SELECT UUID()`).Scan(&uuid); err != nil {
		return fmt.Errorf("%w: minting a schema uuid: %v", gojob.ErrProtocol, err)
	}
	if _, err := db.ExecContext(r.Context(), `
		INSERT INTO schema_identity (lock_row, tenant, schema_uuid, schema_version, created_at)
		VALUES (1, ?, ?, ?, ?)`,
		body.Tenant, uuid, control.SchemaVersion, a.cfg.Clock.Now()); err != nil {
		return fmt.Errorf("%w: claiming the schema for %q: %v", gojob.ErrProtocol, body.Tenant, err)
	}

	a.log.Warn("tenant schema provisioned through the UI",
		"tenant", body.Tenant, "schema_uuid", uuid, "actor", ActorFrom(r.Context()))
	writeJSON(w, http.StatusCreated, map[string]any{
		"tenant": body.Tenant, "schema_uuid": uuid, "schema_version": control.SchemaVersion,
	})
	return nil
}

// splitDDL cuts a schema file into statements.
//
// Comments come out FIRST. This file's own comments contain semicolons — "…business Location;
// admission asserts it." and "-- defaults; merged with trigger overrides" — so splitting on
// `;` before stripping them cuts a column definition in half, and the error you get names a
// table that looks fine.
func splitDDL(ddl string) []string {
	var kept []string
	for _, l := range strings.Split(ddl, "\n") {
		if i := strings.Index(l, "--"); i >= 0 {
			l = l[:i]
		}
		if strings.TrimSpace(l) == "" {
			continue
		}
		kept = append(kept, l)
	}
	var out []string
	for _, raw := range strings.Split(strings.Join(kept, "\n"), ";") {
		if s := strings.TrimSpace(raw); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func firstLineOf(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 80 {
		s = s[:80]
	}
	return strings.TrimSpace(s)
}
