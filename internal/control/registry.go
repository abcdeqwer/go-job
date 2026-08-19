// Package control owns the one database that knows more than one tenant exists.
//
// It holds the tenant registry, admin accounts and the control audit, and nothing else — no
// business data, no execution state, no job configuration. A tenant's scheduling lives
// entirely in its own coordination schema, so the isolation property survives the existence
// of this database.
//
// It is also a LEASE ON THE RIGHT TO OPERATE. An instance that cannot read the registry stops
// claiming, materializing and renewing, for every tenant. That is the part an earlier design
// got backwards by letting a partitioned instance keep renewing "so nothing is stranded":
// nothing is stranded — its leases expire and other instances recover them normally — while
// renewal would preserve exactly the thing being ruled out, an owner nobody can see.
package control

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	gojob "github.com/abcdeqwer/go-job"
)

// Tenant is one row of the registry, as the scheduler uses it.
type Tenant struct {
	Name          string
	Enabled       bool
	Generation    int64
	SchemaUUID    string
	SchemaVersion string
	AdmittedAt    sql.NullTime
	LastError     string

	// DSN is decrypted in memory and never leaves this process. Reads for display go through
	// MaskedDSN instead.
	DSN string
}

// MaskedDSN renders a DSN for a human without its password.
//
// There is no "show me the current password" affordance anywhere in the API. The table is
// reachable from an admin UI, and no legitimate use for revealing a stored database password
// outweighs what it costs when the UI is reachable by one more person than intended.
func MaskedDSN(dsn string) string {
	at := strings.LastIndex(dsn, "@")
	if at < 0 {
		return "(unparseable)"
	}
	userinfo := dsn[:at]
	if colon := strings.Index(userinfo, ":"); colon >= 0 {
		userinfo = userinfo[:colon]
	}
	return userinfo + ":***@" + dsn[at+1:]
}

// Store is the control database.
type Store struct {
	db    *sql.DB
	clock gojob.Clock
	aead  cipher.AEAD
}

// New opens a control store. key must be 16, 24 or 32 bytes; DSNs are encrypted under it with
// AES-GCM because they contain database passwords and this table is reachable from a UI.
func New(db *sql.DB, clock gojob.Clock, key []byte) (*Store, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("control: DSN encryption key must be 16, 24 or 32 bytes: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("control: build AEAD: %w", err)
	}
	return &Store{db: db, clock: clock, aead: aead}, nil
}

// DB exposes the pool for the admin API's own queries.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) seal(plain string) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("control: read nonce: %w", err)
	}
	return s.aead.Seal(nonce, nonce, []byte(plain), nil), nil
}

func (s *Store) open(sealed []byte) (string, error) {
	n := s.aead.NonceSize()
	if len(sealed) < n {
		return "", errors.New("control: stored DSN is shorter than a nonce")
	}
	plain, err := s.aead.Open(nil, sealed[:n], sealed[n:], nil)
	if err != nil {
		// Almost always the wrong key. Say so, because the alternative reading — "the registry
		// is corrupt" — sends an operator to restore a backup they do not need.
		return "", fmt.Errorf("control: cannot decrypt DSN; is the encryption key the one it was stored with? %w", err)
	}
	return string(plain), nil
}

// Tenants reads the whole registry.
//
// Schedulers poll this on a short interval, which is what makes adding a site a row rather
// than a redeploy. Every instance polls independently and they need not agree instantly — a
// tenant admitted a second later than its neighbours simply starts a second later.
func (s *Store) Tenants(ctx context.Context) ([]Tenant, error) {
	const q = `
		SELECT tenant, coordination_dsn, enabled, generation, schema_uuid,
		       COALESCE(schema_version, ''), admitted_at, COALESCE(last_error, '')
		FROM tenant_registry ORDER BY tenant`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("read tenant registry: %w", err)
	}
	defer rows.Close()

	var out []Tenant
	for rows.Next() {
		var (
			t      Tenant
			sealed []byte
		)
		if err := rows.Scan(&t.Name, &sealed, &t.Enabled, &t.Generation, &t.SchemaUUID,
			&t.SchemaVersion, &t.AdmittedAt, &t.LastError); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		if t.DSN, err = s.open(sealed); err != nil {
			// One tenant's unreadable DSN must not blind the scheduler to the others: it is
			// recorded against that tenant and the rest are admitted normally.
			t.LastError = err.Error()
			t.Enabled = false
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// CurrentGeneration reads a tenant's generation and whether it is enabled, right now.
//
// Admission uses it as its LAST act before publishing an engine. The generation it started
// with came from a poll, and a poll is a cache: an instance that paused during admission can
// resume holding a generation two cutovers old and install an engine against a schema the
// tenant has already left, which is the split brain the whole procedure exists to prevent.
func (s *Store) CurrentGeneration(ctx context.Context, name string) (int64, bool, error) {
	var (
		generation int64
		enabled    bool
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT generation, enabled FROM tenant_registry WHERE tenant = ?`, name).
		Scan(&generation, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read generation of %q: %w", name, err)
	}
	return generation, enabled, nil
}

// Identity is one row of executor_identity, as an operator sees it. The token is never
// among these fields: only its SHA-256 is stored, and nothing can turn that back.
type Identity struct {
	Identity  string
	Tenant    string
	Group     string
	HasToken  bool
	Disabled  bool
	CreatedAt time.Time
}

// Identities lists who may register as an executor.
func (s *Store) Identities(ctx context.Context) ([]Identity, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT identity, tenant, executor_group, token_sha256 IS NOT NULL, disabled, created_at
		FROM executor_identity ORDER BY tenant, identity`)
	if err != nil {
		return nil, fmt.Errorf("list executor identities: %w", err)
	}
	defer rows.Close()

	var out []Identity
	for rows.Next() {
		var i Identity
		if err := rows.Scan(&i.Identity, &i.Tenant, &i.Group, &i.HasToken, &i.Disabled,
			&i.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan identity: %w", err)
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// AddIdentity authorises an identity for one tenant, optionally with a shared token.
//
// tokenSHA is the hex SHA-256 of a token the CALLER generated; empty means this identity
// authenticates by client certificate, which is the arrangement to prefer. The token itself
// never reaches this package, and is never stored anywhere: an operator who loses it issues a
// new one rather than looking the old one up.
func (s *Store) AddIdentity(ctx context.Context, identity, tenant, group, tokenSHA, actor, reason string) error {
	if identity == "" || tenant == "" {
		return fmt.Errorf("%w: an identity needs a name and a tenant", gojob.ErrProtocol)
	}
	if reason == "" {
		return fmt.Errorf("%w: authorising an executor needs a reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO executor_identity (identity, tenant, executor_group, token_sha256, created_at)
			VALUES (?, ?, ?, ?, ?)`,
			identity, tenant, group, sql.NullString{String: tokenSHA, Valid: tokenSHA != ""}, now); err != nil {
			return fmt.Errorf("authorise %q for %q: %w", identity, tenant, err)
		}
		how := "by client certificate"
		if tokenSHA != "" {
			how = "by shared token"
		}
		scope := "any group"
		if group != "" {
			scope = "group " + group
		}
		return audit(ctx, tx, now, actor, "executor_authorized", tenant,
			fmt.Sprintf("%s (%s, %s, %s)", reason, identity, how, scope))
	})
}

// SetIdentityDisabled revokes or restores an identity.
//
// Revoking is immediate for REGISTRATION, not for work already dispatched: an executor that is
// mid-handler keeps the execution it holds, because taking it away would leave a running
// handler nobody is tracking. It simply cannot register again.
func (s *Store) SetIdentityDisabled(ctx context.Context, identity, tenant string, disabled bool, actor, reason string) error {
	if reason == "" {
		return fmt.Errorf("%w: revoking or restoring an executor needs a reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE executor_identity SET disabled = ?
			WHERE identity = ? AND tenant = ? AND disabled <> ?`,
			disabled, identity, tenant, disabled)
		if err != nil {
			return fmt.Errorf("set %q disabled=%v: %w", identity, disabled, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: %q is already disabled=%v for %q",
				gojob.ErrProtocol, identity, disabled, tenant)
		}
		action := "executor_restored"
		if disabled {
			action = "executor_revoked"
		}
		return audit(ctx, tx, now, actor, action, tenant, reason+" ("+identity+")")
	})
}

// AddTenant registers a new site.
func (s *Store) AddTenant(ctx context.Context, name, dsn, schemaUUID, actor, reason string) error {
	if reason == "" {
		return fmt.Errorf("%w: adding a tenant needs a reason", gojob.ErrProtocol)
	}
	if name == "" || dsn == "" || schemaUUID == "" {
		return fmt.Errorf("%w: a tenant needs a name, a DSN and the schema uuid it expects",
			gojob.ErrProtocol)
	}
	sealed, err := s.seal(dsn)
	if err != nil {
		return err
	}
	now := s.clock.Now()

	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO tenant_registry
			    (tenant, coordination_dsn, enabled, generation, schema_uuid, created_at, updated_at)
			VALUES (?, ?, 1, 1, ?, ?, ?)`,
			name, sealed, schemaUUID, now, now); err != nil {
			return fmt.Errorf("add tenant %q: %w", name, err)
		}
		// One transaction with the action. The reason is REQUIRED, so committing the action
		// and then failing to record it leaves an audit trail that is missing exactly the
		// entry the requirement exists for — and returns a 500 that invites a retry of an
		// action that already happened.
		return audit(ctx, tx, now, actor, "tenant_added", name,
			reason+" (expects schema "+schemaUUID+"; DSN stored encrypted)")
	})
}

// SetTenantEnabled enables or disables a tenant and bumps its generation.
//
// The generation is what makes a DSN cutover safe: every enable and disable moves it, and
// each instance records the generation it has applied, so "has everybody seen the disable"
// becomes a query rather than a guess about how long to wait.
func (s *Store) SetTenantEnabled(ctx context.Context, name string, enabled bool, actor, reason string) error {
	if reason == "" {
		return fmt.Errorf("%w: enabling or disabling a tenant needs a reason", gojob.ErrProtocol)
	}
	now := s.clock.Now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE tenant_registry
			SET enabled = ?, generation = generation + 1, updated_at = ?
			WHERE tenant = ? AND enabled <> ?`,
			enabled, now, name, enabled)
		if err != nil {
			return fmt.Errorf("set tenant %q enabled=%v: %w", name, enabled, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: tenant %q is already enabled=%v", gojob.ErrProtocol, name, enabled)
		}
		action := "tenant_disabled"
		if enabled {
			action = "tenant_enabled"
		}
		return audit(ctx, tx, now, actor, action, name, reason)
	})
}

// ErrNotQuiesced means a DSN change was refused because some instance still holds work.
var ErrNotQuiesced = errors.New("gojob: tenant is not quiesced; a DSN change would split brain")

// SetTenantDSN re-points a tenant at a different coordination schema.
//
// This is deliberately NOT a hot change. Schedulers poll the registry independently, so
// instance A can adopt a new DSN while instance B is still working the old schema — two
// job_state rows for one tenant, in two databases, each correctly excluding only itself, and
// the same job dispatched twice.
//
// So it is three audited steps: disable, prove quiescence, then change and re-enable. The
// middle step has to be mechanical. "Wait a bit after disabling" proves nothing about an
// instance that is partitioned from THIS database and still perfectly able to reach the
// tenant's — which is why the caller must additionally prove quiescence by looking at the old
// coordination schema itself, and why an instance that cannot read this registry self-fences.
// blockersTx is Blockers, inside a transaction that is about to act on the answer.
//
// The handler's earlier check is a snapshot, and everything after it — verifying the new
// schema, opening a pool — takes time an instance can finish admitting in. Re-reading here
// makes the gate and the write atomic with respect to every other control-plane writer.
func blockersTx(ctx context.Context, tx *sql.Tx, tenant string, generation int64,
	liveness time.Duration) ([]string, error) {

	// FOR UPDATE, and the predicate is the TENANT alone.
	//
	// Locking tenant_registry does not serialise this table: Observe upserts into it
	// independently, and there is no shared row between them. A plain SELECT here is a read
	// whose answer an instance can invalidate a microsecond later by inserting its first
	// observation — which is exactly the instance this gate exists to catch, since one that
	// has never observed anything is one that is still starting up.
	//
	// Scanning the whole tenant range under FOR UPDATE takes next-key locks across it, so a
	// concurrent INSERT for this tenant waits for this transaction rather than slipping in
	// behind the read. The liveness and generation tests are applied in Go for the same
	// reason: narrowing them in SQL would narrow the locked range with them.
	rows, err := tx.QueryContext(ctx, `
		SELECT instance_id, generation, quiesced,
		       observed_at >= TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP())
		FROM tenant_observation
		WHERE tenant = ?
		ORDER BY instance_id
		FOR UPDATE`, -roundUpSeconds(liveness), tenant)
	if err != nil {
		return nil, fmt.Errorf("re-check blockers for %q: %w", tenant, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var (
			id       string
			gen      int64
			quiesced bool
			live     bool
		)
		if err := rows.Scan(&id, &gen, &quiesced, &live); err != nil {
			return nil, fmt.Errorf("scan blocker: %w", err)
		}
		if live && !(gen >= generation && quiesced) {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

func (s *Store) SetTenantDSN(ctx context.Context, name, dsn, schemaUUID, actor, reason string, quiescedElsewhere bool, liveness time.Duration) error {
	if reason == "" {
		return fmt.Errorf("%w: re-pointing a tenant needs a reason", gojob.ErrProtocol)
	}
	if !quiescedElsewhere {
		return fmt.Errorf("%w: %s", ErrNotQuiesced, name)
	}
	sealed, err := s.seal(dsn)
	if err != nil {
		return err
	}
	now := s.clock.Now()

	return s.tx(ctx, func(tx *sql.Tx) error {
		// The gate, re-read here rather than trusted from the caller's earlier snapshot.
		//
		// Between that snapshot and this write the handler verifies the new schema and opens a
		// pool, which is time enough for an instance to finish admitting the OLD one and start
		// an engine against it. Read inside the transaction, the answer cannot go stale before
		// the DSN moves.
		var generation int64
		if err := tx.QueryRowContext(ctx,
			`SELECT generation FROM tenant_registry WHERE tenant = ? FOR UPDATE`,
			name).Scan(&generation); err != nil {
			return fmt.Errorf("read generation of %q: %w", name, err)
		}
		blockers, err := blockersTx(ctx, tx, name, generation, liveness)
		if err != nil {
			return err
		}
		if len(blockers) > 0 {
			return fmt.Errorf("%w: %s is still held by %s", ErrNotQuiesced, name,
				strings.Join(blockers, ", "))
		}

		res, err := tx.ExecContext(ctx, `
			UPDATE tenant_registry
			SET coordination_dsn = ?, schema_uuid = ?, schema_version = NULL,
			    admitted_at = NULL, generation = generation + 1, updated_at = ?
			WHERE tenant = ? AND enabled = 0`,
			sealed, schemaUUID, now, name)
		if err != nil {
			return fmt.Errorf("re-point tenant %q: %w", name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: tenant %q must be disabled before its DSN can change",
				ErrNotQuiesced, name)
		}
		return audit(ctx, tx, now, actor, "tenant_dsn_changed", name,
			reason+" (now expects schema "+schemaUUID+")")
	})
}

// RecordAdmission notes that a tenant's schema was verified, or why it was not.
func (s *Store) RecordAdmission(ctx context.Context, name, schemaVersion string, admitErr error) error {
	now := s.clock.Now()
	var (
		admitted any
		lastErr  any
	)
	if admitErr == nil {
		admitted = now
	} else {
		msg := admitErr.Error()
		if len(msg) > 512 {
			msg = msg[:512]
		}
		lastErr = msg
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE tenant_registry
		SET schema_version = ?, admitted_at = ?, last_error = ?, updated_at = ?
		WHERE tenant = ?`,
		nullEmpty(schemaVersion), admitted, lastErr, now, name); err != nil {
		return fmt.Errorf("record admission of %q: %w", name, err)
	}
	return nil
}

// Observe records what this instance has applied for a tenant.
//
// observed_at is written and compared with UTC_TIMESTAMP(), because this is an OWNERSHIP
// clock: "is that instance still live" decides whether a DSN cutover may proceed. Writing it
// from a Go clock and comparing it against the database's would make a live instance look
// stale under host skew or across a DST offset change — and an instance wrongly judged dead is
// omitted from the blockers, after which a cutover proceeds while it is still claiming.
//
// quiesced means it holds nothing for that tenant. The pair (generation, quiesced) is what a
// DSN change is gated on, and observed_at is what makes "live" answerable — an instance that
// stops reporting drops out of the live set, which is safe only because such an instance also
// self-fences.
func (s *Store) Observe(ctx context.Context, tenant, instanceID string, generation int64, quiesced bool) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO tenant_observation (tenant, instance_id, generation, quiesced, observed_at)
		VALUES (?, ?, ?, ?, UTC_TIMESTAMP())
		ON DUPLICATE KEY UPDATE
		    generation  = VALUES(generation),
		    quiesced    = VALUES(quiesced),
		    observed_at = UTC_TIMESTAMP()`,
		tenant, instanceID, generation, quiesced); err != nil {
		return fmt.Errorf("record observation of %q by %q: %w", tenant, instanceID, err)
	}
	return nil
}

// Blocker names an instance that has not acknowledged a generation.
type Blocker struct {
	InstanceID string
	Generation int64
	Quiesced   bool
	ObservedAt time.Time
}

// Blockers lists live instances that have not applied `generation` with quiesced = 1.
//
// The API names them rather than making an operator guess which one is holding things up.
func (s *Store) Blockers(ctx context.Context, tenant string, generation int64, liveness time.Duration) ([]Blocker, error) {
	const q = `
		SELECT instance_id, generation, quiesced, observed_at
		FROM tenant_observation
		WHERE tenant = ?
		  AND observed_at >= TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP())
		  AND NOT (generation >= ? AND quiesced = 1)
		ORDER BY instance_id`

	rows, err := s.db.QueryContext(ctx, q, tenant, -roundUpSeconds(liveness), generation)
	if err != nil {
		return nil, fmt.Errorf("find blockers for %q: %w", tenant, err)
	}
	defer rows.Close()

	var out []Blocker
	for rows.Next() {
		var b Blocker
		if err := rows.Scan(&b.InstanceID, &b.Generation, &b.Quiesced, &b.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan blocker: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ReapObservations drops records from instances that have been gone a long time, so a rolled
// deployment does not accumulate a blocker list of processes that no longer exist.
func (s *Store) ReapObservations(ctx context.Context, retention time.Duration) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM tenant_observation WHERE observed_at < TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP())`,
		-roundUpSeconds(retention)); err != nil {
		return fmt.Errorf("reap observations: %w", err)
	}
	return nil
}

// Audit appends a control-plane action.
func (s *Store) Audit(ctx context.Context, actor, action, tenant, detail string) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		return audit(ctx, tx, s.clock.Now(), actor, action, tenant, detail)
	})
}

func audit(ctx context.Context, tx *sql.Tx, now time.Time, actor, action, tenant, detail string) error {
	if actor == "" {
		return fmt.Errorf("%w: control action %q with no actor", gojob.ErrProtocol, action)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO control_audit (actor, action, tenant, detail, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		actor, action, nullEmpty(tenant), nullEmpty(detail), now); err != nil {
		return fmt.Errorf("control audit %s by %s: %w", action, actor, err)
	}
	return nil
}

func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("control: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("control: commit: %w", err)
	}
	committed = true
	return nil
}

func nullEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func roundUpSeconds(d time.Duration) int {
	s := int((d + time.Second - 1) / time.Second)
	if s < 1 {
		return 1
	}
	return s
}
