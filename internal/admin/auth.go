package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	gojob "github.com/abcdeqwer/go-job"
)

// Role is what an account may do. Two, deliberately.
//
// Finer-grained authorization is a property of the organization running the scheduler, not of
// the scheduler, and every gradation added here is one that does not match what some host
// already has.
type Role string

const (
	RoleViewer   Role = "VIEWER"
	RoleOperator Role = "OPERATOR"
)

// allows reports whether a role satisfies a requirement.
func (r Role) allows(required Role) bool {
	if required == RoleViewer {
		return r == RoleViewer || r == RoleOperator
	}
	return r == RoleOperator
}

type ctxKey int

const (
	ctxActor ctxKey = iota
	ctxRole
)

// ActorFrom returns the authenticated identity, or empty when there is none.
func ActorFrom(ctx context.Context) string {
	s, _ := ctx.Value(ctxActor).(string)
	return s
}

// RoleFrom returns the authenticated role.
func RoleFrom(ctx context.Context) Role {
	r, _ := ctx.Value(ctxRole).(Role)
	return r
}

// TrustedHeader configures identity from a reverse proxy.
//
// Hosts that already run SSO put the UI behind their proxy, disable built-in login, and pass
// an identity header. The library does not attempt to be an identity provider and will not
// grow OIDC, LDAP or SAML support.
type TrustedHeader struct {
	// Enabled must be set deliberately. It is off by default because a header-trusting mode
	// that switches itself on is a full authentication bypass for anyone who can reach the
	// port directly — the proxy is only a control if nothing else can get past it.
	Enabled bool

	UserHeader string
	RoleHeader string

	// DefaultRole applies when the proxy sends an identity but no role. VIEWER, so a
	// misconfigured proxy under-grants rather than handing out OPERATOR.
	DefaultRole Role
}

// Auth is session authentication against the control database's local accounts.
//
// Sessions live in the control database, not in this process. The scheduler runs as a cluster
// behind a load balancer, and a process-local map means signing in through one replica and
// getting a 401 from the next — and, worse, signing OUT through one replica while the token
// stays valid on every other.
type Auth struct {
	db      *sql.DB
	clock   gojob.Clock
	ttl     time.Duration
	trusted TrustedHeader
	secure  bool
}

// NewAuth builds the authenticator. secure marks the session cookie Secure, which a
// deployment behind TLS must set; it is separate from the trusted-header switch because the
// two are independent facts about the deployment.
func NewAuth(db *sql.DB, clock gojob.Clock, ttl time.Duration, trusted TrustedHeader, secure bool) *Auth {
	if trusted.DefaultRole == "" {
		trusted.DefaultRole = RoleViewer
	}
	return &Auth{db: db, clock: clock, ttl: ttl, trusted: trusted, secure: secure}
}

const sessionCookie = "gojob_session"

// Require wraps a handler with an authorization requirement.
func (a *Auth) Require(required Role, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actor, role, ok := a.identify(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "not signed in"})
			return
		}
		if !role.allows(required) {
			// Explicit about what was required. An operator who cannot tell "I am not signed
			// in" from "my account cannot do this" will retry the wrong fix.
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": fmt.Sprintf("this action requires %s; you are %s", required, role),
			})
			return
		}
		ctx := context.WithValue(r.Context(), ctxActor, actor)
		ctx = context.WithValue(ctx, ctxRole, role)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *Auth) identify(r *http.Request) (string, Role, bool) {
	if a.trusted.Enabled {
		user := strings.TrimSpace(r.Header.Get(a.trusted.UserHeader))
		if user == "" {
			return "", "", false
		}
		role := a.trusted.DefaultRole
		if got := Role(strings.ToUpper(strings.TrimSpace(r.Header.Get(a.trusted.RoleHeader)))); got == RoleOperator || got == RoleViewer {
			role = got
		}
		return user, role, true
	}

	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return "", "", false
	}

	var (
		username string
		role     string
	)
	err = a.db.QueryRowContext(r.Context(), `
		SELECT username, role FROM admin_session
		WHERE token_sha256 = ? AND expires_at > ?`,
		hashToken(c.Value), a.clock.Now()).Scan(&username, &role)
	if err != nil {
		return "", "", false
	}
	return username, Role(role), true
}

// hashToken is what is stored and compared. The token itself never reaches the database, so a
// backup is not a set of live sessions and nothing downstream can log a usable one.
func hashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// login authenticates a local account and issues a session cookie.
func (a *API) login(w http.ResponseWriter, r *http.Request) {
	if a.auth.trusted.Enabled {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "built-in login is disabled; identity comes from the proxy",
		})
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed body"})
		return
	}

	role, err := a.auth.verify(r.Context(), body.Username, body.Password)
	if err != nil {
		// One message for every failure mode. Distinguishing "no such user" from "wrong
		// password" turns the login form into an account enumerator.
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	token, err := a.auth.issue(r.Context(), body.Username, role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   a.auth.secure,
		SameSite: http.SameSiteStrictMode,
		Expires:  a.auth.clock.Now().Add(a.auth.ttl),
	})
	writeJSON(w, http.StatusOK, map[string]any{"actor": body.Username, "role": role})
}

func (a *API) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		a.auth.revoke(r.Context(), c.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", HttpOnly: true,
		Secure: a.auth.secure, SameSite: http.SameSiteStrictMode, MaxAge: -1,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "signed out"})
}

var errBadCredentials = errors.New("invalid credentials")

// verify checks a password against the stored bcrypt hash.
//
// A missing or disabled account still pays the cost of one bcrypt comparison against a dummy
// hash. Returning early would make "no such user" measurably faster than "wrong password",
// which is the same enumeration the shared error message exists to prevent.
func (a *Auth) verify(ctx context.Context, username, password string) (Role, error) {
	const dummy = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

	var (
		hash     string
		role     string
		disabled bool
	)
	err := a.db.QueryRowContext(ctx,
		`SELECT password_hash, role, disabled FROM admin_user WHERE username = ?`, username).
		Scan(&hash, &role, &disabled)
	if errors.Is(err, sql.ErrNoRows) || disabled {
		_ = bcrypt.CompareHashAndPassword([]byte(dummy), []byte(password))
		return "", errBadCredentials
	}
	if err != nil {
		return "", fmt.Errorf("read account: %w", err)
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return "", errBadCredentials
	}
	got := Role(strings.ToUpper(role))
	if got != RoleViewer && got != RoleOperator {
		return "", fmt.Errorf("account %q has an unknown role %q", username, role)
	}
	return got, nil
}

func (a *Auth) issue(ctx context.Context, actor string, role Role) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	now := a.clock.Now()

	if _, err := a.db.ExecContext(ctx, `
		INSERT INTO admin_session (token_sha256, username, role, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)`,
		hashToken(token), actor, string(role), now, now.Add(a.ttl)); err != nil {
		return "", fmt.Errorf("record session: %w", err)
	}

	// Expired rows are swept on login rather than by a timer: the table only grows when
	// someone signs in, so that is exactly when it is worth looking, and a scheduler that
	// nobody logs into should not be running a background job to tidy an empty table.
	if _, err := a.db.ExecContext(ctx,
		`DELETE FROM admin_session WHERE expires_at <= ?`, now); err != nil {
		// Not fatal: a session that could not be swept is still refused on its expiry.
		_ = err
	}
	return token, nil
}

func (a *Auth) revoke(ctx context.Context, token string) {
	_, _ = a.db.ExecContext(ctx,
		`DELETE FROM admin_session WHERE token_sha256 = ?`, hashToken(token))
}

// HashPassword produces a stored credential, for provisioning the first account.
func HashPassword(password string) (string, error) {
	if len(password) < 12 {
		return "", fmt.Errorf("a password for an account that can trigger production jobs must be at least 12 characters")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(h), nil
}
