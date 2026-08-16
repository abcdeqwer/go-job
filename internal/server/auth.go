package server

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Identity is an authenticated executor caller.
type Identity struct {
	// Subject is the mTLS certificate subject, or the name a shared token is registered under.
	Subject string

	// Tenant and Group are what this identity is authorized for. Empty tenant means the
	// identity is authorized for every tenant, which is only ever appropriate for a
	// single-tenant installation.
	Tenant string
	Group  string
}

// Authenticator decides who is calling and what they may act as.
//
// This exists because the executor-facing service is the one surface that takes work
// descriptions — including a job's parameters — and hands them out. An unauthenticated one
// lets anything that can reach the port register for an arbitrary tenant and receive that
// tenant's work. The scheduler does not get to assume the network is the boundary.
type Authenticator interface {
	// Authenticate returns the caller's identity, or an error the caller sees as
	// UNAUTHENTICATED.
	Authenticate(ctx context.Context) (Identity, error)

	// Authorize checks that an identity may act for a tenant and group.
	Authorize(ctx context.Context, id Identity, tenant, group string) error
}

// ErrNoIdentity means the call carried no usable credential.
var ErrNoIdentity = errors.New("gojob: the caller presented no recognised credential")

// DBAuthenticator authenticates against the control database's executor_identity table.
//
// Two credentials are accepted, and a deployment picks one:
//
//   - **mTLS**: the certificate subject is the identity. This is the one to prefer — it is
//     revocable, it is not replayable, and it does not have to be distributed to every
//     executor as a secret in an environment variable.
//   - **a shared token**, presented as `authorization: Bearer <token>`. Compared against a
//     SHA-256 stored in the identity row, in constant time.
//
// The row also carries the tenant and group the identity may act as, which is what stops a
// compromised executor in one group registering as another — a partial rollout's canary
// registering as the main group would silently take production traffic.
type DBAuthenticator struct {
	// DB is the CONTROL database. Identities live there rather than in a tenant schema
	// because the row's whole purpose is to say WHICH tenant an identity may act for, and a
	// per-tenant copy could only ever answer for itself — an identity would authorize itself
	// by connecting to the tenant it wanted.
	DB *sql.DB

	// RequireCredential refuses calls that present nothing at all. It defaults to on; turning
	// it off is a deliberate choice for a deployment whose network genuinely is the boundary,
	// and it is logged at startup so nobody discovers it by reading the code.
	RequireCredential bool
}

// Authenticate reads a credential off the call.
func (a *DBAuthenticator) Authenticate(ctx context.Context) (Identity, error) {
	if sub, ok := mtlsSubject(ctx); ok {
		return Identity{Subject: sub}, nil
	}
	if tok, ok := bearerToken(ctx); ok {
		// The token is resolved to a NAME here rather than carried onward, so nothing
		// downstream — a log line, an audit row, an error message — can print it.
		name, err := a.identityForToken(ctx, tok)
		if err != nil {
			return Identity{}, err
		}
		return Identity{Subject: name}, nil
	}
	if a.RequireCredential {
		return Identity{}, status.Error(codes.Unauthenticated,
			"present a client certificate or an authorization bearer token")
	}
	return Identity{Subject: "anonymous"}, nil
}

// Authorize checks the identity against the tenant's executor_identity rows.
//
// An installation with NO identity rows for a tenant is open for that tenant, deliberately:
// requiring rows before anything can register would make a fresh installation impossible to
// start without a chicken-and-egg step. The moment one row exists, the table is authoritative
// and an unlisted identity is refused — so turning it on is adding a row, not flipping a flag
// somewhere else.
func (a *DBAuthenticator) Authorize(ctx context.Context, id Identity, tenant, group string) error {
	var configured int
	if err := a.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM executor_identity WHERE disabled = 0`).Scan(&configured); err != nil {
		// A table this query cannot read must not fail open. The whole point of the check is
		// that it is the last thing between a reachable port and a tenant's work.
		return status.Errorf(codes.Internal, "read executor identities: %v", err)
	}
	if configured == 0 {
		return nil
	}

	var allowedGroup string
	err := a.DB.QueryRowContext(ctx, `
		SELECT executor_group FROM executor_identity
		WHERE identity = ? AND tenant = ? AND disabled = 0`,
		id.Subject, tenant).Scan(&allowedGroup)
	if errors.Is(err, sql.ErrNoRows) {
		return status.Errorf(codes.PermissionDenied,
			"identity %q is not authorized for this tenant", id.Subject)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "read executor identity: %v", err)
	}
	if allowedGroup != "" && allowedGroup != group {
		return status.Errorf(codes.PermissionDenied,
			"identity %q may act only as group %q, not %q", id.Subject, allowedGroup, group)
	}
	return nil
}

// identityForToken resolves a bearer token to the identity it was issued to.
//
// The lookup is by hash, so the token is never stored and a database dump does not contain
// credentials. Every failure returns the same message: distinguishing "no such token" from
// "that token is disabled" turns this into an oracle for guessing them.
func (a *DBAuthenticator) identityForToken(ctx context.Context, tok string) (string, error) {
	sum := sha256.Sum256([]byte(tok))
	hash := hex.EncodeToString(sum[:])

	var name string
	err := a.DB.QueryRowContext(ctx,
		`SELECT identity FROM executor_identity WHERE token_sha256 = ? AND disabled = 0 LIMIT 1`,
		hash).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", status.Error(codes.Unauthenticated, "unrecognised credential")
	}
	if err != nil {
		return "", status.Errorf(codes.Internal, "read executor identity: %v", err)
	}
	return name, nil
}

func mtlsSubject(ctx context.Context) (string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return "", false
	}
	tls, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tls.State.VerifiedChains) == 0 || len(tls.State.VerifiedChains[0]) == 0 {
		// An unverified chain is not an identity. Reading a subject off a certificate the
		// server did not verify would accept any certificate at all.
		return "", false
	}
	return subjectOf(tls.State.VerifiedChains[0][0]), true
}

func subjectOf(cert *x509.Certificate) string {
	if cn := cert.Subject.CommonName; cn != "" {
		return cn
	}
	if len(cert.DNSNames) > 0 {
		return cert.DNSNames[0]
	}
	return cert.Subject.String()
}

func bearerToken(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	for _, v := range md.Get("authorization") {
		if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
			if tok := strings.TrimSpace(v[7:]); tok != "" {
				return tok, true
			}
		}
	}
	return "", false
}

// HashToken renders a token for storage in executor_identity.token_sha256.
func HashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// UnaryAuthInterceptor authenticates every executor-facing call.
//
// It runs before the handler, so no method can be reached without a credential — which is the
// only arrangement that works. A per-handler check is one someone forgets to add to the
// handler they write next, and the one they forget is the one that matters.
func UnaryAuthInterceptor(auth Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {

		id, err := auth.Authenticate(ctx)
		if err != nil {
			return nil, err
		}
		tenant, group := tenantAndGroupOf(req)
		if tenant != "" {
			if err := auth.Authorize(ctx, id, tenant, group); err != nil {
				return nil, err
			}
		}
		return handler(withIdentity(ctx, id), req)
	}
}

type identityKey struct{}

func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFrom returns the authenticated caller, for logging and audit.
func IdentityFrom(ctx context.Context) Identity {
	id, _ := ctx.Value(identityKey{}).(Identity)
	return id
}

// tenantAndGroupOf pulls the tenant and group out of whichever request this is.
//
// A request whose tenant cannot be determined is authenticated but not authorized against a
// tenant, and every handler resolves the tenant itself anyway — so an unrecognised message
// type fails in the handler rather than silently skipping a check it was supposed to make.
func tenantAndGroupOf(req any) (tenant, group string) {
	type tenanted interface{ GetTenant() string }
	type grouped interface{ GetGroup() string }

	if t, ok := req.(tenanted); ok {
		tenant = t.GetTenant()
	}
	if g, ok := req.(grouped); ok {
		group = g.GetGroup()
	}
	return tenant, group
}
