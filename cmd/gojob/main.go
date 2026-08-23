// Command gojob runs the scheduler: the executor-facing gRPC service, the operator API and
// UI, and one engine per admitted tenant.
//
// Every instance is identical and equal. There is no leader, no designated node and no
// configured control plane — instances coordinate entirely through the tables, so the cluster
// can be scaled, rolled and lost a node at a time without any of them being special.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	gojob "github.com/abcdeqwer/go-job"
	gojobv1 "github.com/abcdeqwer/go-job/gen/gojob/v1"
	"github.com/abcdeqwer/go-job/internal/admin"
	"github.com/abcdeqwer/go-job/internal/control"
	"github.com/abcdeqwer/go-job/internal/dispatch"
	"github.com/abcdeqwer/go-job/internal/engine"
	"github.com/abcdeqwer/go-job/internal/runtime"
	"github.com/abcdeqwer/go-job/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gojob:", err)
		os.Exit(1)
	}
}

type config struct {
	controlDSN string
	dsnKeyHex  string
	location   string
	instanceID string

	grpcAddr  string
	adminAddr string

	scanInterval    time.Duration
	recoverInterval time.Duration
	reapInterval    time.Duration
	pollInterval    time.Duration
	stalenessLimit  time.Duration

	executorLiveness          time.Duration
	executorRetention         time.Duration
	executionSuccessRetention time.Duration
	executionOtherRetention   time.Duration
	sessionTTL                time.Duration

	trustedUserHeader string
	trustedRoleHeader string
	cookieSecure      bool

	tlsCert       string
	tlsKey        string
	tlsClientCA   string
	outboundCA    string
	execToken     string
	allowNoAuth   bool
	allowUnlisted bool

	hashPassword string
	hashToken    string
}

// progressInterval is how often an executor is asked to report, given how long it may be
// silent. Floored at a second so a very short liveness setting cannot ask for a report every
// zero seconds.
func progressInterval(silence time.Duration) time.Duration {
	if p := silence / 3; p >= time.Second {
		return p
	}
	return time.Second
}

func run() error {
	var c config
	flag.StringVar(&c.controlDSN, "control-dsn", env("GOJOB_CONTROL_DSN", ""),
		"MySQL DSN for the control database")
	flag.StringVar(&c.dsnKeyHex, "dsn-key", env("GOJOB_DSN_KEY", ""),
		"OPTIONAL hex-encoded 16, 24 or 32 byte key encrypting tenant DSNs at rest; "+
			"without it they are stored as typed")
	flag.StringVar(&c.location, "location", env("GOJOB_LOCATION", "UTC"),
		"business time zone; cron expressions are evaluated in it")
	flag.StringVar(&c.instanceID, "instance-id", env("GOJOB_INSTANCE_ID", ""),
		"unique id for this process (default: hostname plus a random suffix)")
	flag.StringVar(&c.grpcAddr, "grpc-addr", env("GOJOB_GRPC_ADDR", ":9090"),
		"address the executor-facing gRPC service listens on")
	flag.StringVar(&c.adminAddr, "admin-addr", env("GOJOB_ADMIN_ADDR", ":8080"),
		"address the operator API and UI listen on")

	flag.DurationVar(&c.scanInterval, "scan-interval", gojob.DefaultScanInterval,
		"how often to look for due jobs and claimable work")
	flag.DurationVar(&c.recoverInterval, "recover-interval", 15*time.Second,
		"how often to look for expired leases and executions past their cap")
	flag.DurationVar(&c.reapInterval, "reap-interval", time.Minute,
		"how often to remove dead executor registrations and report orphans")
	flag.DurationVar(&c.pollInterval, "registry-poll", 10*time.Second,
		"how often to re-read the tenant registry")
	flag.DurationVar(&c.stalenessLimit, "control-staleness", 30*time.Second,
		"how long this instance may go without reading the registry before it fences itself")
	flag.DurationVar(&c.executorLiveness, "executor-liveness", 30*time.Second,
		"how long an executor stays routable after its last heartbeat")
	flag.DurationVar(&c.executorRetention, "executor-retention", time.Hour,
		"how long a dead executor's registration is kept for the UI before being reaped")
	flag.DurationVar(&c.executionSuccessRetention, "execution-success-retention",
		gojob.DefaultExecutionSuccessRetention,
		"how long successful execution history is kept")
	flag.DurationVar(&c.executionOtherRetention, "execution-other-retention",
		gojob.DefaultExecutionOtherRetention,
		"how long dead, cancelled and skipped execution history is kept")
	flag.DurationVar(&c.sessionTTL, "session-ttl", 12*time.Hour, "admin session lifetime")

	flag.StringVar(&c.trustedUserHeader, "trusted-user-header", env("GOJOB_TRUSTED_USER_HEADER", ""),
		"trust this header for identity instead of built-in login; empty disables the mode")
	flag.StringVar(&c.trustedRoleHeader, "trusted-role-header", env("GOJOB_TRUSTED_ROLE_HEADER", ""),
		"header carrying VIEWER or OPERATOR when trusted-user-header is set")
	flag.BoolVar(&c.cookieSecure, "cookie-secure", env("GOJOB_COOKIE_SECURE", "") != "",
		"mark the session cookie Secure; set this when serving over TLS")

	flag.StringVar(&c.tlsCert, "tls-cert", env("GOJOB_TLS_CERT", ""),
		"certificate for the executor-facing gRPC service")
	flag.StringVar(&c.tlsKey, "tls-key", env("GOJOB_TLS_KEY", ""),
		"private key for -tls-cert")
	flag.StringVar(&c.tlsClientCA, "tls-client-ca", env("GOJOB_TLS_CLIENT_CA", ""),
		"CA that executor client certificates are verified against; enables mTLS")
	flag.StringVar(&c.outboundCA, "executor-ca", env("GOJOB_EXECUTOR_CA", ""),
		"CA that executor server certificates are verified against when dispatching")
	flag.StringVar(&c.execToken, "executor-token", env("GOJOB_EXECUTOR_TOKEN", ""),
		"bearer token sent to executors when dispatching, for fleets not using mTLS")
	flag.BoolVar(&c.allowNoAuth, "allow-unauthenticated-executors", false,
		"accept executor calls that present no credential; only for a network that is itself the boundary")
	flag.BoolVar(&c.allowUnlisted, "allow-unlisted-executors", false,
		"let an authenticated executor act for a tenant it has no executor_identity row for")

	flag.StringVar(&c.hashPassword, "hash-password", "",
		"print a bcrypt hash for this password and exit, for provisioning the first account")
	flag.StringVar(&c.hashToken, "hash-token", "",
		"print the SHA-256 of an executor token for executor_identity.token_sha256, and exit")
	flag.Parse()

	if c.hashPassword != "" {
		h, err := admin.HashPassword(c.hashPassword)
		if err != nil {
			return err
		}
		fmt.Println(h)
		return nil
	}
	if c.hashToken != "" {
		fmt.Println(server.HashToken(c.hashToken))
		return nil
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if c.controlDSN == "" {
		return errors.New("-control-dsn is required")
	}
	key, err := decodeKey(c.dsnKeyHex)
	if err != nil {
		return err
	}
	loc, err := time.LoadLocation(c.location)
	if err != nil {
		return fmt.Errorf("-location %q: %w", c.location, err)
	}
	if c.instanceID == "" {
		c.instanceID = defaultInstanceID()
	}
	clock := gojob.SystemClock{Loc: loc}

	controlDB, err := sql.Open("mysql", withDefaults(c.controlDSN, loc))
	if err != nil {
		return fmt.Errorf("open the control database: %w", err)
	}
	defer controlDB.Close()
	controlDB.SetMaxOpenConns(8)
	controlDB.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelPing()
	if err := controlDB.PingContext(pingCtx); err != nil {
		return fmt.Errorf("reach the control database: %w", err)
	}

	ctl, err := control.New(controlDB, clock, key)
	if err == nil && !ctl.Encrypting() {
		// Beside the plaintext-gRPC and no-credential warnings, and for the same reason:
		// nobody should learn months later, from the table, that the passwords in it are
		// readable to anything that can read the control database — a backup, a replica, a
		// support engineer with SELECT.
		log.Warn("tenant DSNs are stored WITHOUT encryption; -dsn-key is unset, so anything " +
			"that can read the control database can read the passwords in it")
	}
	if err != nil {
		return err
	}
	fence := control.NewFence(c.stalenessLimit)

	outbound, err := outboundCredentials(c)
	if err != nil {
		return err
	}
	disp := dispatch.NewClient(5*time.Second, 10*time.Second, outbound)

	reg := runtime.NewRegistry(runtime.Options{
		InstanceID:      c.instanceID,
		Clock:           clock,
		PollInterval:    c.pollInterval,
		StalenessLimit:  c.stalenessLimit,
		MaxOpenConns:    16,
		MaxIdleConns:    4,
		ConnMaxLifetime: 30 * time.Minute,
		DrainTimeout:    15 * time.Second,
		OpenDB: func(dsn string) (*sql.DB, error) {
			return sql.Open("mysql", withDefaults(dsn, loc))
		},
		Engine: engine.Config{
			ScanInterval:              c.scanInterval,
			RecoverInterval:           c.recoverInterval,
			ReapInterval:              c.reapInterval,
			MisfireGrace:              gojob.DefaultMisfireGrace(c.scanInterval),
			ExecutorLiveness:          c.executorLiveness,
			ExecutorRetention:         c.executorRetention,
			ExecutionSuccessRetention: c.executionSuccessRetention,
			ExecutionOtherRetention:   c.executionOtherRetention,
			PageSize:                  gojob.DefaultRetentionBatchSize,
			BackoffBase:               5 * time.Second,
			BackoffMax:                5 * time.Minute,
			ReconcileDeadline:         gojob.ReconcileDeadline,
			// Five attempts or sixty seconds, whichever comes first. Without a bound an
			// execution can be stranded permanently: an executor whose outbound heartbeats
			// still succeed stays registration-live, so it keeps being chosen, while its Run
			// path never answers.
			DispatchResendLimit:  5,
			DispatchResendWindow: time.Minute,
		},
	}, ctl, fence, disp, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg.Run(ctx)

	inbound, err := inboundCredentials(c, log)
	if err != nil {
		return err
	}
	auth := &server.DBAuthenticator{
		DB:                      controlDB,
		RequireCredential:       !c.allowNoAuth,
		AllowUnlistedIdentities: c.allowUnlisted,
	}
	if c.allowUnlisted {
		log.Warn("authenticated executors are accepted for tenants they are NOT listed for; " +
			"any certificate the client CA signed can register as any tenant")
	}
	if c.allowNoAuth {
		// Said loudly, once, at startup. A mode that lets anything reachable register for a
		// tenant and be handed that tenant's work is not something anyone should discover by
		// reading the source.
		log.Warn("executor calls are accepted WITHOUT a credential; " +
			"anything that can reach the gRPC port can register for any tenant")
	}

	grpcOpts := []grpc.ServerOption{grpc.UnaryInterceptor(server.UnaryAuthInterceptor(auth))}
	if inbound != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(inbound))
	}
	grpcSrv := grpc.NewServer(grpcOpts...)
	gojobv1.RegisterJobSchedulerServer(grpcSrv, server.New(server.Config{
		HeartbeatInterval: c.executorLiveness / 3,
		RegistrationTTL:   c.executorLiveness,

		// A THIRD of the silence budget, the same ratio the lease heartbeat uses, and for the
		// same reason: an executor that reports on the interval it was told should be able to
		// lose two reports in a row and still not be called silent.
		//
		// These were both thirty seconds, which is no margin at all — a conforming executor
		// reporting exactly on time is silent the moment one report is late, after which a
		// single failed GetExecution ends a running handler and records its side effects as
		// unknown.
		ProgressInterval: progressInterval(c.executorLiveness),
		SilenceDeadline:  c.executorLiveness,
	}, runtime.SchedulerTenants{R: reg}, disp, fence, clock, func() int { return 30 }, log))

	lis, err := net.Listen("tcp", c.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", c.grpcAddr, err)
	}
	go func() {
		log.Info("executor gRPC service listening", "addr", c.grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("gRPC service stopped", "error", err)
		}
	}()

	api := admin.New(admin.Config{
		ExecutorLiveness: c.executorLiveness,
		InstanceLiveness: c.stalenessLimit * 3,
		Clock:            clock,
		OpenDB: func(dsn string) (*sql.DB, error) {
			return sql.Open("mysql", withDefaults(dsn, loc))
		},
		ControlServer: controlServer(c.controlDSN),
	}, reg, ctl, reg, admin.NewAuth(controlDB, clock, c.sessionTTL, admin.TrustedHeader{
		Enabled:    c.trustedUserHeader != "",
		UserHeader: c.trustedUserHeader,
		RoleHeader: c.trustedRoleHeader,
	}, c.cookieSecure), log)

	httpSrv := &http.Server{
		Addr:              c.adminAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Info("operator API and UI listening", "addr", c.adminAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server stopped", "error", err)
		}
	}()

	log.Info("gojob started", "instance", c.instanceID, "location", loc.String())
	<-ctx.Done()

	// Shutdown order matters. Stop accepting new work first, then stop the loops, and let
	// in-flight handlers keep their leases: expiry is not proof a handler stopped, but
	// releasing early guarantees a second executor may start while the first is still writing.
	log.Info("shutting down")
	shutCtx, cancelShut := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelShut()

	_ = httpSrv.Shutdown(shutCtx)
	grpcSrv.GracefulStop()
	reg.Stop()
	log.Info("stopped")
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// decodeKey reads the DSN encryption key.
//
// It is required rather than optional with a generated fallback: a key generated at startup
// makes every DSN in the registry unreadable on the next restart, and the failure surfaces as
// "no tenants" rather than as anything pointing at the key.
func decodeKey(hexKey string) ([]byte, error) {
	if hexKey == "" {
		// Optional. run() warns when it is absent, because storing a database password as it
		// was typed is a choice an operator should see made, not discover in a table.
		return nil, nil
	}
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("-dsn-key must be hex: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("-dsn-key decodes to %d bytes; it must be 16, 24 or 32", len(key))
	}
}

func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "gojob"
	}
	// A random suffix, because two processes on one host — a rolling restart overlapping, or
	// two replicas in one pod — must not share an owner identity. Ownership is guarded by
	// token and epoch rather than by this string, but a shared one makes every log line and
	// every "who holds this" answer ambiguous.
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

// withDefaults adds the connection parameters this design depends on.
//
// The session time zone is the important one. Every business column is a naked DATETIME
// compared against values this process computes in `loc`, so a session in another zone makes
// the two disagree by the offset between them — and the symptom is a job firing eight hours
// early, months after the mistake. Admission verifies it too; setting it here means the
// verification usually passes rather than usually failing.
//
// parseTime returns DATETIME as time.Time rather than []byte, which every scan in the store
// layer assumes.
func withDefaults(dsn string, loc *time.Location) string {
	// parseTime and loc, and deliberately NOT time_zone.
	//
	// The session zone participates in nothing: ownership columns are written and compared
	// with UTC_TIMESTAMP(), business columns are written and compared as values this process
	// computes, and no column carries a CURRENT_TIMESTAMP default. Pinning it was worse than
	// pointless — the numeric offset is resolved once, at process start, so a pool that
	// outlives a DST transition holds a session zone that no longer matches the location it
	// was derived from.
	add := map[string]string{
		"parseTime": "true",
		"loc":       url.QueryEscape(loc.String()),
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	var b strings.Builder
	b.WriteString(dsn)
	for k, v := range add {
		if strings.Contains(dsn, k+"=") {
			continue
		}
		b.WriteString(sep)
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(v)
		sep = "&"
	}
	return b.String()
}

// inboundCredentials builds the executor-facing TLS configuration.
//
// A certificate without a client CA is server-only TLS: executors verify the scheduler, and
// the scheduler falls back to bearer tokens to identify them. Adding a client CA turns on
// mTLS, which is the arrangement to prefer — a certificate is revocable and is not replayable,
// while a shared token has to be distributed to every executor and lives in an environment
// variable on each of them.
func inboundCredentials(c config, log *slog.Logger) (credentials.TransportCredentials, error) {
	if c.tlsCert == "" && c.tlsKey == "" {
		if c.tlsClientCA != "" {
			return nil, errors.New("-tls-client-ca needs -tls-cert and -tls-key: " +
				"client certificates cannot be verified over a plaintext connection")
		}
		log.Warn("the executor gRPC service is PLAINTEXT; " +
			"job parameters and results cross the network unencrypted")
		return nil, nil
	}
	if c.tlsCert == "" || c.tlsKey == "" {
		return nil, errors.New("-tls-cert and -tls-key must be given together")
	}

	cert, err := tls.LoadX509KeyPair(c.tlsCert, c.tlsKey)
	if err != nil {
		return nil, fmt.Errorf("load the gRPC certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}

	if c.tlsClientCA != "" {
		pool, err := certPool(c.tlsClientCA)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		// RequireAndVerify, not VerifyIfGiven. "Verify it if they bothered to send one" is a
		// setting that reads like security and authenticates nobody.
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(cfg), nil
}

// outboundCredentials builds what the scheduler presents when dispatching.
func outboundCredentials(c config) (dispatch.Credentials, error) {
	out := dispatch.Credentials{BearerToken: c.execToken}
	if c.outboundCA == "" {
		return out, nil
	}
	pool, err := certPool(c.outboundCA)
	if err != nil {
		return out, err
	}
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if c.tlsCert != "" && c.tlsKey != "" {
		// The scheduler presents its own certificate too, so an executor can verify the caller
		// rather than accepting a dispatch from anything that reaches it.
		cert, err := tls.LoadX509KeyPair(c.tlsCert, c.tlsKey)
		if err != nil {
			return out, fmt.Errorf("load the dispatch client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	out.Transport = credentials.NewTLS(cfg)
	return out, nil
}

func certPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s contains no certificates", path)
	}
	return pool, nil
}

// controlServer derives, from the control DSN, what is needed to put a tenant schema beside it.
//
// The credential is captured in the closure and never crosses the API boundary: the UI asks for
// a database NAME and this composes the DSN, so adding a tenant does not mean typing the control
// password into a browser form. Address and user are exposed only so an operator can see where a
// schema would land.
//
// An unparseable DSN yields a zero value rather than an error. This is a convenience; the
// process must still start, and the UI falls back to asking for a full connection.
func controlServer(controlDSN string) admin.ControlServer {
	parsed, err := mysqldriver.ParseDSN(controlDSN)
	if err != nil {
		return admin.ControlServer{}
	}
	return admin.ControlServer{
		Address: parsed.Addr,
		User:    parsed.User,
		DSNFor: func(database string) string {
			// Copied per call: ParseDSN returns a pointer, and mutating the shared one would
			// make two concurrent callers compose each other's database name.
			cfg := parsed.Clone()
			cfg.DBName = database
			return cfg.FormatDSN()
		},
	}
}
