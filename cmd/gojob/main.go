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
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"

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

	executorLiveness  time.Duration
	executorRetention time.Duration
	sessionTTL        time.Duration

	trustedUserHeader string
	trustedRoleHeader string
	cookieSecure      bool

	hashPassword string
}

func run() error {
	var c config
	flag.StringVar(&c.controlDSN, "control-dsn", env("GOJOB_CONTROL_DSN", ""),
		"MySQL DSN for the control database")
	flag.StringVar(&c.dsnKeyHex, "dsn-key", env("GOJOB_DSN_KEY", ""),
		"hex-encoded 16, 24 or 32 byte key that tenant DSNs are encrypted with")
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
	flag.DurationVar(&c.sessionTTL, "session-ttl", 12*time.Hour, "admin session lifetime")

	flag.StringVar(&c.trustedUserHeader, "trusted-user-header", env("GOJOB_TRUSTED_USER_HEADER", ""),
		"trust this header for identity instead of built-in login; empty disables the mode")
	flag.StringVar(&c.trustedRoleHeader, "trusted-role-header", env("GOJOB_TRUSTED_ROLE_HEADER", ""),
		"header carrying VIEWER or OPERATOR when trusted-user-header is set")
	flag.BoolVar(&c.cookieSecure, "cookie-secure", env("GOJOB_COOKIE_SECURE", "") != "",
		"mark the session cookie Secure; set this when serving over TLS")

	flag.StringVar(&c.hashPassword, "hash-password", "",
		"print a bcrypt hash for this password and exit, for provisioning the first account")
	flag.Parse()

	if c.hashPassword != "" {
		h, err := admin.HashPassword(c.hashPassword)
		if err != nil {
			return err
		}
		fmt.Println(h)
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
	if err != nil {
		return err
	}
	fence := control.NewFence(clock, c.stalenessLimit)
	disp := dispatch.NewClient(5*time.Second, 10*time.Second)

	reg := runtime.NewRegistry(runtime.Options{
		InstanceID:      c.instanceID,
		Clock:           clock,
		PollInterval:    c.pollInterval,
		StalenessLimit:  c.stalenessLimit,
		MaxOpenConns:    16,
		MaxIdleConns:    4,
		ConnMaxLifetime: 30 * time.Minute,
		OpenDB: func(dsn string) (*sql.DB, error) {
			return sql.Open("mysql", withDefaults(dsn, loc))
		},
		Engine: engine.Config{
			ScanInterval:      c.scanInterval,
			RecoverInterval:   c.recoverInterval,
			ReapInterval:      c.reapInterval,
			MisfireGrace:      gojob.DefaultMisfireGrace(c.scanInterval),
			ExecutorLiveness:  c.executorLiveness,
			ExecutorRetention: c.executorRetention,
			PageSize:          100,
			BackoffBase:       5 * time.Second,
			BackoffMax:        5 * time.Minute,
			ReconcileDeadline: gojob.ReconcileDeadline,
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

	grpcSrv := grpc.NewServer()
	gojobv1.RegisterJobSchedulerServer(grpcSrv, server.New(server.Config{
		HeartbeatInterval: c.executorLiveness / 3,
		RegistrationTTL:   c.executorLiveness,
		ProgressInterval:  30 * time.Second,
		SilenceDeadline:   c.executorLiveness,
	}, reg, disp, fence, clock, func() int { return 30 }, log))

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
		return nil, errors.New("-dsn-key is required; it decrypts the DSNs in the registry, " +
			"and it must be the same key across every instance and every restart")
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
	add := map[string]string{
		"parseTime": "true",
		"loc":       loc.String(),
		"time_zone": "'" + offsetOf(loc) + "'",
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

// offsetOf renders a location as the numeric offset MySQL's time_zone accepts without the
// timezone tables loaded — which most installations do not have.
func offsetOf(loc *time.Location) string {
	_, off := time.Now().In(loc).Zone()
	sign := "+"
	if off < 0 {
		sign, off = "-", -off
	}
	return fmt.Sprintf("%s%02d:%02d", sign, off/3600, (off%3600)/60)
}
