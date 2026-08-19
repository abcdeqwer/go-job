package main

import (
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/abcdeqwer/go-job/internal/admin"
	"github.com/abcdeqwer/go-job/internal/control"
)

//go:embed schema/001_control.sql
var controlDDL string

// storedConfig is what setup writes so the next start needs no flags.
//
// Only the control DSN. Everything else already has a working default, and the encryption key
// deliberately does NOT live here: a key stored beside the thing it protects is not a key, and
// an operator who wants one should place it the way they place their other secrets.
type storedConfig struct {
	ControlDSN string `json:"control_dsn"`
}

func loadConfig(path string) (storedConfig, bool) {
	var c storedConfig
	b, err := os.ReadFile(path)
	if err != nil {
		return c, false
	}
	if json.Unmarshal(b, &c) != nil || strings.TrimSpace(c.ControlDSN) == "" {
		return c, false
	}
	return c, true
}

func saveConfig(path string, c storedConfig) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	// 0600: it holds a database password.
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// setupServer is what runs when there is no control database to talk to yet.
//
// It serves the same UI and exactly two endpoints, both behind a token printed to the startup
// log. The token is the whole security model here, and it has to be: before a control database
// exists there are no accounts, so this cannot be authenticated the ordinary way — and an
// UNAUTHENTICATED endpoint that takes a database address and connects to it is not a
// convenience, it is a complete takeover. Point it at a MySQL you control and you own the
// installation: you create its first administrator and read every tenant DSN it is later given.
//
// Printing the token to the log means the person who can configure this is the person who can
// read the container's output, which is the closest thing to "whoever deployed it" that exists
// at this stage.
type setupServer struct {
	token   string
	cfgPath string
	openDB  func(string) (*sql.DB, error)
	log     *slog.Logger
	done    chan struct{}
}

func newSetupToken() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		// Without a token there is no gate, and serving the endpoints anyway would be worse
		// than not starting.
		panic("gojob: cannot generate a setup token: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func (s *setupServer) authorized(r *http.Request) bool {
	got := strings.TrimSpace(r.Header.Get("X-Setup-Token"))
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1
}

func (s *setupServer) handler(ui http.Handler) http.Handler {
	mux := http.NewServeMux()

	// Unauthenticated on purpose: it says only "this installation is not configured", which is
	// already obvious from every other endpoint refusing.
	mux.HandleFunc("GET /api/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"bootstrap_needed": true})
	})

	mux.HandleFunc("POST /api/bootstrap/probe", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "wrong setup token"})
			return
		}
		var body struct {
			DSN string `json:"dsn"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		db, err := s.openDB(body.DSN)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		defer db.Close()
		if err := db.PingContext(r.Context()); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		var tables int
		if err := db.QueryRowContext(r.Context(), `
			SELECT COUNT(*) FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name IN
			      ('tenant_registry','admin_user','executor_identity','control_audit')`).
			Scan(&tables); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"reachable": true, "tables": tables})
	})

	mux.HandleFunc("POST /api/bootstrap/apply", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "wrong setup token"})
			return
		}
		var body struct {
			DSN          string `json:"dsn"`
			CreateTables bool   `json:"create_tables"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if err := s.apply(r, body.DSN, body.CreateTables); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		s.log.Warn("control database configured through setup",
			"config", s.cfgPath, "remote", r.RemoteAddr)
		writeJSON(w, http.StatusOK, map[string]any{"restarting": true})
		// Restart AFTER the response is on its way. The browser polls until the ordinary UI
		// answers; see the setup page.
		go func() {
			time.Sleep(500 * time.Millisecond)
			close(s.done)
		}()
	})

	mux.Handle("/", ui)
	return mux
}

func (s *setupServer) apply(r *http.Request, dsn string, create bool) error {
	db, err := s.openDB(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(r.Context()); err != nil {
		return err
	}

	var tables int
	if err := db.QueryRowContext(r.Context(), `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN
		      ('tenant_registry','admin_user','executor_identity','control_audit')`).
		Scan(&tables); err != nil {
		return err
	}
	switch {
	case tables == 4:
		// Already provisioned — adopting it is the normal "point a new container at the
		// existing database" case.
	case tables == 0 && create:
		for _, stmt := range admin.SplitDDL(controlDDL) {
			if _, err := db.ExecContext(r.Context(), stmt); err != nil {
				return fmt.Errorf("applying the control schema failed: %w", err)
			}
		}
	case tables == 0:
		return errors.New("that database is empty; tick \"create the tables\" to provision it")
	default:
		return fmt.Errorf("that database holds %d of the four control tables — a partial "+
			"schema. Finish or drop it by hand; guessing here is worse", tables)
	}
	return saveConfig(s.cfgPath, storedConfig{ControlDSN: dsn})
}

// restart replaces this process with a fresh one, same arguments.
//
// Re-exec rather than wiring the whole runtime up lazily: everything downstream of the control
// database — the registry, the engines, the gRPC service, the admin API — is built once at
// startup from a DSN that now exists, and a second construction path for it would be a second
// place for startup to be subtly different. The cost is about a second of downtime on an
// installation that is not yet running anything.
func restart() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	return syscall.Exec(exe, os.Args, os.Environ())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var _ = control.SchemaVersion

// runSetup serves the setup page until it is configured, then re-execs.
func runSetup(c config, log *slog.Logger) error {
	loc, err := time.LoadLocation(c.location)
	if err != nil {
		return fmt.Errorf("-location %q: %w", c.location, err)
	}
	s := &setupServer{
		token:   newSetupToken(),
		cfgPath: c.configPath,
		openDB:  func(dsn string) (*sql.DB, error) { return sql.Open("mysql", withDefaults(dsn, loc)) },
		log:     log,
		done:    make(chan struct{}),
	}

	// Framed so it survives a wall of JSON. This is the one line an operator has to find.
	fmt.Fprintf(os.Stderr, "\n"+
		"┌───────────────────────────────────────────────────────────────────┐\n"+
		"│  go-job is NOT CONFIGURED — open http://%-26s│\n"+
		"│  and enter this setup token:                                      │\n"+
		"│                                                                   │\n"+
		"│      %-61s│\n"+
		"│                                                                   │\n"+
		"│  It is printed once, only while unconfigured, and gates the form  │\n"+
		"│  that tells this process which database to use.                   │\n"+
		"└───────────────────────────────────────────────────────────────────┘\n\n",
		c.adminAddr, s.token)

	srv := &http.Server{
		Addr:              c.adminAddr,
		Handler:           s.handler(admin.UI()),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-s.done
		_ = srv.Close()
	}()
	log.Info("setup listening", "addr", c.adminAddr, "config", c.configPath)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	log.Info("configured; restarting")
	return restart()
}
