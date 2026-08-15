package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"

	gojob "github.com/abcdeqwer/go-job"
)

func TestResolvedStatus(t *testing.T) {
	base := Stale{Status: gojob.StatusRunning, AttemptNo: 1, MaxAttempts: 3, RecoveryCount: 0, MaxRecoveries: 3}

	with := func(f func(*Stale)) Stale {
		v := base
		f(&v)
		return v
	}

	cases := []struct {
		name string
		in   Stale
		want gojob.Status
	}{
		{"budget left, retry", base, gojob.StatusReady},
		{"attempts exhausted", with(func(v *Stale) { v.AttemptNo = 3 }), gojob.StatusDead},
		{"attempts overshot", with(func(v *Stale) { v.AttemptNo = 4 }), gojob.StatusDead},
		{"this recovery is the last", with(func(v *Stale) { v.RecoveryCount = 2 }), gojob.StatusDead},
		{"recoveries left", with(func(v *Stale) { v.RecoveryCount = 1 }), gojob.StatusReady},

		// A cancel that has been asked for never goes back to ready, whatever budget remains:
		// an executor dying is not a reason to start work someone asked to stop.
		{"cancel requested", with(func(v *Stale) { v.Status = gojob.StatusCancelRequested }), gojob.StatusCancelled},
		{"cancel requested with budget", with(func(v *Stale) {
			v.Status = gojob.StatusCancelRequested
			v.AttemptNo = 0
		}), gojob.StatusCancelled},

		// The cap outranks remaining attempt budget. A cap that applies only when the budget
		// happens to be spent is not a cap.
		{"timed out with budget left", with(func(v *Stale) { v.TimeoutExpired = true }), gojob.StatusDead},

		// ...but a cancel outranks the cap, because the cancel names the outcome an operator
		// asked for and `dead` would report a failure nobody caused.
		{"cancel requested and timed out", with(func(v *Stale) {
			v.Status = gojob.StatusCancelRequested
			v.TimeoutExpired = true
		}), gojob.StatusCancelled},

		// dispatching is recovered exactly like running: the scheduler may have died after
		// the executor accepted.
		{"dispatching retries", with(func(v *Stale) { v.Status = gojob.StatusDispatching }), gojob.StatusReady},
	}
	for _, c := range cases {
		if got := resolvedStatus(c.in); got != c.want {
			t.Errorf("%s: resolvedStatus = %q, want %q", c.name, got, c.want)
		}
	}
}

// Every UPDATE in this package must carry a guard beyond its primary key.
//
// This is the invariant the whole protocol rests on: "no handler updates status with a bare
// WHERE id = ?". It is asserted by reading the package's own source rather than by exercising
// a database, because the failure it guards against is a future edit dropping a token or
// epoch predicate — which no functional test notices until two executors overlap in
// production.
func TestEveryUpdateIsGuarded(t *testing.T) {
	stmts := sqlStatementsInPackage(t)
	if len(stmts) < 10 {
		t.Fatalf("found only %d SQL literals; the extractor is probably broken", len(stmts))
	}

	var updates int
	for _, s := range stmts {
		norm := strings.Join(strings.Fields(s), " ")
		if !strings.HasPrefix(strings.ToUpper(norm), "UPDATE ") {
			continue
		}
		updates++

		where := whereClause(norm)
		if where == "" {
			t.Errorf("UPDATE with no WHERE clause:\n  %s", norm)
			continue
		}
		// A primary-key predicate alone is never sufficient.
		guards := strings.Count(where, "AND")
		if guards == 0 {
			t.Errorf("UPDATE guarded only by its key, with no status/token/epoch predicate:\n  %s", norm)
		}
	}
	if updates < 8 {
		t.Fatalf("found only %d UPDATE statements; the extractor is probably broken", updates)
	}
}

// Ownership columns must be written from the database clock, never from a Go time.
//
// A scheduler comparing a lease against its own host clock makes ownership depend on skew
// between machines, and the resulting double-execution appears only when two hosts drift —
// so it survives every test that runs on one machine. Asserting it on the source is the only
// place it is cheap to catch.
func TestOwnershipColumnsUseDatabaseClock(t *testing.T) {
	ownership := map[string]bool{
		"lease_until": true, "heartbeat_at": true, "deadline_at": true, "timeout_at": true,
	}

	var assignments int
	for _, s := range sqlStatementsInPackage(t) {
		norm := strings.Join(strings.Fields(s), " ")
		if !strings.HasPrefix(strings.ToUpper(norm), "UPDATE ") {
			continue
		}
		for _, a := range setAssignments(norm) {
			if !ownership[strings.ToLower(a.column)] {
				continue
			}
			assignments++
			upper := strings.ToUpper(a.value)
			// A conditional assignment is fine as long as every branch is NULL or NOW()-
			// derived; checking for the absence of a bare placeholder catches the case the
			// rule exists for, which is a Go time.Time being bound into an ownership column.
			if upper != "NULL" && !strings.Contains(upper, "NOW()") {
				t.Errorf("ownership column %s assigned %q, which is neither NULL nor derived from NOW():\n  %s",
					a.column, a.value, norm)
			}
		}
	}
	if assignments < 8 {
		t.Fatalf("checked only %d ownership assignments; the extractor is probably broken", assignments)
	}

	// And the comparisons: an ownership column may only be compared against NOW().
	cmp := regexp.MustCompile(`(?i)\b(lease_until|heartbeat_at|deadline_at|timeout_at)\s*(<=|>=|<|>|=)\s*([A-Za-z0-9_?]+\(?\)?)`)
	var comparisons int
	for _, s := range sqlStatementsInPackage(t) {
		norm := strings.Join(strings.Fields(s), " ")
		i := strings.Index(strings.ToUpper(norm), " WHERE ")
		if i < 0 {
			continue
		}
		for _, m := range cmp.FindAllStringSubmatch(norm[i:], -1) {
			comparisons++
			if !strings.EqualFold(m[3], "NOW()") {
				t.Errorf("ownership column %s compared against %q rather than NOW():\n  %s", m[1], m[3], norm)
			}
		}
	}
	if comparisons < 4 {
		t.Fatalf("checked only %d ownership comparisons; the extractor is probably broken", comparisons)
	}
}

// Business columns must never be compared against the database clock — the mirror of the rule
// above, and the one that fires an hour early after a zone change.
func TestBusinessColumnsDoNotCompareAgainstNow(t *testing.T) {
	cmp := regexp.MustCompile(`(?i)\b(available_at|scheduled_at|next_fire_at|next_poll_at|started_at|finished_at)\s*(<=|>=|<|>|=)\s*([A-Za-z0-9_?]+\(?\)?)`)
	for _, s := range sqlStatementsInPackage(t) {
		norm := strings.Join(strings.Fields(s), " ")
		i := strings.Index(strings.ToUpper(norm), " WHERE ")
		if i < 0 {
			continue
		}
		for _, m := range cmp.FindAllStringSubmatch(norm[i:], -1) {
			if strings.EqualFold(m[3], "NOW()") {
				t.Errorf("business column %s compared against NOW(); it must take the business clock:\n  %s",
					m[1], norm)
			}
		}
	}
}

type assignment struct{ column, value string }

// setAssignments splits an UPDATE's SET clause on top-level commas, so a function call's own
// arguments — TIMESTAMPADD(SECOND, ?, NOW()) — are not mistaken for separate assignments.
func setAssignments(norm string) []assignment {
	upper := strings.ToUpper(norm)
	start := strings.Index(upper, " SET ")
	if start < 0 {
		return nil
	}
	clause := norm[start+len(" SET "):]
	if i := strings.Index(strings.ToUpper(clause), " WHERE "); i >= 0 {
		clause = clause[:i]
	}

	var out []assignment
	depth, last := 0, 0
	flush := func(part string) {
		eq := strings.Index(part, "=")
		if eq < 0 {
			return
		}
		out = append(out, assignment{
			column: strings.TrimSpace(part[:eq]),
			value:  strings.TrimSpace(part[eq+1:]),
		})
	}
	for i, r := range clause {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				flush(clause[last:i])
				last = i + 1
			}
		}
	}
	flush(clause[last:])
	return out
}

// The canonical lock order is job_state then job_execution. A transaction that took them the
// other way round would deadlock against every completion. Asserted by checking that no
// function in this package locks an execution row before a state row.
func TestNoStatementLocksExecutionBeforeState(t *testing.T) {
	for _, s := range sqlStatementsInPackage(t) {
		norm := strings.ToUpper(strings.Join(strings.Fields(s), " "))
		if !strings.Contains(norm, "FOR UPDATE") {
			continue
		}
		if strings.Contains(norm, "JOB_EXECUTION") && !strings.Contains(norm, "JOB_STATE") {
			t.Errorf("locking read on job_execution outside the state-row lock; "+
				"the canonical order is job_state then job_execution:\n  %s", norm)
		}
	}
}

func whereClause(norm string) string {
	i := strings.Index(strings.ToUpper(norm), " WHERE ")
	if i < 0 {
		return ""
	}
	return norm[i+len(" WHERE "):]
}

// sqlStatementsInPackage returns every string literal in the package's non-test files that
// looks like a SQL statement.
func sqlStatementsInPackage(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	var out []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				v := strings.Trim(lit.Value, "`\"")
				head := strings.ToUpper(strings.TrimSpace(v))
				for _, kw := range []string{"SELECT ", "UPDATE ", "INSERT ", "DELETE "} {
					if strings.HasPrefix(head, kw) {
						out = append(out, v)
						break
					}
				}
				return true
			})
		}
	}
	return out
}
