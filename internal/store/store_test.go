package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
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

// Every ownership-bearing UPDATE must carry the fence: a token predicate AND fence_epoch.
//
// This is the invariant the whole protocol rests on — a revived zombie is harmless only
// because its epoch no longer matches, so every statement it attempts affects zero rows. It
// is asserted by reading the package's own source rather than by exercising a database,
// because the failure it guards against is a future edit dropping one of the two predicates,
// which no functional test notices until two executors overlap in production.
//
// Counting predicates is not enough: an UPDATE stripped of both run_token and fence_epoch
// still has a status predicate, and would pass. The check therefore names them.
//
// The exceptions are listed explicitly, each with the reason it has no owner to fence
// against. A new statement is fenced or it is on this list with an argument.
func TestOwnershipUpdatesCarryTheFence(t *testing.T) {
	// An UPDATE needs a fence only when it can affect a RUNNING attempt. Two exemptions,
	// both derived from the statement itself rather than from a list of blessed strings, so
	// that a statement which stops qualifying also stops being exempt:
	//
	//  1. it writes no ownership or lifecycle column at all — the schedule-clock writes in
	//     materialize.go, which run under the state row's lock and concern when the job is
	//     next due, not who is running it;
	//  2. its guard proves no owner exists, or names the holder deliberately.
	ownershipColumns := map[string]bool{
		"active_kind": true, "active_execution": true, "active_owner": true,
		"active_run_token": true, "dispatched_to": true, "owner_instance": true,
		"run_token": true, "fence_epoch": true, "lease_until": true, "heartbeat_at": true,
		"deadline_at": true, "timeout_at": true, "status": true,
		"attempt_no": true, "recovery_count": true,
	}
	// The first two exemptions are self-proving: a row whose status is pinned to `ready`, or
	// a job whose active_kind is pinned NULL, cannot have an owner, so there is nothing a
	// fence could protect. They cannot be abused, because "acts on a running attempt" and
	// "guarded on status = 'ready'" are contradictory.
	//
	// The third is NOT self-proving — it acts on a row that IS owned — so it additionally
	// requires the statement to be the operator cancel itself. Without that second condition
	// the entry is a door any future ownership write could walk through by copying one
	// predicate.
	guardExemptions := []struct {
		predicate  string
		alsoAssign string
		why        string
	}{
		{"active_kind IS NULL", "", "acquisition: the job is unheld, and that IS the guard"},
		{"status = 'ready'", "", "the row has never been dispatched, so it carries no token"},
		{"status IN ('dispatching', 'running')", "status = 'cancel_requested'",
			"operator cancel: acts against whoever holds the job, by design"},
	}

	var owned, exempt int
	for _, s := range sqlStatementsInPackage(t) {
		norm := strings.Join(strings.Fields(s), " ")
		if !strings.HasPrefix(strings.ToUpper(norm), "UPDATE ") {
			continue
		}
		// Scoped to the two tables that carry the ownership protocol. job_executor is a
		// registration table: it has no owner, no token and no epoch, and an executor's
		// heartbeat is authenticated by the id it presents rather than fenced.
		if !touchesProtocolTable(norm) {
			continue
		}
		where := whereClause(norm)
		if where == "" {
			t.Errorf("UPDATE with no WHERE clause:\n  %s", norm)
			continue
		}

		touchesOwnership := false
		for _, a := range setAssignments(norm) {
			col := unqualified(a.column)
			if ownershipColumns[col] {
				touchesOwnership = true
				break
			}
		}
		if !touchesOwnership {
			exempt++
			continue
		}

		// A disjunction voids every exemption. The predicates below are only self-proving as
		// top-level conjuncts: `WHERE status = 'ready' OR status = 'running'` contains the
		// exempting substring while being able to mutate a running row without a fence, so a
		// substring match alone would let the test pass the exact defect it exists to prevent.
		disjunctive := strings.Contains(strings.ToUpper(where), " OR ")

		var excused bool
		for _, e := range guardExemptions {
			if disjunctive || !strings.Contains(where, e.predicate) {
				continue
			}
			if e.alsoAssign != "" && !strings.Contains(norm, e.alsoAssign) {
				continue
			}
			excused = true
			break
		}
		if excused {
			exempt++
			continue
		}

		hasToken := strings.Contains(where, "run_token = ?") || strings.Contains(where, "active_run_token = ?")
		hasEpoch := strings.Contains(where, "fence_epoch = ?")
		if !hasToken || !hasEpoch {
			t.Errorf("ownership UPDATE missing its fence (token=%v epoch=%v); "+
				"a zombie holding a stale epoch would be able to write:\n  %s", hasToken, hasEpoch, norm)
			continue
		}
		owned++
	}
	if owned < 6 {
		t.Fatalf("found only %d fenced UPDATEs; the extractor is probably broken", owned)
	}
	if exempt < 3 {
		t.Fatalf("only %d statements matched an exemption; the predicates have drifted from the SQL", exempt)
	}
}

// Every guarded UPDATE must increment write_seq.
//
// Without it, "zero rows affected" is ambiguous: MySQL reports rows CHANGED, and a heartbeat
// or progress report redelivered inside the same whole second writes its DATETIME columns
// back unchanged. The protocol reads zero rows as fencing, so the ambiguity would abort a
// healthy long-running handler because one response packet was lost.
func TestEveryUpdateBumpsWriteSeq(t *testing.T) {
	var checked int
	for _, s := range sqlStatementsInPackage(t) {
		norm := strings.Join(strings.Fields(s), " ")
		if !strings.HasPrefix(strings.ToUpper(norm), "UPDATE ") {
			continue
		}
		checked++
		if !strings.Contains(norm, "write_seq = write_seq + 1") &&
			!strings.Contains(norm, "js.write_seq = js.write_seq + 1") {
			t.Errorf("UPDATE does not increment write_seq, so a no-op repeat would be "+
				"indistinguishable from a failed guard:\n  %s", norm)
		}
	}
	if checked < 8 {
		t.Fatalf("found only %d UPDATE statements; the extractor is probably broken", checked)
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
			if !ownership[unqualified(a.column)] {
				continue
			}
			assignments++
			if !ownershipValueIsSafe(unqualified(a.column), a.value) {
				t.Errorf("ownership column %s assigned %q, which is not one of the forms that "+
					"provably carry no Go time.Time:\n  %s", a.column, a.value, norm)
			}
		}
	}
	if assignments < 8 {
		t.Fatalf("checked only %d ownership assignments; the extractor is probably broken", assignments)
	}

	// And the comparisons, by the same rule as the assignments: whatever an ownership column
	// is measured against must derive from NOW() and nothing else. `heartbeat_at >=
	// TIMESTAMPADD(SECOND, ?, NOW())` is how every liveness window is expressed, so the check
	// has to understand expressions rather than match a single token.
	cmp := regexp.MustCompile(`(?i)(?:\w+\.)?\b(lease_until|heartbeat_at|deadline_at|timeout_at)\s*(<=|>=|<|>|=)\s*`)
	var comparisons int
	for _, s := range sqlStatementsInPackage(t) {
		norm := strings.Join(strings.Fields(s), " ")
		i := strings.Index(strings.ToUpper(norm), " WHERE ")
		if i < 0 {
			continue
		}
		where := norm[i:]
		for _, loc := range cmp.FindAllStringSubmatchIndex(where, -1) {
			col := strings.ToLower(where[loc[2]:loc[3]])
			rhs := balancedExpr(where[loc[1]:])
			comparisons++
			if !ownershipValueIsSafe(col, rhs) {
				t.Errorf("ownership column %s compared against %q, which is not derived from NOW():\n  %s",
					col, rhs, norm)
			}
		}
	}
	if comparisons < 4 {
		t.Fatalf("checked only %d ownership comparisons; the extractor is probably broken", comparisons)
	}
}

// balancedExpr reads one SQL expression from the front of s, stopping at the first top-level
// boundary — a closing paren that was never opened here, or a keyword that ends the term.
func balancedExpr(s string) string {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return strings.TrimSpace(s[:i])
			}
			depth--
		case ' ':
			if depth == 0 {
				rest := strings.ToUpper(s[i+1:])
				for _, kw := range []string{"AND ", "OR ", "ORDER ", "LIMIT ", "GROUP "} {
					if strings.HasPrefix(rest, kw) {
						return strings.TrimSpace(s[:i])
					}
				}
			}
		case ',':
			if depth == 0 {
				return strings.TrimSpace(s[:i])
			}
		}
	}
	return strings.TrimSpace(s)
}

// Business columns must never be compared against the database clock — the mirror of the rule
// above, and the one that fires an hour early after a zone change.
func TestBusinessColumnsDoNotCompareAgainstNow(t *testing.T) {
	cmp := regexp.MustCompile(`(?i)(?:\w+\.)?\b(available_at|scheduled_at|next_fire_at|next_poll_at|started_at|finished_at)\s*(<=|>=|<|>|=)\s*([A-Za-z0-9_?]+\(?\)?)`)
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

// unqualified strips any table alias, so `je.lease_until` is recognised as the ownership
// column it is. Stripping one hard-coded prefix meant a future `SET je.status = ...` would
// have been treated as touching nothing — and the count floors elsewhere in these tests are
// met by unrelated statements, so nothing would have failed.
func unqualified(col string) string {
	col = strings.ToLower(strings.TrimSpace(col))
	if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
		col = col[dot+1:]
	}
	return col
}

// ownershipValueIsSafe validates an assignment to an ownership column against the exhaustive
// list of forms that cannot carry a Go time.Time.
//
// "NULL, or contains NOW() somewhere" was too crude in both directions. It rejected
// `IF(? = 'ready', NULL, timeout_at)`, whose every branch is safe, and it would have accepted
// anything at all with NOW() buried in it. A flat "no ? placeholders" rule cannot work either,
// because `TIMESTAMPADD(SECOND, ?, NOW())` binds a seconds count and is the normal way to set
// a lease.
//
// So the safe shapes are named and anything else fails. A new shape is a deliberate addition
// here — which is the point, because this is the check that keeps ownership independent of
// clock skew between scheduler hosts, and that failure only ever appears once two hosts drift.
func ownershipValueIsSafe(col, value string) bool {
	v := strings.TrimSpace(value)
	upper := strings.ToUpper(v)

	switch {
	case upper == "NULL", upper == "NOW()":
		return true
	case unqualified(v) == col: // preserved unchanged
		return true
	}

	if args, ok := callArgs(upper, "TIMESTAMPADD"); ok && len(args) == 3 {
		return strings.TrimSpace(args[2]) == "NOW()"
	}
	if args, ok := callArgs(upper, "COALESCE"); ok {
		for _, a := range args {
			if !ownershipValueIsSafe(col, a) {
				return false
			}
		}
		return true
	}
	if args, ok := callArgs(upper, "IF"); ok && len(args) == 3 {
		// The condition is a discriminator, not a timestamp; only the branches matter.
		return ownershipValueIsSafe(col, args[1]) && ownershipValueIsSafe(col, args[2])
	}
	return false
}

// callArgs splits `NAME(a, b, c)` into its top-level arguments.
func callArgs(v, name string) ([]string, bool) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, name+"(") || !strings.HasSuffix(v, ")") {
		return nil, false
	}
	return splitTopLevel(v[len(name)+1 : len(v)-1]), true
}

// splitTopLevel splits on commas that are not inside parentheses.
func splitTopLevel(s string) []string {
	var out []string
	depth, last := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[last:i]))
				last = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[last:]))
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
// other way round would deadlock against every completion, and the deadlock needs two
// transactions racing on one job to appear — so it shows up under production load and almost
// never in a test.
//
// Checking individual statements cannot catch it: most locking in this package happens across
// several statements, and the state row is taken by a helper the entry point merely calls. So
// this walks each exported method, expands calls into other functions in the package in
// source order, flattens the result into the sequence of tables that method touches, and
// requires job_state to appear before job_execution in every method that touches both.
func TestCanonicalLockOrder(t *testing.T) {
	fns := packageFunctions(t)

	var checked int
	for name, fn := range fns {
		if !fn.exported {
			continue
		}
		seq := flattenTouches(fns, name, map[string]bool{})
		firstState, firstExec := -1, -1
		for i, table := range seq {
			if table == "job_state" && firstState < 0 {
				firstState = i
			}
			if table == "job_execution" && firstExec < 0 {
				firstExec = i
			}
		}
		if firstState < 0 || firstExec < 0 {
			continue // touches at most one of the two; nothing to order
		}
		checked++
		if firstExec < firstState {
			t.Errorf("%s touches job_execution before job_state; the canonical order is "+
				"job_state then job_execution, and reversing it deadlocks against completion.\n  sequence: %v",
				name, seq)
		}
	}
	if checked < 5 {
		t.Fatalf("only %d methods touched both tables; the extractor is probably broken", checked)
	}
}

type packageFunc struct {
	exported bool
	// events is the function body in source order: either "table:<name>" for a SQL statement's
	// table references, or "call:<name>" for a call to another function in this package.
	events []string
}

// packageFunctions builds the call/statement graph the lock-order check walks.
func packageFunctions(t *testing.T) map[string]packageFunc {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	// Package-level SQL constants, so a statement held in one is attributed to the function
	// that uses it rather than being invisible.
	consts := map[string]string{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				for _, spec := range gd.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, n := range vs.Names {
						if i < len(vs.Values) {
							if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
								consts[n.Name] = strings.Trim(lit.Value, "`\"")
							}
						}
					}
				}
			}
		}
	}

	fns := map[string]packageFunc{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				pf := packageFunc{exported: fd.Name.IsExported()}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					switch v := n.(type) {
					case *ast.BasicLit:
						if v.Kind == token.STRING {
							pf.events = append(pf.events, tableEvents(strings.Trim(v.Value, "`\""))...)
						}
					case *ast.Ident:
						if sql, ok := consts[v.Name]; ok {
							pf.events = append(pf.events, tableEvents(sql)...)
						}
					case *ast.CallExpr:
						if id, ok := v.Fun.(*ast.Ident); ok {
							pf.events = append(pf.events, "call:"+id.Name)
						}
						if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
							pf.events = append(pf.events, "call:"+sel.Sel.Name)
						}
					}
					return true
				})
				fns[fd.Name.Name] = pf
			}
		}
	}
	return fns
}

// tableEvents lists the coordination tables a statement references, in the order they appear.
func tableEvents(sql string) []string {
	upper := strings.ToUpper(sql)
	if !strings.HasPrefix(strings.TrimSpace(upper), "SELECT") &&
		!strings.HasPrefix(strings.TrimSpace(upper), "UPDATE") &&
		!strings.HasPrefix(strings.TrimSpace(upper), "INSERT") &&
		!strings.HasPrefix(strings.TrimSpace(upper), "DELETE") {
		return nil
	}
	type hit struct {
		at    int
		table string
	}
	var hits []hit
	for _, name := range []string{"JOB_STATE", "JOB_EXECUTION"} {
		for i := 0; ; {
			j := strings.Index(upper[i:], name)
			if j < 0 {
				break
			}
			at := i + j
			i = at + len(name)
			// JOB_EXECUTION_ATTEMPT is a different table and must not be counted as one.
			if rest := upper[at+len(name):]; strings.HasPrefix(rest, "_") {
				continue
			}
			hits = append(hits, hit{at, strings.ToLower(name)})
		}
	}
	sort.Slice(hits, func(a, b int) bool { return hits[a].at < hits[b].at })
	out := make([]string, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.table)
	}
	return out
}

// flattenTouches expands a function's events into the flat sequence of tables it touches,
// following calls into other functions in this package.
func flattenTouches(fns map[string]packageFunc, name string, seen map[string]bool) []string {
	if seen[name] {
		return nil // recursion; the cycle adds no new ordering information
	}
	seen[name] = true
	defer delete(seen, name)

	var out []string
	for _, ev := range fns[name].events {
		if callee, ok := strings.CutPrefix(ev, "call:"); ok {
			if _, known := fns[callee]; known {
				out = append(out, flattenTouches(fns, callee, seen)...)
			}
			continue
		}
		out = append(out, ev)
	}
	return out
}

// touchesProtocolTable reports whether a statement writes one of the two tables that carry
// the ownership protocol.
func touchesProtocolTable(norm string) bool {
	for _, t := range tableEvents(norm) {
		if t == "job_state" || t == "job_execution" {
			return true
		}
	}
	return false
}

func whereClause(norm string) string {
	i := strings.Index(strings.ToUpper(norm), " WHERE ")
	if i < 0 {
		return ""
	}
	return norm[i+len(" WHERE "):]
}

// Statements must be whole string literals, never assembled with `+`.
//
// Every other static check in this file reads SQL out of the source. A statement built by
// concatenation reaches those checks as a fragment — so its fence, its clocks and its
// write_seq stop being verified, silently, and the checks keep reporting success. That is
// worse than not having them.
func TestNoConcatenatedSQL(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				bin, ok := n.(*ast.BinaryExpr)
				if !ok || bin.Op != token.ADD {
					return true
				}
				for _, side := range []ast.Expr{bin.X, bin.Y} {
					lit, ok := side.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					if looksLikeSQL(strings.Trim(lit.Value, "`\"")) {
						t.Errorf("%s: SQL assembled with `+`; the static checks read whole "+
							"literals and would silently stop verifying this statement",
							fset.Position(bin.Pos()))
					}
				}
				return true
			})
		}
	}
}

func looksLikeSQL(v string) bool {
	head := strings.ToUpper(strings.TrimSpace(v))
	for _, kw := range []string{"SELECT ", "UPDATE ", "INSERT ", "DELETE "} {
		if strings.HasPrefix(head, kw) {
			return true
		}
	}
	return false
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
