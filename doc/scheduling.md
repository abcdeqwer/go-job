# Scheduling

Two kinds of scheduled work exist, and they are modelled separately because they are
different things. A cron job's identity is a **fire instant**; a poller's identity is a
**loop**. Forcing either into the other's model produces either lost runs or millions of
rows recording that nothing happened.

---

## 1. Cron jobs

Each fire instant becomes one durable execution with a deterministic key.

### The state row is the authority; the timer is not

`job_state.next_fire_at` is what makes a job due. The in-process timer heap is a **latency
optimization and nothing more**.

This ordering is worth stating explicitly because the obvious design gets it backwards. If
the timer inserted the execution row and a poll over `job_execution` were the fallback,
then losing a timer callback would lose the run outright: no row was ever written, so
polling executions finds nothing to recover. The fallback would be watching the wrong
table, and any test asserting "a lost timer still runs" would be unsatisfiable by
construction.

### Materialization

One transaction over the state row creates cron executions, and nothing else does:

```text
scan job_state for next_fire_at <= business now
  -> lock the state row (FOR UPDATE SKIP LOCKED)
  -> re-read job_definition: enabled, schedule, version
  -> if config_version lags, recompute from the current schedule
  -> compute the due fire instant(s) and apply the misfire policy
  -> INSERT the execution with its deterministic execution_key
  -> advance next_fire_at and config_version in the same statement
  -> commit
```

Every replica runs this scan on a short interval, and correctness never depends on the
timer:

- **a lost timer callback** costs at most one scan interval, because the state row still
  says the job is due;
- **a process restart** needs no separate heap-rebuild step — the scan rebuilds the heap as
  a side effect of finding due rows;
- **two replicas racing** do not both reach the insert: the state row is taken
  `FOR UPDATE SKIP LOCKED`, so the loser skips this job on that pass and on its next pass
  observes a `next_fire_at` the winner already advanced. The unique key on `execution_key`
  remains as defence in depth for paths that do not hold this lock — a manual trigger, a
  retried transaction — not as the primary guard against concurrent scanners;
- **`next_fire_at` has exactly one writer path**, the locked transaction above, which is
  what removes the lost-update hazard a separately-owned timer heap would create.

The timer heap then does what it is good at: for a job due in the next few seconds, wake
the worker at the instant rather than at the next scan boundary. Losing it degrades
dispatch latency to the scan interval. It cannot lose a run.

### Cron dialect

Six fields, seconds first:

```text
second  minute  hour  day-of-month  month  day-of-week
```

with steps (`*/15`), ranges (`1-5`), lists (`1,3,5`), and named weekdays and months
(`MON`, `FRI`, `JAN`). Expressions are evaluated in the configured business `Location`.

The dialect is fixed rather than pluggable. A scheduler with two cron dialects has two sets
of edge cases around day-of-week numbering, `L`/`#` handling and month boundaries, and the
cases where they differ are exactly the ones nobody tests.

### Misfire

Applies to fire instants that passed while nothing was running:

| Policy | Behaviour |
| --- | --- |
| `SKIP` | advance to the first future fire; record how many were missed |
| `FIRE_ONCE` | create one catch-up execution for the **latest** missed fire, then advance |

Unbounded replay is not offered. An outage of an hour must not become an hour of catch-up
executions arriving at once — that turns a recovery into a second incident. A bounded
catch-up policy would need an explicit maximum, a defined ordering and a load test before
it could exist.

---

## 2. Fixed-delay pollers

A poller waits a configured delay after each pass completes. It is a long-lived loop, not a
sequence of scheduled instants.

- the durable unit of work stays the poller's own source table, with its own claim
  contract;
- a scheduler execution row is written only when real work is accepted, a manual trigger
  runs, or a failure needs durable evidence;
- loop liveness, empty-poll rate, source backlog and last successful poll are metrics, so
  an idle poller is still visibly alive;
- a poller is never reshaped into cron just to fit the row model.

The reason for not writing a row per tick is arithmetic: a three-second poller produces
28,800 executions a day, per tenant, almost all recording that there was nothing to do.
That is not history, it is noise with a storage bill.

### One loop per job, held by a lease

Every fixed-delay job runs as a **singleton loop**. The loop holds a lease on the job's
`job_state` row with `active_kind = 'LOOP'`, renewed by heartbeat exactly like an execution
lease, so only one loop per tenant and job is active anywhere in the fleet.

Acquire:

```sql
UPDATE job_state
SET active_kind      = 'LOOP',
    active_worker_id = ?,
    active_run_token = ?,
    fence_epoch      = fence_epoch + 1,
    lease_until      = TIMESTAMPADD(SECOND, ?, NOW()),
    heartbeat_at     = NOW(),
    updated_at       = ?
WHERE job_name = ? AND ops_paused = 0 AND active_kind IS NULL;
```

Renew is the state-row heartbeat of `protocol.md` §5. Release on clean exit:

```sql
UPDATE job_state
SET active_kind = NULL, active_worker_id = NULL, active_run_token = NULL,
    lease_until = NULL, updated_at = ?
WHERE job_name = ? AND active_run_token = ? AND fence_epoch = ?;
```

A loop that loses its renewal stops immediately, exactly like an execution that loses its
fence. A loop whose process dies leaves an expired lease that recovery clears
(`protocol.md` §7).

This is also why a poller does not need a separate concurrency policy: the same single
holder that excludes two executions excludes two loops, and excludes a loop from racing a
manual run.

### Manual trigger: the loop consumes it

A manual trigger for a fixed-delay job **must not go through the normal claim path.**

This is the trap worth naming, because the natural design is wrong in a way that looks
right: if the manual execution had to claim the job like any other, it would need
`active_kind IS NULL` — and a healthy loop holds `active_kind = 'LOOP'` indefinitely. Under
`QUEUE` the manual run is deferred forever; under `FORBID` it is marked `skipped`
immediately. Mutual exclusion would be perfectly preserved and the trigger button would
silently do nothing, forever, for every polling job.

The fix is that **the lock holder does the work**, rather than a second actor competing for
a lock it can never win:

- the control plane creates the manual execution row exactly as for a cron job,
  `trigger_type = 'manual'`, status `ready`;
- the running loop checks for one at the top of each pass and, if present, claims it
  **under the lease it already holds** — same token, same fence epoch, no new acquisition —
  runs the handler once, and completes the execution normally;
- if no loop is running (paused, crashed, disabled), the row stays `ready` and is picked up
  by the next loop that acquires the lease, so a trigger issued during a restart is delayed
  rather than lost;
- the execution row records the run like any other, so history, retry and audit are
  unchanged.

The bound is therefore one poll interval, not indefinite. `verification.md` asserts this as
a **liveness** property, because an assertion that merely proves the manual run does not
overlap the loop would pass against the broken design.

---

## 3. Configuration changes

### Drift needs its own scan

The due scan cannot be the only reader of `config_version`, because it only visits rows
that are already due. An operator who changes a weekly job's cron expression would
otherwise see nothing happen until the old `next_fire_at` arrives a week later — the edit
accepted, audited, displayed in the UI, and silently inert.

A second short-interval scan looks for drift rather than due-ness:

```sql
SELECT s.job_name
FROM job_state s
JOIN job_definition d ON d.job_name = s.job_name
WHERE s.config_version <> d.version;
```

Each hit is recomputed in the same locked transaction shape as materialization: lock the
state row, re-read the definition, recompute `next_fire_at` from the new schedule, set
`config_version = d.version`. It is a join over two small tables, so it costs nothing at
any interval worth choosing.

An operator therefore sees a schedule edit take effect within seconds, regardless of when
the job was next due.

### Business clock changes

If the host's `Clock` is shifted — which is a testing facility, not a production one — every
cron `next_fire_at` computed under the old clock is wrong.

The rule is deliberately blunt: **changing the clock recomputes every cron state row for
that tenant**, synchronously, one locked row at a time, using the same recomputation
transaction. A tenant has a bounded number of jobs, so this is a short loop that needs no
revision counter and no second notion of drift.

Because a process might be down when a shift happens, acquiring the `CONTROL_PLANE` role
also recomputes all cron rows once at startup. That closes the only window in which a shift
could be missed.

One recomputation transaction, reached from three triggers — due scan, config drift, clock
change — rather than three paths that can disagree.
