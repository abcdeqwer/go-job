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
the scheduler at the instant rather than at the next scan boundary. Losing it degrades
dispatch latency to the scan interval. It cannot lose a run.

### Cron dialect

Six fields, seconds first:

```text
second  minute  hour  day-of-month  month  day-of-week
```

with steps (`*/15`), ranges (`1-5`), lists (`1,3,5`), and named weekdays and months
(`MON`, `FRI`, `JAN`). Expressions are evaluated in the configured business `Location`.

The dialect is fixed rather than pluggable. A scheduler with two cron dialects has two sets
of edge cases, and the cases where they differ are exactly the ones nobody tests.

Syntax alone is not a specification, so the ambiguous cases are decided here. Without these
rules, two conforming implementations produce different fire instants and the cron
differential test has no single correct answer:

| Case | Rule |
| --- | --- |
| day-of-month **and** day-of-week both restricted | **OR** — fire when either matches, as Vixie cron and Quartz do. `0 0 0 1 * MON` fires on the 1st *and* every Monday |
| day-of-month or day-of-week is `*` | the other alone decides; `*` adds nothing |
| day-of-week numbering | `0` and `7` both mean Sunday |
| `L`, `W`, `#` | **not supported.** Rejected at validation, not silently ignored |
| a field that cannot match (`0 0 0 31 2 *`) | rejected at validation rather than never firing |
| **nonexistent local time** (spring forward) | fire at the first valid instant after the gap, once |
| **ambiguous local time** (fall back) | fire at the **first** occurrence only; the repeat is skipped |
| zone | always the tenant's configured `Location`, never the host's |

The two DST rules exist because `DATETIME` carries no offset. During a fall-back hour two
real instants share one wall time, so an execution key derived from wall time would collide
and the second occurrence would be silently deduplicated. Firing once, on the first, makes
that deliberate rather than accidental. A zone without DST — the common case for a business
calendar — never reaches either rule.

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

A poller waits a configured delay after each pass **completes**. It is a repeating pass, not
a sequence of scheduled instants, and the difference matters: a cron job that fires every
three seconds will pile up when a run takes four, while a fixed-delay job cannot.

- the durable unit of work stays the poller's own source table, with its own claim
  contract. The scheduler tracks passes; the source table tracks work;
- an execution row is written only when a pass actually did something, or failed;
- empty-pass rate, source backlog and last successful pass are metrics, so an idle poller is
  still visibly alive;
- a poller is never reshaped into cron just to fit the row model.

The reason for not writing a row per pass is arithmetic: a three-second poller produces
28,800 executions a day, per tenant, almost all recording that there was nothing to do.
That is not history, it is noise with a storage bill.

### The loop lives in the scheduler, one pass at a time

There is no long-lived loop inside an executor. The scheduler drives each pass:

```text
next_poll_at <= now
  -> take the job_state lock
  -> create an ordinary execution row (trigger_type = 'poll')
  -> dispatch it, exactly like a cron execution
  -> executor runs one pass, reports {did_work, summary}
  -> release the lock, set next_poll_at = result time + delay
  -> if did_work = false and the pass succeeded, DELETE the execution row
```

Because `next_poll_at` is computed from the **result**, the delay is measured from
completion, which is what makes a fixed-delay job a poller rather than a cron job: a pass
that takes longer than the delay never overlaps its successor.

Two properties fall out of using the same `job_state` lock as everything else:

- **exactly one pass at a time**, across the whole scheduler cluster and every executor, for
  free. No separate mechanism, no separate policy;
- **a manual trigger cannot overlap a pass**, because it competes for the same lock. And it
  cannot starve either, because a pass holds the lock only for its own duration — unlike a
  long-lived loop lease, which would hold it forever and make the trigger button silently
  useless.

That second point is worth dwelling on: an earlier design had the executor hold a loop lease
indefinitely, which preserved mutual exclusion perfectly while making manual triggers
unreachable for every polling job. Dispatching one pass at a time removes the possibility
rather than working around it.

### A pass is an ordinary execution, deleted if it found nothing

A pass creates a real `job_execution` row **before** it is dispatched, and is claimed,
leased, fenced, recovered and completed by exactly the same code as a cron execution. On a
successful pass reporting `did_work = false`, the terminal transaction **deletes** the row.

An earlier revision tried to avoid the write entirely by holding the in-flight pass only on
the state row, with no execution row until the result proved it worth keeping. That is
broken in two ways, and both are worth recording so the idea is not reinvented:

- **it breaks exclusion across a scheduler failure.** Scheduler A dispatches a pass to
  executor X and dies. The state-row lease expires. Recovery has no execution row to
  reconcile from, so it cannot ask X whether the pass is still running — it can only clear
  the holder, after which B dispatches a second pass into the same queue while X is still
  writing;
- **it cannot reconstruct the run.** If the executor reports `did_work = true` to a
  *different* scheduler instance, that instance must create the history row — but the
  parameters, scheduled instant, attempt number and budgets existed only in the dead
  instance's memory.

Creating the row first and deleting empty ones costs an insert and a delete per idle pass —
roughly one write per three seconds for the fastest poller — in exchange for one execution
model instead of two, and for exclusion that survives a scheduler dying. A failed pass is
never deleted: an error is evidence regardless of whether it found work.

The end state is the same as the goal that motivated the original idea: **a three-second
poller does not accumulate 28,800 rows a day.** It simply reaches that by deleting rather
than by never writing.

Liveness for an idle poller comes from metrics — last successful pass, empty-poll rate,
source backlog — not from execution rows. `admin.md` shows these on the job page so an idle
poller is still visibly alive rather than looking abandoned.

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
