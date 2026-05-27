# Crash Recovery & Execution Model — Design Note

**Status:** Design only — not implemented as of 2026-05-27. This is the agreed
direction for the data model and crash recovery; it will be built later. The
current engine has the durability gap described below and uses a single
`instances` table.

## Problem

The engine persists an instance after every transition and at every rest point
(suspend / terminal / fail). Two lifecycle states behave very differently when a
worker process crashes:

- **`suspended`** instances are safe. They sit durably until a signal
  (`SignalInstance`) or a timeout wakes them. A crash loses nothing.
- **`running`** instances are *not* safe. `running` means "a worker is actively
  driving this right now." If that worker dies mid-drive, nothing ever moves the
  instance again: it is not `suspended` (so `SignalInstance` returns
  `ErrNotWaiting`), not `done`, not `failed`. It is **stranded forever**.

`running` is entered in two places — `Start` (initial drive) and `Claim`
(resume on a signal) — so both are exposed.

## Agreed direction: split instance state from execution records

Move from one table to **two**, separating the long-lived instance (its
canonical *current* state) from per-**execution** records (what happened during
each *drive*). This is the standard model (cf. Temporal's execution state vs.
event history), and it unifies three things at once: crash recovery, an audit /
journey trail, and signal deduplication.

### `instances` — "where is it now"

The canonical current state. One row per workflow instance.

- `id`, chart reference (`chart_def` is pinned on the instance today),
  `current`, `payload` (current), `status`, `wake_on`, `wake_at`, `version`,
  timestamps.
- Mutated in place as the instance progresses.

### `executions` — "what happened on each drive"

Append-only. One row per drive: a `Start`, a signal resume, or a recovery
re-drive.

- `execution_id`, `instance_id`
- `trigger` — `start` | `signal:<name>` | `recovery`
- `signal_delivery_id` (nullable) — the inbound delivery id, for dedup
- `started_at`, `finished_at`
- `outcome` — `suspended` | `done` | `failed` | `crashed`
- `suspended_at_state`, `suspended_wake_on` (when `outcome = suspended`) — the
  rollback checkpoint
- `error` (when `outcome = failed`)
- **`heartbeat_at` / lease** — liveness for *this* drive

The lease/heartbeat lives on the **execution**, not the instance — liveness is a
property of a drive, not of the long-lived identity.

### Why this is the right move

1. **Crash detection becomes explicit.** "Crashed" = an execution with
   `outcome` still null (in-flight) and an expired heartbeat — rather than
   inferring it from a stale flag on the instance.
2. **Audit / journey for free.** The deferred history feature *is* the
   `executions` log: every drive, when, what triggered it, how it ended.
3. **Signal dedup / exactly-once.** A duplicate webhook delivery is rejected by
   `signal_delivery_id`, stronger than the status-only `Claim` check.

## Recovery: interim policy vs. target policy

Both are detected the same way (a crashed execution); they differ in what they
do to the instance.

### Interim — roll back to the last suspended point (build first)

On detecting a crashed execution, reset the instance to where it last parked:
restore `current` and `wake_on` from the most recent execution whose
`outcome = suspended` (its `suspended_at_state` / `suspended_wake_on`), and set
`status = suspended`. The caller re-signals and the instance re-drives from that
park.

- **No executor re-run, no chart load, no idempotency concern** — it is a pure
  state reset. This is why it is the easy first step.
- **Trade-off:** discards mid-drive progress. A drive that auto-completed *N*
  states before crashing is redone from the last park, not resumed at *N*.
- **Payload:** left as-is (the partial namespaces from the crashed drive). The
  re-drive overwrites namespaces it re-enters; a branch not re-entered may leave
  a stale namespace. Acceptable for the interim; full payload snapshotting is a
  later option.

### Target — re-run from the crash point (build later)

Keep per-transition persistence (the current behavior), so the instance's
`current` reflects the furthest committed progress. On recovery, re-drive from
`current` (a new `recovery` execution). This **preserves mid-drive progress** —
resume at step *N*, not back at the last park — which is why it is the eventual
goal.

- Requires the **idempotency contract** below, because it re-runs the current
  state's executor.

## Idempotency contract (target policy only)

The re-run policy re-executes the current state's executor, so executors must
tolerate re-execution:

- Read / compute / routing executors: re-running is naturally safe.
- Side-effecting executors (HTTP, payments): need an idempotency key, or
  dispatch via the outbox (Tier 3). State recovery does **not** undo a side
  effect that already happened before the crash.

The interim rollback policy does not re-run executors, so it is exempt — but the
re-drive *after* a rollback still hits the executors of the states it traverses,
so the contract applies there too.

## Mid-resume signal nuance

If a crash happens *during* a `SignalInstance` — after the instance is claimed
but before the drive completes — recovery (either policy) leaves the instance
back at a park *without* having applied that signal. The signal is effectively
dropped. This is acceptable **if callers retry failed signals**: the
`SignalInstance` call errored (the crash), the caller retries, recovery has left
the instance suspended, and the retry succeeds. Operating contract:
**recovery + at-least-once signal delivery from the caller.**

## Scheduling — consumer-driven

The engine exposes a single pass:

```go
func (e *Engine) RecoverStale(ctx context.Context) (recovered int, err error)
```

The consumer chooses the cadence (a `time.Ticker`, a River periodic job, cron).
The engine takes no scheduler dependency, consistent with the dependency-
injection design.

## Costs to accept

- **Transactional consistency.** Advancing the instance and opening/closing its
  execution must happen in one transaction, or the two tables drift.
- **Store-interface growth.** `Save` becomes "advance instance + record
  execution" rather than a plain upsert; new methods for opening executions,
  closing them, and finding crashed ones. Both `Memory` and `Postgres`
  implement.
- **Granularity decision.** Per-*drive* executions (agreed) vs per-*transition*
  events (full event sourcing). Per-drive now; per-transition events only if
  true replay is needed later.

## Concurrency safety

- Claiming an execution (start/resume/recovery) uses the same optimistic
  pattern as today's `Claim` (`FOR UPDATE SKIP LOCKED` + guarded update), so
  concurrent drivers and recoverers cannot double-drive an instance.
- A signal for an instance whose execution is in-flight sees it as not
  claimable → `ErrNotWaiting`, safely retried later.

## Out of scope

- **Side-effect rollback / exactly-once side effects** — recovery resets or
  re-runs *state*; external effects are the outbox's responsibility (Tier 3).
- **Per-transition event sourcing / full replay** — deferred; the per-drive
  `executions` log is the agreed granularity.
- **Distributed leader election** for the scan — Postgres row locking makes
  multiple recoverer processes safe without it.

## Open questions to resolve at implementation time

1. Default lease/heartbeat duration; whether to add heartbeat renewal for long
   single steps or rely on a generous lease.
2. No-checkpoint crash (a `Start` drive that crashed before its first suspend):
   mark the instance `failed` + log, or leave it? Leaning **mark failed + log**
   (no silently-stuck `running`).
3. Max recovery attempts before parking an instance as `failed` (poison-instance
   guard).
4. Whether recovery emits an observability hook / log line (`OnRecover`).
5. Whether executions snapshot the payload (enables full point-in-time replay)
   or only record metadata + outcome. Default: metadata only; current payload
   stays on the instance.

## Implementation checklist

Phase 1 — execution model + interim rollback recovery:
- [ ] Add the `executions` table; move the lease/heartbeat onto it.
- [ ] Record an execution row on `Start`, `Claim` (resume), and recovery; close
      it with an `outcome` at each rest point; capture `suspended_at_state` /
      `suspended_wake_on` on suspend.
- [ ] Make instance-advance + execution-write transactional.
- [ ] `Store` methods: open/close execution, find crashed executions
      (in-flight + expired heartbeat). Implement in `Memory` and `Postgres`.
- [ ] `Engine.RecoverStale` — for each crashed execution, roll the instance back
      to its last suspended checkpoint and close the crashed execution.
- [ ] Decide no-checkpoint policy (open question #2).
- [ ] Tests: simulated crash → rollback to last park → retried signal succeeds;
      concurrent recoverers don't double-act; duplicate `signal_delivery_id`
      deduped.

Phase 2 — switch to re-run-from-crash-point (target):
- [ ] Re-drive from `current` instead of rolling back (per-transition
      persistence already keeps `current` at furthest progress).
- [ ] Document and enforce the idempotency contract for executors.

Phase 3 (optional) — per-transition events / full replay if a real need appears.
