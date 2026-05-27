# Crash Recovery — Design Note

**Status:** Design only — not implemented as of 2026-05-27. This documents how
to add crash recovery so it can be built later. The current engine has the
durability gap described below.

## Problem

The engine persists an instance after every transition and at every rest point
(suspend / terminal / fail). Two lifecycle states behave very differently when a
worker process crashes:

- **`suspended`** instances are safe. They sit durably in the store until a
  signal (`SignalInstance`) or a timeout wakes them. A crash loses nothing.
- **`running`** instances are *not* safe. `running` means "a worker is actively
  driving this right now." If that worker dies mid-drive, nothing ever moves the
  instance again: it is not `suspended` (so `SignalInstance` returns
  `ErrNotWaiting`), not `done`, not `failed`. It is **stranded forever**.

`running` is entered in two places — `Start` (initial drive) and `Claim`
(resume on a signal) — so both are exposed to this gap.

## Goal

Detect stranded `running` instances and **continue them from where they
crashed** — i.e. re-drive from the instance's current `Current` state — without
manual intervention.

### Why re-run, not rollback

A drive between rest points can pass through *N* auto-completing states (no
suspend). If the crash happens at step *N*, we must resume from step *N*, **not**
roll back to the last suspend at step *N−5* and redo the intervening work. So we
keep **per-transition persistence** (the current behavior): each transition is
committed, so `Current` always reflects the furthest committed progress, and
recovery picks up from there.

(The alternative — commit only at rest points, giving automatic rollback — was
rejected precisely because it discards mid-drive progress.)

## Mechanism

Three pieces: a **lease**, a **recovery scan**, and a **re-drive**.

### 1. Lease

Add to `store.Instance`:

- `LockedUntil time.Time` — when the current worker's lease expires.
- `LockedBy string` — worker/owner id (optional; useful for debugging).

Whenever an instance becomes `running` (in `Start` and `Claim`), set
`LockedUntil = now() + leaseDuration`. A live worker holds a valid (future)
lease; a dead worker's lease expires.

### 2. Recovery scan

A periodic pass finds and re-claims stale instances:

```
running AND locked_until < now()   →   presumed abandoned
```

Re-claim atomically so two recoverers never grab the same instance (the same
optimistic-claim pattern as `Claim`):

```sql
UPDATE instances
SET locked_by = $me, locked_until = now() + $lease
WHERE status = 'running' AND locked_until < now()
ORDER BY locked_until
LIMIT $batch
FOR UPDATE SKIP LOCKED
RETURNING <columns>;
```

`FOR UPDATE SKIP LOCKED` lets multiple recoverer processes run concurrently and
pick disjoint instances.

### 3. Re-drive

For each re-claimed instance: resolve its pinned chart (`chartFor` — the chart
is stored on the instance, so this works after any restart) and call
`stepWith` from its `Current` state with **no inbound signal**. The current
state's executor re-runs and the instance continues — suspends, transitions, or
completes.

## Idempotency contract (non-negotiable)

Recovery **re-runs the current state's executor**. Therefore executors must
tolerate re-execution:

- Read / compute / routing executors: re-running is naturally safe.
- Side-effecting executors (HTTP, payments): must use an idempotency key, or
  dispatch via the outbox (Tier 3). State recovery does **not** undo a side
  effect that already happened before the crash.

This is DESIGN.md open question #3 made load-bearing: `Execute` must be
idempotent or self-guarded.

## Mid-resume signal nuance

If a crash happens *during* a `SignalInstance` — after `Claim` flips the
instance to `running` but before the first transition is saved — recovery
re-drives from `Current` **without** the inbound signal. A parked state will
then re-park (cold start), so that signal is effectively dropped.

This is acceptable **if callers retry failed signals**: the `SignalInstance`
call errored (the crash), the caller retries, recovery has left the instance
suspended again, and the retry succeeds. The operating contract is therefore
**recovery + at-least-once signal delivery from the caller**.

## Scheduling — consumer-driven

The engine exposes a single pass:

```go
func (e *Engine) RecoverStale(ctx context.Context) (recovered int, err error)
```

The **consumer** decides the cadence — a `time.Ticker`, a River periodic job, a
cron, etc. The engine takes no scheduler dependency, consistent with the
dependency-injection design (the store is injected; the schedule is the
consumer's).

## Store / interface changes

- `store.Instance`: add `LockedUntil` (and optionally `LockedBy`).
- Postgres schema: add `locked_until TIMESTAMPTZ`, `locked_by TEXT`; index
  `(status, locked_until)` for the scan, e.g. partial index
  `WHERE status = 'running'`.
- New `Store` method, implemented by both `Memory` (mutex scan + re-lease) and
  `Postgres` (the guarded UPDATE above):

  ```go
  // RecoverStale atomically re-leases up to `limit` running instances whose
  // lease has expired and returns them for re-driving.
  RecoverStale(ctx context.Context, lease time.Duration, limit int) ([]*Instance, error)
  ```
- `Start` and `Claim` set the lease when entering `running`.

## Lease tuning & heartbeat

- Set `leaseDuration` comfortably longer than the slowest expected single step.
  Guidance: executors should be quick or suspend — long waits belong in a
  suspend, not a blocking executor.
- If a single step can legitimately exceed the lease (a slow synchronous call),
  add **heartbeat renewal**: the driving worker periodically extends
  `LockedUntil` so it isn't recovered out from under itself. Deferred for v1; a
  generous lease avoids it.

## Concurrency safety

- Recovery re-claim uses the same optimistic mechanism as `Claim`
  (`FOR UPDATE SKIP LOCKED` + re-lease), so concurrent recoverers and a live
  `SignalInstance` cannot double-drive an instance.
- A signal arriving for an instance that recovery is mid-driving sees it as
  `running` (not `suspended`) → `ErrNotWaiting`, and is safely retried later.

## Out of scope

- **Side-effect rollback / exactly-once.** Recovery re-runs state; it does not
  undo external effects. That is the outbox's responsibility (Tier 3).
- **Distributed leader election** for the scan. Postgres row locking
  (`FOR UPDATE SKIP LOCKED`) makes multiple recoverer processes safe without it.

## Open questions to resolve at implementation time

1. Default `leaseDuration` and whether to expose it per `Start`/`Claim` or
   engine-wide.
2. Whether to add heartbeat renewal in v1 or rely on a generous lease.
3. Max re-drive attempts before parking an instance as `failed` (to avoid a
   poison instance that crashes the worker every recovery cycle).
4. Whether recovery emits an observability hook (e.g. `OnRecover`) for metrics.

## Implementation checklist

- [ ] Add `LockedUntil` / `LockedBy` to `store.Instance` (+ clone).
- [ ] Postgres: columns + index; set lease in `Save` paths that enter `running`.
- [ ] Set the lease in engine `Start` and in store `Claim`.
- [ ] Add `Store.RecoverStale`; implement in `Memory` and `Postgres`.
- [ ] Add `Engine.RecoverStale` that re-drives each returned instance via its
      pinned chart.
- [ ] Tests: simulate a crash (leave an instance `running` with an expired
      lease), run recovery, assert it continues from `Current`; assert the
      mid-resume case re-parks and a retried signal succeeds; assert two
      concurrent recoverers don't double-drive.
- [ ] Document the idempotency contract and the at-least-once signal expectation
      for consumers.
