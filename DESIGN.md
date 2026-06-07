# config-fsm — Design

A generic, configuration-driven finite state machine engine in Go with pluggable executors, durable suspend/resume, and a compiler that expands business-level state definitions into technical states.

## Goals

- **Config-driven**: machine definitions live in YAML, not Go code. Authoring is the primary UX.
- **Pluggable executors**: states delegate work to registered executors; the engine knows nothing about what they do.
- **Durable suspend/resume**: states can park for arbitrary durations (seconds to months) and resume on external signals, timeouts, or scheduled wake-ups.
- **No workflow-engine dependency**: Postgres + a job queue (River) is the entire runtime substrate. No Temporal/Cadence required for the core engine.
- **Versioned definitions**: in-flight instances are pinned to the compiled chart version they started on; redeploying new charts never breaks running instances.
- **Business DSL compiler**: hand-authored YAML stays at the business abstraction level. A macro compiler expands business states (e.g. `submit_and_wait`) into the technical states the engine actually runs.

## Non-goals

- Hierarchical states or orthogonal regions (Harel statecharts). The engine is a flat FSM. Concurrent independent steps are out of scope; the assumption is sequential.
- Active orchestration with retries, sagas, fan-out/fan-in. For those, use Temporal as a *library* around individual risky executors (see "Reliability boundary" below) — not as the engine.
- Visual designers, BPMN import/export, SCXML compatibility.
- Multi-tenancy, RBAC, UI. These belong to consumers.

## Core abstractions

```go
// Event flows through transitions, carries payload.
type Event struct {
    Name    string
    Payload map[string]any
}

// Result is what an executor returns.
type Result struct {
    Event   string    // emitted event name; selects a transition
    Suspend bool      // park the instance
    WakeAt  *time.Time // optional deadline; fires "timeout" if reached
}
// Wake signals are NOT returned here — they are chart-owned (StateConfig.Signals).
// The executor only decides whether to suspend; the engine reads the wake set
// from the state.

// Executor is the plugin contract. Stateless; registered by name.
type Executor interface {
    Name() string
    Execute(ctx context.Context, e *Event) (Result, error)
}

// Transition: event + optional guard → target state.
type Transition struct {
    On    string
    To    string
    Guard string // optional CEL expression evaluated against payload
}

// StateConfig: one state in the compiled chart.
type StateConfig struct {
    Name        string
    Executor    string         // executor registry key
    Config      map[string]any // per-state executor config
    Signals     []string       // wake signals (fixed vocabulary; each maps to a transition On)
    Transitions []Transition
    Terminal    bool
}

// Chart: the compiled machine.
type Chart struct {
    ID      string
    Version string  // content-addressed hash or semver tag
    Initial string
    States  []StateConfig
}
```

## Instance store

```go
type Instance struct {
    ID            string                 // workflow instance id
    ChartID       string                 // logical chart name
    ChartVersion  string                 // pinned at creation
    Current       string                 // current technical state
    BusinessState string                 // current business state (pre-compilation name)
    Payload       map[string]any
    Status        Status                 // running | suspended | done | failed
    WakeOn        []string               // signal names this instance is waiting for
    WakeAt        *time.Time             // deadline, if any
    Version       int                    // optimistic locking
    CreatedAt     time.Time
    UpdatedAt     time.Time
}

type Store interface {
    Save(ctx context.Context, i *Instance) error
    Load(ctx context.Context, id string) (*Instance, error)
    FindByWakeSignal(ctx context.Context, signal string) ([]*Instance, error)
    FindDue(ctx context.Context, before time.Time, limit int) ([]*Instance, error)
}
```

Reference implementation: Postgres via `sqlc`. `payload` as `jsonb`. `wake_on` as `text[]` with a GIN index. `wake_at` indexed for due-scans. Optimistic locking via `Version` to prevent double-firing.

## Engine loop

The engine is a synchronous step function. One call drives the instance until it either suspends, terminates, or hits an error. Suspended instances live in the store until a signal wakes them.

```go
func (e *Engine) Step(ctx context.Context, inst *Instance) error {
    for {
        state := e.chart(inst.ChartVersion).States[inst.Current]
        if state.Terminal {
            inst.Status = StatusDone
            return e.store.Save(ctx, inst)
        }

        exec, _ := e.registry.Get(state.Executor)
        res, err := exec.Execute(ctx, &Event{Payload: inst.Payload})
        if err != nil {
            inst.Status = StatusFailed
            return e.store.Save(ctx, inst)
        }

        if res.Suspend {
            inst.Status = StatusSuspended
            inst.WakeOn, inst.WakeAt = state.Signals, res.WakeAt // wake signals are chart-owned
            return e.store.Save(ctx, inst)
        }

        next, ok := resolveTransition(state, res.Event, inst.Payload)
        if !ok {
            return fmt.Errorf("no transition from %q on %q", state.Name, res.Event)
        }
        inst.Current = next
        if err := e.store.Save(ctx, inst); err != nil { return err }
    }
}
```

Save after every transition. Return cleanly on suspend.

## Resuming

Three wake channels, all funneled into a single `Signal` entry point:

1. **External signal** (webhook): `Signal(ctx, signalName, data)` finds suspended instances waiting on that signal, merges payload, and calls `Step`.
2. **Scheduled timeout**: a River periodic job scans `wake_at < now()` and signals `"timeout"`.
3. **Polling fallback**: a worker that asks external systems for completions and signals matches.

```go
func (e *Engine) Signal(ctx context.Context, signal string, data map[string]any) error {
    instances, _ := e.store.FindByWakeSignal(ctx, signal)
    for _, inst := range instances {
        // Conditional update guards against double-fire on duplicate deliveries.
        for k, v := range data { inst.Payload[k] = v }
        inst.Status = StatusRunning
        if err := e.Step(ctx, inst); err != nil { return err }
    }
    return nil
}
```

Idempotency is enforced at the store layer: `UPDATE instances SET status='running' WHERE id=? AND status='suspended' AND version=?`. A late duplicate signal sees zero rows affected and is a no-op.

## Reliability boundary: outbox for risky dispatch

Executors that perform side effects (HTTP calls, message publishes) must be safe against the "succeeded but crashed before saving" failure mode. The engine provides an outbox primitive:

```go
type Outbox interface {
    Enqueue(ctx context.Context, tx Tx, msg OutboxMessage) error
}
```

Risky executors write to the outbox in the *same transaction* as the instance save. A separate River worker drains the outbox, performs the dispatch, and marks rows sent. Retries, backoff, and dead-lettering are River's job.

This is the design decision behind "no Temporal needed": for single-step risky dispatches, the outbox is sufficient. Multi-step sagas with compensation are out of scope; if a consumer needs them, they can wrap the relevant executor with Temporal externally.

## Business DSL → compiled chart

The hand-authored YAML is the **business layer**. A compiler expands business states (declared via `macro:`) into multiple technical states. The engine only runs the compiled chart.

### Business layer (human-authored)

```yaml
id: phyto_application
version: 1
initial: submission

states:
  - name: submission
    executor: user_input
    on_success: review

  - name: review
    macro: submit_and_wait
    config:
      partner: agency_x
      timeout: 72h
    on_success: approved
    on_failure: rejected

  - name: approved
    terminal: true
  - name: rejected
    terminal: true
```

### Compiled layer (what the engine runs)

```yaml
initial: submission
states:
  - name: submission
    executor: user_input
    transitions:
      - { on: success, to: review__submit }
      - { on: failure, to: rejected }

  - name: review__submit
    executor: partner_submit
    config: { partner: agency_x }
    transitions:
      - { on: submitted, to: review__await }
      - { on: failure,   to: rejected }

  - name: review__await
    executor: partner_await
    config: { timeout: 72h }
    transitions:
      - { on: received, to: review__receive }
      - { on: timeout,  to: rejected }

  - name: review__receive
    executor: partner_receive
    transitions:
      - { on: success, to: approved }
      - { on: failure, to: rejected }

  - name: approved
    terminal: true
  - name: rejected
    terminal: true
```

### Compiler

Two-pass:

1. **Expand**: walk business states, invoke registered macros, collect technical states. Record each business state's *entry point* (first technical state).
2. **Rewrite**: replace transition targets that point to business names with their entry points.

```go
type Macro interface {
    Name() string
    Expand(bs BusinessState, ctx ExpansionContext) ([]StateConfig, error)
}
```

Built-in macros: `submit_and_wait` (the canonical pattern), plus identity passthrough for non-macro states.

### Versioning

Compiled charts are immutable, content-addressed (`sha256` of canonical JSON) or semver-tagged. The instance store records `ChartVersion` at creation; the engine loads that specific version when stepping. New deploys add new versions; old instances finish on the version they started.

## Package layout

```
config-fsm/
├── chart/        # chart types, YAML loading, validation
├── compiler/     # business DSL types, macro registry, two-pass compiler
├── engine/       # Step loop, Signal, transition resolution
├── executor/     # Executor interface, Registry
├── store/        # Store interface, Postgres impl (sqlc)
├── scheduler/    # River integration: timeout scans, outbox drain
├── outbox/       # Outbox interface + Postgres impl
├── guard/        # CEL evaluator for transition guards
└── cmd/
    └── fsmctl/   # CLI: compile, validate, visualize, replay
```

The `engine` package depends on `chart`, `executor`, `store`, `guard` — nothing else. `scheduler` and `outbox` are optional integrations; an in-memory store and a fake outbox suffice for tests and library-only use.

## Dependencies

- `gopkg.in/yaml.v3` — YAML parsing.
- `riverqueue/river` — scheduled jobs (timeouts, outbox drain). Postgres-backed.
- `google/cel-go` — guard expression evaluation.
- `sqlc-dev/sqlc` + `pressly/goose` — type-safe queries and migrations for the Postgres store.
- stdlib for everything else.

No Temporal SDK dependency in the core library. Consumers may add it externally if they need saga support around a specific executor.

## Configuration choices

- **YAML, not JSON.** Charts are hand-authored; comments, block scalars, and bare expressions in `guard:` matter. JSON support can be added later as a machine-to-machine format.
- **Named events**, not implicit. Transitions declare event names (`form_submitted`, `received`, `timeout`). Executors return `Result.Event` to select.
- **Guards via CEL**, restricted subset. Equality, comparison, boolean ops, payload field access. No arbitrary function calls.
- **Per-state config map** on `StateConfig.Config`. Schema validation per executor is the executor's responsibility, validated at chart-compile time via an optional `ConfigSchema()` method.

## Open questions to resolve before v1

1. **Guard language scope.** CEL is the right pick, but do we expose all of CEL or restrict to a documented subset? Restriction is easier to evolve.
2. **Event payload merging.** When `Signal` delivers `data`, does it overwrite, deep-merge, or namespace-scope into payload? Consumer experience suggests namespace-scoping (each transition declares its output namespace) — worth committing to in v1.
3. **Replay safety.** If an executor is non-idempotent and crashes mid-execute, the engine will re-call it on next `Step`. Outbox handles dispatch; what about reads? Document explicitly that `Execute` must be idempotent or guarded by the executor.
4. **Hot-reload of charts.** Versioning makes this safe in principle, but the file-watching and chart-cache invalidation story needs spec.
5. **Observability hooks.** Per-transition events for tracing/metrics. Probably a simple `Hook(event TransitionEvent)` callback registered on the engine.

## Not in v1

- Hierarchical/orthogonal states (deliberate — see non-goals).
- Distributed leader election for the scheduler (River handles it well enough).
- Per-executor circuit breaking (executor's job, not the engine's).
- DSL features beyond macros: conditionals, loops in the business layer. Defer until real demand.

## First milestones

1. `chart` + `engine` + in-memory `store` + one trivial executor. Boots, runs a 3-state chart end-to-end synchronously.
2. Add Postgres `store` + `Signal` + suspend/resume across process restarts.
3. Add `scheduler` for timeouts.
4. Add `outbox` + a real HTTP-calling executor that uses it.
5. Add `compiler` + the `submit_and_wait` macro. Port a realistic flow (e.g. the nsw-task-flow phyto application) as proof.
6. Add `guard` (CEL) and conditional transitions.

Each milestone is independently shippable as a library version.

## Relationship to nsw-task-flow

`nsw-task-flow` will become a consumer of this library, not its parent. It contributes:

- Concrete executors (`USER_INPUT`, `EXTERNAL_REVIEW`, `PAYMENT`, `API_CALL`) implementing the `Executor` interface.
- A renderer adapter that maps business state names to UI components.
- The hierarchical parent-task / task-subtask coordination, which is application-level concern, not engine concern.

The library knows nothing about NSW, tasks, renderers, or government workflows. It must remain neutral; otherwise it'll accrete domain assumptions and stop being reusable.