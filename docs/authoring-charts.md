# Authoring Charts — Configuration Guide

A **chart** is a YAML definition of a finite state machine: a set of **states**
connected by **transitions**. The engine runs an **instance** of a chart —
driving it from the initial state, suspending when it needs to wait for the
outside world, resuming when a signal arrives, until it reaches a terminal
state.

This guide covers the configuration format, what each field means, and the
runtime model you author against. It documents **current behavior only**; see
[Not yet supported](#not-yet-supported) for things you should not author
against.

---

## A minimal chart

```yaml
id: greeting
version: "1"
initial: start
states:
  - name: start
    executor: log
    transitions:
      - { on: success, to: end }
  - name: end
    terminal: true
```

This runs the `log` executor in `start`; when it emits the `success` event the
instance transitions to `end`, which is terminal, and the instance completes.

---

## Top-level fields

| Field     | Type   | Required | Meaning |
|-----------|--------|----------|---------|
| `id`      | string | yes | Logical chart name. Recorded on every instance as `ChartID`. |
| `version` | string | yes | Version label. **Quote it** (`"1"`) so YAML doesn't read it as a number. Pinned onto each instance at start — an instance always runs the chart version it began on, even if you redeploy a newer one. |
| `initial` | string | yes | Name of the starting state. Must match a defined state. |
| `states`  | list   | yes | The states (see below). |

---

## States

```yaml
- name: review
  executor: external_review
  config:
    service_id: fcau
    task_code: fcau_application_review_v1
  signals:
    - approve
    - requires_rework
  transitions:
    - { on: approve,         to: approved }
    - { on: requires_rework, to: submission }
```

| Field         | Type   | Required              | Meaning                                                                                                                                |
|---------------|--------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `name`        | string | yes                   | Unique state identifier. **Also the namespace** its output is stored under in the payload (see [Data model](#the-data-model-payload)). |
| `executor`    | string | yes (unless terminal) | Registry key of the executor that runs when the instance is in this state.                                                             |
| `config`      | map    | no                    | Per-state configuration handed to the executor as `Event.Config`. Opaque to the engine — its meaning is entirely up to the executor.   |
| `signals`     | list   | no                    | The external signal names that may **wake** this state while it is suspended. Chart-owned, not chosen by the executor. Each must name an outgoing transition's `on` event (see [Suspending](#suspending-and-resuming)). |
| `transitions` | list   | no                    | Outgoing transitions.                                                                                                                  |
| `terminal`    | bool   | no                    | If `true`, entering this state completes the instance (`status = done`).                                                               |

**Terminal states** take no executor and must declare **no** transitions —
reaching one ends the instance.

---

## Transitions

```yaml
transitions:
  - { on: success, to: next_state }
```

| Field   | Type   | Required | Meaning                                                                                                                                                                                  |
|---------|--------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `on`    | string | yes      | The event name that selects this transition. Matched against the event the executor emits (`Result.Event`), or the signal name on a resume (see [Suspending](#suspending-and-resuming)). |
| `to`    | string | yes      | Target state name. Must be a defined state.                                                                                                                                              |
| `guard` | string | no       | **Reserved** for conditional transitions. Parsed but **not yet evaluated** — do not rely on it.                                                                                          |

A state may have several transitions; the engine picks the first whose `on`
matches the emitted event. Transitions may point to **earlier** states — loops
are allowed (e.g. `requires_rework → submission`).

---

## Executors — what a state *does*

The chart references executors **by name** (`executor: external_review`); you
register the implementations in code. The engine knows nothing about what an
executor does.

An executor receives an **Event** and returns a **Result**:

```go
type Event struct {
    Name    string         // the signal name on a resume; empty on the initial run
    Data    map[string]any // the inbound signal payload (resume only)
    Payload map[string]any // read-only deep clone of the full instance payload
    Config  map[string]any // this state's `config:` block
}

type Result struct {
    Event   string         // names the transition to take
    Output  map[string]any // produced data; filed under this state's namespace
    Suspend bool           // park the instance and wait
    WakeAt  *time.Time     // optional deadline (see Not yet supported)
}
```

Rules of thumb:

- To **advance**, return `Result{Event: "<name>"}` matching a transition's `on`.
- To **produce data**, return it in `Result.Output` — never mutate `Event.Payload`
  (it's a discarded clone).
- To **wait**, return `Result{Suspend: true}`. The signals that may wake the
  instance come from the state's `signals:` field in the chart — the executor
  does not restate them. (A state that suspends but declares no `signals` and
  sets no `WakeAt` is unwakeable and the engine fails it.)
- To **reject bad input** on a resume, return an error wrapping
  `executor.ErrInvalidInput` — the instance stays suspended and retriable.

---

## The data model (payload)

The payload is one JSON-like map. Two kinds of data live in it:

- **Seed / input data** — set when you start the instance — lives at the **top
  level** (e.g. `payload.reference_number`).
- **A state's output** — lives under a key equal to the **state name** (its
  namespace). The engine files `Result.Output` there automatically.

```jsonc
{
  // seed data (from Start)
  "reference_number": "ABC-123",
  "callback_url": "https://upstream/done",

  // one namespace per state that produced output
  "submission":      { "name": "alice" },
  "awaiting_review": { "reason": "missing field" }
}
```

This makes the data **collision-free** (each state owns one key) and
**predictable** (a downstream state reads `payload.<upstream-state>.<field>`).
You don't configure namespaces — the state name *is* the namespace.

On a re-run of a state (e.g. a loop), its namespace is **replaced** with the
latest output.

> Note: state names are part of the data contract your consumers (e.g. a UI
> renderer) read from. Renaming a state changes where its data lands.

---

## Suspending and resuming

Most real flows wait for the outside world — a user submitting a form, an
external system calling back, a payment gateway notifying. You model a wait as a
state whose executor **suspends**:

```go
// inside the executor for `awaiting_review`
return executor.Result{Suspend: true}, nil
```

The wake signals are declared **in the chart**, not in the executor — the state
lists them under `signals:`. The instance parks, and you resume it from your
application (e.g. a webhook handler):

```go
eng.SignalInstance(ctx, instanceID, "approve", map[string]any{"reference_number": "ABC-123"})
```

- The signal name (`"approve"`) must be one of the state's `signals:` values, or
  the call returns `store.ErrNotWaiting`.
- The signal data is filed under the suspended state's namespace.
- The signal name doubles as the **event** that selects the transition — so a
  resumed executor typically just emits the signal name it woke on, and the
  chart branches on it. This is why every declared signal must have a matching
  transition `on`:

```yaml
- name: awaiting_review
  executor: external_review        # suspends; on resume emits the wake signal
  signals:
    - approve
    - requires_rework
  transitions:
    - { on: approve,         to: approved }
    - { on: requires_rework, to: submission }
```

This is the **"lift the outcome to an event"** pattern: instead of branching on
a payload condition, the caller delivers the outcome *as the signal name*, and
the transitions branch on it. (It's why CEL guards aren't needed for these
flows.)

---

## A complete example: form → review with a rework loop

```yaml
id: application
version: "1"
initial: submission
states:
  - name: submission          # user fills a form; suspends until form_submitted
    executor: user_input
    signals:
      - form_submitted
    transitions:
      - { on: form_submitted, to: dispatch }

  - name: dispatch            # send to the external org; auto-completes
    executor: org_api
    transitions:
      - { on: dispatched, to: awaiting_review }

  - name: awaiting_review     # suspends until the org calls back
    executor: external_review
    signals:
      - approve
      - requires_rework
    transitions:
      - { on: approve,         to: approved }
      - { on: requires_rework, to: submission }   # loop back to the form

  - name: approved
    terminal: true
```

Driven from your application:

```go
eng.Start(ctx, chart, instanceID, map[string]any{"reference_number": "ABC-123"})
eng.SignalInstance(ctx, instanceID, "form_submitted", formData)   // → dispatch → awaiting_review
eng.SignalInstance(ctx, instanceID, "requires_rework", reason)    // → back to submission
eng.SignalInstance(ctx, instanceID, "form_submitted", formData2)  // → awaiting_review again
eng.SignalInstance(ctx, instanceID, "approve", nil)               // → approved (done)
```

---

## Versioning

You don't manage versions by hand. At `Start`, the engine serializes the chart
and pins it onto the instance. Every step and resume runs **that** definition,
reloaded from the instance — so:

- Redeploying a changed chart never affects in-flight instances.
- New instances use the new definition.
- Resume works after a process restart (the chart travels with the instance).

Set a meaningful `version:` string mostly for your own bookkeeping and logs.

---

## Validation rules

A chart is rejected at load time if:

- `id` or `initial` is missing.
- `initial` names a state that doesn't exist.
- A state has no `name`, or two states share a name.
- A non-terminal state has no `executor`.
- A terminal state declares `transitions` or `signals`.
- A transition has an empty `on`, or a `to` that names an undefined state.
- A `signal` is empty, duplicated within a state, or has no matching transition `on`.

---

## Running a chart (code side, for context)

```go
reg := executor.NewRegistry()
reg.Register(myUserInputExecutor)   // Name() == "user_input"
reg.Register(myOrgAPIExecutor)      // Name() == "org_api"
reg.Register(myReviewExecutor)      // Name() == "external_review"

eng := engine.New(reg, store.NewPostgres(db))   // or store.NewMemory()
chart, _ := chart.Parse(yamlBytes)               // or chart.Load(path)

eng.Start(ctx, chart, uuid.NewString(), seed)
```

One engine runs **any number of charts** — the chart is passed to `Start`, not
bound to the engine.

---

## Not yet supported

Do **not** author against these — they are designed but not implemented:

- **`guard:` / CEL conditions.** Transitions are selected by event name only.
  Use the lift-to-event pattern instead.
- **`input_mapping` / `output_mapping`.** Data flows by state-name namespace;
  there is no field-level mapping or renaming layer.
- **`WakeAt` timeouts.** A state can set a deadline, but nothing scans and fires
  it yet — there is no timeout scheduler. Model expiries as an external signal
  for now.
- **Crash recovery.** A worker that dies mid-drive leaves the instance stranded
  `running`. See `RECOVERY.md` for the planned design.

---

## Quick reference

```yaml
id: <chart-name>          # required
version: "<label>"        # required, quote it
initial: <state-name>     # required, must exist
states:                   # required
  - name: <unique-name>   # required; also the output namespace
    executor: <reg-key>   # required unless terminal
    config: { ... }       # optional; passed to the executor
    signals: [ ... ]      # optional; wake signals (each must match a transition on)
    transitions:          # optional
      - { on: <event>, to: <state> }   # guard: reserved, ignored
  - name: <terminal-name>
    terminal: true        # no executor, no transitions
```
