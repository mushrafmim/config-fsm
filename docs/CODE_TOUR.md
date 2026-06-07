# Code Tour — understanding config-fsm from scratch

This is a reading guide. It assumes you understand *what* the system does (runs
state machines that can pause for days and resume on external signals) but not
*how the code is laid out*. Follow it top to bottom with the files open beside
you.

---

## 1. The one-paragraph mental model

A **chart** is a static description of a state machine (states + the events that
move between them) — it's just data, loaded from YAML. An **executor** is a
piece of code attached to a state that *does the work* of that state. An
**instance** is one live run of a chart for one real-world case (e.g. "Alice's
certificate application"). The **engine** is a loop that, for the instance's
current state, runs that state's executor, looks at what it returned, and either
moves to the next state, pauses ("suspends"), or finishes. The **store** is
where instances are saved so they survive restarts and long pauses. That's the
whole system: *chart = the map, executor = the work, instance = the journey,
engine = the driver, store = the memory.*

```
        chart (static)                 instance (live, saved in store)
   ┌───────────────────────┐        ┌──────────────────────────────┐
   │ state → executor name  │        │ ChartDef (pinned chart copy)  │
   │ transitions: on → to   │        │ Current state                 │
   │ signals: [...]         │        │ Payload (data so far)         │
   └───────────────────────┘        │ Status: running/suspended/... │
              ▲                       └──────────────────────────────┘
              │ runs                              ▲
        ┌─────┴───────────────────────────────────┴────┐
        │                  ENGINE loop                   │
        │  look up current state → run its executor →    │
        │  transition / suspend / finish → save → repeat │
        └────────────────────────────────────────────────┘
```

---

## 2. Read the packages in this order

Each package is small. Read them in dependency order — earlier ones don't know
about later ones, so the picture builds up cleanly.

### Step 1 — `pkg/chart/chart.go` *(the static shape)*

Start here. This is pure data with no behavior, so it's the easiest entry point.

Read, in this order:
- `Transition` — `{on, to}`: "when event `on` happens, go to state `to`."
- `StateConfig` — one state: its `Executor` name, its `Config`, its `Signals`
  (what can wake it), its `Transitions`, and whether it's `Terminal` (an end).
- `Chart` — the whole machine: `ID`, `Initial` state, and the list of `States`.
- `Validate()` — **read this carefully.** It's the list of rules a chart must
  obey, and reading the rules teaches you the model (e.g. "every signal must
  match a transition", "terminal states have no executor"). The error messages
  are a spec.
- `Load` / `Parse` / `Bytes` / `FromBytes` — turn YAML/JSON into a `Chart` and
  back. `Bytes`/`FromBytes` matter because each instance stores its *own* copy
  of the chart (see store, step 3).

**Takeaway:** a chart is immutable data describing nodes and the named edges
between them. Nothing here *runs*.

> Pair this with `docs/authoring-charts.md`, which is the same thing from a
> chart-author's point of view.

### Step 2 — `pkg/executor/executor.go` *(the work contract)*

This defines how the engine talks to your code.

- `Event` — what the engine hands an executor: `Name` (the wake signal, empty on
  first run), `Data` (the signal's payload), `Payload` (everything known so far,
  read-only), `Config` (this state's config block from the chart).
- `Result` — what an executor hands back: `Event` (which transition to take),
  `Output` (data to save), `Suspend` (pause and wait), `WakeAt` (a timer).
- `Executor` interface — just `Name()` and `Execute(...)`. That's the whole
  plugin contract.
- `Registry` — a name→executor lookup table. Charts reference executors by name
  (a string); the registry resolves the name to actual code at runtime.
- `Func` — an adapter so you can write a quick executor as a plain function
  instead of a struct. Used a lot in tests.
- `ErrInvalidInput` — the special error meaning "this signal's data was bad,
  don't fail the instance, just stay parked and let them retry."

**Takeaway:** the engine never knows what an executor *does*. It only speaks
`Event` in, `Result` out. This is the seam that keeps the engine generic.

### Step 3 — `pkg/store/store.go` *(the memory)*

- `Instance` — **the most important struct in the system.** It's one live run.
  Note `ChartDef` (the instance carries its own frozen copy of the chart),
  `Current` (which state it's in), `Payload` (accumulated data), `Status`,
  `WakeOn` (signals it's parked on), `Version` (for safe concurrent updates).
- `Status` constants — `running`, `suspended`, `done`, `failed`. The lifecycle.
- `Store` interface — the four operations any backend must provide: `Save`,
  `Load`, `Claim`, `FindDue`. **`Claim` is the subtle one:** it atomically flips
  a suspended instance to running so two signals arriving at once can't both
  drive it (one wins, the other gets `ErrNotWaiting`). This is how duplicate
  webhooks are made safe.
- `Memory` — the in-process implementation. Read it to see the interface made
  concrete; it clones on every read/write so callers can't accidentally corrupt
  stored state.

Then skim `pkg/store/postgres.go` — same interface, backed by SQL. Don't study
it line by line yet; just confirm it implements the same four methods. It's the
"real" production store.

**Takeaway:** an instance is self-contained (it carries its chart), and `Claim`
is the concurrency guard that makes resume-by-signal safe.

### Step 4 — `pkg/engine/engine.go` *(the driver — the heart)*

Now the pieces connect. This is the file to understand deeply.

- `Engine` struct + `New` + the `Option`s (`WithLogger`, `WithCompletionHook`) —
  construction. Note the engine holds a registry and a store, but **no chart** —
  charts arrive per `Start`.
- `Start` — create a new instance, pin the chart into it, save, then drive it.
- `SignalInstance` — the resume path. **Read the doc comment in full.** It
  `Claim`s the instance, loads its pinned chart, applies the signal data, and
  drives it. Note the rejection handling (`ErrInvalidInput` → release back to
  suspended).
- **`stepWith` — THE loop. Read this slowly; everything else serves it.** Walk
  one iteration:
  1. Find the current state in the chart.
  2. If terminal → mark done, fire the completion hook, stop.
  3. Look up the executor by name.
  4. Build the `Event`, call `Execute`.
  5. If it returned `Suspend` → save `WakeOn` from the chart's `Signals`, file
     any output, park, stop.
  6. Otherwise file `Output`, find the transition matching `Result.Event`, move
     `Current`, save, and **loop again** (this is why a chain of non-suspending
     states runs in one call).
- `resolveTransition`, `fail`, `fireComplete`, `clonePayload` — small helpers
  the loop uses. `clonePayload` is why executors get a read-only view.

**Takeaway:** the engine is intentionally dumb and small — a `for` loop over
"run executor → act on result → save". All cleverness lives in executors and the
chart. Read `stepWith` until it feels obvious; then you understand the system.

### Step 5 — `pkg/builtin/builtin.go` *(generic ready-made executors)*

Now that you know the contract, these are easy and concrete. They're the
executors we just built so people don't have to write their own:
- `InteractiveTask` — park, and on resume turn the wake signal into the event.
- `HTTPCall` — make an HTTP request, emit `success`/`error`.
- `RegisterAndWait` — HTTP-register, then park.

Read these to see the `Event`-in/`Result`-out contract used for real. Each one
is just an `if e.Name == "" { ... } else { ... }` shape.

### Step 6 — `internal/testfixtures/` *(tiny example executors + charts)*

- `executors.go` — `Always`, `Parker`, `Gate`, `Echo`: minimal executors the
  tests use. Great for seeing the contract stripped to one behavior each.
- `charts.go` — small example chart YAMLs.

### Step 7 — `demo/main.go` + `demo/fcau_1_application.yaml` *(it all wired up)*

The payoff. A runnable HTTP server: it builds a registry (`builtin.Register`), a
store, an engine, loads a chart, and exposes `/start`, `/instances/{id}`, and
`/instances/{id}/signal/{signal}`. Read `main` top to bottom — you'll recognise
every type. Then read the YAML and trace it against the engine loop in your head.

---

## 3. Trace one request end-to-end (do this with the code open)

This is the single most useful exercise. Follow "Alice submits her application":

1. **`POST /start`** (`demo/main.go`) → `engine.Start`.
2. `Start` builds an `Instance`, pins the chart, `store.Save`s it, calls
   `stepWith`.
3. `stepWith`: current state is `applicant_submission`, executor
   `interactive_task`. It returns `Suspend: true`. Engine sets `WakeOn` from the
   state's `signals: [submit]`, saves, returns. **Instance is now parked.**
4. Later, **`POST /instances/alice/signal/submit`** → `engine.SignalInstance`.
5. `SignalInstance` → `store.Claim` (suspended→running), loads the pinned chart,
   files the submitted data under the `applicant_submission` namespace, calls
   `stepWith`.
6. `stepWith`: executor runs with `Event.Name == "submit"`, returns
   `Event: "submit"`. Engine matches transition `on: submit → officer_review`,
   moves `Current`, saves, loops. The next state parks again. Returns.
7. The HTTP handler returns the updated instance as JSON.

If you can narrate those seven steps without looking, you understand the engine.

---

## 4. Key vocabulary (one line each)

| Term | What it is |
|------|-----------|
| **Chart** | Static state-machine definition (data, from YAML). |
| **State / node** | One step; names exactly one executor. Owns a payload namespace. |
| **Transition / edge** | `on: <event> → to: <state>`. Pure routing, no code. |
| **Signal** | An external event name that can wake a suspended state (chart-owned). |
| **Executor** | The code attached to a state. `Event` in, `Result` out. |
| **Event (runtime)** | The input to an executor for one run. |
| **Result** | The output of an executor: transition, suspend, or output. |
| **Instance** | One live run of a chart, saved in the store. |
| **Payload** | The instance's accumulated data; each state writes under its own name. |
| **Engine** | The loop that drives an instance through its chart. |
| **Store** | Persistence (`Memory` for tests, Postgres for prod). |
| **Claim** | Atomic suspended→running flip; prevents double-processing. |
| **Suspend** | Pause the instance until a signal or timer wakes it. |
| **Terminal** | An end state; reaching it completes the instance. |

---

## 5. How to poke at it

- **Run the tests** — they are the best documentation of intended behavior:
  ```
  go test ./...
  ```
  Open `pkg/engine/engine_test.go` and read the test *names* — each is a
  sentence describing one rule of the system.
- **Run the demo:**
  ```
  go run ./demo
  ```
  then `curl` the `/start` and `/signal` endpoints and watch the logs.
- **Change a chart** (`demo/fcau_1_application.yaml`), restart, and see the
  behavior change without touching Go — that's the whole point of the design.

---

## 6. What to skip on a first pass

- `pkg/store/postgres.go` internals (SQL details) — just know it exists.
- `RECOVERY.md` / crash-recovery design — a future concern, not built yet.
- `DESIGN.md` "Open questions" and CEL/guards/mapping — all marked "not yet
  supported"; ignore until the basics are solid.

Read order recap: **chart → executor → store → engine → builtin → fixtures →
demo**, then trace one request. The engine's `stepWith` loop is the keystone —
everything else exists to feed it.