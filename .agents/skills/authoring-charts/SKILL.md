---
name: authoring-charts
description: "Use when you need to author, edit, or validate YAML configuration charts (FSMs) for config-fsm."
---

# Authoring config-fsm Charts

This skill provides the syntax, validation rules, and structural guidelines for defining and modifying configuration charts (finite state machines) in the `config-fsm` repository.

## Overview
A **chart** is a YAML definition of a finite state machine composed of a set of **states** and **transitions**.
* **Engine Execution**: The engine runs an instance of a chart starting from the state defined in `initial`.
* **State Execution**: Each state delegates work to an **executor**.
* **Suspend/Resume**: If an executor suspends, the engine parks the instance until an external signal wakes it.
* **Terminal States**: Reaching a terminal state completes the run.

---

## 1. Top-Level Fields

| Field     | Type   | Required | Description                                                                                                                |
|-----------|--------|----------|----------------------------------------------------------------------------------------------------------------------------|
| `id`      | string | **Yes**  | The unique identifier / logical name of the chart.                                                                         |
| `version` | string | **Yes**  | **Must be quoted** (e.g., `"1"`). Unquoted integers or decimals are parsed as numbers by YAML, which will fail validation. |
| `initial` | string | **Yes**  | The name of the starting state.                                                                                            |
| `states`  | list   | **Yes**  | List of state definitions.                                                                                                 |

---

## 2. State Definitions

Each state in the `states` list has the following schema:

| Field         | Type            | Required                  | Description                                                                                                  |
|---------------|-----------------|---------------------------|--------------------------------------------------------------------------------------------------------------|
| `name`        | string          | **Yes**                   | Unique identifier for the state. Also defines the payload namespace under which the state's output is saved. |
| `executor`    | string          | **Yes** (except terminal) | The registry key of the Go executor code that runs when this state is entered.                               |
| `config`      | map             | No                        | Opaque key-value configuration passed directly to the executor's `Event.Config`.                             |
| `signals`     | list of strings | No                        | List of external signal names that can wake the state if it suspends.                                        |
| `transitions` | list            | No                        | List of outgoing transitions from this state.                                                                |
| `terminal`    | boolean         | No                        | If `true`, entering this state immediately completes the instance.                                           |

### Terminal State Rules
* Must set `terminal: true`.
* **Must NOT** define an `executor`.
* **Must NOT** define `transitions` or `signals`.

---

## 3. Transitions

Transitions route execution to subsequent states based on the event returned by the executor (or the wake signal name):

| Field   | Type   | Required | Description                                                  |
|---------|--------|----------|--------------------------------------------------------------|
| `on`    | string | **Yes**  | The event name or signal name that triggers this transition. |
| `to`    | string | **Yes**  | Target state name (must exist in `states`).                  |
| `guard` | string | No       | **Reserved for future CEL support. Do not use.**             |

---

## 4. Design Patterns & Runtime Model

### The "Lift Outcome to Event" Pattern
For interactive tasks or external integrations that suspend, specify the outcomes as signals in the chart:
1. The state's executor returns a result with `Suspend: true`.
2. The state declares `signals:` (e.g., `approve`, `requires_rework`).
3. Your external application resumes the state by sending the signal name.
4. The signal name acts as the event that matches a transition `on` value to route to the next state:
```yaml
  - name: awaiting_review
    executor: external_review
    signals:
      - approve
      - requires_rework
    transitions:
      - { on: approve,         to: approved }
      - { on: requires_rework, to: submission }
```

### The Data Model (Payload Namespacing)
All outputs produced by a state's executor are placed under a namespace matching the state's `name` (e.g. `payload.submission.field_name`).
* Seed/input data passed when starting the instance lives at the root of the payload.
* If a state loops back and runs again, its namespace content is entirely replaced with the newest execution output.

---

## 5. Chart Validation Checklist

Before saving or updating any chart, verify it obeys the following rules:
- [ ] `id` is present.
- [ ] `version` is present and enclosed in quotes (e.g. `"1"` or `"1.0"`).
- [ ] `initial` is present and matches the `name` of an existing state.
- [ ] Every state has a unique `name`.
- [ ] Non-terminal states declare an `executor`.
- [ ] Terminal states do not have `executor`, `transitions`, or `signals` declared.
- [ ] All transition target `to` fields reference states that are defined in the chart.
- [ ] Every signal listed in a state's `signals` matches the `on` field of a transition defined in that same state.
- [ ] Transition `on` is not empty.
- [ ] No duplicate signal names in the same state.
- [ ] Do not use `guard` or conditional CEL expressions (unsupported).

---

## Examples

### A Minimal Chart
```yaml
id: minimal_flow
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

### A Complex Loop / Review Flow
```yaml
id: application_approval
version: "1"
initial: submission
states:
  - name: submission
    executor: user_input
    signals:
      - form_submitted
    transitions:
      - { on: form_submitted, to: dispatch }

  - name: dispatch
    executor: org_api
    transitions:
      - { on: dispatched, to: awaiting_review }

  - name: awaiting_review
    executor: external_review
    signals:
      - approve
      - requires_rework
    transitions:
      - { on: approve,         to: approved }
      - { on: requires_rework, to: submission }

  - name: approved
    terminal: true
```
