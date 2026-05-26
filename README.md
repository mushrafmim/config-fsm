# config-fsm

A generic, configuration-driven finite state machine engine in Go with pluggable executors, durable suspend/resume, and a compiler that expands business-level state definitions into technical states.

## Overview

`config-fsm` is designed to run flat FSMs where the machine definitions live in YAML. It delegates work to registered, pluggable executors and supports durable suspend/resume for arbitrary durations. It is backed by Postgres and a job queue (River), removing the need for a complex workflow engine like Temporal/Cadence for the core engine.

## Key Features

- **Configuration-Driven**: State machines are defined in YAML (Business DSL).
- **Pluggable Executors**: The core engine is agnostic to what states do. They delegate to registered Go executors.
- **Durable Suspend/Resume**: States can park and wait for external signals, timeouts, or scheduled wake-ups.
- **Business DSL Compiler**: A macro compiler expands human-authored business states (e.g., `submit_and_wait`) into engine-runnable technical states.
- **Versioned Definitions**: Running instances are pinned to the compiled chart version they started on, ensuring safe redeployments.
- **Outbox for Risky Dispatch**: Provides a transactional outbox primitive for safe execution of side-effecting executors.

## Core Abstractions

- `Event`: Flows through transitions and carries the payload.
- `Result`: Returned by an executor to trigger transitions or suspensions.
- `Executor`: The plugin contract that performs the work for a state.
- `Transition`: An event and optional guard linking to a target state.
- `StateConfig`: A compiled state in the chart.
- `Chart`: The compiled state machine definition.
- `Instance`: The runtime state of an executing chart.

## Documentation

- [Design Document](DESIGN.md) - In-depth architecture, goals, non-goals, and technical design.
- [Development Plan](PLAN.md) - Prioritized build order and milestones.

## Dependencies

- `gopkg.in/yaml.v3` — YAML parsing.
- `riverqueue/river` — scheduled jobs (timeouts, outbox drain).
- `google/cel-go` — guard expression evaluation.
- `sqlc-dev/sqlc` + `pressly/goose` — type-safe queries and migrations for the Postgres store.
