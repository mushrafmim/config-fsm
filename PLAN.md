# config-fsm — Development Plan

Prioritized build order derived from [DESIGN.md](./DESIGN.md), ordered by what unlocks the most downstream value and de-risks the architecture earliest.

## Tier 1 — Core engine (nothing works without these)

1. **`chart` package** — types (`Chart`, `StateConfig`, `Transition`), YAML loading, validation. Everything else compiles against these.
2. **`executor` package** — `Executor` interface + `Registry`. The contract every state delegates to.
3. **`engine.Step` loop** — synchronous step function, transition resolution, terminal handling. The heart of the FSM.
4. **In-memory `store`** — enough to run end-to-end tests without Postgres.
5. **Milestone 1 proof**: boot a 3-state chart end-to-end with one trivial executor.

## Tier 2 — Durability (what makes this not just a toy)

6. **Postgres `store`** via `sqlc` — `jsonb` payload, `text[]` + GIN on `wake_on`, index on `wake_at`, optimistic locking via `Version`.
7. **Suspend/resume + `Signal` entry point** — the conditional `UPDATE … WHERE status='suspended' AND version=?` idempotency guarantee. This is the single most load-bearing correctness property.
8. **`scheduler` (River)** — periodic timeout scan firing synthetic `"timeout"` signals.

## Tier 3 — Reliability boundary

9. **`outbox` package** — interface + Postgres impl, transactional `Enqueue` alongside instance save, River drain worker. This is what justifies "no Temporal needed" — ship it before any real HTTP executor.
10. **First real side-effecting executor** using the outbox (proves the boundary).

## Tier 4 — Authoring UX (business DSL)

11. **`compiler` package** — two-pass expand + rewrite, `Macro` interface, identity passthrough.
12. **`submit_and_wait` macro** — canonical pattern; port a realistic flow (phyto application) as proof.
13. **Chart versioning** — content-addressed hash, pin `ChartVersion` at instance creation, version-aware load in `Step`.

## Tier 5 — Expressiveness

14. **`guard` package (CEL)** — restricted subset; conditional transitions.
15. **`cmd/fsmctl`** — compile, validate, visualize, replay.

## Tier 6 — Resolve before v1 ships

16. **Event payload merging semantics** (overwrite vs. namespace-scope). Commit to namespace-scoping per DESIGN.md's lean.
17. **Replay/idempotency documentation** for executors (`Execute` must be idempotent or self-guarded).
18. **Observability hooks** — `Hook(event TransitionEvent)` callback.
19. **Guard language scope** — pick the documented CEL subset.
20. **Hot-reload spec** — file-watching + chart-cache invalidation.

## Explicitly deferred (not in v1)

- Hierarchical/orthogonal states
- Sagas/compensation
- Distributed leader election
- Per-executor circuit breaking
- DSL conditionals/loops
- Visual designers

## Notes on ordering

- Tiers 1–3 are strict — each blocks the next.
- Tier 4 onwards can be reordered if a consumer (e.g. nsw-task-flow) needs guards before macros, but DESIGN.md's milestone list implies macros first.