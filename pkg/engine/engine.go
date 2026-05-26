// Package engine drives an Instance through a Chart.
//
// The engine is intentionally a thin loop: look up the current state, call
// its executor, take a transition or suspend, persist, repeat. It owns no
// goroutines and no scheduling — those concerns live in scheduler/outbox.
package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/mushrafmim/config-fsm/pkg/chart"
	"github.com/mushrafmim/config-fsm/pkg/executor"
	"github.com/mushrafmim/config-fsm/pkg/store"
)

// CompletionHook is invoked when an instance reaches a final state — either
// terminal (StatusDone) or failed (StatusFailed). It runs synchronously
// within the Step/Signal call that drove the instance there, after the final
// state has been persisted. Inspect inst.Status and inst.Current to determine
// the outcome (e.g. paid vs rejected vs errored) and read routing data such
// as a callback URL from inst.Payload.
//
// The hook is registered on the Engine, not per instance, so it survives
// process restarts: every process that builds an Engine wires the same hook,
// and it fires from whichever process completes the instance — including a
// webhook-driven Signal call in a different process from the one that called
// Start.
//
// The hook owns its own delivery reliability; it returns no error, so any
// failure must be handled internally (log, retry, enqueue). For guaranteed
// delivery, dispatch via the outbox (Tier 3) rather than calling upstream
// inline.
type CompletionHook func(ctx context.Context, inst *store.Instance)

// Option configures an Engine at construction.
type Option func(*Engine)

// WithCompletionHook registers a hook fired when an instance reaches a final
// state. Only one hook is supported; a later WithCompletionHook wins.
func WithCompletionHook(h CompletionHook) Option {
	return func(e *Engine) { e.onComplete = h }
}

// Engine binds a single compiled chart to an executor registry and a store.
// Chart versioning (multiple charts in one engine) is deferred to Tier 4.
type Engine struct {
	chart      *chart.Chart
	registry   *executor.Registry
	store      store.Store
	onComplete CompletionHook
}

func New(c *chart.Chart, reg *executor.Registry, s store.Store, opts ...Option) *Engine {
	e := &Engine{chart: c, registry: reg, store: s}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Start creates a new instance pinned to this engine's chart, persists it,
// and drives it via Step until it suspends, terminates, or errors.
func (e *Engine) Start(ctx context.Context, id string, payload map[string]any) (*store.Instance, error) {
	if payload == nil {
		payload = map[string]any{}
	}
	inst := &store.Instance{
		ID:           id,
		ChartID:      e.chart.ID,
		ChartVersion: e.chart.Version,
		Current:      e.chart.Initial,
		Payload:      payload,
		Status:       store.StatusRunning,
	}
	if err := e.store.Save(ctx, inst); err != nil {
		return nil, fmt.Errorf("save new instance: %w", err)
	}
	if err := e.Step(ctx, inst); err != nil {
		return inst, err
	}
	return inst, nil
}

// Step drives the instance forward until it terminates, suspends, or errors.
// On error the instance is marked failed and persisted before returning.
func (e *Engine) Step(ctx context.Context, inst *store.Instance) error {
	return e.stepWith(ctx, inst, "")
}

// Signal wakes every suspended instance waiting on the named signal and
// drives each one synchronously through the engine until it suspends again,
// terminates, or errors. Per-instance errors are collected via errors.Join
// so that one bad instance does not block the others.
//
// Idempotency note: this implementation re-loads each candidate before
// resuming and skips any that are no longer suspended (e.g. a duplicate
// webhook delivery whose predecessor already woke the instance). True
// conditional-update idempotency under contention arrives with the Postgres
// store; the in-memory store is correct only because Save is mutex-guarded.
//
// Payload merge semantics: shallow overwrite. Each key in data replaces the
// corresponding key on the instance payload. Namespace-scoped merging is
// DESIGN.md open question #2 and deferred.
func (e *Engine) Signal(ctx context.Context, signal string, data map[string]any) error {
	candidates, err := e.store.FindByWakeSignal(ctx, signal)
	if err != nil {
		return fmt.Errorf("find by wake signal %q: %w", signal, err)
	}
	var errs []error
	for _, snapshot := range candidates {
		inst, err := e.store.Load(ctx, snapshot.ID)
		if err != nil {
			errs = append(errs, fmt.Errorf("reload %s: %w", snapshot.ID, err))
			continue
		}
		if inst.Status != store.StatusSuspended {
			// Already resumed by a prior delivery — skip cleanly.
			continue
		}
		if inst.Payload == nil {
			inst.Payload = map[string]any{}
		}
		maps.Copy(inst.Payload, data)
		inst.Status = store.StatusRunning
		inst.WakeOn = nil
		inst.WakeAt = nil
		if err := e.store.Save(ctx, inst); err != nil {
			errs = append(errs, fmt.Errorf("save %s before resume: %w", inst.ID, err))
			continue
		}
		if err := e.stepWith(ctx, inst, signal); err != nil {
			errs = append(errs, fmt.Errorf("resume %s on %q: %w", inst.ID, signal, err))
		}
	}
	return errors.Join(errs...)
}

// stepWith is the engine loop. inboundSignal is the event name passed to the
// FIRST executor invocation — used by Signal to surface the wake signal into
// the resumed state. Subsequent iterations use an empty Event.Name because
// they represent internal transitions, not external wakes.
func (e *Engine) stepWith(ctx context.Context, inst *store.Instance, inboundSignal string) error {
	for {
		state, ok := e.chart.State(inst.Current)
		if !ok {
			return e.fail(ctx, inst, fmt.Errorf("state %q not found in chart %q", inst.Current, e.chart.ID))
		}

		if state.Terminal {
			inst.Status = store.StatusDone
			if err := e.store.Save(ctx, inst); err != nil {
				return err
			}
			e.fireComplete(ctx, inst)
			return nil
		}

		exec, ok := e.registry.Get(state.Executor)
		if !ok {
			return e.fail(ctx, inst, fmt.Errorf("executor %q not registered (state %q)", state.Executor, state.Name))
		}

		evt := &executor.Event{
			Name:    inboundSignal,
			Payload: inst.Payload,
			Config:  state.Config,
		}
		inboundSignal = "" // only the first iteration carries the wake signal

		res, err := exec.Execute(ctx, evt)
		if err != nil {
			return e.fail(ctx, inst, fmt.Errorf("executor %q in state %q: %w", state.Executor, state.Name, err))
		}

		if res.Suspend {
			inst.Status = store.StatusSuspended
			inst.WakeOn = res.WakeOn
			inst.WakeAt = res.WakeAt
			return e.store.Save(ctx, inst)
		}

		next, ok := resolveTransition(state, res.Event)
		if !ok {
			return e.fail(ctx, inst, fmt.Errorf("no transition from %q on event %q", state.Name, res.Event))
		}

		inst.Current = next
		inst.WakeOn = nil
		inst.WakeAt = nil
		if err := e.store.Save(ctx, inst); err != nil {
			return fmt.Errorf("save after transition to %q: %w", next, err)
		}
	}
}

// resolveTransition returns the target state for the named event, or false
// if no transition matches. Guards (CEL) are not yet evaluated.
func resolveTransition(state *chart.StateConfig, event string) (string, bool) {
	for _, t := range state.Transitions {
		if t.On == event {
			return t.To, true
		}
	}
	return "", false
}

// fail marks the instance failed and persists it, returning the original
// error to the caller. A save error is wrapped alongside the cause.
func (e *Engine) fail(ctx context.Context, inst *store.Instance, cause error) error {
	inst.Status = store.StatusFailed
	if saveErr := e.store.Save(ctx, inst); saveErr != nil {
		return fmt.Errorf("%w (also: persist failed status: %v)", cause, saveErr)
	}
	e.fireComplete(ctx, inst)
	return cause
}

// fireComplete notifies the completion hook, if registered, that the instance
// has reached a final state. Fired exactly once per instance: a completed
// instance is never re-driven (FindByWakeSignal only returns suspended ones,
// and Signal re-checks status before resuming).
func (e *Engine) fireComplete(ctx context.Context, inst *store.Instance) {
	if e.onComplete != nil {
		e.onComplete(ctx, inst)
	}
}
