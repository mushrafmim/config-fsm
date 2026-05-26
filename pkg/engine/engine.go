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
	"slices"

	"github.com/mushrafmim/config-fsm/pkg/chart"
	"github.com/mushrafmim/config-fsm/pkg/executor"
	"github.com/mushrafmim/config-fsm/pkg/store"
)

// ErrNotWaiting is returned by SignalInstance when the target instance is not
// currently suspended on the given signal — because it has already advanced
// (e.g. a duplicate delivery), reached a final state, or is parked waiting on
// a different signal. Callers can map it to an idempotent ack.
var ErrNotWaiting = errors.New("instance not waiting on signal")

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

// Instance fetches a single instance by ID. It returns store.ErrNotFound if
// no instance with that ID exists. This is a read-through to the store so
// consumers can inspect instance state (status, current state, payload)
// without holding a reference to the store directly.
func (e *Engine) Instance(ctx context.Context, id string) (*store.Instance, error) {
	return e.store.Load(ctx, id)
}

// Step drives the instance forward until it terminates, suspends, or errors.
// On error the instance is marked failed and persisted before returning.
func (e *Engine) Step(ctx context.Context, inst *store.Instance) error {
	return e.stepWith(ctx, inst, nil)
}

// SignalInstance wakes the single suspended instance identified by id that is
// waiting on the named signal, and drives it synchronously until it suspends
// again, terminates, or errors. It returns the resulting instance so the
// caller can render the new state without a separate read.
//
// It returns store.ErrNotFound if no such instance exists, and ErrNotWaiting
// if the instance is not currently suspended on this signal (already advanced,
// in a final state, or parked on a different signal).
//
// Commit-or-discard: the wake (payload data + status change) is applied
// in-memory only and is not persisted until the first successful step. If the
// resumed state rejects the input with executor.ErrInvalidInput, nothing is
// saved — the instance stays suspended with its WakeOn intact, retriable — and
// the returned error wraps ErrInvalidInput (use errors.Is to map it to a 4xx).
//
// Payload: data is filed under the suspended state's namespace (its name);
// empty data leaves the namespace untouched.
func (e *Engine) SignalInstance(ctx context.Context, id, signal string, data map[string]any) (*store.Instance, error) {
	inst, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.Status != store.StatusSuspended || !slices.Contains(inst.WakeOn, signal) {
		return nil, fmt.Errorf("%w: instance %q is %s, waiting on %v", ErrNotWaiting, id, inst.Status, inst.WakeOn)
	}
	// Apply the wake in-memory only. The first successful step inside stepWith
	// persists it; a rejection persists nothing, leaving the stored instance
	// suspended.
	if len(data) > 0 {
		if inst.Payload == nil {
			inst.Payload = map[string]any{}
		}
		inst.Payload[inst.Current] = data
	}
	inst.Status = store.StatusRunning
	inst.WakeOn = nil
	inst.WakeAt = nil
	if err := e.stepWith(ctx, inst, &resumeInput{signal: signal, data: data}); err != nil {
		return nil, fmt.Errorf("resume %q on %q: %w", id, signal, err)
	}
	return inst, nil
}

// resumeInput carries the wake signal and its data into the first step of a
// Signal-driven resume. It is nil for a cold Step (from Start).
type resumeInput struct {
	signal string
	data   map[string]any
}

// stepWith is the engine loop. On a resume (resume != nil) the first executor
// invocation receives the wake signal name and data and may reject the input
// with executor.ErrInvalidInput — in which case nothing is persisted and the
// instance is left in its prior (suspended) state. Subsequent iterations are
// internal transitions: they carry no signal and cannot reject.
func (e *Engine) stepWith(ctx context.Context, inst *store.Instance, resume *resumeInput) error {
	firstStep := true
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
			Payload: clonePayload(inst.Payload),
			Config:  state.Config,
		}
		if firstStep && resume != nil {
			evt.Name = resume.signal
			evt.Data = resume.data
		}

		res, err := exec.Execute(ctx, evt)
		if err != nil {
			// A resumed state may reject just-arrived input. The resume path
			// persists nothing before the first transition, so we can leave the
			// instance untouched (still suspended) and surface the error rather
			// than failing the whole instance.
			if firstStep && resume != nil && errors.Is(err, executor.ErrInvalidInput) {
				return fmt.Errorf("state %q rejected signal %q: %w", state.Name, resume.signal, err)
			}
			return e.fail(ctx, inst, fmt.Errorf("executor %q in state %q: %w", state.Executor, state.Name, err))
		}

		if res.Suspend {
			inst.Status = store.StatusSuspended
			inst.WakeOn = res.WakeOn
			inst.WakeAt = res.WakeAt
			return e.store.Save(ctx, inst)
		}

		// File this state's output under its namespace (the state name),
		// replacing any prior value. Empty output leaves the payload untouched.
		if len(res.Output) > 0 {
			if inst.Payload == nil {
				inst.Payload = map[string]any{}
			}
			inst.Payload[state.Name] = res.Output
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
		firstStep = false
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

// clonePayload returns a deep copy of the payload so executors get a
// read-only view: any mutation they make is discarded, and writes happen only
// through Result.Output. Nested maps and slices are copied; scalars and other
// types are carried by value/reference.
func clonePayload(p map[string]any) map[string]any {
	if p == nil {
		return map[string]any{}
	}
	return deepClone(p).(map[string]any)
}

func deepClone(v any) any {
	switch t := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(t))
		for k, val := range t {
			cp[k] = deepClone(val)
		}
		return cp
	case []any:
		cp := make([]any, len(t))
		for i, val := range t {
			cp[i] = deepClone(val)
		}
		return cp
	default:
		return v
	}
}
