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

// Engine is a chart-agnostic executor: charts are supplied per Start and
// pinned to each instance (stored with it), so one engine runs any number of
// charts and in-flight instances are unaffected by redeploys.
type Engine struct {
	registry   *executor.Registry
	store      store.Store
	onComplete CompletionHook
}

func New(reg *executor.Registry, s store.Store, opts ...Option) *Engine {
	e := &Engine{registry: reg, store: s}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Start creates a new instance running the given chart, pins the chart to the
// instance (serialized into it for durable resume), persists it, and drives it
// until it suspends, terminates, or errors. The caller supplies the chart;
// resume (SignalInstance) reloads it from the instance, so no chart is needed
// then.
func (e *Engine) Start(ctx context.Context, c *chart.Chart, id string, payload map[string]any) (*store.Instance, error) {
	if payload == nil {
		payload = map[string]any{}
	}
	def, err := c.Bytes()
	if err != nil {
		return nil, fmt.Errorf("serialize chart %q: %w", c.ID, err)
	}
	inst := &store.Instance{
		ID:           id,
		ChartID:      c.ID,
		ChartVersion: c.Version,
		ChartDef:     def,
		Current:      c.Initial,
		Payload:      payload,
		Status:       store.StatusRunning,
	}
	if err := e.store.Save(ctx, inst); err != nil {
		return nil, fmt.Errorf("save new instance: %w", err)
	}
	if err := e.stepWith(ctx, c, inst, nil); err != nil {
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

// Step drives an instance forward against the given chart until it terminates,
// suspends, or errors. On error the instance is marked failed and persisted.
// Most callers use Start and SignalInstance; Step is for advanced use when you
// already hold both the chart and the instance.
func (e *Engine) Step(ctx context.Context, c *chart.Chart, inst *store.Instance) error {
	return e.stepWith(ctx, c, inst, nil)
}

// chartFor reconstructs the chart an instance is pinned to from its stored
// definition.
func chartFor(inst *store.Instance) (*chart.Chart, error) {
	if len(inst.ChartDef) == 0 {
		return nil, fmt.Errorf("instance %q has no stored chart definition", inst.ID)
	}
	c, err := chart.FromBytes(inst.ChartDef)
	if err != nil {
		return nil, fmt.Errorf("load chart for instance %q: %w", inst.ID, err)
	}
	return c, nil
}

// SignalInstance wakes the single suspended instance identified by id that is
// waiting on the named signal, and drives it synchronously until it suspends
// again, terminates, or errors. It returns the resulting instance so the
// caller can render the new state without a separate read.
//
// It returns store.ErrNotFound if no such instance exists, and
// store.ErrNotWaiting if the instance is not currently suspended on this
// signal (already advanced — e.g. a duplicate delivery — in a final state, or
// parked on a different signal). Callers can map ErrNotWaiting to an
// idempotent ack.
//
// Concurrency: the suspended → running transition is an atomic Claim in the
// store, so two concurrent deliveries cannot both drive the instance — the
// loser gets ErrNotWaiting.
//
// Rejection: if the resumed state rejects the input with
// executor.ErrInvalidInput, the claim is released back to suspended (the
// rejected data is never persisted) so the instance stays retriable, and the
// returned error wraps ErrInvalidInput (use errors.Is to map it to a 4xx).
//
// Payload: data is filed under the suspended state's namespace (its name);
// empty data leaves the namespace untouched.
func (e *Engine) SignalInstance(ctx context.Context, id, signal string, data map[string]any) (*store.Instance, error) {
	inst, err := e.store.Claim(ctx, id, signal)
	if err != nil {
		return nil, err
	}
	c, err := chartFor(inst)
	if err != nil {
		return nil, err
	}
	// Apply the wake in-memory only. The first successful step inside stepWith
	// persists it; on rejection it is never saved and the claim is released.
	if len(data) > 0 {
		if inst.Payload == nil {
			inst.Payload = map[string]any{}
		}
		inst.Payload[inst.Current] = data
	}
	if err := e.stepWith(ctx, c, inst, &resumeInput{signal: signal, data: data}); err != nil {
		if errors.Is(err, executor.ErrInvalidInput) {
			if relErr := e.releaseToSuspended(ctx, id); relErr != nil {
				return nil, errors.Join(fmt.Errorf("resume %q on %q: %w", id, signal, err), relErr)
			}
		}
		return nil, fmt.Errorf("resume %q on %q: %w", id, signal, err)
	}
	return inst, nil
}

// releaseToSuspended returns a claimed-but-rejected instance to the suspended
// state. It reloads the persisted (pre-data) instance so the rejected input is
// not written, then flips status back. WakeOn is intact because Claim retains
// it. It is a no-op if the instance has already moved on.
func (e *Engine) releaseToSuspended(ctx context.Context, id string) error {
	fresh, err := e.store.Load(ctx, id)
	if err != nil {
		return err
	}
	if fresh.Status != store.StatusRunning {
		return nil
	}
	fresh.Status = store.StatusSuspended
	return e.store.Save(ctx, fresh)
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
func (e *Engine) stepWith(ctx context.Context, c *chart.Chart, inst *store.Instance, resume *resumeInput) error {
	firstStep := true
	for {
		state, ok := c.State(inst.Current)
		if !ok {
			return e.fail(ctx, inst, fmt.Errorf("state %q not found in chart %q", inst.Current, c.ID))
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
