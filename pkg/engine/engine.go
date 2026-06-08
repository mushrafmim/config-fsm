// Package engine drives an Instance through a Chart.
//
// The engine is intentionally a thin loop: look up the current state, call
// its executor, take a transition or suspend, persist, repeat. It owns no
// goroutines and no scheduling — those concerns live in scheduler/outbox.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

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
// The hook owns its own liveness reliability; it returns no error, so any
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

// WithLogger sets the slog.Logger the engine logs lifecycle events to. If not
// provided, the engine uses slog.Default(). A nil logger is ignored.
func WithLogger(l *slog.Logger) Option {
	return func(e *Engine) {
		if l != nil {
			e.logger = l
		}
	}
}

// WithLeaseDuration configures the heartbeat lease duration for execution liveness.
func WithLeaseDuration(d time.Duration) Option {
	return func(e *Engine) {
		e.leaseDuration = d
	}
}

// Engine is a chart-agnostic executor: charts are supplied per Start and
// pinned to each instance (stored with it), so one engine runs any number of
// charts and in-flight instances are unaffected by redeploys.
type Engine struct {
	registry      *executor.Registry
	store         store.Store
	onComplete    CompletionHook
	logger        *slog.Logger
	leaseDuration time.Duration
}

func New(reg *executor.Registry, s store.Store, opts ...Option) *Engine {
	e := &Engine{
		registry:      reg,
		store:         s,
		logger:        slog.Default(),
		leaseDuration: 5 * time.Minute,
	}
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
	execID := generateID()
	exec := &store.Execution{
		ID:         execID,
		InstanceID: id,
		Trigger:    "start",
	}
	if err := e.store.StartInstance(ctx, inst, exec); err != nil {
		return nil, fmt.Errorf("save new instance: %w", err)
	}
	e.logger.InfoContext(ctx, "instance started", "id", id, "chart", c.ID, "version", c.Version)
	if err := e.stepWith(ctx, c, inst, execID, nil); err != nil {
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
	return e.stepWith(ctx, c, inst, "", nil)
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

// SignalOption configures options on a SignalInstance call.
type SignalOption func(*signalOpts)

type signalOpts struct {
	deliveryID string
}

// WithDeliveryID sets a unique signal delivery ID for deduplication.
func WithDeliveryID(id string) SignalOption {
	return func(o *signalOpts) {
		o.deliveryID = id
	}
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
func (e *Engine) SignalInstance(ctx context.Context, id, signal string, data map[string]any, opts ...SignalOption) (*store.Instance, error) {
	var sOpts signalOpts
	for _, opt := range opts {
		opt(&sOpts)
	}

	e.logger.DebugContext(ctx, "signal received", "id", id, "signal", signal)

	execID := generateID()
	exec := &store.Execution{
		ID:         execID,
		InstanceID: id,
		Trigger:    "signal:" + signal,
	}
	if sOpts.deliveryID != "" {
		exec.SignalDeliveryID = &sOpts.deliveryID
	}

	inst, err := e.store.Claim(ctx, id, signal, exec)
	if err != nil {
		// Not found, already advanced, or waiting on a different signal — a
		// no-op. Logged so a "nothing happened" delivery is traceable.
		e.logger.DebugContext(ctx, "signal ignored", "id", id, "signal", signal, "reason", err)
		return nil, err
	}
	c, err := chartFor(inst)
	if err != nil {
		e.logger.ErrorContext(ctx, "failed to load instance chart", "id", id, "error", err)
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
	if err := e.stepWith(ctx, c, inst, execID, &resumeInput{signal: signal, data: data}); err != nil {
		if errors.Is(err, executor.ErrInvalidInput) {
			if relErr := e.releaseToSuspended(ctx, id, execID); relErr != nil {
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
func (e *Engine) releaseToSuspended(ctx context.Context, id, execID string) error {
	fresh, err := e.store.Load(ctx, id)
	if err != nil {
		return err
	}
	if fresh.Status != store.StatusRunning {
		return nil
	}
	fresh.Status = store.StatusSuspended
	if err := e.store.CloseExecution(ctx, fresh, execID, store.OutcomeSuspended, nil); err != nil {
		return err
	}
	e.logger.DebugContext(ctx, "claim released to suspended", "id", id, "state", fresh.Current)
	return nil
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
func (e *Engine) stepWith(ctx context.Context, c *chart.Chart, inst *store.Instance, execID string, resume *resumeInput) error {
	firstStep := true
	for {
		state, ok := c.State(inst.Current)
		if !ok {
			return e.fail(ctx, inst, execID, fmt.Errorf("state %q not found in chart %q", inst.Current, c.ID))
		}

		if state.Terminal {
			inst.Status = store.StatusDone
			if err := e.store.CloseExecution(ctx, inst, execID, store.OutcomeDone, nil); err != nil {
				return err
			}
			e.logger.InfoContext(ctx, "instance completed", "id", inst.ID, "chart", c.ID, "state", state.Name)
			e.fireComplete(ctx, inst)
			return nil
		}

		exec, ok := e.registry.Get(state.Executor)
		if !ok {
			return e.fail(ctx, inst, execID, fmt.Errorf("executor %q not registered (state %q)", state.Executor, state.Name))
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
				e.logger.WarnContext(ctx, "signal input rejected", "id", inst.ID, "state", state.Name, "signal", resume.signal, "error", err)
				return fmt.Errorf("state %q rejected signal %q: %w", state.Name, resume.signal, err)
			}
			return e.fail(ctx, inst, execID, fmt.Errorf("executor %q in state %q: %w", state.Executor, state.Name, err))
		}

		if res.Suspend {
			// Wake signals are chart-owned: the engine reads them from the state,
			// not from the executor. A state that suspends with neither a declared
			// signal nor a WakeAt deadline could never be woken — fail it loudly
			// rather than park it forever.
			if len(state.Signals) == 0 && res.WakeAt == nil {
				return e.fail(ctx, inst, execID, fmt.Errorf("state %q suspended but declares no signals and set no WakeAt — instance would be unwakeable", state.Name))
			}
			// A suspending executor may still produce output (e.g. the handle of
			// the external task it is now waiting on). File it under the state's
			// namespace, exactly as the transition path does, before parking.
			if len(res.Output) > 0 {
				if inst.Payload == nil {
					inst.Payload = map[string]any{}
				}
				inst.Payload[state.Name] = res.Output
			}
			inst.Status = store.StatusSuspended
			inst.WakeOn = state.Signals
			inst.WakeAt = res.WakeAt
			e.logger.DebugContext(ctx, "instance suspended", "id", inst.ID, "state", state.Name, "wakeOn", state.Signals)
			return e.store.CloseExecution(ctx, inst, execID, store.OutcomeSuspended, nil)
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
			return e.fail(ctx, inst, execID, fmt.Errorf("no transition from %q on event %q", state.Name, res.Event))
		}

		e.logger.DebugContext(ctx, "transition", "id", inst.ID, "from", state.Name, "on", res.Event, "to", next)
		inst.Current = next
		inst.WakeOn = nil
		inst.WakeAt = nil
		if err := e.store.Save(ctx, inst, execID); err != nil {
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
func (e *Engine) fail(ctx context.Context, inst *store.Instance, execID string, cause error) error {
	inst.Status = store.StatusFailed
	if saveErr := e.store.CloseExecution(ctx, inst, execID, store.OutcomeFailed, cause); saveErr != nil {
		return fmt.Errorf("%w (also: persist failed status: %v)", cause, saveErr)
	}
	e.logger.ErrorContext(ctx, "instance failed", "id", inst.ID, "state", inst.Current, "error", cause)
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

// RecoverStale identifies executions whose process died (expired heartbeat lease)
// and rolls them back to their last suspend point (interim rollback policy).
func (e *Engine) RecoverStale(ctx context.Context) (int, error) {
	threshold := time.Now().Add(-e.leaseDuration)
	crashed, err := e.store.FindCrashedExecutions(ctx, threshold, 0)
	if err != nil {
		return 0, fmt.Errorf("find crashed executions: %w", err)
	}

	recovered := 0
	for _, exec := range crashed {
		e.logger.InfoContext(ctx, "recovering crashed execution", "exec_id", exec.ID, "instance_id", exec.InstanceID)

		inst, err := e.store.Load(ctx, exec.InstanceID)
		if err != nil {
			e.logger.ErrorContext(ctx, "failed to load instance for recovery", "instance_id", exec.InstanceID, "error", err)
			continue
		}

		if inst.Status != store.StatusRunning {
			// Already closed or moved on. Clean up the execution.
			if err := e.store.CloseExecution(ctx, inst, exec.ID, store.OutcomeCrashed, errors.New("execution crashed (process died)")); err != nil {
				e.logger.ErrorContext(ctx, "failed to close stale execution", "exec_id", exec.ID, "error", err)
			}
			continue
		}

		checkpoint, err := e.store.FindLastCheckpoint(ctx, inst.ID)
		if err != nil && err != store.ErrNotFound {
			e.logger.ErrorContext(ctx, "failed to search for checkpoint", "instance_id", inst.ID, "error", err)
			continue
		}

		if err == store.ErrNotFound {
			// No-checkpoint policy: mark instance failed
			inst.Status = store.StatusFailed
			e.logger.WarnContext(ctx, "no checkpoint found for crashed instance; marking failed", "instance_id", inst.ID)
			closeErr := e.store.CloseExecution(ctx, inst, exec.ID, store.OutcomeCrashed, errors.New("crashed before first suspend point"))
			if closeErr != nil {
				e.logger.ErrorContext(ctx, "failed to close no-checkpoint execution", "exec_id", exec.ID, "error", closeErr)
			}
			e.fireComplete(ctx, inst)
			recovered++
			continue
		}

		// Roll back to checkpoint
		inst.Status = store.StatusSuspended
		inst.Current = checkpoint.SuspendedAtState
		inst.WakeOn = checkpoint.SuspendedWakeOn
		inst.WakeAt = nil // Clear wake at; let caller retry signal or timeout

		closeErr := e.store.CloseExecution(ctx, inst, exec.ID, store.OutcomeCrashed, errors.New("execution crashed; rolled back to last suspend point"))
		if closeErr != nil {
			e.logger.ErrorContext(ctx, "failed to close crashed execution on rollback", "exec_id", exec.ID, "error", closeErr)
			continue
		}

		e.logger.InfoContext(ctx, "recovered instance by rolling back to checkpoint", "instance_id", inst.ID, "checkpoint_state", checkpoint.SuspendedAtState)
		recovered++
	}

	return recovered, nil
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
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
