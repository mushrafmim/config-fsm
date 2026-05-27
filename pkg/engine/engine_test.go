package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mushrafmim/config-fsm/internal/testfixtures"
	"github.com/mushrafmim/config-fsm/pkg/chart"
	"github.com/mushrafmim/config-fsm/pkg/executor"
	"github.com/mushrafmim/config-fsm/pkg/store"
)

func loadChart(t *testing.T, yaml string) *chart.Chart {
	t.Helper()
	c, err := chart.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("chart parse: %v", err)
	}
	return c
}

// engineFor builds a chart-agnostic engine and parses the chart the test will
// pass to Start — the engine no longer binds a chart at construction.
func engineFor(t *testing.T, yaml string, reg *executor.Registry, st store.Store, opts ...Option) (*Engine, *chart.Chart) {
	t.Helper()
	return New(reg, st, opts...), loadChart(t, yaml)
}

// Tier 1 milestone: a 3-state chart runs end-to-end synchronously.
func TestStep_ThreeStateChartRunsToTerminal(t *testing.T) {
	rec := &testfixtures.Recorder{}
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Always{Named: "emit", Event: "success", Recorder: rec})

	eng, c := engineFor(t, testfixtures.LinearThreeStates, reg, store.NewMemory())
	inst, err := eng.Start(context.Background(), c, "i1", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != store.StatusDone {
		t.Fatalf("status = %s, want done", inst.Status)
	}
	if inst.Current != "end" {
		t.Fatalf("current = %s, want end", inst.Current)
	}
	if len(rec.Calls) != 2 {
		t.Fatalf("executor calls = %d, want 2", len(rec.Calls))
	}
}

func TestStep_PassesPayloadAndConfigToExecutor(t *testing.T) {
	const y = `
id: c
version: "1"
initial: a
states:
  - name: a
    executor: echo
    config:
      value: "hello"
    transitions:
      - { on: ok, to: done }
  - name: done
    terminal: true
`
	rec := &testfixtures.Recorder{}
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Echo{Named: "echo", Event: "ok", WriteKey: "greeting", Recorder: rec})

	eng, c := engineFor(t, y, reg, store.NewMemory())
	inst, err := eng.Start(context.Background(), c, "i1", map[string]any{"user": "alice"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if len(rec.Calls) != 1 {
		t.Fatalf("calls = %d", len(rec.Calls))
	}
	if got := rec.Calls[0].Payload["user"]; got != "alice" {
		t.Fatalf("payload.user = %v", got)
	}
	if got := rec.Calls[0].Config["value"]; got != "hello" {
		t.Fatalf("config.value = %v", got)
	}
	// Output is namespaced under the state name ("a"), not written flat.
	ns, ok := inst.Payload["a"].(map[string]any)
	if !ok || ns["greeting"] != "hello" {
		t.Fatalf("output not namespaced under state: %v", inst.Payload)
	}
	// Seed data stays at the top level.
	if inst.Payload["user"] != "alice" {
		t.Fatalf("seed data lost: %v", inst.Payload)
	}
}

func TestStep_BranchesOnEmittedEvent(t *testing.T) {
	cases := []struct {
		emit     string
		wantTerm string
	}{
		{emit: "approve", wantTerm: "approved"},
		{emit: "reject", wantTerm: "rejected"},
	}
	for _, tc := range cases {
		t.Run(tc.emit, func(t *testing.T) {
			reg := executor.NewRegistry()
			_ = reg.Register(testfixtures.Always{Named: "chooser", Event: tc.emit})

			eng, c := engineFor(t, testfixtures.BranchOnEvent, reg, store.NewMemory())
			inst, err := eng.Start(context.Background(), c, "i1", nil)
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if inst.Current != tc.wantTerm || inst.Status != store.StatusDone {
				t.Fatalf("ended at %s/%s, want %s/done", inst.Current, inst.Status, tc.wantTerm)
			}
		})
	}
}

func TestStep_NoMatchingTransitionFails(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Always{Named: "chooser", Event: "neither"})

	eng, c := engineFor(t, testfixtures.BranchOnEvent, reg, store.NewMemory())
	inst, err := eng.Start(context.Background(), c, "i1", nil)
	if err == nil || !strings.Contains(err.Error(), "no transition") {
		t.Fatalf("err = %v", err)
	}
	if inst.Status != store.StatusFailed {
		t.Fatalf("status = %s, want failed", inst.Status)
	}
}

func TestStep_ExecutorErrorMarksFailed(t *testing.T) {
	boom := errors.New("kaboom")
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Erroring{Named: "emit", Err: boom})

	eng, c := engineFor(t, testfixtures.LinearThreeStates, reg, store.NewMemory())
	inst, err := eng.Start(context.Background(), c, "i1", nil)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wraps %v", err, boom)
	}
	if inst.Status != store.StatusFailed {
		t.Fatalf("status = %s, want failed", inst.Status)
	}
}

func TestStep_SuspendStopsAndPersistsWakeOn(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}})

	st := store.NewMemory()
	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, st)
	inst, err := eng.Start(context.Background(), c, "i1", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != store.StatusSuspended {
		t.Fatalf("status = %s, want suspended", inst.Status)
	}
	if len(inst.WakeOn) != 1 || inst.WakeOn[0] != "signal" {
		t.Fatalf("WakeOn = %v", inst.WakeOn)
	}
	loaded, err := st.Load(context.Background(), "i1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != store.StatusSuspended || loaded.Current != "wait" {
		t.Fatalf("persisted state mismatch: %+v", loaded)
	}
}

func TestStep_MissingExecutorFails(t *testing.T) {
	// Registry is empty: no "emit" registered, so the chart's first state
	// cannot resolve its executor.
	eng, c := engineFor(t, testfixtures.LinearThreeStates, executor.NewRegistry(), store.NewMemory())
	_, err := eng.Start(context.Background(), c, "i1", nil)
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("err = %v", err)
	}
}

func TestInstance_FetchesByID(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}})
	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, store.NewMemory())
	ctx := context.Background()

	if _, err := eng.Start(ctx, c, "i1", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	got, err := eng.Instance(ctx, "i1")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	if got.Current != "wait" || got.Status != store.StatusSuspended {
		t.Fatalf("fetched %s/%s, want wait/suspended", got.Current, got.Status)
	}

	if _, err := eng.Instance(ctx, "ghost"); err != store.ErrNotFound {
		t.Fatalf("missing instance: err = %v, want ErrNotFound", err)
	}
}

func TestSignal_InvalidInputStaysSuspendedAndRetriable(t *testing.T) {
	rec := &testfixtures.Recorder{}
	reg := executor.NewRegistry()
	// SuspendThenTerminate parks at "wait" on executor "park".
	_ = reg.Register(testfixtures.Gate{Named: "park", WakeOn: []string{"signal"}, Require: "token", Recorder: rec})

	st := store.NewMemory()
	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, st)
	ctx := context.Background()

	if _, err := eng.Start(ctx, c, "i1", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Bad signal: required field missing. Must be rejected, not fail the instance.
	_, err := eng.SignalInstance(ctx, "i1", "signal", map[string]any{"wrong": "field"})
	if err == nil {
		t.Fatal("expected rejection error, got nil")
	}
	if !errors.Is(err, executor.ErrInvalidInput) {
		t.Fatalf("err = %v, want wraps ErrInvalidInput", err)
	}

	// Instance must remain suspended and retriable.
	loaded, _ := st.Load(ctx, "i1")
	if loaded.Status != store.StatusSuspended || loaded.Current != "wait" {
		t.Fatalf("after reject: %s/%s, want suspended/wait", loaded.Current, loaded.Status)
	}
	if len(loaded.WakeOn) != 1 || loaded.WakeOn[0] != "signal" {
		t.Fatalf("WakeOn lost after reject: %v", loaded.WakeOn)
	}
	// The rejected data must not have been persisted.
	if _, exists := loaded.Payload["wait"]; exists {
		t.Fatalf("rejected data was persisted: %v", loaded.Payload)
	}

	// Corrected redelivery wakes the same instance and runs it to completion.
	if _, err := eng.SignalInstance(ctx, "i1", "signal", map[string]any{"token": "abc"}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	loaded, _ = st.Load(ctx, "i1")
	if loaded.Status != store.StatusDone || loaded.Current != "done" {
		t.Fatalf("after good signal: %s/%s, want done/done", loaded.Current, loaded.Status)
	}
}

// A non-validation error on resume must still fail the instance — only
// ErrInvalidInput gets the stay-suspended treatment.
func TestSignal_NonValidationErrorFails(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(executor.Func{N: "park", Fn: func(ctx context.Context, e *executor.Event) (executor.Result, error) {
		if e.Name == "" {
			return executor.Result{Suspend: true, WakeOn: []string{"signal"}}, nil
		}
		return executor.Result{}, errors.New("downstream boom")
	}})

	st := store.NewMemory()
	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, st)
	ctx := context.Background()

	if _, err := eng.Start(ctx, c, "i1", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := eng.SignalInstance(ctx, "i1", "signal", nil); err == nil {
		t.Fatal("expected error from failing resume")
	}
	loaded, _ := st.Load(ctx, "i1")
	if loaded.Status != store.StatusFailed {
		t.Fatalf("status = %s, want failed", loaded.Status)
	}
}

func TestSignalInstance_NotFound(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Always{Named: "emit", Event: "success"})
	eng := New(reg, store.NewMemory())
	if _, err := eng.SignalInstance(context.Background(), "ghost", "anything", nil); err != store.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestSignalInstance_WrongSignalIsNotWaiting(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}})
	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, store.NewMemory())
	ctx := context.Background()

	if _, err := eng.Start(ctx, c, "i1", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Instance is parked on "signal" but we send "other".
	_, err := eng.SignalInstance(ctx, "i1", "other", nil)
	if !errors.Is(err, store.ErrNotWaiting) {
		t.Fatalf("err = %v, want ErrNotWaiting", err)
	}
}

func TestSignalInstance_WakesAndReturnsInstance(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}})

	st := store.NewMemory()
	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, st)

	if _, err := eng.Start(context.Background(), c, "i1", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// SignalInstance returns the resulting instance directly — no re-read needed.
	returned, err := eng.SignalInstance(context.Background(), "i1", "signal", nil)
	if err != nil {
		t.Fatalf("SignalInstance: %v", err)
	}
	if returned.Status != store.StatusDone || returned.Current != "done" {
		t.Fatalf("returned %s/%s, want done/done", returned.Current, returned.Status)
	}
}

func TestSignal_RoutesDataToStateNamespace(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}})

	st := store.NewMemory()
	// SuspendThenTerminate parks at state "wait".
	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, st)

	if _, err := eng.Start(context.Background(), c, "i1", map[string]any{"keep": "original"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := eng.SignalInstance(context.Background(), "i1", "signal", map[string]any{"added": 42}); err != nil {
		t.Fatalf("SignalInstance: %v", err)
	}
	loaded, _ := st.Load(context.Background(), "i1")

	// Signal data lands under the suspended state's namespace ("wait").
	ns, ok := loaded.Payload["wait"].(map[string]any)
	if !ok || ns["added"] != 42 {
		t.Fatalf("signal data not namespaced under state: %v", loaded.Payload)
	}
	// Top-level seed data is untouched — no collision.
	if loaded.Payload["keep"] != "original" {
		t.Fatalf("seed data clobbered: %v", loaded.Payload)
	}
}

func TestSignalInstance_DuplicateDeliveryIsNotWaiting(t *testing.T) {
	rec := &testfixtures.Recorder{}
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}, Recorder: rec})

	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, store.NewMemory())
	ctx := context.Background()
	if _, err := eng.Start(ctx, c, "i1", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := eng.SignalInstance(ctx, "i1", "signal", nil); err != nil {
		t.Fatalf("first signal: %v", err)
	}
	callsAfterFirst := len(rec.Calls)
	// Instance has advanced past the wait — a duplicate delivery is ErrNotWaiting
	// and must not re-invoke the executor.
	if _, err := eng.SignalInstance(ctx, "i1", "signal", nil); !errors.Is(err, store.ErrNotWaiting) {
		t.Fatalf("duplicate signal err = %v, want ErrNotWaiting", err)
	}
	if len(rec.Calls) != callsAfterFirst {
		t.Fatalf("duplicate delivery re-invoked executor: %d calls, want %d", len(rec.Calls), callsAfterFirst)
	}
}

func TestCompletionHook_FiresOnTerminalWithOutcome(t *testing.T) {
	var got *store.Instance
	calls := 0
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Always{Named: "emit", Event: "success"})

	eng, c := engineFor(t, testfixtures.LinearThreeStates, reg, store.NewMemory(),
		WithCompletionHook(func(ctx context.Context, inst *store.Instance) {
			calls++
			got = inst
		}))
	if _, err := eng.Start(context.Background(), c, "i1", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if calls != 1 {
		t.Fatalf("hook fired %d times, want exactly 1", calls)
	}
	if got.Status != store.StatusDone || got.Current != "end" {
		t.Fatalf("hook saw %s/%s, want done/end", got.Current, got.Status)
	}
}

func TestCompletionHook_FiresOnFailure(t *testing.T) {
	var got *store.Instance
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Erroring{Named: "emit", Err: errors.New("boom")})

	eng, c := engineFor(t, testfixtures.LinearThreeStates, reg, store.NewMemory(),
		WithCompletionHook(func(ctx context.Context, inst *store.Instance) { got = inst }))
	if _, err := eng.Start(context.Background(), c, "i1", nil); err == nil {
		t.Fatal("expected error from failing executor")
	}
	if got == nil || got.Status != store.StatusFailed {
		t.Fatalf("hook on failure saw %+v, want status failed", got)
	}
}

// The key durability property: the hook fires from the Signal call that
// completes the instance — not from Start — and reads the callback target
// that was persisted in the payload at Start time.
func TestCompletionHook_FiresFromSignalWithPersistedTarget(t *testing.T) {
	var notifiedURL any
	hookCalls := 0
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}})

	eng, c := engineFor(t, testfixtures.SuspendThenTerminate, reg, store.NewMemory(),
		WithCompletionHook(func(ctx context.Context, inst *store.Instance) {
			hookCalls++
			notifiedURL = inst.Payload["callback_url"]
		}))

	if _, err := eng.Start(context.Background(), c, "i1", map[string]any{"callback_url": "https://upstream/done"}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Still parked — must not have fired yet.
	if hookCalls != 0 {
		t.Fatalf("hook fired before completion (%d calls)", hookCalls)
	}

	if _, err := eng.SignalInstance(context.Background(), "i1", "signal", nil); err != nil {
		t.Fatalf("SignalInstance: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("hook fired %d times after SignalInstance, want 1", hookCalls)
	}
	if notifiedURL != "https://upstream/done" {
		t.Fatalf("hook read callback_url = %v, want the persisted target", notifiedURL)
	}
}

// TestSignal_FormSubmissionAndReviewFlow models the real-world flow:
// user submits a form, data goes to an org, org callbacks with approve or
// requires_rework — the latter loops back to the submission state.
func TestSignal_FormSubmissionAndReviewFlow(t *testing.T) {
	const flow = `
id: submission_flow
version: "1"
initial: submission
states:
  - name: submission
    executor: form
    transitions:
      - { on: form_submitted, to: dispatch }
  - name: dispatch
    executor: org_api
    transitions:
      - { on: dispatched, to: awaiting_review }
  - name: awaiting_review
    executor: review
    transitions:
      - { on: approve,         to: approved }
      - { on: requires_rework, to: submission }
  - name: approved
    terminal: true
`
	rec := &testfixtures.Recorder{}
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Parker{Named: "form", WakeOn: []string{"form_submitted"}, Recorder: rec})
	_ = reg.Register(testfixtures.Always{Named: "org_api", Event: "dispatched", Recorder: rec})
	_ = reg.Register(testfixtures.Parker{Named: "review", WakeOn: []string{"approve", "requires_rework"}, Recorder: rec})

	st := store.NewMemory()
	eng, c := engineFor(t, flow, reg, st)
	ctx := context.Background()

	// 1. Start: parks at submission, waiting for the user to submit.
	inst, err := eng.Start(ctx, c, "i1", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != store.StatusSuspended || inst.Current != "submission" {
		t.Fatalf("after Start: %s/%s, want suspended/submission", inst.Current, inst.Status)
	}

	// 2. User submits the form. Engine should dispatch to org and park at awaiting_review.
	if _, err := eng.SignalInstance(ctx, "i1", "form_submitted", map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("form_submitted: %v", err)
	}
	loaded, _ := st.Load(ctx, "i1")
	if loaded.Status != store.StatusSuspended || loaded.Current != "awaiting_review" {
		t.Fatalf("after form_submitted: %s/%s, want suspended/awaiting_review", loaded.Current, loaded.Status)
	}
	// Form data lands under the submission state's namespace.
	if nsField(loaded, "submission", "name") != "alice" {
		t.Fatalf("submission data missing: %v", loaded.Payload)
	}

	// 3. Org callbacks with requires_rework: loops back to submission and parks again.
	if _, err := eng.SignalInstance(ctx, "i1", "requires_rework", map[string]any{"reason": "missing field"}); err != nil {
		t.Fatalf("requires_rework: %v", err)
	}
	loaded, _ = st.Load(ctx, "i1")
	if loaded.Status != store.StatusSuspended || loaded.Current != "submission" {
		t.Fatalf("after rework: %s/%s, want suspended/submission", loaded.Current, loaded.Status)
	}
	// Review outcome data lands under the awaiting_review namespace — distinct
	// from the submission namespace, so no collision with the form data.
	if nsField(loaded, "awaiting_review", "reason") != "missing field" {
		t.Fatalf("rework reason missing: %v", loaded.Payload)
	}

	// 4. User resubmits.
	if _, err := eng.SignalInstance(ctx, "i1", "form_submitted", nil); err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	loaded, _ = st.Load(ctx, "i1")
	if loaded.Current != "awaiting_review" {
		t.Fatalf("after resubmit: current = %s, want awaiting_review", loaded.Current)
	}

	// 5. Org approves: run to terminal.
	if _, err := eng.SignalInstance(ctx, "i1", "approve", nil); err != nil {
		t.Fatalf("approve: %v", err)
	}
	loaded, _ = st.Load(ctx, "i1")
	if loaded.Status != store.StatusDone || loaded.Current != "approved" {
		t.Fatalf("after approve: %s/%s, want done/approved", loaded.Current, loaded.Status)
	}

	// Sanity: the submission namespace survived the whole journey, including
	// the rework loop, without being clobbered by the review namespace.
	if nsField(loaded, "submission", "name") != "alice" {
		t.Fatalf("alice's name lost across the flow: %v", loaded.Payload)
	}
}

// One engine runs multiple different charts at once; each instance resumes
// against its own pinned chart (reloaded from storage), with no chart bound to
// the engine.
func TestEngine_RunsMultipleChartsOnOneEngine(t *testing.T) {
	reg := executor.NewRegistry()
	_ = reg.Register(testfixtures.Always{Named: "emit", Event: "success"})           // for LinearThreeStates
	_ = reg.Register(testfixtures.Parker{Named: "park", WakeOn: []string{"signal"}}) // for SuspendThenTerminate

	eng := New(reg, store.NewMemory())
	linear := loadChart(t, testfixtures.LinearThreeStates)
	parker := loadChart(t, testfixtures.SuspendThenTerminate)
	ctx := context.Background()

	// Start two instances of two different charts on the same engine.
	a, err := eng.Start(ctx, linear, "a", nil)
	if err != nil {
		t.Fatalf("start a: %v", err)
	}
	if a.Status != store.StatusDone {
		t.Fatalf("a status = %s, want done", a.Status)
	}

	b, err := eng.Start(ctx, parker, "b", nil)
	if err != nil {
		t.Fatalf("start b: %v", err)
	}
	if b.Status != store.StatusSuspended {
		t.Fatalf("b status = %s, want suspended", b.Status)
	}

	// Resume b — the engine holds no chart, so it must reload b's pinned chart
	// from the instance itself.
	resumed, err := eng.SignalInstance(ctx, "b", "signal", nil)
	if err != nil {
		t.Fatalf("signal b: %v", err)
	}
	if resumed.Status != store.StatusDone || resumed.Current != "done" {
		t.Fatalf("b resumed to %s/%s, want done/done", resumed.Current, resumed.Status)
	}
	// Each instance recorded its own chart id.
	if a.ChartID != "linear" || b.ChartID != "park" {
		t.Fatalf("chart ids: a=%q b=%q, want linear/park", a.ChartID, b.ChartID)
	}
}

// nsField reads payload[namespace][field], or nil if either level is absent.
func nsField(inst *store.Instance, namespace, field string) any {
	ns, ok := inst.Payload[namespace].(map[string]any)
	if !ok {
		return nil
	}
	return ns[field]
}
