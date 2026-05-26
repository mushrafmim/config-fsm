// Package testfixtures provides reference Executor implementations and
// small chart YAMLs that the rest of the module's tests build on.
//
// Anything here is test-only by intent: it lives under internal/ so external
// consumers cannot import it. As production-grade reference executors emerge
// (e.g. an HTTP executor that uses the outbox), they will move to a public
// package; for now everything is exemplary.
package testfixtures

import (
	"context"
	"maps"

	"github.com/mushrafmim/config-fsm/pkg/executor"
)

// Call captures one invocation of an executor — useful for assertions about
// what the engine actually drove.
type Call struct {
	Executor string
	Event    string         // the inbound Event.Name (empty on initial step)
	Payload  map[string]any // shallow snapshot at call time
	Config   map[string]any // per-state config the engine passed in
}

// Recorder is a shared sink. Pass a pointer to any fixture executor that
// accepts one and inspect Calls after the run.
type Recorder struct {
	Calls []Call
}

func (r *Recorder) record(name string, e *executor.Event) {
	if r == nil {
		return
	}
	r.Calls = append(r.Calls, Call{
		Executor: name,
		Event:    e.Name,
		Payload:  cloneMap(e.Payload),
		Config:   cloneMap(e.Config),
	})
}

// Always is an executor that always emits the configured Event.
type Always struct {
	Named    string
	Event    string
	Recorder *Recorder
}

func (a Always) Name() string { return a.Named }

func (a Always) Execute(ctx context.Context, e *executor.Event) (executor.Result, error) {
	a.Recorder.record(a.Named, e)
	return executor.Result{Event: a.Event}, nil
}

// Erroring returns Err on every invocation.
type Erroring struct {
	Named string
	Err   error
}

func (x Erroring) Name() string { return x.Named }

func (x Erroring) Execute(ctx context.Context, e *executor.Event) (executor.Result, error) {
	return executor.Result{}, x.Err
}

// Parker is a signal-routing executor.
//
// On a cold invocation (Event.Name == ""), it suspends with the configured
// WakeOn signals. On resume (Event.Name carries the signal that woke the
// instance), it emits that signal name as the transition event — letting the
// engine route to whichever transition matches.
type Parker struct {
	Named    string
	WakeOn   []string
	Recorder *Recorder
}

func (p Parker) Name() string { return p.Named }

func (p Parker) Execute(ctx context.Context, e *executor.Event) (executor.Result, error) {
	p.Recorder.record(p.Named, e)
	if e.Name != "" {
		return executor.Result{Event: e.Name}, nil
	}
	return executor.Result{Suspend: true, WakeOn: p.WakeOn}, nil
}

// Echo copies a configured payload key into the instance payload and emits
// the configured event. Handy for asserting payload mutation flows.
type Echo struct {
	Named    string
	Event    string
	WriteKey string // payload key to set from Config["value"]
	Recorder *Recorder
}

func (x Echo) Name() string { return x.Named }

func (x Echo) Execute(ctx context.Context, e *executor.Event) (executor.Result, error) {
	x.Recorder.record(x.Named, e)
	if x.WriteKey != "" {
		e.Payload[x.WriteKey] = e.Config["value"]
	}
	return executor.Result{Event: x.Event}, nil
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	return maps.Clone(m)
}
