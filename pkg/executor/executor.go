// Package executor defines the runtime contract that states delegate to.
//
// Executors are stateless plugins registered by name. The engine looks up an
// executor for the current state, hands it an Event (payload + optional name
// + per-state config), and uses the returned Result to decide what to do
// next: take a transition, suspend, or fail.
package executor

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Event is the runtime input to an Executor.
//
// Name is set when the engine resumes an instance from an external signal;
// it is empty on the initial step. Payload is the instance's mutable state.
// Config is the per-state config block taken from the chart.
type Event struct {
	Name    string
	Payload map[string]any
	Config  map[string]any
}

// Result is the runtime output of an Executor.
//
// Exactly one of {Suspend, Event} is meaningful per call:
//   - Suspend=true: the engine parks the instance with WakeOn / WakeAt.
//   - Suspend=false: Event names the transition the engine should take.
type Result struct {
	Event   string
	Suspend bool
	WakeOn  []string
	WakeAt  *time.Time
}

// Executor is the plugin contract.
type Executor interface {
	Name() string
	Execute(ctx context.Context, e *Event) (Result, error)
}

// Registry is a thread-safe name → Executor map.
type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
}

func NewRegistry() *Registry {
	return &Registry{executors: make(map[string]Executor)}
}

// Register adds an executor. Duplicate names are rejected.
func (r *Registry) Register(e Executor) error {
	if e == nil {
		return fmt.Errorf("executor is nil")
	}
	name := e.Name()
	if name == "" {
		return fmt.Errorf("executor name is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.executors[name]; dup {
		return fmt.Errorf("executor %q already registered", name)
	}
	r.executors[name] = e
	return nil
}

// Get looks up an executor by name.
func (r *Registry) Get(name string) (Executor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.executors[name]
	return e, ok
}

// Func adapts a plain function into an Executor — handy for tests and small
// inline executors.
type Func struct {
	N  string
	Fn func(ctx context.Context, e *Event) (Result, error)
}

func (f Func) Name() string { return f.N }

func (f Func) Execute(ctx context.Context, e *Event) (Result, error) {
	return f.Fn(ctx, e)
}
