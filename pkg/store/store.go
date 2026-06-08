// Package store holds the instance persistence layer.
//
// An Instance is a single in-flight execution of a Chart. The Store interface
// is the contract every backend implements; this package ships an in-memory
// implementation suitable for tests and library-only consumers. The Postgres
// implementation lives elsewhere (Tier 2).
package store

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"
)

// Status enumerates the lifecycle states of an Instance.
type Status string

const (
	StatusRunning   Status = "running"
	StatusSuspended Status = "suspended"
	StatusDone      Status = "done"
	StatusFailed    Status = "failed"
)

// Instance is a single live execution of a chart.
//
// ChartDef holds the serialized chart definition the instance is pinned to,
// captured at Start. Carrying it with the instance makes each instance
// self-contained: resume reloads its own chart, so a redeploy never affects
// in-flight instances and there is no separate chart registry to manage. The
// store treats it as opaque bytes.
type Instance struct {
	ID            string
	ChartID       string
	ChartVersion  string
	ChartDef      []byte
	Current       string
	BusinessState string
	Payload       map[string]any
	Status        Status
	WakeOn        []string
	WakeAt        *time.Time
	Version       int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ExecutionOutcome enumerates the outcomes of an Execution.
type ExecutionOutcome string

const (
	OutcomeSuspended ExecutionOutcome = "suspended"
	OutcomeDone      ExecutionOutcome = "done"
	OutcomeFailed    ExecutionOutcome = "failed"
	OutcomeCrashed   ExecutionOutcome = "crashed"
)

// Execution tracks a single drive of an Instance.
type Execution struct {
	ID               string
	InstanceID       string
	Trigger          string            // "start", "signal:<name>", "recovery"
	SignalDeliveryID *string           // nullable, for dedup
	StartedAt        time.Time
	FinishedAt       *time.Time        // nullable
	Outcome          *ExecutionOutcome // nullable, "suspended" | "done" | "failed" | "crashed"
	SuspendedAtState string            // state where the instance suspended
	SuspendedWakeOn  []string          // signals it's waiting on
	Error            string            // error message if outcome = failed
	HeartbeatAt      time.Time         // liveness lease
}

// ErrNotFound is returned by Load when no instance with the given ID exists.
var ErrNotFound = errors.New("instance not found")

// ErrNotWaiting is returned by Claim when the instance exists but is not
// currently suspended on the given signal — already advanced, in a final
// state, or parked on a different signal.
var ErrNotWaiting = errors.New("instance not waiting on signal")

// Store is the persistence contract.
type Store interface {
	Save(ctx context.Context, i *Instance, activeExecutionID string) error
	Load(ctx context.Context, id string) (*Instance, error)
	// Claim atomically transitions a suspended instance waiting on signal to
	// running and returns it, and creates an execution record.
	Claim(ctx context.Context, id, signal string, exec *Execution) (*Instance, error)
	FindDue(ctx context.Context, before time.Time, limit int) ([]*Instance, error)

	StartInstance(ctx context.Context, i *Instance, exec *Execution) error
	CloseExecution(ctx context.Context, i *Instance, execID string, outcome ExecutionOutcome, err error) error
	FindCrashedExecutions(ctx context.Context, before time.Time, limit int) ([]*Execution, error)
	LoadExecution(ctx context.Context, id string) (*Execution, error)
	FindLastCheckpoint(ctx context.Context, instanceID string) (*Execution, error)
}

// Memory is an in-process Store. Clones on Save/Load so callers cannot mutate
// stored state by accident.
type Memory struct {
	mu         sync.RWMutex
	instances  map[string]*Instance
	executions map[string]*Execution
}

func NewMemory() *Memory {
	return &Memory{
		instances:  make(map[string]*Instance),
		executions: make(map[string]*Execution),
	}
}

func (m *Memory) Save(ctx context.Context, i *Instance, activeExecutionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if i.CreatedAt.IsZero() {
		i.CreatedAt = now
	}
	i.UpdatedAt = now
	i.Version++
	m.instances[i.ID] = cloneInstance(i)

	if activeExecutionID != "" {
		if exec, ok := m.executions[activeExecutionID]; ok {
			exec.HeartbeatAt = now
			m.executions[activeExecutionID] = cloneExecution(exec)
		}
	}
	return nil
}

func (m *Memory) Load(ctx context.Context, id string) (*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inst, ok := m.instances[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneInstance(inst), nil
}

// Claim atomically moves a suspended instance to running under the lock,
// serializing concurrent signal deliveries, and creates an execution record.
func (m *Memory) Claim(ctx context.Context, id, signal string, exec *Execution) (*Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.instances[id]
	if !ok {
		return nil, ErrNotFound
	}
	if inst.Status != StatusSuspended || !slices.Contains(inst.WakeOn, signal) {
		return nil, ErrNotWaiting
	}
	if _, ok := m.executions[exec.ID]; ok {
		return nil, errors.New("execution already exists")
	}
	if exec.SignalDeliveryID != nil {
		for _, e := range m.executions {
			if e.SignalDeliveryID != nil && *e.SignalDeliveryID == *exec.SignalDeliveryID {
				return nil, errors.New("duplicate signal delivery id")
			}
		}
	}

	inst.Status = StatusRunning
	inst.Version++
	inst.UpdatedAt = time.Now()
	m.instances[id] = cloneInstance(inst)

	exec.StartedAt = time.Now()
	exec.HeartbeatAt = time.Now()
	m.executions[exec.ID] = cloneExecution(exec)
	return cloneInstance(inst), nil
}

func (m *Memory) FindDue(ctx context.Context, before time.Time, limit int) ([]*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Instance
	for _, inst := range m.instances {
		if inst.Status != StatusSuspended || inst.WakeAt == nil {
			continue
		}
		if inst.WakeAt.Before(before) {
			out = append(out, cloneInstance(inst))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (m *Memory) StartInstance(ctx context.Context, i *Instance, exec *Execution) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.instances[i.ID]; ok {
		return errors.New("instance already exists")
	}
	if _, ok := m.executions[exec.ID]; ok {
		return errors.New("execution already exists")
	}
	if exec.SignalDeliveryID != nil {
		for _, e := range m.executions {
			if e.SignalDeliveryID != nil && *e.SignalDeliveryID == *exec.SignalDeliveryID {
				return errors.New("duplicate signal delivery id")
			}
		}
	}

	now := time.Now()
	if i.CreatedAt.IsZero() {
		i.CreatedAt = now
	}
	i.UpdatedAt = now
	i.Version = 1
	m.instances[i.ID] = cloneInstance(i)

	exec.StartedAt = now
	exec.HeartbeatAt = now
	m.executions[exec.ID] = cloneExecution(exec)
	return nil
}

func (m *Memory) CloseExecution(ctx context.Context, i *Instance, execID string, outcome ExecutionOutcome, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if i.CreatedAt.IsZero() {
		i.CreatedAt = now
	}
	i.UpdatedAt = now
	i.Version++
	m.instances[i.ID] = cloneInstance(i)

	if execID != "" {
		exec, ok := m.executions[execID]
		if ok {
			exec.FinishedAt = &now
			exec.Outcome = &outcome
			if outcome == OutcomeSuspended {
				exec.SuspendedAtState = i.Current
				exec.SuspendedWakeOn = append([]string(nil), i.WakeOn...)
			}
			if err != nil {
				exec.Error = err.Error()
			}
			exec.HeartbeatAt = now
			m.executions[execID] = cloneExecution(exec)
		}
	}
	return nil
}

func (m *Memory) FindCrashedExecutions(ctx context.Context, before time.Time, limit int) ([]*Execution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Execution
	for _, exec := range m.executions {
		if exec.Outcome == nil && exec.HeartbeatAt.Before(before) {
			out = append(out, cloneExecution(exec))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (m *Memory) LoadExecution(ctx context.Context, id string) (*Execution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	exec, ok := m.executions[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneExecution(exec), nil
}

func (m *Memory) FindLastCheckpoint(ctx context.Context, instanceID string) (*Execution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var best *Execution
	for _, exec := range m.executions {
		if exec.InstanceID == instanceID && exec.Outcome != nil && *exec.Outcome == OutcomeSuspended {
			if best == nil || exec.StartedAt.After(best.StartedAt) {
				best = exec
			}
		}
	}
	if best == nil {
		return nil, ErrNotFound
	}
	return cloneExecution(best), nil
}

func cloneInstance(in *Instance) *Instance {
	cp := *in
	if in.ChartDef != nil {
		cp.ChartDef = append([]byte(nil), in.ChartDef...)
	}
	if in.Payload != nil {
		cp.Payload = maps.Clone(in.Payload)
	}
	if in.WakeOn != nil {
		cp.WakeOn = append([]string(nil), in.WakeOn...)
	}
	if in.WakeAt != nil {
		t := *in.WakeAt
		cp.WakeAt = &t
	}
	return &cp
}

func cloneExecution(in *Execution) *Execution {
	cp := *in
	if in.SignalDeliveryID != nil {
		s := *in.SignalDeliveryID
		cp.SignalDeliveryID = &s
	}
	if in.FinishedAt != nil {
		t := *in.FinishedAt
		cp.FinishedAt = &t
	}
	if in.Outcome != nil {
		o := *in.Outcome
		cp.Outcome = &o
	}
	if in.SuspendedWakeOn != nil {
		cp.SuspendedWakeOn = append([]string(nil), in.SuspendedWakeOn...)
	}
	return &cp
}
