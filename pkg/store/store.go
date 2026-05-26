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
type Instance struct {
	ID            string
	ChartID       string
	ChartVersion  string
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

// ErrNotFound is returned by Load when no instance with the given ID exists.
var ErrNotFound = errors.New("instance not found")

// Store is the persistence contract.
type Store interface {
	Save(ctx context.Context, i *Instance) error
	Load(ctx context.Context, id string) (*Instance, error)
	FindDue(ctx context.Context, before time.Time, limit int) ([]*Instance, error)
}

// Memory is an in-process Store. Clones on Save/Load so callers cannot mutate
// stored state by accident.
type Memory struct {
	mu        sync.RWMutex
	instances map[string]*Instance
}

func NewMemory() *Memory {
	return &Memory{instances: make(map[string]*Instance)}
}

func (m *Memory) Save(ctx context.Context, i *Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if i.CreatedAt.IsZero() {
		i.CreatedAt = now
	}
	i.UpdatedAt = now
	i.Version++
	m.instances[i.ID] = cloneInstance(i)
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

func cloneInstance(in *Instance) *Instance {
	cp := *in
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
