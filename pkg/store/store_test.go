package store

import (
	"context"
	"testing"
	"time"
)

func TestMemory_SaveLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	inst := &Instance{
		ID:      "i1",
		ChartID: "c",
		Current: "start",
		Payload: map[string]any{"k": "v"},
		Status:  StatusRunning,
	}
	if err := m.Save(ctx, inst); err != nil {
		t.Fatal(err)
	}
	got, err := m.Load(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Current != "start" || got.Payload["k"] != "v" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	if got.Version != 1 {
		t.Fatalf("Version = %d, want 1 after one Save", got.Version)
	}
}

func TestMemory_LoadMissing(t *testing.T) {
	_, err := NewMemory().Load(context.Background(), "ghost")
	if err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestMemory_FindByWakeSignal(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	m.Save(ctx, &Instance{ID: "a", Status: StatusSuspended, WakeOn: []string{"sig1", "sig2"}})
	m.Save(ctx, &Instance{ID: "b", Status: StatusSuspended, WakeOn: []string{"sig2"}})
	m.Save(ctx, &Instance{ID: "c", Status: StatusRunning, WakeOn: []string{"sig1"}})

	got, err := m.FindByWakeSignal(ctx, "sig1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("FindByWakeSignal(sig1) = %v", got)
	}
}

func TestMemory_FindDue(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	past := time.Now().Add(-1 * time.Hour)
	future := time.Now().Add(1 * time.Hour)
	m.Save(ctx, &Instance{ID: "a", Status: StatusSuspended, WakeAt: &past})
	m.Save(ctx, &Instance{ID: "b", Status: StatusSuspended, WakeAt: &future})

	got, err := m.FindDue(ctx, time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("FindDue = %v", got)
	}
}

func TestMemory_LoadReturnsClone(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	m.Save(ctx, &Instance{ID: "i", Payload: map[string]any{"k": "v"}})

	loaded, _ := m.Load(ctx, "i")
	loaded.Payload["k"] = "mutated"

	again, _ := m.Load(ctx, "i")
	if again.Payload["k"] != "v" {
		t.Fatalf("store leaked mutation: %v", again.Payload)
	}
}
