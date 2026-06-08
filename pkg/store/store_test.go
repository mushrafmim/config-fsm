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
	exec := &Execution{
		ID:         "e1",
		InstanceID: "i1",
		Trigger:    "start",
	}

	if err := m.StartInstance(ctx, inst, exec); err != nil {
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
		t.Fatalf("Version = %d, want 1 after StartInstance", got.Version)
	}

	// Save with active execution refreshes heartbeat
	inst.Current = "next"
	if err := m.Save(ctx, inst, "e1"); err != nil {
		t.Fatal(err)
	}

	got, err = m.Load(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Current != "next" || got.Version != 2 {
		t.Fatalf("after Save: Current=%s Version=%d", got.Current, got.Version)
	}

	gotExec, err := m.LoadExecution(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if gotExec.InstanceID != "i1" {
		t.Fatalf("execution ID mismatch: %+v", gotExec)
	}
}

func TestMemory_LoadMissing(t *testing.T) {
	_, err := NewMemory().Load(context.Background(), "ghost")
	if err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestMemory_FindDue(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	past := time.Now().Add(-1 * time.Hour)
	future := time.Now().Add(1 * time.Hour)

	_ = m.StartInstance(ctx, &Instance{ID: "a", Status: StatusSuspended, WakeAt: &past}, &Execution{ID: "ea", InstanceID: "a"})
	_ = m.StartInstance(ctx, &Instance{ID: "b", Status: StatusSuspended, WakeAt: &future}, &Execution{ID: "eb", InstanceID: "b"})

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
	_ = m.StartInstance(ctx, &Instance{ID: "i", Payload: map[string]any{"k": "v"}}, &Execution{ID: "e", InstanceID: "i"})

	loaded, _ := m.Load(ctx, "i")
	loaded.Payload["k"] = "mutated"

	again, _ := m.Load(ctx, "i")
	if again.Payload["k"] != "v" {
		t.Fatalf("store leaked mutation: %v", again.Payload)
	}
}

func TestMemory_Executions(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()

	inst := &Instance{ID: "i1", Status: StatusSuspended, WakeOn: []string{"sig"}}
	execStart := &Execution{ID: "e1", InstanceID: "i1", Trigger: "start"}
	if err := m.StartInstance(ctx, inst, execStart); err != nil {
		t.Fatal(err)
	}

	// Close starting execution as suspended
	inst.Status = StatusSuspended
	inst.WakeOn = []string{"sig"}
	if err := m.CloseExecution(ctx, inst, "e1", OutcomeSuspended, nil); err != nil {
		t.Fatal(err)
	}

	execClaim := &Execution{ID: "e2", InstanceID: "i1", Trigger: "signal:sig"}
	claimed, err := m.Claim(ctx, "i1", "sig", execClaim)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != StatusRunning {
		t.Fatalf("claimed status = %s, want running", claimed.Status)
	}

	// Check loading execution
	ec, err := m.LoadExecution(ctx, "e2")
	if err != nil {
		t.Fatal(err)
	}
	if ec.Outcome != nil {
		t.Fatalf("expected outcome nil, got %v", ec.Outcome)
	}

	// Heartbeat checks
	crashed, err := m.FindCrashedExecutions(ctx, time.Now().Add(time.Hour), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(crashed) != 1 || crashed[0].ID != "e2" {
		t.Fatalf("expected e2 as crashed, got %v", crashed)
	}
}
