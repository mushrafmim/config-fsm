package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// openTestDB connects to the Postgres named by CONFIG_FSM_TEST_DSN, skipping
// the test when it is unset. Example:
//
//	CONFIG_FSM_TEST_DSN='postgres://user:pass@localhost:5432/fsm?sslmode=disable' go test ./pkg/store/
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CONFIG_FSM_TEST_DSN")
	if dsn == "" {
		t.Skip("set CONFIG_FSM_TEST_DSN to run Postgres store tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

func TestPostgres_SaveLoadClaim(t *testing.T) {
	ctx := context.Background()
	p := NewPostgres(openTestDB(t))
	if err := p.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = p.db.ExecContext(ctx, "DELETE FROM executions WHERE instance_id = $1", id)
		_, _ = p.db.ExecContext(ctx, "DELETE FROM instances WHERE id = $1", id)
	})

	inst := &Instance{
		ID: id, ChartID: "c", ChartVersion: "1", Current: "wait",
		Status: StatusSuspended, WakeOn: []string{"signal"},
		Payload: map[string]any{"k": "v"},
	}
	exec := &Execution{
		ID:         id + "-e1",
		InstanceID: id,
		Trigger:    "start",
	}
	if err := p.StartInstance(ctx, inst, exec); err != nil {
		t.Fatalf("start instance: %v", err)
	}
	if inst.Version != 1 {
		t.Fatalf("version after first save = %d, want 1", inst.Version)
	}

	got, err := p.Load(ctx, id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Current != "wait" || got.Payload["k"] != "v" || len(got.WakeOn) != 1 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	// Close starting execution as suspended
	inst.Status = StatusSuspended
	if err := p.CloseExecution(ctx, inst, id+"-e1", OutcomeSuspended, nil); err != nil {
		t.Fatalf("close execution: %v", err)
	}

	// Claim moves suspended -> running and retains WakeOn.
	claimExec := &Execution{
		ID:         id + "-e2",
		InstanceID: id,
		Trigger:    "signal:signal",
	}
	claimed, err := p.Claim(ctx, id, "signal", claimExec)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Status != StatusRunning || len(claimed.WakeOn) != 1 {
		t.Fatalf("claimed = %s wakeOn=%v, want running + retained WakeOn", claimed.Status, claimed.WakeOn)
	}

	// Second claim finds it no longer suspended.
	claimExec3 := &Execution{
		ID:         id + "-e3",
		InstanceID: id,
		Trigger:    "signal:signal",
	}
	if _, err := p.Claim(ctx, id, "signal", claimExec3); err != ErrNotWaiting {
		t.Fatalf("second claim err = %v, want ErrNotWaiting", err)
	}
	// Unknown id.
	claimExecGhost := &Execution{
		ID:         id + "-ghost-e",
		InstanceID: "ghost",
		Trigger:    "signal:signal",
	}
	if _, err := p.Claim(ctx, "ghost", "signal", claimExecGhost); err != ErrNotFound {
		t.Fatalf("missing claim err = %v, want ErrNotFound", err)
	}
}

func TestPostgres_ClaimWrongSignal(t *testing.T) {
	ctx := context.Background()
	p := NewPostgres(openTestDB(t))
	if err := p.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = p.db.ExecContext(ctx, "DELETE FROM executions WHERE instance_id = $1", id)
		_, _ = p.db.ExecContext(ctx, "DELETE FROM instances WHERE id = $1", id)
	})

	inst := &Instance{ID: id, ChartID: "c", ChartVersion: "1", Current: "wait", Status: StatusSuspended, WakeOn: []string{"approve"}}
	exec := &Execution{ID: id + "-e1", InstanceID: id, Trigger: "start"}
	if err := p.StartInstance(ctx, inst, exec); err != nil {
		t.Fatalf("start instance: %v", err)
	}
	// Close starting execution as suspended
	if err := p.CloseExecution(ctx, inst, id+"-e1", OutcomeSuspended, nil); err != nil {
		t.Fatalf("close execution: %v", err)
	}

	claimExec := &Execution{ID: id + "-e2", InstanceID: id, Trigger: "signal:reject"}
	if _, err := p.Claim(ctx, id, "reject", claimExec); err != ErrNotWaiting {
		t.Fatalf("claim wrong signal err = %v, want ErrNotWaiting", err)
	}
}

func TestPostgres_FindDue(t *testing.T) {
	ctx := context.Background()
	p := NewPostgres(openTestDB(t))
	if err := p.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	due := fmt.Sprintf("due-%d", time.Now().UnixNano())
	notDue := fmt.Sprintf("notdue-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = p.db.ExecContext(ctx, "DELETE FROM executions WHERE instance_id IN ($1, $2)", due, notDue)
		_, _ = p.db.ExecContext(ctx, "DELETE FROM instances WHERE id IN ($1, $2)", due, notDue)
	})

	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	if err := p.StartInstance(ctx, &Instance{ID: due, ChartID: "c", ChartVersion: "1", Current: "w", Status: StatusSuspended, WakeAt: &past}, &Execution{ID: due + "-e1", InstanceID: due, Trigger: "start"}); err != nil {
		t.Fatal(err)
	}
	if err := p.StartInstance(ctx, &Instance{ID: notDue, ChartID: "c", ChartVersion: "1", Current: "w", Status: StatusSuspended, WakeAt: &future}, &Execution{ID: notDue + "-e1", InstanceID: notDue, Trigger: "start"}); err != nil {
		t.Fatal(err)
	}

	got, err := p.FindDue(ctx, time.Now(), 0)
	if err != nil {
		t.Fatalf("find due: %v", err)
	}
	var ids []string
	for _, inst := range got {
		ids = append(ids, inst.ID)
	}
	if !contains(ids, due) || contains(ids, notDue) {
		t.Fatalf("FindDue returned %v, want %q present and %q absent", ids, due, notDue)
	}
}

func TestPostgres_Executions(t *testing.T) {
	ctx := context.Background()
	p := NewPostgres(openTestDB(t))
	if err := p.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = p.db.ExecContext(ctx, "DELETE FROM executions WHERE instance_id = $1", id)
		_, _ = p.db.ExecContext(ctx, "DELETE FROM instances WHERE id = $1", id)
	})

	sigDelivID := id + "-deliv"
	inst := &Instance{ID: id, ChartID: "c", ChartVersion: "1", Current: "wait", Status: StatusSuspended, WakeOn: []string{"signal"}}
	exec := &Execution{ID: id + "-e1", InstanceID: id, Trigger: "start", SignalDeliveryID: &sigDelivID}

	if err := p.StartInstance(ctx, inst, exec); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Attempt duplicate signal delivery ID - must fail unique constraint
	inst2 := &Instance{ID: id + "-2", ChartID: "c", ChartVersion: "1", Current: "wait", Status: StatusSuspended, WakeOn: []string{"signal"}}
	exec2 := &Execution{ID: id + "-e2", InstanceID: id + "-2", Trigger: "start", SignalDeliveryID: &sigDelivID}
	if err := p.StartInstance(ctx, inst2, exec2); err == nil {
		t.Fatal("expected error for duplicate signal delivery id")
	}

	// Update heartbeat
	if err := p.Save(ctx, inst, id+"-e1"); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Retrieve crashed executions
	crashed, err := p.FindCrashedExecutions(ctx, time.Now().Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("find crashed: %v", err)
	}

	found := false
	for _, cr := range crashed {
		if cr.ID == id+"-e1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected crashed execution to contain %s", id+"-e1")
	}

	// Close execution
	if err := p.CloseExecution(ctx, inst, id+"-e1", OutcomeDone, nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Should no longer show as crashed
	crashed, err = p.FindCrashedExecutions(ctx, time.Now().Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("find crashed: %v", err)
	}
	for _, cr := range crashed {
		if cr.ID == id+"-e1" {
			t.Fatalf("execution %s should not be crashed after close", id+"-e1")
		}
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
