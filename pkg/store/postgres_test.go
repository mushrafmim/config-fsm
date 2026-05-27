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
	t.Cleanup(func() { _, _ = p.db.ExecContext(ctx, "DELETE FROM instances WHERE id = $1", id) })

	inst := &Instance{
		ID: id, ChartID: "c", ChartVersion: "1", Current: "wait",
		Status: StatusSuspended, WakeOn: []string{"signal"},
		Payload: map[string]any{"k": "v"},
	}
	if err := p.Save(ctx, inst); err != nil {
		t.Fatalf("save: %v", err)
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

	// Claim moves suspended -> running and retains WakeOn.
	claimed, err := p.Claim(ctx, id, "signal")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Status != StatusRunning || len(claimed.WakeOn) != 1 {
		t.Fatalf("claimed = %s wakeOn=%v, want running + retained WakeOn", claimed.Status, claimed.WakeOn)
	}

	// Second claim finds it no longer suspended.
	if _, err := p.Claim(ctx, id, "signal"); err != ErrNotWaiting {
		t.Fatalf("second claim err = %v, want ErrNotWaiting", err)
	}
	// Unknown id.
	if _, err := p.Claim(ctx, "ghost", "signal"); err != ErrNotFound {
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
	t.Cleanup(func() { _, _ = p.db.ExecContext(ctx, "DELETE FROM instances WHERE id = $1", id) })

	if err := p.Save(ctx, &Instance{ID: id, ChartID: "c", ChartVersion: "1", Current: "wait", Status: StatusSuspended, WakeOn: []string{"approve"}}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := p.Claim(ctx, id, "reject"); err != ErrNotWaiting {
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
		_, _ = p.db.ExecContext(ctx, "DELETE FROM instances WHERE id IN ($1, $2)", due, notDue)
	})

	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	if err := p.Save(ctx, &Instance{ID: due, ChartID: "c", ChartVersion: "1", Current: "w", Status: StatusSuspended, WakeAt: &past}); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(ctx, &Instance{ID: notDue, ChartID: "c", ChartVersion: "1", Current: "w", Status: StatusSuspended, WakeAt: &future}); err != nil {
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

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
