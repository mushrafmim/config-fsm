package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Schema is the DDL for the instances and executions tables. It is idempotent (IF NOT EXISTS)
// so EnsureSchema can run it on startup, or you can apply it via your own
// migration tooling.
//
// payload and wake_on are JSONB so the store depends only on database/sql —
// no driver-specific array types. wake_on is a JSON array of signal names;
// Claim matches membership with jsonb_exists, backed by the GIN index.
const Schema = `
CREATE TABLE IF NOT EXISTS instances (
    id             TEXT PRIMARY KEY,
    chart_id       TEXT NOT NULL,
    chart_version  TEXT NOT NULL,
    chart_def      JSONB,
    current        TEXT NOT NULL,
    business_state TEXT NOT NULL DEFAULT '',
    payload        JSONB NOT NULL DEFAULT '{}'::jsonb,
    status         TEXT NOT NULL,
    wake_on        JSONB NOT NULL DEFAULT '[]'::jsonb,
    wake_at        TIMESTAMPTZ,
    version        INTEGER NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_instances_due ON instances (wake_at) WHERE status = 'suspended';
CREATE INDEX IF NOT EXISTS idx_instances_wake_on ON instances USING GIN (wake_on);

CREATE TABLE IF NOT EXISTS executions (
    id                  TEXT PRIMARY KEY,
    instance_id         TEXT NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
    trigger             TEXT NOT NULL,
    signal_delivery_id  TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    outcome             TEXT,
    suspended_at_state  TEXT NOT NULL DEFAULT '',
    suspended_wake_on   JSONB NOT NULL DEFAULT '[]'::jsonb,
    error               TEXT NOT NULL DEFAULT '',
    heartbeat_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_executions_instance ON executions (instance_id);
CREATE INDEX IF NOT EXISTS idx_executions_heartbeat ON executions (heartbeat_at) WHERE outcome IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_signal_delivery ON executions (signal_delivery_id) WHERE signal_delivery_id IS NOT NULL;
`

const instanceColumns = `id, chart_id, chart_version, chart_def, current, business_state, payload, status, wake_on, wake_at, version, created_at, updated_at`
const executionColumns = `id, instance_id, trigger, signal_delivery_id, started_at, finished_at, outcome, suspended_at_state, suspended_wake_on, error, heartbeat_at`

// Postgres is a Store backed by Postgres via database/sql. It imports no
// driver — the caller opens a *sql.DB with the driver of their choice (pgx,
// lib/pq, …) and injects it.
type Postgres struct {
	db *sql.DB
}

// NewPostgres wraps an already-configured *sql.DB. The caller owns the DB's
// lifecycle (and brought the driver).
func NewPostgres(db *sql.DB) *Postgres {
	return &Postgres{db: db}
}

// EnsureSchema applies Schema. Safe to call repeatedly.
func (p *Postgres) EnsureSchema(ctx context.Context) error {
	if _, err := p.db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

func (p *Postgres) Save(ctx context.Context, i *Instance, activeExecutionID string) error {
	payload, err := json.Marshal(orEmptyMap(i.Payload))
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	wakeOn, err := json.Marshal(orEmptySlice(i.WakeOn))
	if err != nil {
		return fmt.Errorf("marshal wake_on: %w", err)
	}
	var wakeAt sql.NullTime
	if i.WakeAt != nil {
		wakeAt = sql.NullTime{Time: *i.WakeAt, Valid: true}
	}
	var chartDef any
	if i.ChartDef != nil {
		chartDef = string(i.ChartDef)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin save transaction: %w", err)
	}
	defer tx.Rollback()

	const q = `
INSERT INTO instances (id, chart_id, chart_version, chart_def, current, business_state, payload, status, wake_on, wake_at, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, now(), now())
ON CONFLICT (id) DO UPDATE SET
    chart_id       = EXCLUDED.chart_id,
    chart_version  = EXCLUDED.chart_version,
    chart_def      = EXCLUDED.chart_def,
    current        = EXCLUDED.current,
    business_state = EXCLUDED.business_state,
    payload        = EXCLUDED.payload,
    status         = EXCLUDED.status,
    wake_on        = EXCLUDED.wake_on,
    wake_at        = EXCLUDED.wake_at,
    version        = instances.version + 1,
    updated_at     = now()
RETURNING version, created_at, updated_at`

	row := tx.QueryRowContext(ctx, q,
		i.ID, i.ChartID, i.ChartVersion, chartDef, i.Current, i.BusinessState,
		string(payload), string(i.Status), string(wakeOn), wakeAt)
	if err := row.Scan(&i.Version, &i.CreatedAt, &i.UpdatedAt); err != nil {
		return fmt.Errorf("save instance %q: %w", i.ID, err)
	}

	if activeExecutionID != "" {
		const qExec = `UPDATE executions SET heartbeat_at = now() WHERE id = $1`
		if _, err := tx.ExecContext(ctx, qExec, activeExecutionID); err != nil {
			return fmt.Errorf("update execution heartbeat %q: %w", activeExecutionID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit save transaction: %w", err)
	}
	return nil
}

func (p *Postgres) Load(ctx context.Context, id string) (*Instance, error) {
	q := `SELECT ` + instanceColumns + ` FROM instances WHERE id = $1`
	return scanInstance(p.db.QueryRowContext(ctx, q, id))
}

// Claim atomically transitions a suspended instance waiting on signal to
// running, via a single guarded UPDATE — the row lock makes concurrent
// deliveries serialize, and the loser matches zero rows. It also inserts
// an execution record.
func (p *Postgres) Claim(ctx context.Context, id, signal string, exec *Execution) (*Instance, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer tx.Rollback()

	q := `
UPDATE instances
SET status = 'running', version = version + 1, updated_at = now()
WHERE id = $1 AND status = 'suspended' AND jsonb_exists(wake_on, $2)
RETURNING ` + instanceColumns

	inst, err := scanInstance(tx.QueryRowContext(ctx, q, id, signal))
	if err == ErrNotFound {
		// The UPDATE matched no row: either no such instance, or it exists but
		// is not claimable on this signal. Distinguish for the caller.
		var checkQ = `SELECT id FROM instances WHERE id = $1`
		var checkID string
		errCheck := tx.QueryRowContext(ctx, checkQ, id).Scan(&checkID)
		if errCheck == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, ErrNotWaiting
	}
	if err != nil {
		return nil, fmt.Errorf("claim instance %q: %w", id, err)
	}

	var signalDeliveryID *string
	if exec.SignalDeliveryID != nil {
		signalDeliveryID = exec.SignalDeliveryID
	}

	const qExec = `
INSERT INTO executions (id, instance_id, trigger, signal_delivery_id, started_at, finished_at, outcome, suspended_at_state, suspended_wake_on, error, heartbeat_at)
VALUES ($1, $2, $3, $4, now(), NULL, NULL, '', '[]'::jsonb, '', now())`

	if _, err := tx.ExecContext(ctx, qExec, exec.ID, exec.InstanceID, exec.Trigger, signalDeliveryID); err != nil {
		return nil, fmt.Errorf("insert execution: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim transaction: %w", err)
	}

	exec.StartedAt = time.Now()
	exec.HeartbeatAt = time.Now()
	return inst, nil
}

func (p *Postgres) StartInstance(ctx context.Context, i *Instance, exec *Execution) error {
	payload, err := json.Marshal(orEmptyMap(i.Payload))
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	wakeOn, err := json.Marshal(orEmptySlice(i.WakeOn))
	if err != nil {
		return fmt.Errorf("marshal wake_on: %w", err)
	}
	var wakeAt sql.NullTime
	if i.WakeAt != nil {
		wakeAt = sql.NullTime{Time: *i.WakeAt, Valid: true}
	}
	var chartDef any
	if i.ChartDef != nil {
		chartDef = string(i.ChartDef)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin start transaction: %w", err)
	}
	defer tx.Rollback()

	const qInstance = `
INSERT INTO instances (id, chart_id, chart_version, chart_def, current, business_state, payload, status, wake_on, wake_at, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, now(), now())
RETURNING version, created_at, updated_at`

	row := tx.QueryRowContext(ctx, qInstance,
		i.ID, i.ChartID, i.ChartVersion, chartDef, i.Current, i.BusinessState,
		string(payload), string(i.Status), string(wakeOn), wakeAt)
	if err := row.Scan(&i.Version, &i.CreatedAt, &i.UpdatedAt); err != nil {
		return fmt.Errorf("insert instance %q: %w", i.ID, err)
	}

	var signalDeliveryID *string
	if exec.SignalDeliveryID != nil {
		signalDeliveryID = exec.SignalDeliveryID
	}

	const qExec = `
INSERT INTO executions (id, instance_id, trigger, signal_delivery_id, started_at, finished_at, outcome, suspended_at_state, suspended_wake_on, error, heartbeat_at)
VALUES ($1, $2, $3, $4, now(), NULL, NULL, '', '[]'::jsonb, '', now())`

	if _, err := tx.ExecContext(ctx, qExec, exec.ID, exec.InstanceID, exec.Trigger, signalDeliveryID); err != nil {
		return fmt.Errorf("insert execution: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit start transaction: %w", err)
	}

	exec.StartedAt = time.Now()
	exec.HeartbeatAt = time.Now()
	return nil
}

func (p *Postgres) CloseExecution(ctx context.Context, i *Instance, execID string, outcome ExecutionOutcome, err error) error {
	payload, errPayload := json.Marshal(orEmptyMap(i.Payload))
	if errPayload != nil {
		return fmt.Errorf("marshal payload: %w", errPayload)
	}
	wakeOn, errWakeOn := json.Marshal(orEmptySlice(i.WakeOn))
	if errWakeOn != nil {
		return fmt.Errorf("marshal wake_on: %w", errWakeOn)
	}
	var wakeAt sql.NullTime
	if i.WakeAt != nil {
		wakeAt = sql.NullTime{Time: *i.WakeAt, Valid: true}
	}
	var chartDef any
	if i.ChartDef != nil {
		chartDef = string(i.ChartDef)
	}

	tx, errTx := p.db.BeginTx(ctx, nil)
	if errTx != nil {
		return fmt.Errorf("begin close transaction: %w", errTx)
	}
	defer tx.Rollback()

	const qInstance = `
INSERT INTO instances (id, chart_id, chart_version, chart_def, current, business_state, payload, status, wake_on, wake_at, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, now(), now())
ON CONFLICT (id) DO UPDATE SET
    chart_id       = EXCLUDED.chart_id,
    chart_version  = EXCLUDED.chart_version,
    chart_def      = EXCLUDED.chart_def,
    current        = EXCLUDED.current,
    business_state = EXCLUDED.business_state,
    payload        = EXCLUDED.payload,
    status         = EXCLUDED.status,
    wake_on        = EXCLUDED.wake_on,
    wake_at        = EXCLUDED.wake_at,
    version        = instances.version + 1,
    updated_at     = now()
RETURNING version, created_at, updated_at`

	row := tx.QueryRowContext(ctx, qInstance,
		i.ID, i.ChartID, i.ChartVersion, chartDef, i.Current, i.BusinessState,
		string(payload), string(i.Status), string(wakeOn), wakeAt)
	if err := row.Scan(&i.Version, &i.CreatedAt, &i.UpdatedAt); err != nil {
		return fmt.Errorf("save instance on close: %w", err)
	}

	var errStr string
	if err != nil {
		errStr = err.Error()
	}

	var suspendedAtState string
	var suspendedWakeOn []byte = []byte("[]")
	if outcome == OutcomeSuspended {
		suspendedAtState = i.Current
		var errWake error
		suspendedWakeOn, errWake = json.Marshal(orEmptySlice(i.WakeOn))
		if errWake != nil {
			return fmt.Errorf("marshal suspended_wake_on: %w", errWake)
		}
	}

	const qExec = `
UPDATE executions
SET finished_at = now(), outcome = $1, error = $2, suspended_at_state = $3, suspended_wake_on = $4, heartbeat_at = now()
WHERE id = $5`

	if _, err := tx.ExecContext(ctx, qExec, string(outcome), errStr, suspendedAtState, string(suspendedWakeOn), execID); err != nil {
		return fmt.Errorf("update execution %q: %w", execID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit close transaction: %w", err)
	}
	return nil
}

func (p *Postgres) FindDue(ctx context.Context, before time.Time, limit int) ([]*Instance, error) {
	if limit < 0 {
		limit = 0
	}
	q := `SELECT ` + instanceColumns + `
FROM instances
WHERE status = 'suspended' AND wake_at IS NOT NULL AND wake_at < $1
ORDER BY wake_at
LIMIT NULLIF($2, 0)` // limit 0 → NULL → no limit
	rows, err := p.db.QueryContext(ctx, q, before, limit)
	if err != nil {
		return nil, fmt.Errorf("find due: %w", err)
	}
	defer rows.Close()

	var out []*Instance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func (p *Postgres) FindCrashedExecutions(ctx context.Context, before time.Time, limit int) ([]*Execution, error) {
	if limit < 0 {
		limit = 0
	}
	q := `SELECT ` + executionColumns + `
FROM executions
WHERE outcome IS NULL AND heartbeat_at < $1
ORDER BY heartbeat_at
LIMIT NULLIF($2, 0)`

	rows, err := p.db.QueryContext(ctx, q, before, limit)
	if err != nil {
		return nil, fmt.Errorf("find crashed executions: %w", err)
	}
	defer rows.Close()

	var out []*Execution
	for rows.Next() {
		exec, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, exec)
	}
	return out, rows.Err()
}

func (p *Postgres) LoadExecution(ctx context.Context, id string) (*Execution, error) {
	q := `SELECT ` + executionColumns + ` FROM executions WHERE id = $1`
	return scanExecution(p.db.QueryRowContext(ctx, q, id))
}

func (p *Postgres) FindLastCheckpoint(ctx context.Context, instanceID string) (*Execution, error) {
	q := `SELECT ` + executionColumns + `
FROM executions
WHERE instance_id = $1 AND outcome = 'suspended'
ORDER BY started_at DESC, finished_at DESC
LIMIT 1`
	return scanExecution(p.db.QueryRowContext(ctx, q, instanceID))
}

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanInstance(row rowScanner) (*Instance, error) {
	var (
		i                         Instance
		chartDef, payload, wakeOn []byte
		status                    string
		wakeAt                    sql.NullTime
	)
	err := row.Scan(
		&i.ID, &i.ChartID, &i.ChartVersion, &chartDef, &i.Current, &i.BusinessState,
		&payload, &status, &wakeOn, &wakeAt, &i.Version, &i.CreatedAt, &i.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	i.ChartDef = chartDef
	i.Status = Status(status)
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &i.Payload); err != nil {
			return nil, fmt.Errorf("unmarshal payload for %q: %w", i.ID, err)
		}
	}
	if len(wakeOn) > 0 {
		if err := json.Unmarshal(wakeOn, &i.WakeOn); err != nil {
			return nil, fmt.Errorf("unmarshal wake_on for %q: %w", i.ID, err)
		}
	}
	if wakeAt.Valid {
		t := wakeAt.Time
		i.WakeAt = &t
	}
	return &i, nil
}

func scanExecution(row rowScanner) (*Execution, error) {
	var (
		e                         Execution
		signalDeliveryID, outcome sql.NullString
		finishedAt                sql.NullTime
		suspendedWakeOn           []byte
		startedAt, heartbeatAt    time.Time
	)
	err := row.Scan(
		&e.ID, &e.InstanceID, &e.Trigger, &signalDeliveryID, &startedAt, &finishedAt, &outcome,
		&e.SuspendedAtState, &suspendedWakeOn, &e.Error, &heartbeatAt)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	e.StartedAt = startedAt
	e.HeartbeatAt = heartbeatAt
	if signalDeliveryID.Valid {
		s := signalDeliveryID.String
		e.SignalDeliveryID = &s
	}
	if finishedAt.Valid {
		t := finishedAt.Time
		e.FinishedAt = &t
	}
	if outcome.Valid {
		o := ExecutionOutcome(outcome.String)
		e.Outcome = &o
	}
	if len(suspendedWakeOn) > 0 {
		if err := json.Unmarshal(suspendedWakeOn, &e.SuspendedWakeOn); err != nil {
			return nil, fmt.Errorf("unmarshal suspended_wake_on for %q: %w", e.ID, err)
		}
	}
	return &e, nil
}

func orEmptyMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func orEmptySlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
