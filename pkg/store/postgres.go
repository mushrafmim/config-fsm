package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Schema is the DDL for the instances table. It is idempotent (IF NOT EXISTS)
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
`

const instanceColumns = `id, chart_id, chart_version, chart_def, current, business_state, payload, status, wake_on, wake_at, version, created_at, updated_at`

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

func (p *Postgres) Save(ctx context.Context, i *Instance) error {
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
	// chart_def is nullable; pass NULL when absent. JSON values go to JSONB
	// columns as strings (the reliable database/sql + pgx path for JSONB).
	var chartDef any
	if i.ChartDef != nil {
		chartDef = string(i.ChartDef)
	}

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

	row := p.db.QueryRowContext(ctx, q,
		i.ID, i.ChartID, i.ChartVersion, chartDef, i.Current, i.BusinessState,
		string(payload), string(i.Status), string(wakeOn), wakeAt)
	if err := row.Scan(&i.Version, &i.CreatedAt, &i.UpdatedAt); err != nil {
		return fmt.Errorf("save instance %q: %w", i.ID, err)
	}
	return nil
}

func (p *Postgres) Load(ctx context.Context, id string) (*Instance, error) {
	q := `SELECT ` + instanceColumns + ` FROM instances WHERE id = $1`
	return scanInstance(p.db.QueryRowContext(ctx, q, id))
}

// Claim atomically transitions a suspended instance waiting on signal to
// running, via a single guarded UPDATE — the row lock makes concurrent
// deliveries serialize, and the loser matches zero rows. WakeOn is retained
// for release.
func (p *Postgres) Claim(ctx context.Context, id, signal string) (*Instance, error) {
	q := `
UPDATE instances
SET status = 'running', version = version + 1, updated_at = now()
WHERE id = $1 AND status = 'suspended' AND jsonb_exists(wake_on, $2)
RETURNING ` + instanceColumns
	inst, err := scanInstance(p.db.QueryRowContext(ctx, q, id, signal))
	if err == ErrNotFound {
		// The UPDATE matched no row: either no such instance, or it exists but
		// is not claimable on this signal. Distinguish for the caller.
		if _, lerr := p.Load(ctx, id); lerr == ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, ErrNotWaiting
	}
	if err != nil {
		return nil, fmt.Errorf("claim instance %q: %w", id, err)
	}
	return inst, nil
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
