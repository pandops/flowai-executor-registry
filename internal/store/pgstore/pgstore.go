// Package pgstore is the PostgreSQL implementation of internal/store. It
// creates and migrates only its own schema.
package pgstore

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/pandops/flowai-executor-registry/internal/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Store is the PostgreSQL store.
type Store struct {
	db     *sql.DB
	schema string
}

var _ store.Store = (*Store)(nil)

// Open connects with dsn; schema names the service schema and becomes the
// search_path of every connection.
func Open(ctx context.Context, dsn, schema string) (*Store, error) {
	if !identifier.MatchString(schema) {
		return nil, errors.New("pgstore: invalid schema name")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("pgstore: invalid connection string")
	}
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &Store{db: db, schema: schema}, nil
}

func (s *Store) quoted() string { return `"` + s.schema + `"` }

// Migrate creates the schema when it is missing (checked in pg_namespace)
// and applies the migrations inside it. CREATE SCHEMA IF NOT EXISTS is not
// used: the deployment role has no CREATE on the database.
func (s *Store) Migrate(ctx context.Context) error {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, s.schema).Scan(&exists); err != nil {
		return fmt.Errorf("pgstore: check schema: %w", err)
	}
	if !exists {
		if _, err := s.db.ExecContext(ctx, `CREATE SCHEMA `+s.quoted()); err != nil {
			return fmt.Errorf("pgstore: create schema: %w", err)
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`SET LOCAL search_path TO ` + s.quoted(),
		`SELECT pg_advisory_xact_lock(hashtext('executor-registry-migrate'))`,
		`CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("pgstore: migrate: %w", err)
		}
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		var version int
		if _, err := fmt.Sscanf(name, "%04d_", &version); err != nil {
			return fmt.Errorf("pgstore: migration name %s", name)
		}
		var done bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		body, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("pgstore: migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Close closes the pool.
func (s *Store) Close() error { return s.db.Close() }

// Ping checks the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// InTx runs fn in one transaction and commits when it returns nil.
func (s *Store) InTx(ctx context.Context, fn func(store.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&pgTx{tx: tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// PendingOutbox returns unpublished rows in publisher_seq order.
func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]store.OutboxRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT publisher_seq, subject, header, data FROM outbox ORDER BY publisher_seq LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.OutboxRow
	for rows.Next() {
		var r store.OutboxRow
		var header []byte
		if err := rows.Scan(&r.Seq, &r.Subject, &header, &r.Data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(header, &r.Header); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkPublished removes a row JetStream acknowledged.
func (s *Store) MarkPublished(ctx context.Context, seq int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE publisher_seq = $1`, seq)
	return err
}

type pgTx struct{ tx *sql.Tx }

func (t *pgTx) next(ctx context.Context, seq string) (int64, error) {
	var n int64
	err := t.tx.QueryRowContext(ctx, `SELECT nextval('`+seq+`')`).Scan(&n)
	return n, err
}

func (t *pgTx) NextExecutorID(ctx context.Context) (int64, error) {
	return t.next(ctx, "executor_id_seq")
}

func (t *pgTx) NextPublisherSeq(ctx context.Context) (int64, error) {
	return t.next(ctx, "publisher_seq")
}

func (t *pgTx) InsertExecutor(ctx context.Context, e store.Executor) error {
	if _, err := t.tx.ExecContext(ctx, `INSERT INTO executors (executor_id, team_id, execution_tag, executor_type, created_at,
		user_public_key, token_fingerprint, token_issued_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.ExecutorID, e.TeamID, e.Tag, e.Type, e.CreatedAt, e.UserPublicKey, e.TokenFingerprint, e.TokenIssuedAt); err != nil {
		return err
	}
	_, err := t.tx.ExecContext(ctx, `INSERT INTO connections (executor_id, team_id, state, interval_seconds, miss_limit, wait_seconds)
		VALUES ($1, $2, $3, $4, $5, $6)`, e.ExecutorID, e.TeamID, store.StateNeverConnected,
		store.DefaultIntervalSeconds, store.DefaultMissLimit, store.DefaultWaitSeconds)
	return err
}

const executorColumns = `executor_id, team_id, execution_tag, executor_type, created_at, user_public_key,
	token_fingerprint, token_issued_at, reported_ip, ip_reported_at`

type scanner interface{ Scan(...any) error }

func scanExecutor(r scanner) (store.Executor, error) {
	var e store.Executor
	var ip sql.NullString
	var ipAt sql.NullTime
	if err := r.Scan(&e.ExecutorID, &e.TeamID, &e.Tag, &e.Type, &e.CreatedAt, &e.UserPublicKey,
		&e.TokenFingerprint, &e.TokenIssuedAt, &ip, &ipAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.Executor{}, store.ErrNotFound
		}
		return store.Executor{}, err
	}
	e.CreatedAt, e.TokenIssuedAt = e.CreatedAt.UTC(), e.TokenIssuedAt.UTC()
	if ip.Valid {
		v := ip.String
		e.ReportedIP = &v
	}
	if ipAt.Valid {
		v := ipAt.Time.UTC()
		e.IPReportedAt = &v
	}
	return e, nil
}

func (t *pgTx) GetExecutor(ctx context.Context, teamID, executorID int64) (store.Executor, error) {
	return scanExecutor(t.tx.QueryRowContext(ctx, `SELECT `+executorColumns+` FROM executors WHERE team_id = $1 AND executor_id = $2`, teamID, executorID))
}

func (t *pgTx) GetExecutorByID(ctx context.Context, executorID int64) (store.Executor, error) {
	return scanExecutor(t.tx.QueryRowContext(ctx, `SELECT `+executorColumns+` FROM executors WHERE executor_id = $1`, executorID))
}

func affected(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (t *pgTx) UpdateToken(ctx context.Context, executorID int64, userPublicKey, fingerprint string, issuedAt time.Time) error {
	return affected(t.tx.ExecContext(ctx, `UPDATE executors SET user_public_key = $2, token_fingerprint = $3, token_issued_at = $4 WHERE executor_id = $1`,
		executorID, userPublicKey, fingerprint, issuedAt))
}

func (t *pgTx) UpdateReportedIP(ctx context.Context, executorID int64, ip string, at time.Time) error {
	return affected(t.tx.ExecContext(ctx, `UPDATE executors SET reported_ip = $2, ip_reported_at = $3 WHERE executor_id = $1`, executorID, ip, at))
}

func (t *pgTx) DeleteExecutor(ctx context.Context, executorID int64) error {
	return affected(t.tx.ExecContext(ctx, `DELETE FROM executors WHERE executor_id = $1`, executorID))
}

func (t *pgTx) ListExecutors(ctx context.Context, teamID, after int64, limit int) ([]store.Executor, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT `+executorColumns+` FROM executors WHERE team_id = $1 AND executor_id > $2
		ORDER BY executor_id LIMIT $3`, teamID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Executor
	for rows.Next() {
		e, err := scanExecutor(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (t *pgTx) CountExecutors(ctx context.Context, teamID int64) (int, error) {
	var n int
	err := t.tx.QueryRowContext(ctx, `SELECT count(*) FROM executors WHERE team_id = $1`, teamID).Scan(&n)
	return n, err
}

func (t *pgTx) ListTags(ctx context.Context, teamID int64, after string, limit int) ([]string, error) {
	q := `SELECT DISTINCT execution_tag COLLATE "C" AS tag FROM executors WHERE team_id = $1 AND execution_tag COLLATE "C" > $2 ORDER BY tag`
	args := []any{teamID, after}
	if limit > 0 {
		q += ` LIMIT $3`
		args = append(args, limit)
	}
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		out = append(out, tag)
	}
	return out, rows.Err()
}

func (t *pgTx) TaskConsumerRecorded(ctx context.Context, teamID int64, tag string) (bool, error) {
	var ok bool
	err := t.tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM task_consumers WHERE team_id = $1 AND execution_tag = $2)`, teamID, tag).Scan(&ok)
	return ok, err
}

func (t *pgTx) RecordTaskConsumer(ctx context.Context, teamID int64, tag, name string) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO task_consumers (team_id, execution_tag, consumer_name) VALUES ($1, $2, $3)
		ON CONFLICT (team_id, execution_tag) DO NOTHING`, teamID, tag, name)
	return err
}

const connectionColumns = `executor_id, team_id, state, last_heartbeat_at, interval_seconds, miss_limit, wait_seconds,
	max_parallel, slots_in_use, held_claims, status_published, status_state, status_max, status_held, status_available, lost_seq`

func scanConnection(r scanner) (store.Connection, error) {
	var c store.Connection
	var last sql.NullTime
	if err := r.Scan(&c.ExecutorID, &c.TeamID, &c.State, &last, &c.IntervalSeconds, &c.MissLimit, &c.WaitSeconds,
		&c.MaxParallel, &c.SlotsInUse, &c.HeldClaims, &c.StatusPublished, &c.StatusState, &c.StatusMax, &c.StatusHeld,
		&c.StatusAvailable, &c.LostSeq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.Connection{}, store.ErrNotFound
		}
		return store.Connection{}, err
	}
	if last.Valid {
		v := last.Time.UTC()
		c.LastHeartbeatAt = &v
	}
	return c, nil
}

// GetConnection locks the row for the rest of the transaction.
func (t *pgTx) GetConnection(ctx context.Context, executorID int64) (store.Connection, error) {
	return scanConnection(t.tx.QueryRowContext(ctx, `SELECT `+connectionColumns+` FROM connections WHERE executor_id = $1 FOR UPDATE`, executorID))
}

func (t *pgTx) PutConnection(ctx context.Context, c store.Connection) error {
	return affected(t.tx.ExecContext(ctx, `UPDATE connections SET state = $2, last_heartbeat_at = $3, interval_seconds = $4,
		miss_limit = $5, wait_seconds = $6, max_parallel = $7, slots_in_use = $8, held_claims = $9, status_published = $10,
		status_state = $11, status_max = $12, status_held = $13, status_available = $14, lost_seq = $15 WHERE executor_id = $1`,
		c.ExecutorID, c.State, c.LastHeartbeatAt, c.IntervalSeconds, c.MissLimit, c.WaitSeconds, c.MaxParallel, c.SlotsInUse,
		c.HeldClaims, c.StatusPublished, c.StatusState, c.StatusMax, c.StatusHeld, c.StatusAvailable, c.LostSeq))
}

// TrackedConnections locks the connected and connection_lost rows; rows
// another sweep holds are skipped.
func (t *pgTx) TrackedConnections(ctx context.Context) ([]store.Connection, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT `+connectionColumns+` FROM connections
		WHERE state IN ('connected', 'connection_lost') ORDER BY executor_id FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Connection
	for rows.Next() {
		c, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (t *pgTx) InsertOutbox(ctx context.Context, row store.OutboxRow) error {
	header, err := json.Marshal(row.Header)
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(ctx, `INSERT INTO outbox (publisher_seq, subject, header, data) VALUES ($1, $2, $3, $4)`,
		row.Seq, row.Subject, header, row.Data)
	return err
}
