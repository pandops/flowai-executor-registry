//go:build integration

package test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/store"
	"github.com/pandops/flowai-executor-registry/internal/store/pgstore"
)

// Coverage: "Each service writes only its own <name>_schema with its own
// role; another schema is refused" (1.7; service-platform).

func TestSchemaIsolation(t *testing.T) {
	ctx := context.Background()

	t.Run("Migration runs in the deployment-created schema without CREATE on the database", func(t *testing.T) {
		d := newTestDB(t, true)
		s := d.openStore(t)
		// Second migrate is a no-op, not an error.
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("second migrate: %v", err)
		}
		var schemas []string
		rows, err := d.admin.Query(`SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var n string
			rows.Scan(&n)
			schemas = append(schemas, n)
		}
		rows.Close()
		if strings.Join(schemas, ",") != serviceSchema+","+foreignSchema {
			t.Fatalf("tables in schemas %v", schemas)
		}
		var owner string
		if err := d.admin.QueryRow(`SELECT DISTINCT tableowner FROM pg_tables WHERE schemaname = $1`, serviceSchema).Scan(&owner); err != nil || owner != serviceRole {
			t.Fatalf("table owner %q %v", owner, err)
		}
		// No column can hold the bearer JWT or a seed.
		var n int
		d.admin.QueryRow(`SELECT count(*) FROM information_schema.columns WHERE table_schema = $1 AND (column_name ILIKE '%jwt%' OR column_name ILIKE '%seed%' OR column_name ILIKE '%secret%')`, serviceSchema).Scan(&n)
		if n != 0 {
			t.Fatalf("%d secret-like columns", n)
		}
	})

	t.Run("Missing schema is created", func(t *testing.T) {
		d := newTestDB(t, false)
		s, err := pgstore.Open(ctx, pg.dsn("flowai", pg.password, d.name), serviceSchema)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		var ok bool
		d.admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, serviceSchema).Scan(&ok)
		if !ok {
			t.Fatal("schema not created")
		}
	})

	t.Run("Access to a foreign schema is denied", func(t *testing.T) {
		d := newTestDB(t, true)
		d.openStore(t)
		role, err := sql.Open("pgx", pg.dsn(serviceRole, rolePassword, d.name))
		if err != nil {
			t.Fatal(err)
		}
		defer role.Close()
		for _, q := range []string{
			`SELECT * FROM ` + foreignSchema + `.teams`,
			`INSERT INTO ` + foreignSchema + `.teams VALUES (1)`,
			`CREATE TABLE ` + foreignSchema + `.x (a int)`,
			`CREATE TABLE public.x (a int)`,
			`CREATE SCHEMA other_schema`,
		} {
			if _, err := role.Exec(q); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("%s: %v", q, err)
			}
		}
		// The foreign role cannot read executor-registry data.
		foreign, _ := sql.Open("pgx", pg.dsn(foreignRole, rolePassword, d.name))
		defer foreign.Close()
		var table string
		d.admin.QueryRow(`SELECT tablename FROM pg_tables WHERE schemaname = $1 LIMIT 1`, serviceSchema).Scan(&table)
		if _, err := foreign.Exec(`SELECT * FROM ` + serviceSchema + `.` + table); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("foreign role read executor-registry: %v", err)
		}
	})

	t.Run("Store round-trip under the service role", func(t *testing.T) {
		d := newTestDB(t, true)
		s := d.openStore(t)
		now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
		if err := s.InTx(ctx, func(tx store.Tx) error {
			id, err := tx.NextExecutorID(ctx)
			if err != nil {
				return err
			}
			return tx.InsertExecutor(ctx, store.Executor{ExecutorID: id, TeamID: 3, Tag: "gpu", Type: "executor_docker_openhands", CreatedAt: now, UserPublicKey: "UABC", TokenFingerprint: strings.Repeat("f", 64), TokenIssuedAt: now})
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.InTx(ctx, func(tx store.Tx) error {
			if _, err := tx.GetExecutor(ctx, 4, 1); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("foreign team get: %v", err)
			}
			e, err := tx.GetExecutor(ctx, 3, 1)
			if err != nil || e.Tag != "gpu" || !e.TokenIssuedAt.Equal(now) {
				t.Errorf("get %+v %v", e, err)
			}
			c, err := tx.GetConnection(ctx, 1)
			if err != nil || c.State != store.StateNeverConnected || c.IntervalSeconds != 15 || c.MissLimit != 3 || c.WaitSeconds != 600 {
				t.Errorf("connection %+v %v", c, err)
			}
			tags, err := tx.ListTags(ctx, 3, "", 0)
			if err != nil || len(tags) != 1 || tags[0] != "gpu" {
				t.Errorf("tags %v %v", tags, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
