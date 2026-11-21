//go:build integration

package test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pandops/flowai-executor-registry/internal/store/pgstore"
)

// One PostgreSQL 16 container serves the package; every test gets its own
// database, prepared the way the deployment prepares it (deployment
// contract §5): a LOGIN role, the schema AUTHORIZATION that role, CONNECT
// only, no CREATE on the database, nothing for PUBLIC.

const (
	pgImage        = "docker.io/library/postgres:16"
	pgLabel        = "flowai.executor-registry.test"
	serviceSchema  = "executor_registry_schema"
	serviceRole    = "executor_registry"
	foreignSchema  = "team_registry_schema"
	foreignRole    = "team_registry"
	rolePassword   = "role-test-password"
	pgReadyTimeout = 60 * time.Second
)

type pgFixture struct {
	binary, container, password string
	port                        int
}

var pg *pgFixture

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	f, err := startPostgres(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres fixture start failed: %v\n", err)
		os.Exit(1)
	}
	pg = f
	code := m.Run()
	stopCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	if out, err := exec.CommandContext(stopCtx, f.binary, "rm", "--force", f.container).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "container teardown: %v: %s\n", err, out)
		if code == 0 {
			code = 1
		}
	}
	stop()
	os.Exit(code)
}

func startPostgres(ctx context.Context) (*pgFixture, error) {
	var binary string
	for _, c := range []string{"docker", "podman"} {
		p, err := exec.LookPath(c)
		if err != nil {
			continue
		}
		ictx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = exec.CommandContext(ictx, p, "info").Run()
		cancel()
		if err == nil {
			binary = p
			break
		}
	}
	if binary == "" {
		return nil, errors.New("no usable docker or podman CLI found")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	f := &pgFixture{binary: binary, container: "flowai-er-" + token(8), password: token(16), port: port}
	if ledger := os.Getenv("FLOWAI_RESOURCE_LEDGER"); ledger != "" {
		rec, _ := json.Marshal(map[string]string{"kind": "containers", "selector": "name=^" + f.container + "$"})
		fh, err := os.OpenFile(ledger, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		_, werr := fh.Write(append(rec, '\n'))
		if err := errors.Join(werr, fh.Close()); err != nil {
			return nil, err
		}
	}
	args := []string{"run", "--detach", "--rm", "--name", f.container, "--label", pgLabel,
		"--publish", fmt.Sprintf("127.0.0.1:%d:5432", port), "--tmpfs", "/var/lib/postgresql/data:rw",
		"--env", "POSTGRES_USER=flowai", "--env", "POSTGRES_PASSWORD=" + f.password, "--env", "POSTGRES_DB=postgres", pgImage}
	if out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("create container: %w: %s", err, strings.ReplaceAll(string(out), f.password, "<redacted>"))
	}
	deadline := time.Now().Add(pgReadyTimeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", f.dsn("flowai", f.password, "postgres"))
		if err == nil {
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = db.PingContext(pctx)
			cancel()
			db.Close()
			if err == nil {
				return f, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = exec.Command(binary, "rm", "--force", f.container).Run()
	return nil, errors.New("postgres not ready")
}

func token(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (f *pgFixture) dsn(user, password, db string) string {
	return fmt.Sprintf("host=127.0.0.1 port=%d user=%s password=%s dbname=%s sslmode=disable", f.port, user, password, db)
}

// testDB is one fresh database prepared like the deployment.
type testDB struct {
	name  string
	admin *sql.DB
}

// newTestDB creates a database; with roles it also creates the service
// role and a foreign service role, each owning its own schema.
func newTestDB(t *testing.T, roles bool) *testDB {
	t.Helper()
	ctx := context.Background()
	name := "er_" + token(8)
	root, err := sql.Open("pgx", pg.dsn("flowai", pg.password, "postgres"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	mustExec(t, root, `CREATE DATABASE "`+name+`"`)
	if roles {
		for _, r := range []string{serviceRole, foreignRole} {
			_, _ = root.ExecContext(ctx, `CREATE ROLE `+r+` LOGIN PASSWORD '`+rolePassword+`'`)
		}
	}
	admin, err := sql.Open("pgx", pg.dsn("flowai", pg.password, name))
	if err != nil {
		t.Fatal(err)
	}
	d := &testDB{name: name, admin: admin}
	if roles {
		mustExec(t, admin, `REVOKE ALL ON DATABASE "`+name+`" FROM PUBLIC`)
		mustExec(t, admin, `REVOKE ALL ON SCHEMA public FROM PUBLIC`)
		for _, p := range [][2]string{{serviceRole, serviceSchema}, {foreignRole, foreignSchema}} {
			mustExec(t, admin, `CREATE SCHEMA `+p[1]+` AUTHORIZATION `+p[0])
			mustExec(t, admin, `GRANT CONNECT ON DATABASE "`+name+`" TO `+p[0])
		}
		mustExec(t, admin, `CREATE TABLE `+foreignSchema+`.teams (team_id bigint PRIMARY KEY)`)
		mustExec(t, admin, `ALTER TABLE `+foreignSchema+`.teams OWNER TO `+foreignRole)
	}
	t.Cleanup(func() {
		admin.Close()
		c, err := sql.Open("pgx", pg.dsn("flowai", pg.password, "postgres"))
		if err != nil {
			t.Errorf("cleanup: %v", err)
			return
		}
		defer c.Close()
		_, _ = c.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '` + name + `' AND pid <> pg_backend_pid()`)
		if _, err := c.Exec(`DROP DATABASE IF EXISTS "` + name + `"`); err != nil {
			t.Errorf("cleanup: drop %s: %v", name, err)
		}
	})
	return d
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// openStore opens and migrates the service store as the service role.
func (d *testDB) openStore(t *testing.T) *pgstore.Store {
	t.Helper()
	ctx := context.Background()
	s, err := pgstore.Open(ctx, pg.dsn(serviceRole, rolePassword, d.name), serviceSchema)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}
