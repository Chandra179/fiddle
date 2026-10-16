package example

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"db"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedDSN is populated once by TestMain and reused by every test.
var sharedDSN string

// TestMain starts one Postgres container for the whole suite, runs the tests,
// then terminates the container. Using a single container rather than one per
// test cuts cold-start overhead from O(n × 3s) to a flat ~3 s regardless of
// how many tests are added.
func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}

	sharedDSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("get connection string: %v", err)
	}

	code := m.Run()

	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("warn: terminate container: %v", err)
	}

	os.Exit(code)
}

// newClient creates a db.DB using the shared container DSN and registers
// client.Close via t.Cleanup — callers do not need to close it manually.
func newClient(t *testing.T, cfg db.ConnectionConfig) db.DB {
	t.Helper()
	client, err := db.NewPostgresClient(sharedDSN, cfg)
	require.NoError(t, err, "NewPostgresClient")
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// newSchema creates a fresh Postgres schema named after the test, runs DDL
// inside it, and returns a client scoped to that schema via search_path.
// The schema is dropped automatically when the test finishes, giving each test
// a completely clean slate with zero interference from other tests.
func newSchema(t *testing.T, cfg db.ConnectionConfig) db.DB {
	t.Helper()
	ctx := context.Background()

	// Admin client — only used to create/drop the schema itself.
	admin, err := db.NewPostgresClient(sharedDSN, db.ConnectionConfig{})
	require.NoError(t, err)
	defer admin.Close()

	schema := safeSchemaName(t)

	_, err = admin.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, schema))
	require.NoError(t, err, "create schema %q", schema)

	t.Cleanup(func() {
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
	})

	// Scoped client — all queries land in this test's private schema.
	scopedDSN := fmt.Sprintf("%s&search_path=%s", sharedDSN, schema)
	client, err := db.NewPostgresClient(scopedDSN, cfg)
	require.NoError(t, err, "NewPostgresClient (scoped)")
	t.Cleanup(func() { _ = client.Close() })

	runMigrations(t, client)
	return client
}

// safeSchemaName derives a valid, max-63-char Postgres identifier from the
// test name by replacing every non-alphanumeric character with an underscore.
func safeSchemaName(t *testing.T) string {
	t.Helper()
	raw := "t_" + t.Name()
	b := make([]byte, len(raw))
	for i, c := range []byte(raw) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			b[i] = c
		} else {
			b[i] = '_'
		}
	}
	if len(b) > 63 {
		b = b[:63]
	}
	return string(b)
}

// runMigrations creates the tables used across the test suite inside whichever
// schema the client's search_path is pointing at.
func runMigrations(t *testing.T, client db.DB) {
	t.Helper()
	ctx := context.Background()

	_, err := client.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id         BIGSERIAL PRIMARY KEY,
			email      TEXT NOT NULL UNIQUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
	require.NoError(t, err, "create users table")

	_, err = client.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS audit_log (
			id        BIGSERIAL PRIMARY KEY,
			entity    TEXT NOT NULL,
			entity_id BIGINT NOT NULL,
			action    TEXT NOT NULL
		)`)
	require.NoError(t, err, "create audit_log table")
}

// ---- Tests -----------------------------------------------------------------

func TestConnect(t *testing.T) {
	client := newClient(t, db.ConnectionConfig{})

	err := client.PingContext(context.Background())
	assert.NoError(t, err)
}

func TestConnect_BadDSN(t *testing.T) {
	_, err := db.NewPostgresClient(
		"postgres://bad:bad@localhost:1/nodb?sslmode=disable",
		db.ConnectionConfig{},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrConnectionFailed)
}

func TestExecAndQuery(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	_, err := client.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1)`, "exec@example.com")
	require.NoError(t, err)

	var email string
	err = client.QueryRowContext(ctx,
		`SELECT email FROM users WHERE email = $1`, "exec@example.com").Scan(&email)
	require.NoError(t, err)
	assert.Equal(t, "exec@example.com", email)

	rows, err := client.QueryContext(ctx, `SELECT email FROM users`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 1, count)
}

func TestPrepareContext(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	stmt, err := client.PrepareContext(ctx, `SELECT email FROM users WHERE email = $1`)
	require.NoError(t, err)
	defer stmt.Close()

	var email string
	err = stmt.QueryRowContext(ctx, "nobody@example.com").Scan(&email)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestWithTransaction_Commit(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	err := client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1)`, "commit@example.com")
		return err
	})
	require.NoError(t, err)

	var count int
	err = client.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM users WHERE email = $1`, "commit@example.com").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "committed row must be visible after transaction")
}

func TestWithTransaction_Rollback(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	sentinel := fmt.Errorf("intentional failure")

	err := client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO users (email) VALUES ($1)`, "rollback@example.com"); err != nil {
			return err
		}
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel, "WithTransaction must propagate the fn error")

	var count int
	_ = client.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM users WHERE email = $1`, "rollback@example.com").Scan(&count)
	assert.Equal(t, 0, count, "rolled-back row must not be visible")
}

func TestWithTransaction_Panic(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	assert.Panics(t, func() {
		_ = client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
			_, _ = tx.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1)`, "panic@example.com")
			panic("something catastrophic")
		})
	}, "panic must be re-raised after rollback")

	var count int
	_ = client.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM users WHERE email = $1`, "panic@example.com").Scan(&count)
	assert.Equal(t, 0, count, "panicked transaction must be rolled back")
}

func TestWithTransaction_MultipleOps(t *testing.T) {
	// Canonical SDK pattern: two repository operations sharing one DBTX.
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	var userID int64
	err := client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
		if err := tx.QueryRowContext(ctx,
			`INSERT INTO users (email) VALUES ($1) RETURNING id`, "multi@example.com").
			Scan(&userID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO audit_log (entity, entity_id, action) VALUES ('user', $1, 'created')`, userID)
		return err
	})
	require.NoError(t, err)

	var auditCount int
	err = client.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM audit_log WHERE entity_id = $1`, userID).Scan(&auditCount)
	require.NoError(t, err)
	assert.Equal(t, 1, auditCount, "both rows must commit atomically")
}

func TestWithTransaction_IsolationLevel(t *testing.T) {
	// Serializable is the strictest level — confirming it works means all
	// lower levels will too.
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	err := client.WithTransaction(ctx, sql.LevelSerializable, func(ctx context.Context, tx db.DBTX) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1)`, "serial@example.com")
		return err
	})
	require.NoError(t, err)
}

func TestIsDuplicateKeyError(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	_, err := client.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1)`, "dup@example.com")
	require.NoError(t, err)

	_, err = client.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1)`, "dup@example.com")
	require.Error(t, err)

	assert.True(t, db.IsDuplicateKeyError(err), "23505 must be classified as duplicate key")
	assert.False(t, db.IsTimeoutError(err), "duplicate key must not be misclassified as timeout")
}

func TestIsTimeoutError_ContextDeadline(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // ensure the deadline has passed

	_, err := client.ExecContext(ctx, `SELECT 1`)
	require.Error(t, err)

	assert.True(t, db.IsTimeoutError(err), "DeadlineExceeded must be classified as timeout")
	assert.False(t, db.IsDuplicateKeyError(err))
}

func TestIsTimeoutError_StatementTimeout(t *testing.T) {
	// schema-scoped client ensures SET statement_timeout does not bleed into
	// other tests running against the same container.
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	_, err := client.ExecContext(ctx, `SET statement_timeout = '50ms'`)
	require.NoError(t, err)

	_, err = client.ExecContext(ctx, `SELECT pg_sleep(5)`)
	require.Error(t, err)
	assert.True(t, db.IsTimeoutError(err), "Postgres 57014 must be classified as timeout")
}

func TestQueryTimeout_CancelsSlow(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{
		QueryTimeout: 50 * time.Millisecond,
	})
	ctx := context.Background()

	_, err := client.ExecContext(ctx, `SELECT pg_sleep(5)`)
	require.Error(t, err)
	assert.True(t, db.IsTimeoutError(err), "QueryTimeout must cancel slow queries")
}

func TestQueryTimeout_CallerDeadlineTighter(t *testing.T) {
	// QueryTimeout is generous; the caller's context is tighter — caller wins.
	client := newSchema(t, db.ConnectionConfig{
		QueryTimeout: 30 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.ExecContext(ctx, `SELECT pg_sleep(5)`)
	require.Error(t, err)
	assert.True(t, db.IsTimeoutError(err), "caller deadline must override QueryTimeout")
}

func TestQueryTimeout_NoTimeout(t *testing.T) {
	// Zero QueryTimeout must not interfere with fast queries.
	client := newSchema(t, db.ConnectionConfig{})

	_, err := client.ExecContext(context.Background(), `SELECT 1`)
	assert.NoError(t, err)
}

func TestClose(t *testing.T) {
	// Deliberately not using newClient — we manage Close ourselves here.
	client, err := db.NewPostgresClient(sharedDSN, db.ConnectionConfig{})
	require.NoError(t, err)

	require.NoError(t, client.Close())

	err = client.PingContext(context.Background())
	assert.Error(t, err, "all operations must fail after Close")
}

func TestQueryTimeout_QueryRowsIterable(t *testing.T) {
	// The timeout context must outlive QueryContext so rows can still be read.
	client := newSchema(t, db.ConnectionConfig{
		QueryTimeout: 5 * time.Second,
	})
	ctx := context.Background()

	rows, err := client.QueryContext(ctx, `SELECT generate_series(1, 100)`)
	require.NoError(t, err)
	defer rows.Close()

	var n int
	for rows.Next() {
		n++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 100, n)
}

func TestQueryTimeout_QueryRowScan(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{
		QueryTimeout: 5 * time.Second,
	})

	var v int
	err := client.QueryRowContext(context.Background(), `SELECT 42`).Scan(&v)
	require.NoError(t, err)
	assert.Equal(t, 42, v)
}

func TestIsSerializationError_SerializableConflict(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{})
	ctx := context.Background()

	_, err := client.ExecContext(ctx, `INSERT INTO users (email) VALUES ('a@example.com')`)
	require.NoError(t, err)

	// Two serializable transactions read-then-write the same row; the second
	// commit must fail with 40001.
	started := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.WithTransaction(ctx, sql.LevelSerializable, func(ctx context.Context, tx db.DBTX) error {
			var c int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&c); err != nil {
				return err
			}
			close(started)
			<-release
			_, err := tx.ExecContext(ctx, `UPDATE users SET email = 'b@example.com'`)
			return err
		})
	}()

	<-started
	err = client.WithTransaction(ctx, sql.LevelSerializable, func(ctx context.Context, tx db.DBTX) error {
		var c int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&c); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE users SET email = 'c@example.com'`)
		return err
	})
	close(release)
	otherErr := <-errCh

	// Exactly one side loses; whichever it is must classify as serialization.
	failed := err
	if failed == nil {
		failed = otherErr
	}
	require.Error(t, failed)
	assert.True(t, db.IsSerializationError(failed), "got %v", failed)
	assert.False(t, db.IsTimeoutError(failed), "serialization failure is not a timeout")
}
