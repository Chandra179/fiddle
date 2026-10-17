//go:build integration

package example

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"db"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedDSN is populated once by TestMain and reused by every test.
var sharedDSN string

// TestMain starts one Postgres container for the whole suite, runs the tests,
// then terminates the container. One shared container keeps startup cost flat
// however many tests there are.
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
	client, err := db.NewPostgresClient(context.Background(), sharedDSN, cfg)
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
	admin, err := db.NewPostgresClient(ctx, sharedDSN, db.ConnectionConfig{})
	require.NoError(t, err)
	// Registered before the DROP below: cleanups run last-in first-out, so the
	// schema is dropped while admin is still open.
	t.Cleanup(func() { _ = admin.Close() })

	schema := safeSchemaName(t)

	_, err = admin.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, schema))
	require.NoError(t, err, "create schema %q", schema)

	t.Cleanup(func() {
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
	})

	// Scoped client — all queries land in this test's private schema.
	scopedDSN := fmt.Sprintf("%s&search_path=%s", sharedDSN, schema)
	client, err := db.NewPostgresClient(ctx, scopedDSN, cfg)
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
		context.Background(),
		"postgres://bad:bad@localhost:1/nodb?sslmode=disable",
		db.ConnectionConfig{},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrConnectionFailed)
}

func TestConnect_ContextCancelsStartupPing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := db.NewPostgresClient(ctx, sharedDSN, db.ConnectionConfig{})
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrConnectionFailed)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestConnect_PingTimeoutIsConfigurable(t *testing.T) {
	// The default is 5s; a 1ns budget must fail the ping immediately.
	_, err := db.NewPostgresClient(context.Background(), sharedDSN, db.ConnectionConfig{
		PingTimeout: time.Nanosecond,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrConnectionFailed)
	assert.True(t, db.IsTimeoutError(err), "got %v", err)
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
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = stmt.Close() }()

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

	sentinel := errors.New("intentional failure")

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
	client, err := db.NewPostgresClient(context.Background(), sharedDSN, db.ConnectionConfig{})
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
	defer func() { _ = rows.Close() }()

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

func TestTxTimeout_BoundsWholeTransaction(t *testing.T) {
	// Each statement is well inside QueryTimeout (which does not apply inside a
	// transaction anyway); only the total exceeds TxTimeout.
	client := newSchema(t, db.ConnectionConfig{
		QueryTimeout: 30 * time.Second,
		TxTimeout:    300 * time.Millisecond,
	})
	ctx := context.Background()

	err := client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO users (email) VALUES ('tx-timeout@example.com')`); err != nil {
			return err
		}
		for range 10 {
			if _, err := tx.ExecContext(ctx, `SELECT pg_sleep(0.1)`); err != nil {
				return err
			}
		}
		return nil
	})
	require.Error(t, err)
	assert.True(t, db.IsTimeoutError(err), "TxTimeout expiry must classify as a timeout, got %v", err)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the original error must stay wrapped, got %v", err)

	var n int
	require.NoError(t, client.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&n))
	assert.Zero(t, n, "the timed-out transaction must have rolled back")
}

func TestTxTimeout_NotHitByFastTransaction(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{TxTimeout: 5 * time.Second})
	ctx := context.Background()

	err := client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO users (email) VALUES ('fast@example.com')`)
		return err
	})
	require.NoError(t, err)

	var n int
	require.NoError(t, client.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&n))
	assert.Equal(t, 1, n)
}

func TestTxTimeout_CallerDeadlineTighter(t *testing.T) {
	client := newSchema(t, db.ConnectionConfig{TxTimeout: 30 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
		_, err := tx.ExecContext(ctx, `SELECT pg_sleep(5)`)
		return err
	})
	require.Error(t, err)
	assert.True(t, db.IsTimeoutError(err), "got %v", err)
	assert.Less(t, time.Since(start), 5*time.Second, "caller deadline must win over TxTimeout")
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

// pgx prepares and caches statements by default, which PgBouncer in
// transaction-pooling mode cannot serve (a statement is prepared on one server
// connection and executed on another). default_query_exec_mode in the DSN picks
// a mode that does not create named prepared statements.
func TestQueryExecMode_ControlsPreparedStatements(t *testing.T) {
	cases := []struct {
		mode         string
		wantPrepared bool
	}{
		{"", true}, // pgx default: statements are prepared and cached
		{"cache_describe", false},
		{"exec", false},
		{"simple_protocol", false},
	}
	for _, tc := range cases {
		t.Run("mode="+tc.mode, func(t *testing.T) {
			dsn := sharedDSN
			if tc.mode != "" {
				dsn += "&default_query_exec_mode=" + tc.mode
			}
			ctx := context.Background()
			// One connection, so every statement runs in the same session.
			client, err := db.NewPostgresClient(ctx, dsn, db.ConnectionConfig{MaxOpenConns: 1})
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })

			for i := range 3 {
				var n int
				require.NoError(t, client.QueryRowContext(ctx, `SELECT $1::int`, i).Scan(&n))
				assert.Equal(t, i, n)
			}

			var prepared int
			require.NoError(t, client.QueryRowContext(ctx, `SELECT count(*) FROM pg_prepared_statements`).Scan(&prepared))
			if tc.wantPrepared {
				assert.Positive(t, prepared, "the default mode keeps prepared statements")
			} else {
				assert.Zero(t, prepared, "this mode must not create named prepared statements")
			}
		})
	}
}

// Common column types scan into plain Go types through database/sql. This pins
// the behaviour of the pgx driver so a driver change shows up here.
func TestScan_CommonColumnTypes(t *testing.T) {
	client := newClient(t, db.ConnectionConfig{})
	ctx := context.Background()

	var (
		jsonCol, bytesCol, numRaw []byte
		jsonb, uuid, numStr       string
		numFloat                  float64
		ts                        time.Time
		flag                      bool
		big                       int64
		missing                   sql.NullString
		date                      time.Time
	)
	err := client.QueryRowContext(ctx, `SELECT
		'{"a":1}'::json,
		'{"a": 1}'::jsonb,
		'123e4567-e89b-12d3-a456-426614174000'::uuid,
		12.50::numeric,
		12.50::numeric,
		12.50::numeric,
		'2026-10-17 10:00:00+00'::timestamptz,
		'\xdeadbeef'::bytea,
		true,
		9007199254740993::bigint,
		NULL::text,
		'2026-10-17'::date`).Scan(&jsonCol, &jsonb, &uuid, &numStr, &numFloat, &numRaw, &ts, &bytesCol, &flag, &big, &missing, &date)
	require.NoError(t, err)

	assert.JSONEq(t, `{"a":1}`, string(jsonCol), "json scans into []byte")
	assert.JSONEq(t, `{"a":1}`, jsonb, "jsonb scans into string")
	assert.Equal(t, "123e4567-e89b-12d3-a456-426614174000", uuid, "uuid scans into string")
	assert.Equal(t, "12.50", numStr, "numeric keeps its scale as a string")
	assert.InDelta(t, 12.5, numFloat, 0)
	assert.Equal(t, "12.50", string(numRaw))
	assert.True(t, ts.Equal(time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)), "timestamptz scans into time.Time")
	assert.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, bytesCol)
	assert.True(t, flag)
	assert.Equal(t, int64(9007199254740993), big, "bigint keeps full precision")
	assert.False(t, missing.Valid, "NULL scans into sql.Null*")
	assert.Equal(t, 2026, date.Year())
}
