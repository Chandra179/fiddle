package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// postgresClient implements the DB interface for PostgreSQL using pgx through
// its database/sql adapter.
type postgresClient struct {
	db     *sql.DB
	config ConnectionConfig
}

// NewPostgresClient creates a new PostgreSQL database client with the given DSN
// and connection pool configuration. The connection is verified with a ping
// before returning, so a non-nil return value is ready to use.
//
// The driver is pgx through database/sql. By default pgx prepares and caches
// statements per connection, which a transaction-pooling proxy such as PgBouncer
// cannot serve (a statement is prepared on one server connection and run on
// another, failing with "prepared statement ... already exists" or "does not
// exist"). Behind such a proxy add default_query_exec_mode=exec (or
// cache_describe, simple_protocol) to the DSN, for example
//
//	postgres://user:pass@pgbouncer:6432/app?default_query_exec_mode=exec
//
// ctx cancels the startup ping, which is also bounded by
// ConnectionConfig.PingTimeout (DefaultPingTimeout when unset). ctx is not
// retained: it has no effect on the returned client after this call.
//
// The caller is responsible for calling Close() when the client is no longer
// needed (typically at process shutdown).
func NewPostgresClient(ctx context.Context, dsn string, config ConnectionConfig) (DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnectionFailed, err)
	}

	// Configure connection pool — zero values preserve sql.DB defaults.
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
	}
	if config.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	}

	// Verify the connection before handing the client to the caller.
	pingTimeout := config.PingTimeout
	if pingTimeout <= 0 {
		pingTimeout = DefaultPingTimeout
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: %w", ErrConnectionFailed, err)
	}

	return &postgresClient{db: db, config: config}, nil
}

// withTimeout wraps ctx with d when d is positive and the context does not
// already carry a shorter deadline.
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	// Caller's deadline is already tighter — don't override it.
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= d {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// withQueryTimeout applies QueryTimeout to ctx.
func (c *postgresClient) withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return withTimeout(ctx, c.config.QueryTimeout)
}

// ExecContext executes a query without returning any rows.
func (c *postgresClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	ctx, cancel := c.withQueryTimeout(ctx)
	defer cancel()
	return c.db.ExecContext(ctx, query, args...)
}

// PrepareContext creates a prepared statement for later use.
func (c *postgresClient) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	ctx, cancel := c.withQueryTimeout(ctx)
	defer cancel()
	return c.db.PrepareContext(ctx, query)
}

// QueryContext executes a query that returns multiple rows.
//
// QueryTimeout bounds both execution and row iteration: database/sql closes
// the returned rows when their context ends, so the timeout context cannot be
// cancelled on return. Its timer is released when the deadline fires.
func (c *postgresClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	ctx, cancel := c.withQueryTimeout(ctx)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		cancel()
		return nil, err
	}
	_ = cancel // cancelling here would close rows before the caller reads them
	return rows, nil
}

// QueryRowContext executes a query that returns at most one row.
//
// Like QueryContext, QueryTimeout also covers the subsequent Scan.
func (c *postgresClient) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	ctx, cancel := c.withQueryTimeout(ctx)
	_ = cancel // cancelling on return would make Scan fail with context.Canceled
	return c.db.QueryRowContext(ctx, query, args...)
}

// WithTransaction executes fn within a database transaction at the requested
// isolation level.
//
// The DBTX passed to fn is the underlying *sql.Tx, which is safe to pass to
// any sqlc-generated query. The transaction is committed when fn returns nil
// and rolled back otherwise. A panic inside fn triggers a rollback and is
// re-raised after the rollback completes.
//
// TxTimeout, when set, bounds the whole call. The context passed to fn carries
// that deadline; once it expires database/sql rolls the transaction back and
// the error returned here satisfies IsTimeoutError.
func (c *postgresClient) WithTransaction(
	ctx context.Context,
	isolationLevel sql.IsolationLevel,
	fn func(ctx context.Context, tx DBTX) error,
) error {
	// The context governs the transaction until it is committed or rolled
	// back, so it is released only when this call returns.
	ctx, cancel := withTimeout(ctx, c.config.TxTimeout)
	defer cancel()

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: isolationLevel})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p) // re-raise after rollback
		}
	}()

	if err := fn(ctx, tx); err != nil {
		// When ctx has ended, database/sql aborts the transaction itself and the
		// driver drops the connection, so Rollback fails with ErrTxDone or a
		// closed-connection error whatever happened: the server discards the
		// transaction either way, and the original error is the one to report.
		if rbErr := tx.Rollback(); rbErr != nil && ctx.Err() == nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return fmt.Errorf("transaction failed: %w, rollback also failed: %w", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Close releases all resources held by the connection pool.
func (c *postgresClient) Close() error {
	return c.db.Close()
}

// PingContext verifies the database is reachable.
func (c *postgresClient) PingContext(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// ---- Error classification helpers ------------------------------------------
//
// These helpers are intentionally separate from the sentinel errors so callers
// can decide how to handle driver-level errors without importing pgx
// themselves. They fall back to string matching when the error is not a
// *pgconn.PgError (e.g. errors from other drivers).

// Driver-agnostic fallbacks: lower-case substrings of an error message.
var (
	timeoutKeywords = []string{
		"timeout",
		"deadline exceeded",
		"connection timed out",
		"query_canceled",
		"57014",
		"55p03",
	}
	serializationKeywords = []string{
		"could not serialize access",
		"deadlock detected",
		"40001",
		"40p01",
	}
	// "already exists" is deliberately absent: it also covers DDL errors such
	// as 42P07 (duplicate_table).
	duplicateKeyKeywords = []string{
		"duplicate key",
		"unique constraint",
		"unique violation",
		"23505",
	}
)

// messageContainsAny reports whether the lower-cased message of err contains
// any of keywords.
func messageContainsAny(err error, keywords []string) bool {
	msg := strings.ToLower(err.Error())
	return slices.ContainsFunc(keywords, func(kw string) bool { return strings.Contains(msg, kw) })
}

// IsTimeoutError reports whether err represents a query, lock or connection timeout.
//
// It recognises:
//   - context.DeadlineExceeded and net.Error timeouts
//   - PostgreSQL error codes 57014 (query_canceled, incl. statement_timeout),
//     55P03 (lock_not_available, incl. lock_timeout),
//     25P03 (idle_in_transaction_session_timeout), 57P05 (idle_session_timeout)
//   - Common timeout-related substrings for driver-agnostic fallback
//
// Serialization failures and deadlocks are not timeouts; use IsSerializationError.
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return true
	}

	// pgx fast path — checked first to avoid string allocation.
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case "57014", // query_canceled (statement_timeout, cancel request)
			"55P03", // lock_not_available (lock_timeout)
			"25P03", // idle_in_transaction_session_timeout
			"57P05": // idle_session_timeout
			return true
		}
		return false
	}

	// Driver-agnostic fallback (covers other drivers, wrapped errors, etc.).
	return messageContainsAny(err, timeoutKeywords)
}

// IsSerializationError reports whether err is a transient transaction conflict
// that is safe to retry by re-running the whole transaction.
//
// It recognises PostgreSQL error codes 40001 (serialization_failure) and
// 40P01 (deadlock_detected).
func IsSerializationError(err error) bool {
	if err == nil {
		return false
	}

	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}

	return messageContainsAny(err, serializationKeywords)
}

// IsDuplicateKeyError reports whether err is a unique-constraint violation.
//
// It recognises:
//   - PostgreSQL error code 23505 (unique_violation)
//   - Common duplicate-key substrings for driver-agnostic fallback
func IsDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	// pgx fast path.
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "23505" // unique_violation
	}

	// Driver-agnostic fallback.
	return messageContainsAny(err, duplicateKeyKeywords)
}
