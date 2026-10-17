package db

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrConnectionFailed is returned (wrapped) by NewPostgresClient when the
// connection cannot be opened or verified. Classify other failures with
// IsTimeoutError, IsSerializationError and IsDuplicateKeyError.
var ErrConnectionFailed = errors.New("database connection failed")

// DBTX is the interface for database operations that both *sql.DB and *sql.Tx implement.
// This is compatible with sqlc's generated interface and allows using either
// a connection pool or a transaction interchangeably.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// SQLExecutor extends DBTX with transaction management capabilities.
// Repositories that need transaction support should depend on this interface.
type SQLExecutor interface {
	DBTX
	// WithTransaction executes fn within a database transaction using the given
	// isolation level. The transaction is automatically committed if fn returns
	// nil, or rolled back if fn returns an error or panics.
	//
	// The tx argument passed to fn implements DBTX, so it can be passed directly
	// to any sqlc-generated query function.
	WithTransaction(ctx context.Context, isolationLevel sql.IsolationLevel, fn func(ctx context.Context, tx DBTX) error) error
}

// DefaultPingTimeout bounds the startup ping when ConnectionConfig.PingTimeout is unset.
const DefaultPingTimeout = 5 * time.Second

// ConnectionConfig holds connection pool, startup and timeout configuration.
//
// Zero values are ignored — the underlying sql.DB defaults are preserved for
// any field left unset.
type ConnectionConfig struct {
	// MaxOpenConns sets the maximum number of open connections to the database.
	MaxOpenConns int
	// MaxIdleConns sets the maximum number of idle connections in the pool.
	MaxIdleConns int
	// ConnMaxLifetime sets the maximum time a connection may be reused.
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime sets the maximum time a connection may be idle before closing.
	ConnMaxIdleTime time.Duration
	// QueryTimeout, when non-zero, is applied as a deadline to every query that
	// does not already carry a shorter deadline via its context.
	//
	// It does not apply to statements run through the DBTX handed to a
	// WithTransaction callback; those are bounded by TxTimeout.
	QueryTimeout time.Duration
	// TxTimeout, when non-zero, bounds a whole WithTransaction call (begin, the
	// callback and commit) unless its context already carries a shorter
	// deadline. When it fires the transaction is rolled back and the callback's
	// context is cancelled. Set it larger than QueryTimeout: it covers many
	// statements, not one.
	TxTimeout time.Duration
	// PingTimeout bounds the connectivity check done by the constructor.
	// Zero uses DefaultPingTimeout. The check also stops when the context given
	// to the constructor is done.
	PingTimeout time.Duration
}

// DB is the top-level database handle returned by a driver constructor such as
// NewPostgresClient. It combines query execution with lifecycle management.
//
// Only the owner of the DB (typically main or a dependency-injection root)
// should hold this interface and call Close. Pass SQLExecutor or DBTX to
// everything else so that no downstream package can accidentally shut down the
// shared connection pool.
type DB interface {
	SQLExecutor
	// Close releases all resources held by the connection pool. It should be
	// called exactly once, by the component that created the DB.
	Close() error
	// PingContext verifies that the database is reachable. Useful for health
	// checks and startup probes.
	PingContext(ctx context.Context) error
}
