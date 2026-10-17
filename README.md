# Ghost

This is a utility project for Golang. Each module lives in its own subdirectory with an independent `go.mod`.

## Adding a new module

1. **Create a subdirectory** named after your module:
   ```
   ghost/
   └── yourmodule/
       ├── go.mod
       ├── go.sum
       ├── yourmodule.go             # public API / interfaces
       ├── impl.go                   # implementation(s)
       ├── Makefile                  # at minimum: a `test` target; include ../Makefile.common for lint/vuln
       └── example/
           └── integration_test.go  # integration tests using testcontainers or similar
   ```

2. **`go.mod`** — use a simple module name matching the directory:
   ```
   module yourmodule

   go 1.27.2
   ```

3. **Public API file** (`yourmodule.go`) — define sentinel errors, interfaces, and config structs. Keep the public surface small; consumers should only need to import this package.

4. **Implementation file** — implement the interfaces declared in the API file. Unexported types are fine; expose only constructors (e.g. `NewXxxClient`).

5. **`Makefile`** — include at least an integration test target, and the shared `lint` and `vuln` targets:
   ```makefile
   .PHONY: test

   include ../Makefile.common

   test:
   	go test -tags integration -v -count=1 -timeout 300s ./...
   ```
   Put the `//go:build integration` tag on every test file that needs Docker (or any other external service), so that a plain `go test ./...` runs only the unit tests. Add the module to `MODULES` in the root `Makefile` and to the matrices in `.github/workflows/ci.yml`.

6. **`example/` integration tests** — use `TestMain` to spin up any required infrastructure (e.g. a Testcontainers container) once for the whole suite, then share the connection across tests to keep suite startup time flat:
   ```go
   func TestMain(m *testing.M) {
       // start container / external dependency once
       // populate a package-level DSN / client var
       code := m.Run()
       // teardown
       os.Exit(code)
   }
   ```

   Helpers like `newClient(t, cfg)` should register cleanup via `t.Cleanup` so individual tests never call `Close` manually.

## Example: `db` module

The `db/` module is the canonical reference implementation. It provides a PostgreSQL client with connection-pool configuration, automatic query timeouts, transaction helpers, and error classification utilities.

| File | Purpose |
|------|---------|
| `db/client.go` | Sentinel errors, `DBTX`, `SQLExecutor`, `DB` interfaces, `ConnectionConfig` |
| `db/postgres.go` | `NewPostgresClient` constructor and `postgresClient` implementation (pgx through `database/sql`) |
| `db/Makefile` | `test` target running integration tests with `-tags integration`, plus `test-unit`, `lint` and `vuln` |
| `db/example/postgres_integration_test.go` | Full integration test suite using Testcontainers |

### Interface hierarchy

```
DBTX               — raw query methods (compatible with sqlc)
  └── SQLExecutor  — DBTX + WithTransaction
        └── DB     — SQLExecutor + Close + PingContext
```

Pass `DBTX` or `SQLExecutor` to repositories so they can't accidentally close the shared pool. Only the dependency-injection root should hold `DB`.

### Quick start

```go
import "db"

// ctx cancels the startup ping, which is also bounded by PingTimeout (default 5s).
client, err := db.NewPostgresClient(ctx, dsn, db.ConnectionConfig{
    MaxOpenConns:    10,
    MaxIdleConns:    5,
    ConnMaxLifetime: 30 * time.Minute,
    QueryTimeout:    5 * time.Second,  // per query
    TxTimeout:       30 * time.Second, // per WithTransaction call, begin to commit
    PingTimeout:     3 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Single query
var name string
err = client.QueryRowContext(ctx, `SELECT name FROM users WHERE id = $1`, id).Scan(&name)

// Transaction
err = client.WithTransaction(ctx, sql.LevelReadCommitted, func(ctx context.Context, tx db.DBTX) error {
    _, err := tx.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1)`, email)
    return err
})

// Error classification
if db.IsDuplicateKeyError(err) { ... }
if db.IsTimeoutError(err)      { ... }
```

### Driver notes

The driver is pgx through `database/sql`.

- **PgBouncer (transaction pooling) and similar proxies.** pgx prepares and caches statements per connection by default, which such a proxy cannot serve. Add `default_query_exec_mode=exec` (or `cache_describe`, `simple_protocol`) to the DSN. The `db` integration tests check that these modes create no named prepared statements.
- **Column types.** `json`, `jsonb`, `uuid`, `numeric` (scale kept, e.g. `"12.50"`), `timestamptz`, `bytea`, `bool` and `bigint` scan into `[]byte`/`string`, `string`, `time.Time`, `[]byte`, `bool` and `int64` as usual (see `TestScan_CommonColumnTypes`). Array columns are not covered: use a driver-specific helper for them.

## Development

Run from the repo root to cover every module, or `make <target>` inside one module:

```bash
make test-unit   # tests that need no Docker
make test        # everything; the integration tests need Docker
make lint        # golangci-lint, configured by .golangci.yml
make vuln        # govulncheck
make check       # tidy, vet, race tests, lint and vuln: what CI checks without Docker
```

CI (`.github/workflows/ci.yml`) runs the same targets per module, plus the integration tests.

## Prompt
see proven implementation from big companies, do we need abstraction? is it general implementation? do the implementation handle specific usecase only or general? is it configurable or not? document it in architecture decision record in adr.md in the directory if not exist create one.