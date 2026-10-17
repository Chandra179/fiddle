# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Structure

Multi-module monorepo — each subdirectory is an independent Go module with its own `go.mod`. Modules: `db/`, `cache/`, `kafka/`, `rabbitmq/`, `logger/`, `oauth2/`, `i18n/`, `cryptoutil/`. All target Go 1.27.2.

## Commands

Each module is tested independently. Integration tests require Docker (testcontainers).

```bash
# Run tests for a module (those with a Makefile)
cd db/ && make test
# Expands to: go test -tags integration -v -count=1 -timeout 300s ./...

# Unit tests only (no Docker): integration tests carry //go:build integration
cd cache/ && go test -v -count=1 ./...
cd logger/ && make test
make test-unit            # from the repo root, every module that has integration tests

# Lint (.golangci.yml at the root) and vulnerability scan
cd db/ && make lint && make vuln
make lint                 # from the repo root, every module
make check                # tidy, vet, race tests, lint, vuln: what CI checks without Docker
```

## Architecture

### Module conventions

Each module follows this pattern:
- **`client.go`** (or `<module>.go`) — public API: sentinel errors, interfaces, config structs. Keep the surface small.
- **`impl.go`** / named files — unexported struct implementing the interface; only constructors are exported (e.g. `NewPostgresClient`).
- **`example/`** — integration tests with `TestMain` starting infrastructure once via testcontainers; per-test cleanup via `t.Cleanup`.
- **Makefile** - add `go test -tags integration -v -count=1 -timeout 300s ./...` and `include ../Makefile.common` (shared `lint` and `vuln` targets)

### Interface-first design

Constructors return interfaces, not concrete types. Consumers receive the narrowest interface that satisfies their needs — this is especially important in `db/`:

```
DBTX               — raw query methods (sqlc-compatible)
  └── SQLExecutor  — DBTX + WithTransaction
        └── DB     — SQLExecutor + Close + PingContext
```

Pass `DBTX` or `SQLExecutor` to repositories. Only the DI root holds `DB`.

### db/ — canonical reference implementation

The `db/` module is the reference for all patterns used across the SDK. Key points:
- `ConnectionConfig` sets pool limits and a `QueryTimeout` applied to every query (respects tighter caller deadline)
- `WithTransaction(ctx, isolationLevel, fn)` handles rollback on error automatically
- `IsTimeoutError(err)` and `IsDuplicateKeyError(err)` do driver-agnostic error classification (covers wrapped errors)

### Integration test pattern

```go
func TestMain(m *testing.M) {
    // start container once, populate package-level DSN/client var
    code := m.Run()
    // teardown
    os.Exit(code)
}
```

Helpers (`newClient(t, cfg)`, `newSchema(t, cfg)`) register cleanup via `t.Cleanup`. Tests never call `Close` manually. Each test gets an isolated schema/topic/exchange for zero interference.

### Adding a new module

See `README.md` for the full template (directory layout, `go.mod`, Makefile, `TestMain` pattern).
