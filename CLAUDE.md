# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Structure

Multi-module monorepo — each subdirectory is an independent Go module with its own `go.mod`. Modules: `db/`, `cache/`, `kafka/`, `rabbitmq/`, `logger/`, `oauth2/`, `i18n/`, `cryptoutil/`. All target Go 1.27.2.

## Commands

Each module is tested independently. No test needs Docker.

```bash
# Run tests for a module
cd db/ && make test
# Expands to: go test -count=1 -timeout 120s ./...

make test                 # from the repo root, every module

# Lint (.golangci.yml at the root) and vulnerability scan
cd db/ && make lint && make vuln
make lint                 # from the repo root, every module
make check                # tidy, vet, race tests, lint, vuln: what CI checks
```

## Architecture

### Module conventions

Each module follows this pattern:
- **`client.go`** (or `<module>.go`) — public API: sentinel errors, interfaces, config structs. Keep the surface small.
- **`impl.go`** / named files — unexported struct implementing the interface; only constructors are exported (e.g. `NewPostgresClient`).
- **Makefile** - add `go test -count=1 -timeout 120s ./...` and `include ../Makefile.common` (shared `lint` and `vuln` targets)

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

### Adding a new module

See `README.md` for the full template (directory layout, `go.mod`, Makefile).
