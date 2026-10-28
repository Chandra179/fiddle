# Ghost — Agent guidance

Multi-module Go monorepo. Each subdirectory is its own Go module.

## Modules & gotchas

| Module | Module path (go.mod) | Has Makefile | Docker needed for tests |
|--------|---------------------|--------------|------------------------|
| db/ | `db` | yes | yes (testcontainers) |
| cache/ | `cache` | yes | no — unit tests use miniredis |
| kafka/ | `kafka` | yes | yes (testcontainers) |
| rabbitmq/ | `rabbitmq` | yes | yes (testcontainers) |
| oauth2/ | `oauth2` | yes | no* — mock OIDC server |
| i18n/ | `i18n` | yes | no — file-based only |
| cryptoutil/ | `cryptoutil` | yes | no — pure Go only |
| logger/ | `logger` | yes | no |

## Commands

```bash
# Every module has a Makefile
cd db/ && make test
# -> go test -tags integration -v -count=1 -timeout 300s ./...

# cache unit tests (miniredis, no Docker)
cd cache/ && go test -v -count=1 ./...

# cryptoutil unit tests (pure Go, no Docker)
cd cryptoutil/ && go test -v -count=1 ./...

# logger (unit tests only)
cd logger/ && make test

# Docker-free tests of every module that has integration tests
make test-unit      # from the repo root

# Lint and vulnerability scan (one module, or all from the repo root)
cd db/ && make lint && make vuln
make lint

# Everything CI checks without Docker: tidy, vet, race tests, lint, vuln
make check
```

- Test files that need Docker carry `//go:build integration`; without that tag `go test ./...` runs only unit tests. `oauth2/example` also carries it but needs no Docker (mock OIDC).
- Never run `go test` from the repo root (no root `go.mod` or `go.work`).
- `oauth2/` unit tests (`*_test.go` at package level) need no Docker; `example/` integration tests need no Docker either (mock OIDC).

## Module conventions

Each module follows: `client.go` (interfaces, sentinel errors, config) + named impl files + `example/` (integration tests).

- Constructors return **interfaces**, not concrete types.
- `db/` interface hierarchy: `DBTX` (sqlc-compatible) → `SQLExecutor` (+ WithTransaction) → `DB` (+ Close, PingContext). Repositories receive `DBTX` or `SQLExecutor`; only DI root gets `DB`.

## Integration test pattern

```go
func TestMain(m *testing.M) {
    // start container once, populate package-level DSN/client var
    code := m.Run()
    os.Exit(code)
}
```

Helpers (`newClient(t, cfg)`) register cleanup via `t.Cleanup`. Tests never call `Close` manually. Each test gets an isolated schema/topic/exchange.

- `db/` uses `newSchema(t, cfg)` — creates a schema per test via `search_path`, auto-dropped on cleanup.
- `i18n/` tests use testdata files relative to `example/testdata/`.

## Adding a module

See `README.md`. Boilerplate: `go.mod` (simple name), `<name>.go` (public API), `impl.go`, `Makefile`, `example/integration_test.go`.

## Conventions

- No `go.work` in the root (`.gitignore`d). Modules are standalone.
- Error classification via sentinel functions: `db.IsTimeoutError(err)`, `db.IsDuplicateKeyError(err)` — driver-agnostic, handles wrapped errors.
- `db.ConnectionConfig.QueryTimeout` is applied to every query unless the caller's context has a tighter deadline.
- `.golangci.yml` at the repo root configures golangci-lint for every module (`make lint`); `make vuln` runs govulncheck. `Makefile.common` holds the shared targets.
- Go `1.27.2` across all modules.
