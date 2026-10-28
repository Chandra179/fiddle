# Ghost — Agent guidance

Multi-module Go monorepo. Each subdirectory is its own Go module.

## Modules & gotchas

| Module | Module path (go.mod) | Has Makefile | Tests |
|--------|---------------------|--------------|-------|
| db/ | `db` | yes | unit |
| cache/ | `cache` | yes | unit (miniredis) |
| kafka/ | `kafka` | yes | unit |
| rabbitmq/ | `rabbitmq` | yes | unit |
| oauth2/ | `oauth2` | yes | unit (mock OIDC server) |
| i18n/ | `i18n` | yes | unit (file-based) |
| cryptoutil/ | `cryptoutil` | yes | unit (pure Go) |
| logger/ | `logger` | yes | unit |

## Commands

```bash
# Every module has a Makefile
cd db/ && make test
# -> go test -count=1 -timeout 120s ./...

# All modules from the repo root
make test

# Lint and vulnerability scan (one module, or all from the repo root)
cd db/ && make lint && make vuln
make lint

# Everything CI checks: tidy, vet, race tests, lint, vuln
make check
```

- No test needs Docker.
- Never run `go test` from the repo root (no root `go.mod` or `go.work`).

## Module conventions

Each module follows: `client.go` (interfaces, sentinel errors, config) + named impl files.

- Constructors return **interfaces**, not concrete types.
- `db/` interface hierarchy: `DBTX` (sqlc-compatible) → `SQLExecutor` (+ WithTransaction) → `DB` (+ Close, PingContext). Repositories receive `DBTX` or `SQLExecutor`; only DI root gets `DB`.

## Adding a module

See `README.md`. Boilerplate: `go.mod` (simple name), `<name>.go` (public API), `impl.go`, `Makefile`, `<name>_test.go`.

## Conventions

- No `go.work` in the root (`.gitignore`d). Modules are standalone.
- Error classification via sentinel functions: `db.IsTimeoutError(err)`, `db.IsDuplicateKeyError(err)` — driver-agnostic, handles wrapped errors.
- `db.ConnectionConfig.QueryTimeout` is applied to every query unless the caller's context has a tighter deadline.
- `.golangci.yml` at the repo root configures golangci-lint for every module (`make lint`); `make vuln` runs govulncheck. `Makefile.common` holds the shared targets.
- Go `1.27.2` across all modules.
