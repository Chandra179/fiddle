# Tech debt and deferred findings

Results of the Go 1.27.2 review (October 2026). High and medium findings were
fixed on branch `chore/go-1.27.2-review-fixes`; the items below were left out
because they break public APIs, need a design decision, or are low severity.

## Breaking API changes worth scheduling

| Module | Item | Why it matters |
|---|---|---|
| db | Migrate `lib/pq` → `pgx/v5` (stdlib adapter keeps `*sql.DB`). | lib/pq is maintenance-mode; pgx is faster, supports binary protocol, `pgconn.PgError`, and better context cancellation. |
| db | `WithTransaction` does not apply `QueryTimeout`. | Long transactions are unbounded unless callers set a deadline. Needs a separate `TxTimeout` rather than reusing the per-query one. |
| db | `NewPostgresClient` has no `ctx` and hard-codes a 5s startup ping. | Startup cannot be cancelled or tuned. |
| kafka | Consumer handler is `func(*kgo.Record) error` with no `context.Context`. | Handlers cannot observe shutdown or carry trace context. |
| kafka | `SlogShim.Level()` is fixed at Info; `Log` uses `context.Background()`. | No way to enable franz-go debug logs. |
| rabbitmq | `MessageHandler` receives `json.Unmarshal`'d `any` (map[string]any). | Allocation-heavy, loses numeric precision, and rejects every non-JSON payload as poison. A typed/generic or raw-bytes handler would fix it. |
| rabbitmq | Classic queues with `RetryEnabled=false` still requeue without a counter. | Only quorum queues (`x-delivery-count`, broker delivery-limit) bound the loop. Recommend quorum queues or `RetryEnabled`. |
| rabbitmq | `Config.ApplyDefaults` only validates. | Misleading name; rename to `Validate` and apply `NewDefaultConfig` values for zero fields. |
| rabbitmq | Topology is restored after the client reports `StateConnected`. | Consumers waiting on `IsHealthy` can re-subscribe before non-durable queues are re-declared. |
| oauth2 | Depends on gin for two handlers. | Pulls a large dependency tree into an SDK; plain `net/http` handlers (adaptable to gin) would be lighter. |
| oauth2 | `GoogleOIDCProvider` is also the generic OIDC provider; `ApprovalForce` + `AccessTypeOffline` always set. | Forces the consent screen on every login. Make prompt/access type configurable and rename the type. |
| logger | Module path `github.com/Chandra179/gosdk` differs from every other module. | Not importable by sibling modules; rename to `logger` (or move all modules under one path prefix). |
| cache | `RedisCache` is exported, contrary to the constructors-return-interfaces convention. | Callers can bypass the `Cache` interface. |

## Security hardening (not bugs today)

- **cryptoutil**: default scrypt `N=2^15` is below the current OWASP minimum
  (`N=2^17, r=8, p=1`). Not changed because existing derived keys would stop
  verifying; introduce a versioned format (store params alongside the key)
  first. For new password storage prefer argon2id (`golang.org/x/crypto/argon2`).
- **kafka**: DLQ headers include `dlq_error` (the handler's error text); make
  sure handler errors never contain secrets or PII.

## Hygiene

- Unused sentinels: `cache.ErrCacheTimeout`, `kafka.ErrProducerClosed`,
  `kafka.ErrConsumerClosed` (remove or start returning them).
- No CI, no `.golangci.yml`, and no `govulncheck` target in the Makefiles.
  `golangci-lint run` with default linters reports many unchecked
  `Close()` errors (errcheck) across modules.
- Integration tests in `db/`, `kafka/`, `rabbitmq/` and `cache/example` have no
  `//go:build integration` tag even though the Makefiles pass `-tags
  integration`, so a plain `go test ./...` without Docker fails in `TestMain`.
- rabbitmq integration tests stop/start the container to simulate outages;
  testcontainers may remap the host port on restart, which makes those tests
  flaky.
