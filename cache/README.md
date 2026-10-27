# cache — caching for Go

A small key-value `Cache` interface with a cache-aside helper. Redis is the bundled implementation. Minimal surface, battle-tested patterns.

## Install

```bash
go get github.com/Chandra179/fiddle/cache@latest
```

Requires Go 1.27.2 or newer.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/Chandra179/fiddle/cache"
    "github.com/Chandra179/fiddle/cache/cacheaside"
)

func main() {
    ctx := context.Background()

    c := cache.NewRedisCacheFromConfig(cache.RedisConfig{
        Addr:        "localhost:6379",
        DialTimeout: 500 * time.Millisecond,
    })
    defer c.Close()

    // Read-through: returns the cached value, or runs Load and stores the result.
    user, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{
        Cache: c,
        Key:   "user:42",
        TTL:   time.Minute,
        Load: func(ctx context.Context) (string, error) {
            return loadUserFromDB(ctx, 42)
        },
        Options: cacheaside.GetOrSetOptions{
            LoadTimeout: 2 * time.Second,
            OnError:     func(err error) { fmt.Println("cache error:", err) },
        },
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(user)

    // Direct use of the Cache interface.
    if err := c.Set(ctx, "greeting", "hello", time.Minute); err != nil {
        panic(err)
    }
    v, err := c.Get(ctx, "greeting")
    if cache.IsCacheMiss(err) {
        fmt.Println("not cached")
    } else if err != nil {
        panic(err)
    } else {
        fmt.Println(v)
    }
}
```

## Current API

```go
type Cache interface {
    Set(ctx, key, value string, ttl time.Duration) error
    SetNX(ctx, key, value string, ttl time.Duration) (bool, error)
    Get(ctx, key string) (string, error)
    Del(ctx, key string) error
    DelIfValue(ctx, key, value string) (bool, error)
    Ping(ctx) error
    Close() error
}
```

- `Cache` is backend-neutral. `cacheaside` works with any implementation; `redis_cache.go` provides one backed by Redis.
- `NewRedisCacheFromConfig(RedisConfig)` builds a `Cache` with auth, TLS, DB, pool, and timeout settings. It does not connect; a failure shows on the first operation. `NewRedisCache(addr)` is the no-auth shortcut.
- `Get` returns `ErrCacheMiss` for a missing key. Check it with `cache.IsCacheMiss(err)`.
- `SetNX` and `DelIfValue` are low-level primitives. `cacheaside.GetOrSet` does not use them.

## Cache-aside (`cache/cacheaside`)

`cacheaside.GetOrSet` is the read path of the cache-aside pattern. A call checks the cache, runs `Load` on a miss, and stores the result. Writes and invalidation stay with the caller. See [diagrams/cache_aside.md](diagrams/cache_aside.md) for the read path, failure behavior, and use cases.

| Behavior | Details |
|---|---|
| Stampede control | Concurrent misses for one key share one `Load` per process. Not coordinated across instances. |
| TTL jitter | Stored TTL varies by ±10%, so keys do not expire together. |
| `LoadTimeout` | Bounds one `Load` (default 5s). Callers waiting on the same key share this budget. |
| Degradation | A failed read counts as a miss. A failed write still returns the value. Both are reported to `OnError`. |
| `CacheNil` | Optionally stores empty results so known-missing keys are not reloaded. |

Each `GetOrSet` call takes a `cacheaside.GetOrSetRequest`. Keys must be unique per source: concurrent loads share one singleflight group across every `Cache` in the process.

## Package layout

| Path | Contents |
|---|---|
| `client.go` | `Cache` interface, `ErrCacheMiss`, `IsCacheMiss` |
| `redis_cache.go` | Redis implementation and constructors |
| `cacheaside/` | `GetOrSet` and its request and option types |
| `example/` | Integration tests (Docker required, build tag `integration`) |
| `diagrams/` | Design notes |

## Testing

```bash
make test-unit                             # unit tests, miniredis, no Docker
make test                                  # all tests, integration needs Docker
```

## Required features (future)

Distilled from big-tech post-mortems and production patterns.

### 1. Connection resilience

| Pattern | Post-mortem |
|---|---|
| Retry w/ exponential backoff + jitter | GitHub 2018 — no retry limit -> cascading |
| Circuit breaker (fail-fast when degraded) | Stripe 2019 — no circuit breaker -> DB overload |
| Connection pool sizing & monitoring | Uber 2020 — connection exhaustion |
| Adaptive timeout (shrink when latency spikes) | Cloudflare 2023 — static timeout -> cascade |

### 2. Thundering herd protection

Cache miss -> N concurrent reqs hit upstream. "Dogpile effect".

- **Singleflight**: coalesce concurrent GETs for same key into 1 upstream call (done in `cacheaside.GetOrSet`, per process only; no cross-instance lock)
- **TTL jitter**: randomize TTL +/- 10% to prevent mass expiry (done in `cacheaside.GetOrSet`)
- **Early refresh**: refresh before TTL expiry if request rate high

| Post-mortem | Lesson |
|---|---|
| Twitter 2016 — hot key event, no coalescing | Must singleflight on miss |
| AWS ElastiCache 2021 — mass key expiry | TTL jitter required |

### 3. Sharding & routing

| Capability | Why |
|---|---|
| Consistent hash ring (w/ virtual nodes) | Minimal rebalance on scale |
| Hash tags `{user:123}` | Pin related keys to same shard |
| Slot-aware routing (cluster mode) | Avoid MOVED redirects cross-region |
| Split-brain detection | GitHub 2018 — no split-brain guard |

Sharding not in go-redis/ClusterClient — wrapper must handle ring topology and fallback.

### 4. Read replica support

- **Write**: primary only
- **Read**: replica (stale ok) or primary (strong consistency)
- **Staleness tolerance**: per-call configurable `ConsistencyLevel` (Eventual / Strong)
- **Failover**: detect replica down -> route to primary

| Post-mortem | Lesson |
|---|---|
| Discord 2022 — no replica reads -> HoL blocking | Must support replica routing |

### 5. Hot key handling

- **Detect**: per-key QPS tracking (sliding window)
- **Mitigate**: auto-replicate hot key across N shards
- **Local cache**: L1 (in-memory) for hot keys, L2 (Redis) for rest

| Post-mortem | Lesson |
|---|---|
| Twitter 2016 — celebrity key took down cache tier | Need L1 + hot key replication |

### 6. Observability (non-negotiable)

| Metric | Why |
|---|---|
| Latency p50/p95/p99 per op | Detect degradation early |
| Hit/miss ratio per key pattern | Tune TTL, pre-warm candidates |
| Error rate by type (timeout / conn / OOM) | Alert on anomalies |
| Pool stats (idle/active/wait) | Uber 2020 — pool starvation invisible |
| Memory pressure estimate | AWS 2021 — no warning before eviction storm |
| OpenTelemetry spans per operation | Trace across service boundaries |

### 7. Graceful degradation

When Redis partially unavailable:

| Degradation level | Behavior |
|---|---|
| Mild (latency > p95 threshold) | Skip non-critical cache writes |
| Moderate (replica down) | All reads to primary, degrade stale-read callers |
| Severe (primary down) | L1-only mode, fail open for non-critical paths |

### 8. Eviction & memory management

- **Monitor**: `used_memory` vs `maxmemory`, evicted_keys rate
- **Alert**: when eviction rate spikes (indicates undersized cluster)
- **Forecast**: trend memory growth, warn before OOM

### 9. Bulk operations

```go
MGet(ctx, keys ...string) (map[string]string, error)
MSet(ctx, pairs map[string]string, ttl time.Duration) error
```

Pipeline under the hood. Critical for batch workloads.

### 10. Rate limiting

Token bucket backed by Redis. Multi-window sliding counter.

```go
Allow(ctx, key string, rate int, burst int) (bool, error)
```

### 11. Serialization layer

Generic typed API:

```go
GetObj(ctx, key string, dest interface{}) error
SetObj(ctx, key string, val interface{}, ttl time.Duration) error
```

Pluggable codec (JSON/gob/protobuf). Default JSON.

### 12. Configuration struct

```go
type Config struct {
    Addresses      []string          // primary + replicas
    Sharding       *ShardConfig
    PoolSize       int
    RetryPolicy    RetryConfig
    CircuitBreaker CBConfig
    Timeout        time.Duration     // per-operation
    Codec          Codec
    Logger         Logger
}
```

## Architectural decisions (why not go-redis directly)

go-redis has `UniversalClient` (single/sentinel/cluster) but missing:

- Read-replica routing
- Hot key detection & replication
- Circuit breaker
- Distributed stampede lock (singleflight and TTL jitter are in `cacheaside`, per process)
- Degradation strategy
- Memory pressure forecasting

Wrapper provides these without exposing every Redis cluster detail.

## References

- [GitHub 2018 post-mortem](https://github.blog/engineering/infrastructure/october-21-post-incident-analysis/) — Redis split-brain, no circuit breaker
- [Twitter 2016 hot key](https://blog.twitter.com/engineering/en_us/topics/infrastructure/2017/the-infrastructure-behind-twitter-scale) — thundering herd, celebrity key
- [Stripe 2019 cache stampede](https://stripe.com/blog/incident-review-2019-01) — no TTL jitter
- [Uber 2020 connection pooling](https://www.uber.com/blog/connection-pool-in-go/) — pool exhaustion
- [Discord 2022 replica routing](https://discord.com/blog/using-redis-at-discord) — no replica reads
- [Cloudflare 2023 adaptive timeout](https://blog.cloudflare.com/adaptive-timeouts/) — static timeout cascade
- [AWS ElastiCache 2021](https://aws.amazon.com/blogs/aws/aws-elasticache-best-practices/) — eviction monitoring gap

## Design principles

1. **Fail-fast > fail-safe** — circuit breaker over silent degradation
2. **Observability first** — every path instrumented
3. **Graceful degradation** — degrade in tiers, never crash
4. **No magic** — explicit `ConsistencyLevel` param, no hidden replica routing
5. **Testable** — mini-redis for unit, testcontainers for integration
