# cache — caching for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/Chandra179/fiddle/cache.svg)](https://pkg.go.dev/github.com/Chandra179/fiddle/cache)

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
    Set(ctx context.Context, key string, value string, ttl time.Duration) error
    SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
    Get(ctx context.Context, key string) (string, error)
    Del(ctx context.Context, key string) error
    DelIfValue(ctx context.Context, key, value string) (bool, error)
    Ping(ctx context.Context) error
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

## License

MIT. See [LICENSE](../LICENSE).
