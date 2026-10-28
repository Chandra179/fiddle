# cache — caching for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/Chandra179/fiddle/cache.svg)](https://pkg.go.dev/github.com/Chandra179/fiddle/cache)

A small key-value `Cache` interface with a cache-aside helper. Redis is the bundled implementation. Minimal surface.

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
    "log"
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

    // Returns the cached value, or runs Load and stores the result.
    user, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{
        Cache: c,
        Key:   "user:42",
        TTL:   time.Minute,
        Load: func(ctx context.Context) (string, error) {
            return loadUserFromDB(ctx, 42)
        },
        Options: cacheaside.GetOrSetOptions{
            LoadTimeout: 2 * time.Second,
            OnError:     func(err error) { log.Println("cache error:", err) },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(user)
}
```

## API

The `Cache` interface (see [pkg.go.dev](https://pkg.go.dev/github.com/Chandra179/fiddle/cache) for signatures):

- `Set`, `Get`, `Del`: basic key-value access. `Get` returns `ErrCacheMiss` for a missing key; test it with `cache.IsCacheMiss(err)`.
- `SetNX`, `DelIfValue`: lock primitives. `SetNX` requires a positive TTL and returns `ErrInvalidTTL` otherwise. `cacheaside.GetOrSet` does not use them.
- `Ping`, `Close`.

`NewRedisCacheFromConfig(RedisConfig)` builds a Redis-backed `Cache` with auth, TLS, DB, pool and timeout settings. It does not connect; a failure shows on the first operation. `NewRedisCache(addr)` is the no-auth shortcut.

## Cache-aside (`cache/cacheaside`)

`cacheaside.GetOrSet` is the read path of the cache-aside pattern: check the cache, run `Load` on a miss, store the result. Concurrent misses for a key share one `Load` per process, stored TTLs vary by ±10%, and cache failures degrade to the loader instead of failing the call. Writes and invalidation stay with the caller. See [diagrams/cache_aside.md](diagrams/cache_aside.md) for the read path, failure behavior, options and use cases.

Caveats:

- `Load` must return failures as errors and must not panic. A panic in `Load` cannot be recovered by the caller and ends the process.
- Stampede control covers one process and one `Cache` instance; instances that miss together each run `Load`.
- A `Load` result equal to the internal nil sentinel (`"\x00nil\x00"`) reads back as an empty string.
- A `Del` that lands while a `Load` is in flight can be overwritten by that load's stale result, which then stays cached for a full TTL.
- A zero or negative `TTL` stores the value without expiry.

## Testing

```bash
make test-unit                             # unit tests, miniredis, no Docker
make test                                  # all tests, integration needs Docker
```

## License

MIT. See [LICENSE](../LICENSE).
