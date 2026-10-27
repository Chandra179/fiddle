# Cache-aside

## Big picture

Cache-aside means the application owns the cache. On a read, it checks the cache
first and loads from the source only on a miss, then stores the result. The
cache fills only with data someone asked for.

`cacheaside.GetOrSet` implements the read path. It adds two protections:

- **Stampede control, per process.** Concurrent misses for the same key share
  one load. Other instances are not coordinated.
- **Degradation.** A Redis failure turns into a slower read, not an error.

It does not handle writes. Callers invalidate or update the key after they
change the source.

## Read path

```
 caller
   |
   |  GetOrSet(ctx, req)
   v
+------------------------------+
| 1. Get(key)                  |---- hit -----------------> return value
+------------------------------+
   |
   | miss, or Redis read error (counted as a miss)
   v
+------------------------------+
| singleflight(key)            |  first caller starts the load;
|                              |  other callers in this process wait
+------------------------------+
   |
   | leader only
   v
+------------------------------+
| 2. Get(key) again            |---- hit -----------------> return value
+------------------------------+
   |
   | miss
   v
+------------------------------+
| 3. Load(ctx)                 |  bounded by LoadTimeout (default 5s)
|    DB query / API call       |
+------------------------------+
   |
   | ok                    error ---> returned to every waiting caller,
   v                                  nothing cached
+------------------------------+
| 4. Set(key, value, TTL)      |  TTL varied by +/- 10%
+------------------------------+  write error ---> OnError, value still returned
   |
   v
 return value to every caller waiting on this key
```

## Failure behavior

| Event | Result |
|---|---|
| Redis read fails | Treated as a miss. Load runs. `OnError` is called. |
| Redis write fails | Value is still returned. `OnError` is called. Next call loads again. |
| `Load` returns an error | Every waiting caller gets it. Nothing is cached. |
| `Load` runs past `LoadTimeout` | Its `ctx` is cancelled. If `Load` honors it, every waiting caller gets `DeadlineExceeded`. |
| A caller's `ctx` ends | That caller returns `ctx.Err()`. The load keeps running for the others. |
| Two instances miss at once | Each runs `Load` once. Cross-instance dedup is not provided. |

## Options

| Field | Default | Purpose |
|---|---|---|
| `LoadTimeout` | 5s | Upper bound on one `Load`. Set above the loader's p99. |
| `CacheNil` | false | Store an empty result as a sentinel so known-missing keys are not reloaded. |
| `OnError` | nil | Receives recovered cache errors. Use it to count Redis failures. |

## Use cases that fit

Each case needs three things: a read-only loader, a few seconds or minutes of
staleness is acceptable, and the same key is requested in bursts.

- **Third-party API wrapper.** A vendor catalog, exchange-rate, or geo-IP lookup
  that is slow and rate-limited. A viral key sends one outbound call per instance,
  not one per request.
- **Tenant or remote config.** Per-tenant settings read on every request, with
  large tenants dominating traffic. A 30 to 60 second TTL is fine.
- **Auth metadata.** JWKS keys or client permissions that change rarely. Caching
  them for minutes avoids a burst against the auth server after expiry.
- **Precomputed reads.** A leaderboard, search suggestions, or dashboard summary
  that takes 200ms to 2s to compute. Recomputing it once a minute per process is
  acceptable.

## Not a fit

- Loaders with side effects, or loaders that must run once across the fleet.
- Loaders under a few milliseconds. The cache round trip costs as much as the load.
- Reads that must be current after every write (use invalidation or no cache).
- Large fleets with expensive loaders where cross-instance duplicate loads are
  measured. That needs a distributed lock, which was removed.
