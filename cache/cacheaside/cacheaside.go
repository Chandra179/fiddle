package cacheaside

import (
	"context"
	"math/rand/v2"
	"time"

	"cache"

	"golang.org/x/sync/singleflight"
)

const (
	defaultLoadTimeout = 5 * time.Second
	nilSentinel        = "\x00nil\x00"
	// jitterFraction spreads expiry by up to ±10% so keys stored together do
	// not expire together.
	jitterFraction = 0.1
)

// loads coalesces concurrent loads for the same key within this process. It is
// shared by every Cache in the process, so the same key on two Caches (for
// example two Redis databases) shares one load.
var loads singleflight.Group

// GetOrSetOptions tunes one GetOrSet call. Zero values select the defaults.
type GetOrSetOptions struct {
	// LoadTimeout bounds one run of Load. Concurrent callers for the key wait for
	// that run, so it also bounds their wait. Set it above the loader's p99
	// latency. Defaults to 5s.
	LoadTimeout time.Duration
	// CacheNil stores an empty result as a sentinel, so a known-missing key is
	// not reloaded on every call.
	CacheNil bool
	// OnError receives each cache error that GetOrSet recovered from: a failed
	// read (treated as a miss) or a failed write (the value is still returned).
	// Use it to count Redis failures. It may be nil.
	OnError func(err error)
}

// GetOrSetRequest describes one GetOrSet call.
type GetOrSetRequest struct {
	// Cache is the store to read from and write to.
	Cache cache.Cache
	// Key is the cache key. Load runs once per key within the process.
	Key string
	// TTL is the stored lifetime, varied by ±10%. Zero means no expiry.
	TTL time.Duration
	// Load computes the value on a miss.
	Load func(context.Context) (string, error)
	// Options tunes the call. Zero values select the defaults.
	Options GetOrSetOptions
}

// GetOrSet is cache-aside with per-process stampede protection. On a miss,
// concurrent callers for the same key share one run of req.Load and receive its
// result. The result is stored with req.TTL, varied by ±10%.
//
// Cache errors degrade to the loader instead of failing the call: a failed read
// counts as a miss, and a failed write still returns the computed value. Both are
// reported to OnError. Errors from Load and ctx cancellation are returned.
//
// Stampede protection covers one process only. Instances that miss at the same
// moment each run Load once. Use it when Load is expensive, brief staleness is
// fine, and many goroutines in a process may ask for the same key. It is not for
// write-through caching, cache+DB transactions, or keys that change every few
// milliseconds.
func GetOrSet(ctx context.Context, req GetOrSetRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	req.Options = withDefaults(req.Options)

	if val, ok := lookup(ctx, req.Cache, req.Key, req.Options); ok {
		return val, nil
	}

	ch := loads.DoChan(req.Key, func() (any, error) {
		return load(ctx, req)
	})
	// The caller stops waiting when its ctx ends. The load keeps running for
	// the other callers.
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return "", res.Err
		}
		return res.Val.(string), nil
	}
}

// load runs req.Load once for the key and stores the result. It runs inside the
// singleflight group, so callers that started it may already have gone.
func load(ctx context.Context, req GetOrSetRequest) (string, error) {
	// The load is not cancelled with the caller that started it, because other
	// callers may be waiting on it. LoadTimeout bounds it instead.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), req.Options.LoadTimeout)
	defer cancel()

	// Another process may have stored the key since our first read.
	if val, ok := lookup(ctx, req.Cache, req.Key, req.Options); ok {
		return val, nil
	}

	result, err := req.Load(ctx)
	if err != nil {
		return "", err
	}

	storeVal := result
	if result == "" && req.Options.CacheNil {
		storeVal = nilSentinel
	}
	if err := req.Cache.Set(ctx, req.Key, storeVal, jitter(req.TTL)); err != nil {
		// The value is still good. The next call will load it again.
		req.Options.report(err)
	}
	return result, nil
}

// lookup returns the cached value and whether it was found. A read error other
// than a miss is reported and treated as a miss. Once ctx has ended, the error
// is a result of the caller leaving, so it is not reported.
func lookup(ctx context.Context, c cache.Cache, key string, o GetOrSetOptions) (string, bool) {
	val, err := c.Get(ctx, key)
	if err == nil {
		return fromStore(val), true
	}
	if !cache.IsCacheMiss(err) && ctx.Err() == nil {
		o.report(err)
	}
	return "", false
}

func (o GetOrSetOptions) report(err error) {
	if o.OnError != nil {
		o.OnError(err)
	}
}

// jitter returns ttl changed by up to ±jitterFraction. A non-positive ttl (no
// expiry) is returned unchanged.
func jitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	spread := float64(ttl) * jitterFraction
	d := ttl + time.Duration((rand.Float64()*2-1)*spread)
	return max(d, time.Nanosecond)
}

func fromStore(val string) string {
	if val == nilSentinel {
		return ""
	}
	return val
}

// withDefaults fills in the defaults for unset options. A non-positive
// LoadTimeout falls back to defaultLoadTimeout, so a load always has a deadline.
func withDefaults(o GetOrSetOptions) GetOrSetOptions {
	if o.LoadTimeout <= 0 {
		o.LoadTimeout = defaultLoadTimeout
	}
	return o
}
