package cache

import (
	"context"
	"crypto/rand"
	"time"
)

const (
	defaultLockTTL = 10 * time.Second
	nilSentinel    = "\x00nil\x00"
	lockSuffix     = ":lock"
	// unlockTimeout bounds lock release, which runs even if ctx is cancelled.
	unlockTimeout = time.Second
	// waitTimeout is how long a caller that lost the lock waits for the winner
	// to populate the key, polling with backoff between initialBackoff and
	// maxBackoff, before computing the value itself.
	waitTimeout    = 500 * time.Millisecond
	initialBackoff = 5 * time.Millisecond
	maxBackoff     = 50 * time.Millisecond
)

type GetOrSetOptions struct {
	LockTTL  time.Duration
	CacheNil bool
}

// GetOrSet implements cache-aside (lazy-load) with stampede protection.
// On cache miss: acquires distributed lock -> calls fn -> stores result -> returns.
// Concurrent callers for same key spin-wait up to ~500ms for leader to populate cache,
// falling back to fn call if lock holder crashed or lock TTL too short.
//
// Use when: fn is expensive (DB query, API call), TTL-bound staleness acceptable,
// multiple goroutines/machines may request same key concurrently.
//
// Not for: write-through / write-behind caching, transactional cache+DB consistency,
// streaming data, hot keys that change every ms (lock contention overhead).
//
// Stampede protection: only 1 caller executes fn per key at a time.
func GetOrSet(ctx context.Context, c Cache, key string, ttl time.Duration, fn func(context.Context) (string, error), opts ...GetOrSetOptions) (string, error) {
	o := defaultGetOrSetOpts(opts...)

	val, err := c.Get(ctx, key)
	if err == nil {
		return fromStore(val), nil
	}
	if !IsCacheMiss(err) {
		return "", err
	}

	lockKey := key + lockSuffix
	// Random token identifies this holder, so release never deletes a lock
	// re-acquired by someone else after ours expired (fn outlived LockTTL).
	token := rand.Text()
	ok, err := c.SetNX(ctx, lockKey, token, o.LockTTL)
	if err != nil {
		return "", err
	}
	if !ok {
		// Another goroutine is computing the value.
		return waitForValue(ctx, c, key, fn)
	}

	// Release survives caller cancellation so a cancelled request still frees
	// the lock for spin-waiters and the next caller.
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()
		_, _ = c.DelIfValue(unlockCtx, lockKey, token)
	}()
	return loadAndStore(ctx, c, key, ttl, fn, o.CacheNil)
}

// loadAndStore runs fn and caches its result. The caller holds the key's lock.
func loadAndStore(ctx context.Context, c Cache, key string, ttl time.Duration, fn func(context.Context) (string, error), cacheNil bool) (string, error) {
	// Double-check cache: between the initial miss and lock acquisition,
	// another lock holder may have populated the key already.
	if val, err := c.Get(ctx, key); err == nil {
		return fromStore(val), nil
	}

	// Still a miss: compute the value via the user-provided fn (e.g. DB query, API call).
	result, err := fn(ctx)
	if err != nil {
		return "", err
	}

	// Store the result so future callers hit instead of calling fn. An empty
	// result with CacheNil enabled is stored as a sentinel, to avoid repeated
	// fn calls for known-empty keys (e.g. user not found).
	storeVal := result
	if result == "" && cacheNil {
		storeVal = nilSentinel
	}
	if err := c.Set(ctx, key, storeVal, ttl); err != nil {
		return "", err
	}
	return result, nil
}

// waitForValue polls for the lock holder to populate key, with capped
// exponential backoff, and falls back to computing the value itself when the
// holder crashed or its lock TTL was too short.
func waitForValue(ctx context.Context, c Cache, key string, fn func(context.Context) (string, error)) (string, error) {
	backoff := initialBackoff
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(backoff):
		}
		if val, err := c.Get(ctx, key); err == nil {
			return fromStore(val), nil
		}
		backoff = min(backoff*2, maxBackoff)
	}
	return fn(ctx)
}

func fromStore(val string) string {
	if val == nilSentinel {
		return ""
	}
	return val
}

// defaultGetOrSetOpts returns the first option, or the defaults. A non-positive
// LockTTL falls back to defaultLockTTL: a lock without an expiry would outlive a
// crashed holder.
func defaultGetOrSetOpts(opts ...GetOrSetOptions) GetOrSetOptions {
	var o GetOrSetOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.LockTTL <= 0 {
		o.LockTTL = defaultLockTTL
	}
	return o
}
