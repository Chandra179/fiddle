package cache

import (
	"context"
	"crypto/rand"
	"time"
)

const (
	defaultLockTTL = 10 * time.Second
	retryTimeout   = 50 * time.Millisecond
	nilSentinel    = "\x00nil\x00"
	lockSuffix     = ":lock"
	// unlockTimeout bounds lock release, which runs even if ctx is cancelled.
	unlockTimeout = time.Second
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
	lockTTL := o.LockTTL
	if lockTTL <= 0 {
		lockTTL = defaultLockTTL
	}

	// Random token identifies this holder, so release never deletes a lock
	// re-acquired by someone else after ours expired (fn outlived LockTTL).
	token := rand.Text()
	ok, lockErr := c.SetNX(ctx, lockKey, token, lockTTL)
	if lockErr != nil {
		return "", lockErr
	}

	// Lock acquired — this goroutine is responsible for computing + caching.
	// Guard: release lock on return so spin-waiters (or next caller) can proceed.
	// Release survives caller cancellation so a cancelled request still frees it.
	if ok {
		defer func() {
			unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
			defer cancel()
			_, _ = c.DelIfValue(unlockCtx, lockKey, token)
		}()

		// Double-check cache: between initial miss and lock acquisition,
		// another lock holder may have populated the key already.
		if val, err := c.Get(ctx, key); err == nil {
			return fromStore(val), nil
		}

		// Still miss → compute value via user-provided fn (e.g. DB query, API call).
		result, fnErr := fn(ctx)
		if fnErr != nil {
			return "", fnErr
		}

		// Store result in cache so future callers hit instead of calling fn.
		// If result is empty and CacheNil enabled, store sentinel to avoid
		// repeated fn calls for known-empty keys (e.g. user not found).
		storeVal := result
		if result == "" && o.CacheNil {
			storeVal = nilSentinel
		}
		if storeErr := c.Set(ctx, key, storeVal, ttl); storeErr != nil {
			return "", storeErr
		}
		return result, nil
	}

	// Lock not acquired — another goroutine computing.
	// Spin-wait with capped exponential backoff for key to appear.
	backoff := 5 * time.Millisecond
	deadline := time.Now().Add(retryTimeout * 10)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(backoff):
		}
		if val, err := c.Get(ctx, key); err == nil {
			return fromStore(val), nil
		}
		backoff *= 2
		if backoff > 50*time.Millisecond {
			backoff = 50 * time.Millisecond
		}
	}

	// Rare: lock holder crashed or lock TTL too short.
	return fn(ctx)
}

func fromStore(val string) string {
	if val == nilSentinel {
		return ""
	}
	return val
}

func defaultGetOrSetOpts(opts ...GetOrSetOptions) GetOrSetOptions {
	if len(opts) > 0 {
		return opts[0]
	}
	return GetOrSetOptions{LockTTL: defaultLockTTL}
}
