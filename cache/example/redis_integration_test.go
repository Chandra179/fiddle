//go:build integration

package example

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cache"
	"cache/cacheaside"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

// sharedCache and sharedAddr are populated once by TestMain and reused by every test.
var (
	sharedCache cache.Cache
	sharedAddr  string
)

// TestMain starts one Redis container for the whole suite, runs the tests,
// then terminates the container.
func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		log.Fatalf("start redis container: %v", err)
	}

	addr, err := ctr.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("get connection string: %v", err)
	}

	// ConnectionString returns "redis://host:port" — strip the scheme.
	sharedAddr = addr[len("redis://"):]

	sharedCache = cache.NewRedisCache(sharedAddr)

	code := m.Run()

	_ = sharedCache.Close()
	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("warn: terminate container: %v", err)
	}

	os.Exit(code)
}

// newCache returns the shared cache and registers a per-test key cleanup so
// tests do not interfere with each other.
func newCache(t *testing.T, keys ...string) cache.Cache {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, k := range keys {
			_ = sharedCache.Del(ctx, k)
		}
	})
	return sharedCache
}

// ---- Tests -----------------------------------------------------------------

func TestPing(t *testing.T) {
	c := newCache(t)
	err := c.Ping(context.Background())
	assert.NoError(t, err)
}

func TestSet_And_Get(t *testing.T) {
	c := newCache(t, "set-get-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "set-get-key", "hello", time.Minute))

	val, err := c.Get(ctx, "set-get-key")
	require.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestSet_Overwrites(t *testing.T) {
	c := newCache(t, "overwrite-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "overwrite-key", "first", time.Minute))
	require.NoError(t, c.Set(ctx, "overwrite-key", "second", time.Minute))

	val, err := c.Get(ctx, "overwrite-key")
	require.NoError(t, err)
	assert.Equal(t, "second", val)
}

func TestSet_TTL_Expires(t *testing.T) {
	c := newCache(t, "ttl-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "ttl-key", "value", 100*time.Millisecond))

	time.Sleep(300 * time.Millisecond)

	_, err := c.Get(ctx, "ttl-key")
	assert.Error(t, err, "key should have expired")
}

func TestGet_Miss(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	_, err := c.Get(ctx, "non-existent-key")
	assert.Error(t, err)
}

func TestDel_ExistingKey(t *testing.T) {
	c := newCache(t, "del-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "del-key", "value", time.Minute))
	require.NoError(t, c.Del(ctx, "del-key"))

	_, err := c.Get(ctx, "del-key")
	assert.Error(t, err)
}

func TestDel_NonExistentKey(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	// Del on a missing key must not error
	err := c.Del(ctx, "missing-del-key")
	assert.NoError(t, err)
}

func TestSetNX_NewKey(t *testing.T) {
	c := newCache(t, "setnx-key")
	ctx := context.Background()

	ok, err := c.SetNX(ctx, "setnx-key", "value", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)

	val, err := c.Get(ctx, "setnx-key")
	require.NoError(t, err)
	assert.Equal(t, "value", val)
}

func TestSetNX_ExistingKey(t *testing.T) {
	c := newCache(t, "setnx-exists-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "setnx-exists-key", "original", time.Minute))

	ok, err := c.SetNX(ctx, "setnx-exists-key", "new", time.Minute)
	require.NoError(t, err)
	assert.False(t, ok)

	val, err := c.Get(ctx, "setnx-exists-key")
	require.NoError(t, err)
	assert.Equal(t, "original", val)
}

func TestDistributedLock(t *testing.T) {
	c := newCache(t, "lock-key")
	ctx := context.Background()

	// Acquire lock
	ok, err := c.SetNX(ctx, "lock-key", "owner-1", 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok, "first acquire must succeed")

	// Competing acquire must fail
	ok, err = c.SetNX(ctx, "lock-key", "owner-2", 5*time.Second)
	require.NoError(t, err)
	assert.False(t, ok, "second acquire must fail while lock is held")

	// Release and re-acquire
	require.NoError(t, c.Del(ctx, "lock-key"))

	ok, err = c.SetNX(ctx, "lock-key", "owner-2", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, ok, "acquire after release must succeed")
}

func TestDelIfValue(t *testing.T) {
	c := newCache(t, "div-lock")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "div-lock", "owner-1", time.Minute))

	deleted, err := c.DelIfValue(ctx, "div-lock", "owner-2")
	require.NoError(t, err)
	assert.False(t, deleted, "non-owner must not release the lock")

	deleted, err = c.DelIfValue(ctx, "div-lock", "owner-1")
	require.NoError(t, err)
	assert.True(t, deleted, "owner must release the lock")

	_, err = c.Get(ctx, "div-lock")
	assert.True(t, cache.IsCacheMiss(err))
}

func TestCacheRefresh(t *testing.T) {
	c := newCache(t, "refresh-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "refresh-key", "v1", time.Minute))

	val, err := c.Get(ctx, "refresh-key")
	require.NoError(t, err)
	assert.Equal(t, "v1", val)

	require.NoError(t, c.Set(ctx, "refresh-key", "v2", time.Minute))

	val, err = c.Get(ctx, "refresh-key")
	require.NoError(t, err)
	assert.Equal(t, "v2", val)

	require.NoError(t, c.Del(ctx, "refresh-key"))

	_, err = c.Get(ctx, "refresh-key")
	assert.Error(t, err)
}

func TestGetOrSet_Hit(t *testing.T) {
	c := newCache(t, "gos-hit")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "gos-hit", "cached-value", time.Minute))

	called := false
	val, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "gos-hit", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		called = true
		return "fn-value", nil
	}})
	require.NoError(t, err)
	assert.Equal(t, "cached-value", val)
	assert.False(t, called)
}

func TestGetOrSet_Miss(t *testing.T) {
	c := newCache(t, "gos-miss")
	ctx := context.Background()

	val, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "gos-miss", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		return "computed", nil
	}})
	require.NoError(t, err)
	assert.Equal(t, "computed", val)

	// Verify stored
	stored, err := c.Get(ctx, "gos-miss")
	require.NoError(t, err)
	assert.Equal(t, "computed", stored)
}

func TestGetOrSet_FnError(t *testing.T) {
	c := newCache(t, "gos-err")
	ctx := context.Background()

	_, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "gos-err", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		return "", assert.AnError
	}})
	assert.Error(t, err)

	// Key must not be stored
	_, err = c.Get(ctx, "gos-err")
	assert.Error(t, err)
}

func TestGetOrSet_ContextCancelled(t *testing.T) {
	c := newCache(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "gos-ctx", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		return "value", nil
	}})
	assert.Error(t, err)
}

func TestGetOrSet_ThunderingHerd(t *testing.T) {
	c := newCache(t, "gos-herd")
	ctx := context.Background()

	var callCount atomic.Int32
	var wg sync.WaitGroup
	n := 10

	for range n {
		wg.Go(func() {
			val, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "gos-herd", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
				callCount.Add(1)
				time.Sleep(10 * time.Millisecond)
				return "herd-value", nil
			}})
			assert.NoError(t, err)
			assert.Equal(t, "herd-value", val)
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), callCount.Load(), "fn must execute exactly once")
}

// Real-world scenario: fetch user profile from simulated DB, cache result.
// Demonstrates cache-aside with stampede protection for a read-heavy profile endpoint.
func TestGetOrSet_UserProfileScenario(t *testing.T) {
	c := newCache(t, "profile:user-42", "profile:user-stampede")
	ctx := context.Background()

	// Simulated DB: user profiles keyed by ID.
	type profile struct{ id, name, email string }
	db := map[string]profile{
		"user-42": {id: "42", name: "Alice", email: "alice@example.com"},
	}

	// Simulate DB call counter.
	var dbCallCount atomic.Int32

	// Cache miss — GetOrSet calls fn (simulated DB query).
	val, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "profile:user-42", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		dbCallCount.Add(1)
		u, ok := db["user-42"]
		if !ok {
			return "", nil
		}
		return u.name + "|" + u.email, nil
	}, Options: cacheaside.GetOrSetOptions{CacheNil: true}})
	require.NoError(t, err)
	assert.Equal(t, "Alice|alice@example.com", val)
	assert.Equal(t, int32(1), dbCallCount.Load(), "DB must be queried exactly once")

	// Second call — cache hit, no DB query.
	val, err = cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "profile:user-42", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		dbCallCount.Add(1)
		return "should-not-reach", nil
	}, Options: cacheaside.GetOrSetOptions{CacheNil: true}})
	require.NoError(t, err)
	assert.Equal(t, "Alice|alice@example.com", val)
	assert.Equal(t, int32(1), dbCallCount.Load(), "DB must NOT be queried on cache hit")

	// Stampede: fresh key with concurrent callers — fn called exactly once.
	var wg sync.WaitGroup
	dbCallCount.Store(0)
	for range 10 {
		wg.Go(func() {
			val, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "profile:user-stampede", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
				dbCallCount.Add(1)
				time.Sleep(10 * time.Millisecond)
				u := db["user-42"]
				return u.name + "|" + u.email, nil
			}, Options: cacheaside.GetOrSetOptions{CacheNil: true}})
			assert.NoError(t, err)
			assert.Equal(t, "Alice|alice@example.com", val)
		})
	}
	wg.Wait()
	assert.Equal(t, int32(1), dbCallCount.Load(), "concurrent requests must not trigger extra DB calls")
}

// Redis unreachable: reads and writes fail, so GetOrSet degrades to the loader
// and reports each cache error.
func TestGetOrSet_RedisUnreachable(t *testing.T) {
	ctx := context.Background()
	down := cache.NewRedisCacheFromConfig(cache.RedisConfig{
		Addr:        "127.0.0.1:1", // nothing listens here
		DialTimeout: 100 * time.Millisecond,
	})
	t.Cleanup(func() { _ = down.Close() })

	var reported atomic.Int32
	var calls atomic.Int32
	val, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: down, Key: "unreachable", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		calls.Add(1)
		return "from-loader", nil
	}, Options: cacheaside.GetOrSetOptions{OnError: func(error) { reported.Add(1) }}})
	require.NoError(t, err)
	assert.Equal(t, "from-loader", val)
	assert.Equal(t, int32(1), calls.Load())
	assert.Positive(t, reported.Load())
}

// Real-world scenario: user not found — CacheNil prevents repeated DB lookups.
func TestGetOrSet_UserNotFoundScenario(t *testing.T) {
	c := newCache(t, "profile:user-999")
	ctx := context.Background()

	var dbCallCount atomic.Int32

	// First call: user not found in DB, fn returns empty string.
	val, err := cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "profile:user-999", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		dbCallCount.Add(1)
		return "", nil
	}, Options: cacheaside.GetOrSetOptions{CacheNil: true}})
	require.NoError(t, err)
	assert.Empty(t, val)
	assert.Equal(t, int32(1), dbCallCount.Load())

	// Second call: CacheNil hit — returns empty without calling fn.
	val, err = cacheaside.GetOrSet(ctx, cacheaside.GetOrSetRequest{Cache: c, Key: "profile:user-999", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		dbCallCount.Add(1)
		return "should-not-reach", nil
	}, Options: cacheaside.GetOrSetOptions{CacheNil: true}})
	require.NoError(t, err)
	assert.Empty(t, val)
	assert.Equal(t, int32(1), dbCallCount.Load(), "repeated not-found lookups must not hit DB")
}
