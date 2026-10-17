package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis creates a miniredis server for testing
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redisCache) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err, "failed to start miniredis")

	cache := NewRedisCache(mr.Addr()).(*redisCache)

	return mr, cache
}

func TestNewRedisCache(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	cache := NewRedisCache(mr.Addr())

	assert.NotNil(t, cache)
	assert.IsType(t, &redisCache{}, cache)

	// Verify the cache is functional
	redisCache := cache.(*redisCache)
	assert.NotNil(t, redisCache.client)
}

func TestRedisCache_Set(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("successful set", func(t *testing.T) {
		err := cache.Set(ctx, "test-key", "test-value", time.Minute)
		assert.NoError(t, err)

		// Verify the value was set in miniredis
		val, err := mr.Get("test-key")
		assert.NoError(t, err)
		assert.Equal(t, "test-value", val)
	})

	t.Run("set with zero TTL", func(t *testing.T) {
		err := cache.Set(ctx, "no-ttl-key", "no-ttl-value", 0)
		assert.NoError(t, err)

		val, err := mr.Get("no-ttl-key")
		assert.NoError(t, err)
		assert.Equal(t, "no-ttl-value", val)
	})

	t.Run("set overwrites existing key", func(t *testing.T) {
		err := cache.Set(ctx, "overwrite-key", "original", time.Minute)
		assert.NoError(t, err)

		err = cache.Set(ctx, "overwrite-key", "updated", time.Minute)
		assert.NoError(t, err)

		val, err := mr.Get("overwrite-key")
		assert.NoError(t, err)
		assert.Equal(t, "updated", val)
	})

	t.Run("set with TTL expires", func(t *testing.T) {
		err := cache.Set(ctx, "expire-key", "expire-value", 1*time.Second)
		assert.NoError(t, err)

		// Fast forward time in miniredis
		mr.FastForward(2 * time.Second)

		exists := mr.Exists("expire-key")
		assert.False(t, exists)
	})
}

func TestRedisCache_Get(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("successful get", func(t *testing.T) {
		require.NoError(t, mr.Set("existing-key", "existing-value"))

		val, err := cache.Get(ctx, "existing-key")
		assert.NoError(t, err)
		assert.Equal(t, "existing-value", val)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		val, err := cache.Get(ctx, "non-existent-key")
		assert.Error(t, err)
		assert.True(t, IsCacheMiss(err))
		assert.Empty(t, val)
	})

	t.Run("get expired key", func(t *testing.T) {
		require.NoError(t, mr.Set("expired-key", "expired-value"))
		mr.SetTTL("expired-key", 1*time.Second)
		mr.FastForward(2 * time.Second)

		val, err := cache.Get(ctx, "expired-key")
		assert.Error(t, err)
		assert.True(t, IsCacheMiss(err))
		assert.Empty(t, val)
	})
}

func TestRedisCache_Del(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("delete existing key", func(t *testing.T) {
		require.NoError(t, mr.Set("delete-key", "delete-value"))

		err := cache.Del(ctx, "delete-key")
		assert.NoError(t, err)

		exists := mr.Exists("delete-key")
		assert.False(t, exists)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		err := cache.Del(ctx, "non-existent-delete-key")
		assert.NoError(t, err) // Redis Del doesn't error on non-existent keys
	})

	t.Run("delete multiple times", func(t *testing.T) {
		require.NoError(t, mr.Set("multi-delete-key", "value"))

		err := cache.Del(ctx, "multi-delete-key")
		assert.NoError(t, err)

		// Delete again
		err = cache.Del(ctx, "multi-delete-key")
		assert.NoError(t, err)
	})
}

func TestRedisCache_SetNX(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("setnx on non-existent key", func(t *testing.T) {
		ok, err := cache.SetNX(ctx, "setnx-key", "setnx-value", time.Minute)
		assert.NoError(t, err)
		assert.True(t, ok)

		val, err := mr.Get("setnx-key")
		assert.NoError(t, err)
		assert.Equal(t, "setnx-value", val)
	})

	t.Run("setnx on existing key", func(t *testing.T) {
		require.NoError(t, mr.Set("existing-setnx-key", "original-value"))

		ok, err := cache.SetNX(ctx, "existing-setnx-key", "new-value", time.Minute)
		assert.NoError(t, err)
		assert.False(t, ok)

		// Verify original value is unchanged
		val, err := mr.Get("existing-setnx-key")
		assert.NoError(t, err)
		assert.Equal(t, "original-value", val)
	})

	t.Run("setnx with TTL", func(t *testing.T) {
		ok, err := cache.SetNX(ctx, "setnx-ttl-key", "setnx-ttl-value", 1*time.Second)
		assert.NoError(t, err)
		assert.True(t, ok)

		// Verify TTL is set
		ttl := mr.TTL("setnx-ttl-key")
		assert.True(t, ttl > 0)

		// Fast forward and verify expiration
		mr.FastForward(2 * time.Second)
		exists := mr.Exists("setnx-ttl-key")
		assert.False(t, exists)
	})

	t.Run("setnx after key expires", func(t *testing.T) {
		ok, err := cache.SetNX(ctx, "expire-then-setnx", "first-value", 1*time.Second)
		assert.NoError(t, err)
		assert.True(t, ok)

		// Fast forward to expire the key
		mr.FastForward(2 * time.Second)

		// Now SetNX should succeed
		ok, err = cache.SetNX(ctx, "expire-then-setnx", "second-value", time.Minute)
		assert.NoError(t, err)
		assert.True(t, ok)

		val, err := mr.Get("expire-then-setnx")
		assert.NoError(t, err)
		assert.Equal(t, "second-value", val)
	})
}

func TestRedisCache_ContextCancellation(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	t.Run("set with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := cache.Set(ctx, "cancel-key", "cancel-value", time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("get with cancelled context", func(t *testing.T) {
		require.NoError(t, mr.Set("cancel-get-key", "value"))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := cache.Get(ctx, "cancel-get-key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func TestRedisCache_Ping(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("successful ping", func(t *testing.T) {
		err := cache.Ping(ctx)
		assert.NoError(t, err)
	})

	t.Run("ping after close returns error", func(t *testing.T) {
		cache2 := NewRedisCache(mr.Addr()).(*redisCache)
		_ = cache2.Close()

		err := cache2.Ping(ctx)
		assert.Error(t, err)
	})
}

func TestRedisCache_Close(t *testing.T) {
	t.Run("successful close", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		cache := NewRedisCache(mr.Addr()).(*redisCache)

		err = cache.Close()
		assert.NoError(t, err)
	})

	t.Run("close multiple times", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		cache := NewRedisCache(mr.Addr()).(*redisCache)

		err = cache.Close()
		assert.NoError(t, err)

		// Close again - redis.Client errors when closing already-closed client
		err = cache.Close()
		assert.Error(t, err)
	})
}

func TestRedisCache_IntegrationScenario(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	// Scenario: Distributed lock pattern
	t.Run("distributed lock pattern", func(t *testing.T) {
		lockKey := "resource-lock"
		lockValue := "process-1"

		// Acquire lock
		acquired, err := cache.SetNX(ctx, lockKey, lockValue, 5*time.Second)
		assert.NoError(t, err)
		assert.True(t, acquired)

		// Try to acquire again (should fail)
		acquired, err = cache.SetNX(ctx, lockKey, "process-2", 5*time.Second)
		assert.NoError(t, err)
		assert.False(t, acquired)

		// Release lock
		err = cache.Del(ctx, lockKey)
		assert.NoError(t, err)

		// Now acquire should succeed
		acquired, err = cache.SetNX(ctx, lockKey, "process-2", 5*time.Second)
		assert.NoError(t, err)
		assert.True(t, acquired)
	})

	// Scenario: Cache with refresh
	t.Run("cache refresh pattern", func(t *testing.T) {
		cacheKey := "user:123"

		// Set initial value
		err := cache.Set(ctx, cacheKey, "user-data-v1", time.Minute)
		assert.NoError(t, err)

		// Get value
		val, err := cache.Get(ctx, cacheKey)
		assert.NoError(t, err)
		assert.Equal(t, "user-data-v1", val)

		// Update cache
		err = cache.Set(ctx, cacheKey, "user-data-v2", time.Minute)
		assert.NoError(t, err)

		// Get updated value
		val, err = cache.Get(ctx, cacheKey)
		assert.NoError(t, err)
		assert.Equal(t, "user-data-v2", val)

		// Delete cache
		err = cache.Del(ctx, cacheKey)
		assert.NoError(t, err)

		// Verify deletion
		_, err = cache.Get(ctx, cacheKey)
		assert.Error(t, err)
		assert.True(t, IsCacheMiss(err))
	})
}

func TestRedisCache_GetOrSet(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("cache hit returns existing value", func(t *testing.T) {
		require.NoError(t, mr.Set("gos-hit", "cached-value"))

		called := false
		val, err := GetOrSet(ctx, cache, "gos-hit", time.Minute, func(_ context.Context) (string, error) {
			called = true
			return "fn-value", nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "cached-value", val)
		assert.False(t, called, "fn must not be called on hit")
	})

	t.Run("cache miss calls fn and stores result", func(t *testing.T) {
		val, err := GetOrSet(ctx, cache, "gos-miss", time.Minute, func(_ context.Context) (string, error) {
			return "computed-value", nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "computed-value", val)

		stored, err := mr.Get("gos-miss")
		assert.NoError(t, err)
		assert.Equal(t, "computed-value", stored)
	})

	t.Run("fn error propagated", func(t *testing.T) {
		_, err := GetOrSet(ctx, cache, "gos-err", time.Minute, func(_ context.Context) (string, error) {
			return "", assert.AnError
		})
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)

		exists := mr.Exists("gos-err")
		assert.False(t, exists, "key must not be stored on fn error")
	})

	t.Run("cache miss with nil result and CacheNil=true", func(t *testing.T) {
		key := "gos-nil"
		val, err := GetOrSet(ctx, cache, key, time.Minute, func(_ context.Context) (string, error) {
			return "", nil
		}, GetOrSetOptions{CacheNil: true})
		assert.NoError(t, err)
		assert.Empty(t, val)

		// Second call must not call fn (sentinel hit)
		called := false
		val, err = GetOrSet(ctx, cache, key, time.Minute, func(_ context.Context) (string, error) {
			called = true
			return "should-not-reach", nil
		}, GetOrSetOptions{CacheNil: true})
		assert.NoError(t, err)
		assert.Empty(t, val)
		assert.False(t, called)
	})

	t.Run("context cancellation propagated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := GetOrSet(ctx, cache, "gos-cancel", time.Minute, func(_ context.Context) (string, error) {
			return "value", nil
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func TestRedisCache_GetOrSet_ThunderingHerd(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	key := "gos-herd"

	var callCount atomic.Int32
	var wg sync.WaitGroup
	n := 10

	for range n {
		wg.Go(func() {
			val, err := GetOrSet(ctx, cache, key, time.Minute, func(_ context.Context) (string, error) {
				callCount.Add(1)
				return "herd-value", nil
			})
			assert.NoError(t, err)
			assert.Equal(t, "herd-value", val)
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), callCount.Load(), "fn must be called exactly once")
}

func TestRedisCache_GetOrSet_CustomLockTTL(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	val, err := GetOrSet(ctx, cache, "gos-lockttl", time.Minute, func(_ context.Context) (string, error) {
		return "custom-lock", nil
	}, GetOrSetOptions{LockTTL: 5 * time.Second})
	assert.NoError(t, err)
	assert.Equal(t, "custom-lock", val)
}

func TestNewRedisCacheFromConfig(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	mr.RequireUserAuth("app", "secret")

	ctx := context.Background()

	t.Run("auth and DB selection", func(t *testing.T) {
		c := NewRedisCacheFromConfig(RedisConfig{
			Addr:         mr.Addr(),
			Username:     "app",
			Password:     "secret",
			DB:           2,
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
			PoolSize:     2,
		})
		t.Cleanup(func() { _ = c.Close() })

		require.NoError(t, c.Ping(ctx))
		require.NoError(t, c.Set(ctx, "cfg-key", "cfg-value", time.Minute))

		val, err := mr.DB(2).Get("cfg-key")
		require.NoError(t, err)
		assert.Equal(t, "cfg-value", val)
		assert.False(t, mr.Exists("cfg-key"), "key must not land in DB 0")
	})

	t.Run("wrong password fails", func(t *testing.T) {
		c := NewRedisCacheFromConfig(RedisConfig{Addr: mr.Addr(), Username: "app", Password: "wrong"})
		t.Cleanup(func() { _ = c.Close() })

		assert.Error(t, c.Ping(ctx))
	})
}

func TestRedisCache_DelIfValue(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("matching value deletes", func(t *testing.T) {
		require.NoError(t, mr.Set("div-match", "token-a"))

		deleted, err := cache.DelIfValue(ctx, "div-match", "token-a")
		require.NoError(t, err)
		assert.True(t, deleted)
		assert.False(t, mr.Exists("div-match"))
	})

	t.Run("different value keeps key", func(t *testing.T) {
		require.NoError(t, mr.Set("div-mismatch", "token-b"))

		deleted, err := cache.DelIfValue(ctx, "div-mismatch", "token-a")
		require.NoError(t, err)
		assert.False(t, deleted)

		val, err := mr.Get("div-mismatch")
		require.NoError(t, err)
		assert.Equal(t, "token-b", val)
	})

	t.Run("missing key", func(t *testing.T) {
		deleted, err := cache.DelIfValue(ctx, "div-missing", "token-a")
		require.NoError(t, err)
		assert.False(t, deleted)
	})
}

// Leader's fn outlives LockTTL, another owner takes the lock; the leader's
// release must leave the new owner's lock intact.
func TestRedisCache_GetOrSet_ReleaseKeepsOtherOwnersLock(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	key := "gos-steal"
	lockKey := key + lockSuffix

	val, err := GetOrSet(ctx, cache, key, time.Minute, func(ctx context.Context) (string, error) {
		token, err := mr.Get(lockKey)
		require.NoError(t, err)
		assert.NotEqual(t, lockKey, token, "lock value must be a random token")

		mr.FastForward(2 * time.Second) // leader's lock expires
		ok, err := cache.SetNX(ctx, lockKey, "other-owner", time.Minute)
		require.NoError(t, err)
		require.True(t, ok, "other owner must acquire expired lock")
		return "leader-value", nil
	}, GetOrSetOptions{LockTTL: time.Second})
	require.NoError(t, err)
	assert.Equal(t, "leader-value", val)

	owner, err := mr.Get(lockKey)
	require.NoError(t, err, "other owner's lock must survive leader's release")
	assert.Equal(t, "other-owner", owner)
}

// A request cancelled while computing must still release its lock.
func TestRedisCache_GetOrSet_CancelledRequestReleasesLock(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	key := "gos-cancel-release"

	_, err := GetOrSet(ctx, cache, key, time.Minute, func(_ context.Context) (string, error) {
		cancel()
		return "value", nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, mr.Exists(key+lockSuffix), "lock must be released despite cancelled ctx")
}
