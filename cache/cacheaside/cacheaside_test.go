package cacheaside

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chandra179/fiddle/cache"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis starts a miniredis server and returns a Cache on it.
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, cache.Cache) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err, "failed to start miniredis")

	return mr, cache.NewRedisCache(mr.Addr())
}

// failingCache wraps a Cache and fails the operations that have an error set.
type failingCache struct {
	cache.Cache
	getErr error
	setErr error
}

func (f failingCache) Get(ctx context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	return f.Cache.Get(ctx, key)
}

func (f failingCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	return f.Cache.Set(ctx, key, value, ttl)
}

func TestGetOrSet_RedisDownDegradesToLoader(t *testing.T) {
	mr, c := setupTestRedis(t)
	mr.Close()

	ctx := context.Background()
	var reported atomic.Int32
	var calls atomic.Int32

	val, err := GetOrSet(ctx, GetOrSetRequest{Cache: c, Key: "down", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		calls.Add(1)
		return "loaded", nil
	}, Options: GetOrSetOptions{OnError: func(error) { reported.Add(1) }}})
	require.NoError(t, err)
	assert.Equal(t, "loaded", val)
	assert.Equal(t, int32(1), calls.Load())
	assert.Positive(t, reported.Load(), "cache errors must reach OnError")
}

func TestGetOrSet_WriteErrorStillReturnsValue(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	writeErr := errors.New("OOM command not allowed")
	var reported []error

	val, err := GetOrSet(ctx, GetOrSetRequest{Cache: failingCache{Cache: c, setErr: writeErr}, Key: "write-fails", TTL: time.Minute, Load: func(_ context.Context) (string, error) { return "computed", nil }, Options: GetOrSetOptions{OnError: func(err error) { reported = append(reported, err) }}})
	require.NoError(t, err)
	assert.Equal(t, "computed", val)
	assert.Contains(t, reported, writeErr)
	assert.False(t, mr.Exists("write-fails"))
}

func TestGetOrSet_ReadErrorIsNotAMiss(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	readErr := errors.New("i/o timeout")
	require.NoError(t, mr.Set("read-fails", "stored"))

	var reported []error
	val, err := GetOrSet(ctx, GetOrSetRequest{Cache: failingCache{Cache: c, getErr: readErr}, Key: "read-fails", TTL: time.Minute, Load: func(_ context.Context) (string, error) { return "loaded", nil }, Options: GetOrSetOptions{OnError: func(err error) { reported = append(reported, err) }}})
	require.NoError(t, err)
	assert.Equal(t, "loaded", val, "a failed read must fall through to the loader")
	assert.Contains(t, reported, readErr)
}

func TestGetOrSet_SlowLoaderRunsOnce(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	var calls atomic.Int32
	var wg sync.WaitGroup

	// Loader takes longer than the old fixed 500ms wait. Every caller must get the
	// result of the one run, not start its own.
	for range 10 {
		wg.Go(func() {
			val, err := GetOrSet(ctx, GetOrSetRequest{Cache: c, Key: "slow", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
				calls.Add(1)
				time.Sleep(700 * time.Millisecond)
				return "slow-value", nil
			}, Options: GetOrSetOptions{LoadTimeout: 2 * time.Second}})
			assert.NoError(t, err)
			assert.Equal(t, "slow-value", val)
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load())
}

func TestGetOrSet_LoadTimeoutBoundsLoad(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	_, err := GetOrSet(ctx, GetOrSetRequest{Cache: c, Key: "timeout", TTL: time.Minute, Load: func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}, Options: GetOrSetOptions{LoadTimeout: 50 * time.Millisecond}})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, mr.Exists("timeout"))
}

func TestGetOrSet_CallerLeavingDoesNotCancelSharedLoad(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	var calls atomic.Int32
	fn := func(_ context.Context) (string, error) {
		calls.Add(1)
		time.Sleep(200 * time.Millisecond)
		return "shared", nil
	}

	leaveCtx, leave := context.WithCancel(ctx)
	var leaveErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		_, leaveErr = GetOrSet(leaveCtx, GetOrSetRequest{Cache: c, Key: "shared", TTL: time.Minute, Load: fn})
	})
	time.Sleep(20 * time.Millisecond) // first caller starts the load

	var waitVal string
	var waitErr error
	wg.Go(func() {
		waitVal, waitErr = GetOrSet(ctx, GetOrSetRequest{Cache: c, Key: "shared", TTL: time.Minute, Load: fn})
	})
	time.Sleep(20 * time.Millisecond) // second caller joins it
	leave()
	wg.Wait()

	assert.ErrorIs(t, leaveErr, context.Canceled)
	require.NoError(t, waitErr)
	assert.Equal(t, "shared", waitVal, "the remaining caller must get the result")
	assert.Equal(t, int32(1), calls.Load())
	assert.True(t, mr.Exists("shared"))
}

func TestGetOrSet_StoresJitteredTTL(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	_, err := GetOrSet(context.Background(), GetOrSetRequest{Cache: c, Key: "jitter-ttl", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		return "v", nil
	}})
	require.NoError(t, err)

	ttl := mr.TTL("jitter-ttl")
	assert.GreaterOrEqual(t, ttl, 54*time.Second)
	assert.LessOrEqual(t, ttl, 66*time.Second)
}

func TestJitter(t *testing.T) {
	assert.Equal(t, time.Duration(0), jitter(0), "no expiry stays no expiry")
	assert.Equal(t, time.Duration(0), jitter(-time.Second), "negative ttl means no expiry")

	base := time.Minute
	seen := map[time.Duration]bool{}
	for range 1000 {
		d := jitter(base)
		assert.GreaterOrEqual(t, d, base*9/10)
		assert.LessOrEqual(t, d, base*11/10)
		seen[d] = true
	}
	assert.Greater(t, len(seen), 1, "jitter must vary")
}

func TestRedisCache_GetOrSet(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("cache hit returns existing value", func(t *testing.T) {
		require.NoError(t, mr.Set("gos-hit", "cached-value"))

		called := false
		val, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: "gos-hit", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
			called = true
			return "fn-value", nil
		}})
		assert.NoError(t, err)
		assert.Equal(t, "cached-value", val)
		assert.False(t, called, "fn must not be called on hit")
	})

	t.Run("cache miss calls fn and stores result", func(t *testing.T) {
		val, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: "gos-miss", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
			return "computed-value", nil
		}})
		assert.NoError(t, err)
		assert.Equal(t, "computed-value", val)

		stored, err := mr.Get("gos-miss")
		assert.NoError(t, err)
		assert.Equal(t, "computed-value", stored)
	})

	t.Run("fn error propagated", func(t *testing.T) {
		_, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: "gos-err", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
			return "", assert.AnError
		}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)

		exists := mr.Exists("gos-err")
		assert.False(t, exists, "key must not be stored on fn error")
	})

	t.Run("cache miss with nil result and CacheNil=true", func(t *testing.T) {
		key := "gos-nil"
		val, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: key, TTL: time.Minute, Load: func(_ context.Context) (string, error) {
			return "", nil
		}, Options: GetOrSetOptions{CacheNil: true}})
		assert.NoError(t, err)
		assert.Empty(t, val)

		// Second call must not call fn (sentinel hit)
		called := false
		val, err = GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: key, TTL: time.Minute, Load: func(_ context.Context) (string, error) {
			called = true
			return "should-not-reach", nil
		}, Options: GetOrSetOptions{CacheNil: true}})
		assert.NoError(t, err)
		assert.Empty(t, val)
		assert.False(t, called)
	})

	t.Run("empty result is not stored when CacheNil=false", func(t *testing.T) {
		key := "gos-nil-off"
		calls := 0
		load := func(_ context.Context) (string, error) {
			calls++
			return "", nil
		}
		for range 2 {
			val, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: key, TTL: time.Minute, Load: load})
			assert.NoError(t, err)
			assert.Empty(t, val)
		}
		assert.Equal(t, 2, calls, "empty result must be reloaded when CacheNil=false")
		assert.False(t, mr.Exists(key))
	})

	t.Run("context cancellation propagated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: "gos-cancel", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
			return "value", nil
		}})
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
			val, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: key, TTL: time.Minute, Load: func(_ context.Context) (string, error) {
				callCount.Add(1)
				return "herd-value", nil
			}})
			assert.NoError(t, err)
			assert.Equal(t, "herd-value", val)
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), callCount.Load(), "fn must be called exactly once")
}

func TestRedisCache_GetOrSet_CustomLoadTimeout(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	val, err := GetOrSet(ctx, GetOrSetRequest{Cache: cache, Key: "gos-loadtimeout", TTL: time.Minute, Load: func(_ context.Context) (string, error) {
		return "custom-timeout", nil
	}, Options: GetOrSetOptions{LoadTimeout: 5 * time.Second}})
	assert.NoError(t, err)
	assert.Equal(t, "custom-timeout", val)
}

// hangingReadCache misses on the first Get, then blocks every later Get until
// its ctx ends, like a cache that stops answering mid-request.
type hangingReadCache struct {
	cache.Cache
	gets atomic.Int32
}

func (h *hangingReadCache) Get(ctx context.Context, _ string) (string, error) {
	if h.gets.Add(1) == 1 {
		return "", cache.ErrCacheMiss
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func TestGetOrSet_SlowCacheDoesNotStarveLoader(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	var remaining time.Duration
	var reported atomic.Int32
	val, err := GetOrSet(context.Background(), GetOrSetRequest{Cache: &hangingReadCache{Cache: c}, Key: "slow-cache", TTL: time.Minute,
		Load: func(ctx context.Context) (string, error) {
			dl, _ := ctx.Deadline()
			remaining = time.Until(dl)
			return "v", nil
		},
		Options: GetOrSetOptions{LoadTimeout: 5 * time.Second, OnError: func(error) { reported.Add(1) }}})
	require.NoError(t, err)
	assert.Equal(t, "v", val)
	// The hung read costs cacheOpTimeout, not the loader's whole budget.
	assert.Greater(t, remaining, 3*time.Second)
	assert.Equal(t, int32(1), reported.Load(), "the read timeout must reach OnError")
}

func TestGetOrSet_SameKeyOnDifferentCachesLoadsSeparately(t *testing.T) {
	mr1, c1 := setupTestRedis(t)
	defer mr1.Close()
	mr2, c2 := setupTestRedis(t)
	defer mr2.Close()

	ctx := context.Background()
	var calls atomic.Int32
	load := func(v string) func(context.Context) (string, error) {
		return func(context.Context) (string, error) {
			calls.Add(1)
			time.Sleep(100 * time.Millisecond)
			return v, nil
		}
	}

	var wg sync.WaitGroup
	var v1, v2 string
	wg.Go(func() {
		v1, _ = GetOrSet(ctx, GetOrSetRequest{Cache: c1, Key: "same", TTL: time.Minute, Load: load("one")})
	})
	wg.Go(func() {
		v2, _ = GetOrSet(ctx, GetOrSetRequest{Cache: c2, Key: "same", TTL: time.Minute, Load: load("two")})
	})
	wg.Wait()

	assert.Equal(t, "one", v1)
	assert.Equal(t, "two", v2)
	assert.Equal(t, int32(2), calls.Load())
}

func TestGetOrSet_NegativeTTLMeansNoExpiry(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()

	_, err := GetOrSet(context.Background(), GetOrSetRequest{Cache: c, Key: "neg-ttl", TTL: -time.Second, Load: func(context.Context) (string, error) {
		return "v", nil
	}})
	require.NoError(t, err)
	assert.True(t, mr.Exists("neg-ttl"))
	assert.Zero(t, mr.TTL("neg-ttl"), "negative ttl must store without expiry")
}

// valueCache is a non-pointer Cache, which flightKey cannot tell apart.
type valueCache struct{ cache.Cache }

func TestFlightKey(t *testing.T) {
	mr, c := setupTestRedis(t)
	defer mr.Close()
	mr2, c2 := setupTestRedis(t)
	defer mr2.Close()

	assert.NotEqual(t, flightKey(c, "k"), flightKey(c2, "k"), "distinct pointer caches get distinct keys")
	assert.Equal(t, flightKey(c, "k"), flightKey(c, "k"))
	assert.Equal(t, "k", flightKey(valueCache{c}, "k"), "non-pointer cache falls back to the bare key")
}
