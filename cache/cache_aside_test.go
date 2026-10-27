package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingCache wraps a Cache and fails the operations that have an error set.
type failingCache struct {
	Cache
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

	val, err := GetOrSet(ctx, c, "down", time.Minute, func(_ context.Context) (string, error) {
		calls.Add(1)
		return "loaded", nil
	}, GetOrSetOptions{OnError: func(error) { reported.Add(1) }})

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

	val, err := GetOrSet(ctx, failingCache{Cache: c, setErr: writeErr}, "write-fails", time.Minute,
		func(_ context.Context) (string, error) { return "computed", nil },
		GetOrSetOptions{OnError: func(err error) { reported = append(reported, err) }})

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
	val, err := GetOrSet(ctx, failingCache{Cache: c, getErr: readErr}, "read-fails", time.Minute,
		func(_ context.Context) (string, error) { return "loaded", nil },
		GetOrSetOptions{OnError: func(err error) { reported = append(reported, err) }})

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
			val, err := GetOrSet(ctx, c, "slow", time.Minute, func(_ context.Context) (string, error) {
				calls.Add(1)
				time.Sleep(700 * time.Millisecond)
				return "slow-value", nil
			}, GetOrSetOptions{LoadTimeout: 2 * time.Second})
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

	_, err := GetOrSet(ctx, c, "timeout", time.Minute, func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}, GetOrSetOptions{LoadTimeout: 50 * time.Millisecond})

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
		_, leaveErr = GetOrSet(leaveCtx, c, "shared", time.Minute, fn)
	})
	time.Sleep(20 * time.Millisecond) // first caller starts the load

	var waitVal string
	var waitErr error
	wg.Go(func() {
		waitVal, waitErr = GetOrSet(ctx, c, "shared", time.Minute, fn)
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

	_, err := GetOrSet(context.Background(), c, "jitter-ttl", time.Minute, func(_ context.Context) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)

	ttl := mr.TTL("jitter-ttl")
	assert.GreaterOrEqual(t, ttl, 54*time.Second)
	assert.LessOrEqual(t, ttl, 66*time.Second)
}

func TestJitter(t *testing.T) {
	assert.Equal(t, time.Duration(0), jitter(0), "no expiry stays no expiry")
	assert.Equal(t, -time.Second, jitter(-time.Second), "negative ttl is unchanged")

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
