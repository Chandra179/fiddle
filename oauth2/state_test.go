package oauth2

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hasState reports whether state is currently held, without consuming it.
func hasState(s *InMemoryStorage, state string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[state]
	return ok
}

func TestNewInMemoryStorage(t *testing.T) {
	storage := NewInMemoryStorage()
	require.NotNil(t, storage)
	assert.NotNil(t, storage.data)
	assert.NotNil(t, storage.done)

	// Cleanup
	storage.Cleanup()
}

func TestInMemoryStorage_SaveState(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	expiresAt := time.Now().Add(5 * time.Minute)
	err := storage.SaveState("test-state", "test-nonce", "test-verifier", expiresAt)
	require.NoError(t, err)

	// Verify state was saved
	data, err := storage.ConsumeState("test-state")
	require.NoError(t, err)
	assert.Equal(t, "test-nonce", data.Nonce)
	assert.Equal(t, "test-verifier", data.CodeVerifier)
	assert.Equal(t, expiresAt.Unix(), data.ExpiresAt.Unix())
}

func TestInMemoryStorage_ConcurrentAccess(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			state := "state-" + string(rune(idx))
			expiresAt := time.Now().Add(5 * time.Minute)
			err := storage.SaveState(state, "nonce", "verifier", expiresAt)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Concurrent consumes
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			state := "state-" + string(rune(idx))
			_, _ = storage.ConsumeState(state)
		}(i)
	}
	wg.Wait()
}

func TestInMemoryStorage_CleanupRoutine(t *testing.T) {
	storage := NewInMemoryStorage()

	// Save expired states
	expiresAt := time.Now().Add(-1 * time.Second)
	for i := range 5 {
		state := "expired-" + string(rune(i))
		err := storage.SaveState(state, "nonce", "verifier", expiresAt)
		require.NoError(t, err)
	}

	// Save valid states
	validExpiresAt := time.Now().Add(5 * time.Minute)
	for i := range 5 {
		state := "valid-" + string(rune(i))
		err := storage.SaveState(state, "nonce", "verifier", validExpiresAt)
		require.NoError(t, err)
	}

	// Verify all states exist
	storage.mu.RLock()
	assert.Len(t, storage.data, 10)
	storage.mu.RUnlock()

	// Trigger cleanup
	storage.removeExpired()

	// Verify only valid states remain
	storage.mu.RLock()
	assert.Len(t, storage.data, 5)
	storage.mu.RUnlock()

	// Verify expired states are gone
	for i := range 5 {
		assert.False(t, hasState(storage, "expired-"+string(rune(i))))
	}

	// Verify valid states still exist
	for i := range 5 {
		assert.True(t, hasState(storage, "valid-"+string(rune(i))))
	}

	storage.Cleanup()
}

func TestInMemoryStorage_Cleanup_Idempotent(t *testing.T) {
	storage := NewInMemoryStorage()

	// Cleanup should not panic when called multiple times
	assert.NotPanics(t, func() {
		storage.Cleanup()
		storage.Cleanup()
		storage.Cleanup()
	})
}

func TestInMemoryStorage_ConsumeState(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	require.NoError(t, storage.SaveState("s", "nonce", "verifier", time.Now().Add(5*time.Minute)))

	data, err := storage.ConsumeState("s")
	require.NoError(t, err)
	assert.Equal(t, "nonce", data.Nonce)
	assert.Equal(t, "verifier", data.CodeVerifier)

	// Second consume fails: state is one-time use
	_, err = storage.ConsumeState("s")
	assert.ErrorIs(t, err, ErrStateNotFound)
}

func TestInMemoryStorage_ConsumeState_Expired(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	require.NoError(t, storage.SaveState("s", "nonce", "verifier", time.Now().Add(-time.Second)))

	_, err := storage.ConsumeState("s")
	assert.ErrorIs(t, err, ErrStateExpired)

	_, err = storage.ConsumeState("s")
	assert.ErrorIs(t, err, ErrStateNotFound)
}

func TestInMemoryStorage_ConsumeState_ConcurrentExactlyOneWins(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	require.NoError(t, storage.SaveState("s", "nonce", "verifier", time.Now().Add(5*time.Minute)))

	const n = 50
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		wins  atomic.Int32
	)
	for range n {
		wg.Go(func() {
			<-start
			if _, err := storage.ConsumeState("s"); err == nil {
				wins.Add(1)
			} else {
				assert.ErrorIs(t, err, ErrStateNotFound)
			}
		})
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), wins.Load())
}

func TestInMemoryStorage_MaxEntries(t *testing.T) {
	storage := NewInMemoryStorageWithMaxEntries(3)
	defer storage.Cleanup()

	valid := time.Now().Add(5 * time.Minute)
	for i := range 3 {
		require.NoError(t, storage.SaveState(fmt.Sprintf("s%d", i), "n", "v", valid))
	}

	err := storage.SaveState("overflow", "n", "v", valid)
	assert.ErrorIs(t, err, ErrStateStoreFull)

	// Overwriting an existing key does not grow the store
	require.NoError(t, storage.SaveState("s0", "n2", "v2", valid))

	// Consuming frees a slot
	_, err = storage.ConsumeState("s1")
	require.NoError(t, err)
	require.NoError(t, storage.SaveState("s3", "n", "v", valid))

	storage.mu.RLock()
	assert.Len(t, storage.data, 3)
	storage.mu.RUnlock()
}

func TestInMemoryStorage_MaxEntries_PurgesExpired(t *testing.T) {
	storage := NewInMemoryStorageWithMaxEntries(2)
	defer storage.Cleanup()

	require.NoError(t, storage.SaveState("expired", "n", "v", time.Now().Add(-time.Second)))
	require.NoError(t, storage.SaveState("valid", "n", "v", time.Now().Add(5*time.Minute)))

	// Full, but the expired entry is purged to make room
	require.NoError(t, storage.SaveState("new", "n", "v", time.Now().Add(5*time.Minute)))

	assert.False(t, hasState(storage, "expired"))
	assert.True(t, hasState(storage, "new"))
}

func TestNewInMemoryStorageWithMaxEntries_Default(t *testing.T) {
	for _, n := range []int{0, -1} {
		storage := NewInMemoryStorageWithMaxEntries(n)
		assert.Equal(t, DefaultMaxStateEntries, storage.maxEntries)
		storage.Cleanup()
	}
	storage := NewInMemoryStorage()
	assert.Equal(t, 10_000, storage.maxEntries)
	storage.Cleanup()
}
