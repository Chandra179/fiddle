package oauth2

import (
	"errors"
	"maps"
	"sync"
	"time"
)

var (
	ErrStateNotFound  = errors.New("state not found")
	ErrStateExpired   = errors.New("state expired")
	ErrStateStoreFull = errors.New("state store full")
)

// DefaultMaxStateEntries caps pending states in InMemoryStorage. States are
// created by unauthenticated requests, so the store must be bounded.
const DefaultMaxStateEntries = 10_000

// StateStorage interface for state, nonce, and PKCE verifier management
type StateStorage interface {
	SaveState(state string, nonce string, codeVerifier string, expiresAt time.Time) error
	// ConsumeState atomically retrieves and deletes state data, so a state can
	// be redeemed at most once even under concurrent callbacks.
	ConsumeState(state string) (*StateData, error)
	Cleanup()
}

// StateData holds all security parameters for OAuth2 flow
type StateData struct {
	Nonce        string
	CodeVerifier string
	ExpiresAt    time.Time
}

// InMemoryStorage implements StateStorage interface using in-memory map
// Note: This implementation is suitable for single-instance deployments.
// For multi-instance/production use, implement StateStorage with Redis or similar.
type InMemoryStorage struct {
	mu         sync.RWMutex
	data       map[string]*StateData
	maxEntries int
	done       chan struct{}
	once       sync.Once
}

// NewInMemoryStorage creates a new in-memory state storage with background cleanup,
// capped at DefaultMaxStateEntries pending states.
func NewInMemoryStorage() *InMemoryStorage {
	return NewInMemoryStorageWithMaxEntries(DefaultMaxStateEntries)
}

// NewInMemoryStorageWithMaxEntries creates a new in-memory state storage holding at
// most maxEntries pending states. A non-positive maxEntries uses DefaultMaxStateEntries.
func NewInMemoryStorageWithMaxEntries(maxEntries int) *InMemoryStorage {
	if maxEntries <= 0 {
		maxEntries = DefaultMaxStateEntries
	}
	s := &InMemoryStorage{
		data:       make(map[string]*StateData),
		maxEntries: maxEntries,
		done:       make(chan struct{}),
	}
	go s.cleanupRoutine()
	return s
}

// SaveState stores state data with expiration time
// Returns ErrStateStoreFull if the store is at capacity after purging expired states
// This method is safe for concurrent use
func (s *InMemoryStorage) SaveState(state string, nonce string, codeVerifier string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[state]; !exists && len(s.data) >= s.maxEntries {
		s.removeExpiredLocked()
		if len(s.data) >= s.maxEntries {
			return ErrStateStoreFull
		}
	}

	s.data[state] = &StateData{
		Nonce:        nonce,
		CodeVerifier: codeVerifier,
		ExpiresAt:    expiresAt,
	}
	return nil
}

// ConsumeState retrieves and deletes state data in one step
// Returns ErrStateNotFound if state doesn't exist (or was already consumed)
// Returns ErrStateExpired if state exists but has expired
// This method is safe for concurrent use
func (s *InMemoryStorage) ConsumeState(state string) (*StateData, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, exists := s.data[state]
	if !exists {
		return nil, ErrStateNotFound
	}
	delete(s.data, state)

	if time.Now().After(data.ExpiresAt) {
		return nil, ErrStateExpired
	}

	return data, nil
}

// Cleanup stops the background cleanup routine
// Safe to call multiple times
func (s *InMemoryStorage) Cleanup() {
	s.once.Do(func() {
		close(s.done)
	})
}

// cleanupRoutine runs periodically to remove expired states
func (s *InMemoryStorage) cleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.removeExpired()
		case <-s.done:
			return
		}
	}
}

// removeExpired deletes all expired states from storage
func (s *InMemoryStorage) removeExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeExpiredLocked()
}

// removeExpiredLocked deletes all expired states; the caller must hold s.mu
func (s *InMemoryStorage) removeExpiredLocked() {
	now := time.Now()
	maps.DeleteFunc(s.data, func(_ string, data *StateData) bool {
		return now.After(data.ExpiresAt)
	})
}
