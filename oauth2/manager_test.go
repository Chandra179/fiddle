package oauth2

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

// MockProvider is a test double for Provider interface
type MockProvider struct {
	name           string
	authURL        string
	handleCallback func(ctx context.Context, code, state, nonce, codeVerifier string) (*UserInfo, *TokenSet, error)
	refreshToken   func(ctx context.Context, refreshToken string) (*TokenSet, error)
}

func (m *MockProvider) GetName() string {
	return m.name
}

func (m *MockProvider) GetAuthURL(state, nonce, codeChallenge string) string {
	return m.authURL + "?state=" + state
}

func (m *MockProvider) HandleCallback(ctx context.Context, code, state, nonce, codeVerifier string) (*UserInfo, *TokenSet, error) {
	if m.handleCallback != nil {
		return m.handleCallback(ctx, code, state, nonce, codeVerifier)
	}
	return &UserInfo{ID: "test-user", Email: "test@example.com"}, &TokenSet{AccessToken: "token"}, nil
}

func (m *MockProvider) RefreshToken(ctx context.Context, refreshToken string) (*TokenSet, error) {
	if m.refreshToken != nil {
		return m.refreshToken(ctx, refreshToken)
	}
	return &TokenSet{AccessToken: "new-token"}, nil
}

// mockCallbackHandler creates a callback handler for testing
func mockCallbackHandler(info *CallbackInfo, err error) CallbackHandler {
	return func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return info, err
	}
}

func TestNewManager_MissingCallbackHandler(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: nil,
	}

	_, err := NewManager(ctx, config)
	assert.ErrorIs(t, err, ErrCallbackHandler)
	assert.Contains(t, err.Error(), "callback handler is required")
}

func TestNewManager_Success(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
		StateTimeout:    5 * time.Minute,
	}

	mgr, err := NewManager(ctx, config)
	require.NoError(t, err)
	assert.NotNil(t, mgr)
}

func TestNewManager_DefaultConfig(t *testing.T) {
	ctx := context.Background()

	mgr, err := NewManager(ctx, &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	})
	require.NoError(t, err)
	assert.NotNil(t, mgr)
	// Default state timeout is validated indirectly via state expiration behavior
}

func TestManager_RegisterProvider(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{name: "test-provider", authURL: "https://example.com/auth"}
	manager.RegisterProvider(mockProvider)

	provider, err := manager.GetProvider("test-provider")
	assert.NoError(t, err)
	assert.Equal(t, mockProvider, provider)
}

func TestManager_RegisterProvider_Concurrent(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	mgr, err := NewManager(ctx, config)
	require.NoError(t, err)

	// Register providers concurrently
	done := make(chan bool, 3)
	for i := range 3 {
		go func(idx int) {
			provider := &MockProvider{name: "provider-" + string(rune('a'+idx))}
			mgr.RegisterProvider(provider)
			done <- true
		}(i)
	}

	for range 3 {
		<-done
	}

	// Verify all providers registered via public API
	for i := range 3 {
		name := "provider-" + string(rune('a'+i))
		_, err := mgr.GetProvider(name)
		assert.NoError(t, err, "provider %s should be registered", name)
	}
}

func TestManager_GetProvider_NotFound(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	_, err = manager.GetProvider("nonexistent")
	assert.ErrorIs(t, err, ErrProviderNotFound)
}

func TestManager_GetAuthURL_Success(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
		StateTimeout:    5 * time.Minute,
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{name: "test-provider", authURL: "https://example.com/auth"}
	manager.RegisterProvider(mockProvider)

	authURL, err := manager.GetAuthURL("test-provider")
	require.NoError(t, err)
	assert.Contains(t, authURL, "https://example.com/auth")
	assert.Contains(t, authURL, "state=")
}

func TestManager_GetAuthURL_ProviderNotFound(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	_, err = manager.GetAuthURL("nonexistent")
	assert.ErrorIs(t, err, ErrProviderNotFound)
}

func TestManager_GetAuthURL_GeneratesUniqueStates(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{name: "test-provider", authURL: "https://example.com/auth"}
	manager.RegisterProvider(mockProvider)

	// Generate multiple auth URLs
	states := make(map[string]bool)
	for range 10 {
		authURL, err := manager.GetAuthURL("test-provider")
		require.NoError(t, err)

		// Extract state from URL
		state := extractStateFromURL(authURL)
		require.NotEmpty(t, state)

		// Ensure state is unique
		assert.False(t, states[state], "State should be unique")
		states[state] = true
	}
}

func TestManager_HandleCallback_Success(t *testing.T) {
	ctx := context.Background()
	callbackInfo := &CallbackInfo{
		SessionCookieName: "session",
		UserID:            "user-123",
		SessionID:         "session-abc",
		CookieMaxAge:      3600,
	}

	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(callbackInfo, nil),
		StateTimeout:    5 * time.Minute,
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{name: "test-provider"}
	manager.RegisterProvider(mockProvider)

	// First generate an auth URL to create state
	authURL, err := manager.GetAuthURL("test-provider")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	// Now handle callback
	info, err := manager.HandleCallback(ctx, "test-provider", "auth-code", state)
	require.NoError(t, err)
	assert.Equal(t, callbackInfo, info)

	// Verify state was deleted (one-time use) by attempting another callback with same state
	_, err = manager.HandleCallback(ctx, "test-provider", "auth-code", state)
	assert.ErrorIs(t, err, ErrInvalidState)
}

func TestManager_HandleCallback_InvalidState(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{name: "test-provider"}
	manager.RegisterProvider(mockProvider)

	_, err = manager.HandleCallback(ctx, "test-provider", "auth-code", "invalid-state")
	assert.ErrorIs(t, err, ErrInvalidState)
}

func TestManager_HandleCallback_ExpiredState(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
		StateTimeout:    1 * time.Millisecond, // Very short timeout
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{name: "test-provider"}
	manager.RegisterProvider(mockProvider)

	// Generate auth URL
	authURL, err := manager.GetAuthURL("test-provider")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	// Wait for state to expire
	time.Sleep(10 * time.Millisecond)

	// Try to use expired state
	_, err = manager.HandleCallback(ctx, "test-provider", "auth-code", state)
	assert.ErrorIs(t, err, ErrInvalidState)
}

func TestManager_HandleCallback_ProviderNotFound(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	_, err = manager.HandleCallback(ctx, "nonexistent", "code", "state")
	assert.ErrorIs(t, err, ErrProviderNotFound)
}

func TestManager_HandleCallback_CallbackHandlerError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("callback failed")
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(nil, expectedErr),
		StateTimeout:    5 * time.Minute, // Long timeout to prevent expiration
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{name: "test-provider"}
	manager.RegisterProvider(mockProvider)

	authURL, err := manager.GetAuthURL("test-provider")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	_, err = manager.HandleCallback(ctx, "test-provider", "auth-code", state)
	assert.ErrorContains(t, err, "callback handler failed")
}

func TestManager_HandleCallback_ProviderError(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
		StateTimeout:    5 * time.Minute, // Long timeout to prevent expiration
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	mockProvider := &MockProvider{
		name: "test-provider",
		handleCallback: func(ctx context.Context, code, state, nonce, codeVerifier string) (*UserInfo, *TokenSet, error) {
			return nil, nil, errors.New("provider error")
		},
	}
	manager.RegisterProvider(mockProvider)

	authURL, err := manager.GetAuthURL("test-provider")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	_, err = manager.HandleCallback(ctx, "test-provider", "auth-code", state)
	assert.ErrorIs(t, err, ErrOAuth2Callback)
}

func TestManager_Cleanup(t *testing.T) {
	ctx := context.Background()
	config := &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	}

	manager, err := NewManager(ctx, config)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		manager.Cleanup()
	})

	// Cleanup should be idempotent
	assert.NotPanics(t, func() {
		manager.Cleanup()
	})
}

// Helper function to extract state from URL
func extractStateFromURL(url string) string {
	// Simple extraction - in real code, use url.Parse
	for i := len(url) - 1; i >= 0; i-- {
		if url[i] == '=' {
			return url[i+1:]
		}
	}
	return ""
}

func TestManager_GetAuthURLWithState(t *testing.T) {
	manager, err := NewManager(context.Background(), &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{name: "test-provider", authURL: "https://example.com/auth"})

	authURL, state, err := manager.GetAuthURLWithState("test-provider")
	require.NoError(t, err)
	assert.Len(t, state, 64)
	assert.Equal(t, state, extractStateFromURL(authURL))

	_, _, err = manager.GetAuthURLWithState("missing")
	assert.ErrorIs(t, err, ErrProviderNotFound)
}

func TestManager_GetAuthURL_StateStoreFull(t *testing.T) {
	manager, err := NewManager(context.Background(), &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
		MaxStateEntries: 2,
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{name: "test-provider", authURL: "https://example.com/auth"})

	for range 2 {
		_, err := manager.GetAuthURL("test-provider")
		require.NoError(t, err)
	}
	_, err = manager.GetAuthURL("test-provider")
	assert.ErrorIs(t, err, ErrStateStoreFull)
}

func TestManager_HandleCallback_ConcurrentReplayExactlyOneWins(t *testing.T) {
	ctx := context.Background()
	manager, err := NewManager(ctx, &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{UserID: "u"}, nil),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{name: "test-provider", authURL: "https://example.com/auth"})

	_, state, err := manager.GetAuthURLWithState("test-provider")
	require.NoError(t, err)

	const n = 50
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		wins  atomic.Int32
	)
	for range n {
		wg.Go(func() {
			<-start
			if _, err := manager.HandleCallback(ctx, "test-provider", "code", state); err == nil {
				wins.Add(1)
			} else {
				assert.ErrorIs(t, err, ErrInvalidState)
				assert.ErrorIs(t, err, ErrStateNotFound)
			}
		})
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), wins.Load())
}

func TestManager_HandleCallback_ProviderErrorIsUnwrappable(t *testing.T) {
	ctx := context.Background()
	providerErr := errors.New("provider error")
	manager, err := NewManager(ctx, &ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{
		name: "test-provider",
		handleCallback: func(ctx context.Context, code, state, nonce, codeVerifier string) (*UserInfo, *TokenSet, error) {
			return nil, nil, providerErr
		},
	})

	_, state, err := manager.GetAuthURLWithState("test-provider")
	require.NoError(t, err)

	_, err = manager.HandleCallback(ctx, "test-provider", "code", state)
	assert.ErrorIs(t, err, ErrOAuth2Callback)
	assert.ErrorIs(t, err, providerErr)

	// A failed callback still consumes the state
	_, err = manager.HandleCallback(ctx, "test-provider", "code", state)
	assert.ErrorIs(t, err, ErrInvalidState)
}
