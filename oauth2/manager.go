package oauth2

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrProviderNotFound = errors.New("provider not found")
	ErrInvalidState     = errors.New("invalid state")
	ErrOAuth2Callback   = errors.New("oauth2 callback error")
	ErrCallbackHandler  = errors.New("callback handler not set")
)

// CallbackInfo contains session information returned by the callback handler
type CallbackInfo struct {
	SessionCookieName string
	UserID            string
	SessionID         string
	CookieMaxAge      int
}

// CallbackHandler is called after a successful login with the provider name,
// user info and tokens. It returns the session details the HTTP handler turns
// into a cookie.
type CallbackHandler func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error)

// ManagerConfig contains configuration for the OAuth2 manager
type ManagerConfig struct {
	// StateTimeout is the duration for which state parameters are valid
	StateTimeout time.Duration
	// CallbackHandler is invoked after successful OAuth2 authentication
	CallbackHandler CallbackHandler
	// MaxStateEntries caps pending (unredeemed) states held in memory.
	// Zero uses DefaultMaxStateEntries.
	MaxStateEntries int
}

// DefaultManagerConfig returns a secure default configuration
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		StateTimeout:    10 * time.Minute,
		MaxStateEntries: DefaultMaxStateEntries,
	}
}

// manager implements the Manager interface.
type manager struct {
	mu              sync.RWMutex
	providers       map[string]Provider
	stateStorage    StateStorage
	callbackHandler CallbackHandler
	stateTimeout    time.Duration
}

// NewManager creates a new manager. A nil cfg or zero fields use
// DefaultManagerConfig's values; cfg itself is not modified.
func NewManager(cfg *ManagerConfig) (Manager, error) {
	cfg = cmp.Or(cfg, DefaultManagerConfig())

	// Validate required callback handler
	if cfg.CallbackHandler == nil {
		return nil, fmt.Errorf("%w: callback handler is required", ErrCallbackHandler)
	}

	return &manager{
		providers:       make(map[string]Provider),
		stateStorage:    NewInMemoryStorageWithMaxEntries(cfg.MaxStateEntries),
		callbackHandler: cfg.CallbackHandler,
		stateTimeout:    cmp.Or(cfg.StateTimeout, DefaultManagerConfig().StateTimeout),
	}, nil
}

// NewManagerWithGoogle is NewManager plus a registered "google" provider. If the
// client ID or secret is empty, no provider is registered.
func NewManagerWithGoogle(ctx context.Context, cfg *ManagerConfig, googleClientID, googleClientSecret, googleRedirectURL string) (Manager, error) {
	mgr, err := NewManager(cfg)
	if err != nil {
		return nil, err
	}

	if googleClientID != "" && googleClientSecret != "" {
		googleProvider, err := NewGoogleOIDCProvider(
			ctx,
			googleClientID,
			googleClientSecret,
			googleRedirectURL,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google provider: %w", err)
		}
		mgr.RegisterProvider(googleProvider)
	}

	return mgr, nil
}

// RegisterProvider registers a new authentication provider
// This method is safe for concurrent use
func (m *manager) RegisterProvider(provider Provider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.providers[provider.GetName()] = provider
}

// GetAuthURLWithState generates authorization URL with state, nonce, and PKCE,
// and also returns the state so the caller can bind it to the browser
// This method is safe for concurrent use
func (m *manager) GetAuthURLWithState(providerName string) (string, string, error) {
	m.mu.RLock()
	provider, exists := m.providers[providerName]
	m.mu.RUnlock()

	if !exists {
		return "", "", ErrProviderNotFound
	}

	state := GenerateRandomString(32)
	nonce := GenerateRandomString(32)

	codeVerifier := GenerateCodeVerifier()
	codeChallenge := GenerateCodeChallenge(codeVerifier)

	// Store state, nonce, and code verifier
	expiresAt := time.Now().Add(m.stateTimeout)
	if err := m.stateStorage.SaveState(state, nonce, codeVerifier, expiresAt); err != nil {
		return "", "", fmt.Errorf("failed to save state: %w", err)
	}

	return provider.GetAuthURL(state, nonce, codeChallenge), state, nil
}

// HandleCallback consumes the state, completes the login with the provider and
// returns what the CallbackHandler returned.
// This method is safe for concurrent use
func (m *manager) HandleCallback(ctx context.Context, providerName, code, state string) (*CallbackInfo, error) {
	m.mu.RLock()
	provider, exists := m.providers[providerName]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrProviderNotFound
	}

	// Consume state atomically (one-time use): concurrent callbacks with the
	// same state cannot both succeed, and a failed callback cannot be replayed
	stateData, err := m.stateStorage.ConsumeState(state)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidState, err)
	}

	userInfo, tokenSet, err := provider.HandleCallback(ctx, code, state, stateData.Nonce, stateData.CodeVerifier)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOAuth2Callback, err)
	}

	// Invoke callback handler (already validated in constructor)
	info, err := m.callbackHandler(ctx, providerName, userInfo, tokenSet)
	if err != nil {
		return nil, fmt.Errorf("callback handler failed: %w", err)
	}

	return info, nil
}

// GetProvider returns a provider by name
// This method is safe for concurrent use
func (m *manager) GetProvider(providerName string) (Provider, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	provider, exists := m.providers[providerName]
	if !exists {
		return nil, ErrProviderNotFound
	}
	return provider, nil
}

// Cleanup cleans up storage resources
func (m *manager) Cleanup() {
	m.stateStorage.Cleanup()
}
