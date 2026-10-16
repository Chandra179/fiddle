package oauth2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrProviderNotFound = errors.New("provider not found")
	ErrInvalidState     = errors.New("invalid state")
	ErrOAuth2State      = errors.New("invalid oauth2 state")
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

// CallbackHandler is invoked after successful OAuth2 authentication
// It receives the provider name, user info, and token set
// Returns custom data that can be used by the caller (e.g., session data)
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

// NewManager creates a new manager with providers from configuration
func NewManager(ctx context.Context, cfg *ManagerConfig) (Manager, error) {
	// Start with defaults
	defaultCfg := DefaultManagerConfig()

	if cfg == nil {
		cfg = defaultCfg
	}

	// Apply defaults for unset fields
	if cfg.StateTimeout == 0 {
		cfg.StateTimeout = defaultCfg.StateTimeout
	}

	// Validate required callback handler
	if cfg.CallbackHandler == nil {
		return nil, fmt.Errorf("%w: callback handler is required", ErrCallbackHandler)
	}

	mgr := &manager{
		providers:       make(map[string]Provider),
		stateStorage:    NewInMemoryStorageWithMaxEntries(cfg.MaxStateEntries),
		callbackHandler: cfg.CallbackHandler,
		stateTimeout:    cfg.StateTimeout,
	}

	return mgr, nil
}

// NewManagerWithGoogle creates a new manager with Google provider configured
func NewManagerWithGoogle(ctx context.Context, cfg *ManagerConfig, googleClientID, googleClientSecret, googleRedirectURL string) (Manager, error) {
	mgr, err := NewManager(ctx, cfg)
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

// GetAuthURL generates authorization URL with state, nonce, and PKCE
// This method is safe for concurrent use
func (m *manager) GetAuthURL(providerName string) (string, error) {
	authURL, _, err := m.GetAuthURLWithState(providerName)
	return authURL, err
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

	state, err := GenerateRandomString(32)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate state: %w", err)
	}

	nonce, err := GenerateRandomString(32)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	codeVerifier, err := GenerateCodeVerifier()
	if err != nil {
		return "", "", fmt.Errorf("failed to generate code verifier: %w", err)
	}
	codeChallenge := GenerateCodeChallenge(codeVerifier)

	// Store state, nonce, and code verifier
	expiresAt := time.Now().Add(m.stateTimeout)
	if err := m.stateStorage.SaveState(state, nonce, codeVerifier, expiresAt); err != nil {
		return "", "", fmt.Errorf("failed to save state: %w", err)
	}

	return provider.GetAuthURL(state, nonce, codeChallenge), state, nil
}

// HandleCallback handles OAuth2/OIDC callback and invokes the callback handler
// Returns userInfo, tokenSet, and any custom result from the callback handler
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
