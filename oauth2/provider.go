package oauth2

import (
	"context"
	"time"
)

// Manager defines the interface for the OAuth2 manager.
// Implementations must be safe for concurrent use.
type Manager interface {
	// RegisterProvider registers a new authentication provider.
	RegisterProvider(provider Provider)

	// GetAuthURL generates an authorization URL with state, nonce, and PKCE for the named provider.
	GetAuthURL(providerName string) (string, error)

	// GetAuthURLWithState is GetAuthURL that also returns the generated state,
	// so callers can bind it to the browser (e.g. a cookie) for login CSRF protection.
	GetAuthURLWithState(providerName string) (authURL string, state string, err error)

	// HandleCallback handles the OAuth2/OIDC callback and invokes the configured callback handler.
	HandleCallback(ctx context.Context, providerName, code, state string) (*CallbackInfo, error)

	// GetProvider returns a registered provider by name.
	GetProvider(providerName string) (Provider, error)

	// Cleanup releases storage resources (e.g. stops background goroutines).
	Cleanup()
}

// Provider defines the interface for OAuth2/OIDC authentication providers.
// Implementations must be safe for concurrent use.
type Provider interface {
	// GetName returns the unique identifier for this provider (e.g., "google", "github")
	GetName() string

	// GetAuthURL generates the authorization URL for the OAuth2 flow.
	// Parameters:
	//   - state: CSRF protection token
	//   - nonce: OIDC nonce for replay attack prevention
	//   - codeChallenge: PKCE code challenge (S256 method)
	// Returns the complete authorization URL
	GetAuthURL(state string, nonce string, codeChallenge string) string

	// HandleCallback processes the OAuth2 callback after user authorization.
	// Parameters:
	//   - ctx: context for cancellation and timeouts
	//   - code: authorization code from OAuth2 provider
	//   - state: state parameter for CSRF validation
	//   - nonce: nonce for ID token validation
	//   - codeVerifier: PKCE code verifier
	// Returns user information and token set on success
	HandleCallback(ctx context.Context, code string, state string, nonce string, codeVerifier string) (*UserInfo, *TokenSet, error)

	// RefreshToken exchanges a refresh token for a new access token.
	// Parameters:
	//   - ctx: context for cancellation and timeouts
	//   - refreshToken: the refresh token obtained from initial authentication
	// Returns new token set on success
	RefreshToken(ctx context.Context, refreshToken string) (*TokenSet, error)
}

// UserInfo represents unified user information across all OAuth2 providers.
// All fields are JSON-serializable for API responses.
type UserInfo struct {
	// ID is the unique identifier from the provider (e.g., Google sub claim)
	ID string `json:"id"`

	// Email is the user's email address (if available)
	Email string `json:"email"`

	// EmailVerified reports the provider's email_verified claim. Do not trust
	// Email for account linking unless this is true.
	EmailVerified bool `json:"email_verified"`

	// Name is the user's display name (if available)
	Name string `json:"name"`

	// Provider identifies which OAuth2 provider authenticated this user
	Provider string `json:"provider"`

	// CreatedAt is the timestamp when this UserInfo was created
	CreatedAt time.Time `json:"created_at"`
}

// TokenSet represents the complete OAuth2 token response from providers.
// This includes access tokens, refresh tokens, and OIDC ID tokens.
type TokenSet struct {
	// AccessToken is the OAuth2 access token for API calls
	AccessToken string `json:"access_token"`

	// TokenType is typically "Bearer"
	TokenType string `json:"token_type"`

	// RefreshToken is used to obtain new access tokens (optional)
	RefreshToken string `json:"refresh_token,omitempty"`

	// IDToken is the OIDC ID token containing user claims (optional)
	IDToken string `json:"id_token,omitempty"`

	// ExpiresAt is the access token expiration time
	ExpiresAt time.Time `json:"expires_at"`
}

// IsExpired returns true if the access token has expired
func (t *TokenSet) IsExpired() bool {
	return time.Now().After(t.ExpiresAt)
}

// IsValid returns true if the token set contains a non-empty access token
func (t *TokenSet) IsValid() bool {
	return t != nil && t.AccessToken != ""
}
