package oauth2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// defaultHTTPTimeout bounds discovery, JWKS, token exchange and refresh requests
const defaultHTTPTimeout = 10 * time.Second

// GoogleOIDCProvider implements Provider interface using OIDC with PKCE
type GoogleOIDCProvider struct {
	config       *oauth2.Config
	provider     *oidc.Provider
	verifier     *oidc.IDTokenVerifier
	httpClient   *http.Client
	logoutURL    string
	providerName string
}

// NewOIDCProvider creates an OIDC provider for the given issuer URL.
// Use this for custom or non-Google OIDC providers, or in tests with a mock server.
// An *http.Client set on ctx via oidc.ClientContext is used as-is; otherwise
// requests use a client with a 10s timeout.
func NewOIDCProvider(ctx context.Context, issuerURL, providerName, clientID, clientSecret, redirectURL string, scopes []string) (*GoogleOIDCProvider, error) {
	httpClient, ok := ctx.Value(oauth2.HTTPClient).(*http.Client)
	if !ok || httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}

	// The provider keeps this context for JWKS refreshes, so the client applies there too
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, httpClient), issuerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create OIDC provider: %w", err)
	}

	var discovery struct {
		EndSessionEndpoint string `json:"end_session_endpoint"`
	}
	if err := provider.Claims(&discovery); err != nil {
		return nil, fmt.Errorf("failed to parse OIDC discovery document: %w", err)
	}
	logoutURL := discovery.EndSessionEndpoint
	if logoutURL == "" {
		logoutURL = issuerURL + "/logout"
	}

	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "profile", "email"}
	}

	config := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       scopes,
	}

	verifier := provider.Verifier(&oidc.Config{
		ClientID: clientID,
	})

	return &GoogleOIDCProvider{
		config:       config,
		provider:     provider,
		verifier:     verifier,
		httpClient:   httpClient,
		logoutURL:    logoutURL,
		providerName: providerName,
	}, nil
}

// NewGoogleOIDCProvider creates a new Google OIDC provider
// Google's discovery document has no end_session_endpoint, so the logout URL
// falls back to https://accounts.google.com/logout
func NewGoogleOIDCProvider(ctx context.Context, clientID, clientSecret, redirectURL string, scopes []string) (*GoogleOIDCProvider, error) {
	return NewOIDCProvider(ctx, "https://accounts.google.com", "google", clientID, clientSecret, redirectURL, scopes)
}

// clientContext attaches the provider's HTTP client (with timeout) to ctx
func (g *GoogleOIDCProvider) clientContext(ctx context.Context) context.Context {
	return oidc.ClientContext(ctx, g.httpClient)
}

// GetName returns the provider name
func (g *GoogleOIDCProvider) GetName() string {
	return g.providerName
}

// GetAuthURL generates the authorization URL with PKCE and nonce
func (g *GoogleOIDCProvider) GetAuthURL(state string, nonce string, codeChallenge string) string {
	opts := []oauth2.AuthCodeOption{
		oauth2.AccessTypeOffline,
		oauth2.ApprovalForce,
		oidc.Nonce(nonce),
		oauth2.SetAuthURLParam("code_challenge", codeChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	}
	return g.config.AuthCodeURL(state, opts...)
}

// HandleCallback processes the OAuth2 callback, exchanges code for tokens, and validates ID token
func (g *GoogleOIDCProvider) HandleCallback(ctx context.Context, code string, state string,
	nonce string, codeVerifier string) (*UserInfo, *TokenSet, error) {
	oauth2Token, err := g.config.Exchange(
		g.clientContext(ctx),
		code,
		oauth2.VerifierOption(codeVerifier),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exchange code: %w", err)
	}

	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		return nil, nil, fmt.Errorf("no id_token in response")
	}

	idToken, err := g.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to verify ID token: %w", err)
	}

	if idToken.Nonce != nonce {
		return nil, nil, fmt.Errorf("nonce mismatch")
	}

	var claims struct {
		Email         string    `json:"email"`
		EmailVerified claimBool `json:"email_verified"`
		Name          string    `json:"name"`
	}

	if err := idToken.Claims(&claims); err != nil {
		return nil, nil, fmt.Errorf("failed to parse claims: %w", err)
	}

	userInfo := &UserInfo{
		ID:            idToken.Subject,
		Email:         claims.Email,
		EmailVerified: bool(claims.EmailVerified),
		Name:          claims.Name,
		Provider:      g.providerName,
		CreatedAt:     time.Now(),
	}

	tokenSet := &TokenSet{
		AccessToken:  oauth2Token.AccessToken,
		TokenType:    oauth2Token.TokenType,
		RefreshToken: oauth2Token.RefreshToken,
		IDToken:      rawIDToken,
		ExpiresAt:    oauth2Token.Expiry,
	}

	return userInfo, tokenSet, nil
}

// RefreshToken refreshes the access token using refresh token
func (g *GoogleOIDCProvider) RefreshToken(ctx context.Context, refreshToken string) (*TokenSet, error) {
	tokenSource := g.config.TokenSource(g.clientContext(ctx), &oauth2.Token{
		RefreshToken: refreshToken,
	})

	newToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	tokenSet := &TokenSet{
		AccessToken:  newToken.AccessToken,
		TokenType:    newToken.TokenType,
		RefreshToken: newToken.RefreshToken,
		ExpiresAt:    newToken.Expiry,
	}

	// Preserve existing ID token if refresh doesn't return one
	if idToken, ok := newToken.Extra("id_token").(string); ok && idToken != "" {
		tokenSet.IDToken = idToken
	}

	return tokenSet, nil
}

// GetEndSessionEndpoint returns the discovered end_session_endpoint, or the
// issuer's /logout URL when discovery does not advertise one
func (g *GoogleOIDCProvider) GetEndSessionEndpoint() string {
	return g.logoutURL
}

// claimBool decodes a boolean claim that some providers send as a string ("true").
type claimBool bool

func (b *claimBool) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch v := v.(type) {
	case nil:
		*b = false
	case bool:
		*b = claimBool(v)
	case string:
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid boolean claim %q: %w", v, err)
		}
		*b = claimBool(parsed)
	default:
		return fmt.Errorf("invalid boolean claim: %s", data)
	}
	return nil
}
