package oauth2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// defaultHTTPTimeout bounds discovery, JWKS, token exchange and refresh requests
const defaultHTTPTimeout = 10 * time.Second

// Values for the OIDC/Google "prompt" authorization parameter, see WithPrompt.
const (
	// PromptConsent forces the consent screen on every login.
	PromptConsent = "consent"
	// PromptSelectAccount asks the user to pick an account.
	PromptSelectAccount = "select_account"
	// PromptLogin forces re-authentication.
	PromptLogin = "login"
)

// Values for the "access_type" authorization parameter, see WithAccessType.
const (
	// AccessTypeOffline asks the provider for a refresh token (Google).
	AccessTypeOffline = "offline"
	// AccessTypeOnline requests no refresh token (Google's default).
	AccessTypeOnline = "online"
)

// OIDCProvider implements Provider interface using OIDC with PKCE.
// It works with any compliant issuer; NewGoogleOIDCProvider preconfigures it
// for Google.
type OIDCProvider struct {
	config       *oauth2.Config
	verifier     *oidc.IDTokenVerifier
	httpClient   *http.Client
	logoutURL    string
	providerName string
	accessType   string
	prompt       string
}

// OIDCOption customizes an OIDCProvider.
type OIDCOption func(*OIDCProvider)

// WithAccessType sets the "access_type" authorization parameter, e.g.
// AccessTypeOffline to receive a refresh token from Google. An empty value
// omits the parameter.
//
// Most OIDC providers ignore access_type and use the "offline_access" scope
// for refresh tokens instead; pass that in the scopes.
func WithAccessType(accessType string) OIDCOption {
	return func(p *OIDCProvider) { p.accessType = accessType }
}

// WithPrompt sets the "prompt" authorization parameter (PromptConsent,
// PromptSelectAccount, PromptLogin; several values are space-delimited). An
// empty list omits the parameter, so a returning user is not asked again.
//
// Google only issues a refresh token on the first consent, or when the consent
// screen is shown again; add PromptConsent only if you must re-obtain one.
func WithPrompt(prompts ...string) OIDCOption {
	return func(p *OIDCProvider) { p.prompt = strings.Join(prompts, " ") }
}

// NewOIDCProvider creates an OIDC provider for the given issuer URL.
// Use this for custom or non-Google OIDC providers, or in tests with a mock server.
// An *http.Client set on ctx via oidc.ClientContext is used as-is; otherwise
// requests use a client with a 10s timeout.
//
// No access_type or prompt parameter is sent unless requested through opts.
func NewOIDCProvider(ctx context.Context, issuerURL, providerName, clientID, clientSecret, redirectURL string, scopes []string, opts ...OIDCOption) (*OIDCProvider, error) {
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
		if logoutURL, err = url.JoinPath(issuerURL, "logout"); err != nil {
			return nil, fmt.Errorf("invalid issuer URL %q: %w", issuerURL, err)
		}
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

	p := &OIDCProvider{
		config:       config,
		verifier:     verifier,
		httpClient:   httpClient,
		logoutURL:    logoutURL,
		providerName: providerName,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// NewGoogleOIDCProvider creates a new Google OIDC provider
// Google's discovery document has no end_session_endpoint, so the logout URL
// falls back to https://accounts.google.com/logout
//
// It requests access_type=offline so Google issues a refresh token, but does
// not force the consent screen on every login. Override either with
// WithAccessType and WithPrompt (later options win).
func NewGoogleOIDCProvider(ctx context.Context, clientID, clientSecret, redirectURL string, scopes []string, opts ...OIDCOption) (*OIDCProvider, error) {
	return NewOIDCProvider(ctx, "https://accounts.google.com", "google", clientID, clientSecret, redirectURL, scopes, googleOptions(opts...)...)
}

// googleOptions returns Google's defaults followed by opts, so opts win.
func googleOptions(opts ...OIDCOption) []OIDCOption {
	return append([]OIDCOption{WithAccessType(AccessTypeOffline)}, opts...)
}

// clientContext attaches the provider's HTTP client (with timeout) to ctx
func (p *OIDCProvider) clientContext(ctx context.Context) context.Context {
	return oidc.ClientContext(ctx, p.httpClient)
}

// GetName returns the provider name
func (p *OIDCProvider) GetName() string {
	return p.providerName
}

// GetAuthURL generates the authorization URL with PKCE and nonce
func (p *OIDCProvider) GetAuthURL(state string, nonce string, codeChallenge string) string {
	opts := []oauth2.AuthCodeOption{
		oidc.Nonce(nonce),
		oauth2.SetAuthURLParam("code_challenge", codeChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	}
	if p.accessType != "" {
		opts = append(opts, oauth2.SetAuthURLParam("access_type", p.accessType))
	}
	if p.prompt != "" {
		opts = append(opts, oauth2.SetAuthURLParam("prompt", p.prompt))
	}
	return p.config.AuthCodeURL(state, opts...)
}

// HandleCallback processes the OAuth2 callback, exchanges code for tokens, and validates ID token
func (p *OIDCProvider) HandleCallback(ctx context.Context, code string, state string,
	nonce string, codeVerifier string) (*UserInfo, *TokenSet, error) {
	oauth2Token, err := p.config.Exchange(
		p.clientContext(ctx),
		code,
		oauth2.VerifierOption(codeVerifier),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exchange code: %w", err)
	}

	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		return nil, nil, errors.New("no id_token in response")
	}

	idToken, err := p.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to verify ID token: %w", err)
	}

	if idToken.Nonce != nonce {
		return nil, nil, errors.New("nonce mismatch")
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
		Provider:      p.providerName,
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
func (p *OIDCProvider) RefreshToken(ctx context.Context, refreshToken string) (*TokenSet, error) {
	tokenSource := p.config.TokenSource(p.clientContext(ctx), &oauth2.Token{
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

	// A refresh response may omit the ID token.
	if idToken, ok := newToken.Extra("id_token").(string); ok && idToken != "" {
		tokenSet.IDToken = idToken
	}

	return tokenSet, nil
}

// GetEndSessionEndpoint returns the discovered end_session_endpoint, or the
// issuer's /logout URL when discovery does not advertise one
func (p *OIDCProvider) GetEndSessionEndpoint() string {
	return p.logoutURL
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
