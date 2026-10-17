//go:build integration

package example

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	josejwt "github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	oauth2pkg "oauth2"
)

// oidcServer is a minimal mock OIDC provider for integration testing.
type oidcServer struct {
	server     *httptest.Server
	privateKey *rsa.PrivateKey
	keyID      string
	// omitEndSession leaves end_session_endpoint out of discovery
	omitEndSession bool
}

func (s *oidcServer) issuer() string { return s.server.URL }

func (s *oidcServer) discovery(w http.ResponseWriter, r *http.Request) {
	doc := map[string]any{
		"issuer":                                s.issuer(),
		"authorization_endpoint":                s.issuer() + "/auth",
		"token_endpoint":                        s.issuer() + "/token",
		"jwks_uri":                              s.issuer() + "/jwks",
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
	}
	if !s.omitEndSession {
		doc["end_session_endpoint"] = s.issuer() + "/end-session"
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(doc)
}

func (s *oidcServer) jwks(w http.ResponseWriter, r *http.Request) {
	jwk := jose.JSONWebKey{
		Key:       &s.privateKey.PublicKey,
		KeyID:     s.keyID,
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}
	set := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{jwk}}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(set)
}

func (s *oidcServer) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// The test encodes the nonce into the code as "code:<nonce>" so the token server
	// can embed the correct nonce in the ID token without a shared state store.
	code := r.FormValue("code")
	nonce := ""
	if len(code) > 5 && code[:5] == "code:" {
		nonce = code[5:]
	}

	clientID := r.FormValue("client_id")
	if clientID == "" {
		// OAuth2 library may send credentials via HTTP Basic Auth
		if id, _, ok := r.BasicAuth(); ok {
			clientID = id
		}
	}

	sig, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: s.privateKey},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", s.keyID),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	now := time.Now()
	claims := josejwt.Claims{
		Issuer:   s.issuer(),
		Subject:  "user-sub-123",
		Audience: josejwt.Audience{clientID},
		IssuedAt: josejwt.NewNumericDate(now),
		Expiry:   josejwt.NewNumericDate(now.Add(time.Hour)),
	}
	extra := struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
		Nonce         string `json:"nonce"`
	}{
		Email:         "test@example.com",
		EmailVerified: true,
		Name:          "Test User",
		Nonce:         nonce,
	}

	idToken, err := josejwt.Signed(sig).Claims(claims).Claims(extra).Serialize()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]any{
		"access_token":  "access-token-value",
		"token_type":    "Bearer",
		"refresh_token": "refresh-token-value",
		"expires_in":    3600,
		"id_token":      idToken,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// package-level variables shared across tests
var (
	mockOIDC *oidcServer
	provider *oauth2pkg.OIDCProvider
	manager  oauth2pkg.Manager
)

func TestMain(m *testing.M) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate RSA key: %v\n", err)
		os.Exit(1)
	}

	mockOIDC = newOIDCServer(privateKey, false)

	ctx := context.Background()

	// Use NewOIDCProvider so we can point at the mock server instead of accounts.google.com
	provider, err = oauth2pkg.NewOIDCProvider(
		ctx,
		mockOIDC.issuer(),
		"google",
		"test-client-id",
		"test-client-secret",
		mockOIDC.issuer()+"/callback",
		nil,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create provider: %v\n", err)
		mockOIDC.server.Close()
		os.Exit(1)
	}

	manager, err = oauth2pkg.NewManager(&oauth2pkg.ManagerConfig{
		StateTimeout: 5 * time.Minute,
		CallbackHandler: func(ctx context.Context, providerName string, userInfo *oauth2pkg.UserInfo, tokenSet *oauth2pkg.TokenSet) (*oauth2pkg.CallbackInfo, error) {
			return &oauth2pkg.CallbackInfo{
				SessionCookieName: "session",
				UserID:            userInfo.ID,
				SessionID:         "session-" + userInfo.ID,
				CookieMaxAge:      3600,
			}, nil
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create manager: %v\n", err)
		mockOIDC.server.Close()
		os.Exit(1)
	}
	manager.RegisterProvider(provider)

	code := m.Run()

	manager.Cleanup()
	mockOIDC.server.Close()
	os.Exit(code)
}

func TestIntegration_GetAuthURLWithState(t *testing.T) {
	authURL, _, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)
	assert.Contains(t, authURL, mockOIDC.issuer())
	assert.Contains(t, authURL, "state=")
	assert.Contains(t, authURL, "code_challenge=")
	assert.Contains(t, authURL, "nonce=")
	// A generic OIDC provider must not send Google-specific parameters or
	// force the consent screen unless asked to.
	assert.NotContains(t, authURL, "prompt=")
	assert.NotContains(t, authURL, "access_type=")
}

func TestIntegration_FullFlow(t *testing.T) {
	ctx := context.Background()

	// Step 1: generate auth URL (creates state + nonce + PKCE in storage)
	authURL, _, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)

	state := extractParam(authURL, "state")
	require.NotEmpty(t, state)

	// Encode nonce into the code so the mock token server can embed it in the ID token.
	nonce := extractParam(authURL, "nonce")
	require.NotEmpty(t, nonce)
	code := "code:" + nonce

	// Step 2: simulate provider callback — code exchange happens against mock server
	info, err := manager.HandleCallback(ctx, "google", code, state)
	require.NoError(t, err)
	assert.Equal(t, "user-sub-123", info.UserID)
	assert.Equal(t, "session", info.SessionCookieName)
	assert.Equal(t, 3600, info.CookieMaxAge)

	// Step 3: verify state is one-time-use (replay attack prevention)
	_, err = manager.HandleCallback(ctx, "google", code, state)
	assert.ErrorIs(t, err, oauth2pkg.ErrInvalidState)
}

func TestIntegration_InvalidState(t *testing.T) {
	_, err := manager.HandleCallback(context.Background(), "google", "test-code", "bogus-state")
	assert.ErrorIs(t, err, oauth2pkg.ErrInvalidState)
}

func TestIntegration_UnknownProvider(t *testing.T) {
	_, _, err := manager.GetAuthURLWithState("github")
	assert.ErrorIs(t, err, oauth2pkg.ErrProviderNotFound)
}

func TestIntegration_ConcurrentAuthURLs(t *testing.T) {
	const n = 10
	results := make(chan string, n)

	for range n {
		go func() {
			url, _, err := manager.GetAuthURLWithState("google")
			if err != nil {
				results <- ""
				return
			}
			results <- extractParam(url, "state")
		}()
	}

	seen := make(map[string]bool)
	for range n {
		state := <-results
		require.NotEmpty(t, state, "state should not be empty")
		assert.False(t, seen[state], "state must be unique across concurrent requests")
		seen[state] = true
	}
}

func TestIntegration_EndSessionEndpoint(t *testing.T) {
	assert.Equal(t, mockOIDC.issuer()+"/end-session", provider.GetEndSessionEndpoint())
}

func TestIntegration_EndSessionEndpoint_Fallback(t *testing.T) {
	srv := newOIDCServer(mockOIDC.privateKey, true)
	t.Cleanup(srv.server.Close)

	p, err := oauth2pkg.NewOIDCProvider(context.Background(), srv.issuer(), "other",
		"test-client-id", "test-client-secret", srv.issuer()+"/callback", nil)
	require.NoError(t, err)
	assert.Equal(t, srv.issuer()+"/logout", p.GetEndSessionEndpoint())
}

func TestIntegration_HTTPFlowWithStateCookie(t *testing.T) {
	var gotUser *oauth2pkg.UserInfo
	mgr, err := oauth2pkg.NewManager(&oauth2pkg.ManagerConfig{
		CallbackHandler: func(ctx context.Context, providerName string, userInfo *oauth2pkg.UserInfo, tokenSet *oauth2pkg.TokenSet) (*oauth2pkg.CallbackInfo, error) {
			gotUser = userInfo
			return &oauth2pkg.CallbackInfo{
				SessionCookieName: "session",
				UserID:            userInfo.ID,
				SessionID:         "session-" + userInfo.ID,
				CookieMaxAge:      3600,
			}, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(mgr.Cleanup)
	mgr.RegisterProvider(provider)

	router := http.NewServeMux()
	router.HandleFunc("GET /login", oauth2pkg.GoogleLoginHandler(mgr, nil))
	router.HandleFunc("GET /callback", oauth2pkg.GoogleCallbackHandler(mgr, nil))

	login := func(t *testing.T) (state, nonce string, stateCookie *http.Cookie) {
		t.Helper()
		w := httptest.NewRecorder()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/login", nil)
		require.NoError(t, err)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusFound, w.Code)

		location, err := url.Parse(w.Header().Get("Location"))
		require.NoError(t, err)
		assert.Equal(t, mockOIDC.issuer()+"/auth", location.Scheme+"://"+location.Host+location.Path)

		for _, c := range w.Result().Cookies() {
			if c.Name == "oauth2_state" {
				stateCookie = c
			}
		}
		require.NotNil(t, stateCookie)
		assert.True(t, stateCookie.HttpOnly)
		assert.Equal(t, http.SameSiteLaxMode, stateCookie.SameSite)
		return location.Query().Get("state"), location.Query().Get("nonce"), stateCookie
	}

	callback := func(t *testing.T, state, nonce string, cookie *http.Cookie) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		q := url.Values{"code": {"code:" + nonce}, "state": {state}}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/callback?"+q.Encode(), nil)
		require.NoError(t, err)
		if cookie != nil {
			req.AddCookie(cookie)
		}
		router.ServeHTTP(w, req)
		return w
	}

	t.Run("matching cookie authenticates", func(t *testing.T) {
		state, nonce, cookie := login(t)
		assert.Equal(t, state, cookie.Value)

		w := callback(t, state, nonce, cookie)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, gotUser)
		assert.Equal(t, "user-sub-123", gotUser.ID)
		assert.Equal(t, "test@example.com", gotUser.Email)
		assert.True(t, gotUser.EmailVerified)
	})

	t.Run("missing cookie is rejected", func(t *testing.T) {
		state, nonce, _ := login(t)
		w := callback(t, state, nonce, nil)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("cookie from another login is rejected", func(t *testing.T) {
		// Login CSRF: attacker's state in the URL, victim's own cookie in the browser
		attackerState, attackerNonce, _ := login(t)
		_, _, victimCookie := login(t)
		w := callback(t, attackerState, attackerNonce, victimCookie)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func newOIDCServer(privateKey *rsa.PrivateKey, omitEndSession bool) *oidcServer {
	s := &oidcServer{privateKey: privateKey, keyID: "test-key-id", omitEndSession: omitEndSession}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", s.discovery)
	mux.HandleFunc("/jwks", s.jwks)
	mux.HandleFunc("/token", s.token)
	// /auth endpoint is not exercised in these tests (we simulate callbacks directly)
	s.server = httptest.NewServer(mux)
	return s
}

// extractParam parses a named query parameter from a raw URL string.
func extractParam(rawURL, param string) string {
	idx := 0
	for i, c := range rawURL {
		if c == '?' {
			idx = i + 1
			break
		}
	}
	if idx == 0 {
		return ""
	}
	query := rawURL[idx:]
	start := 0
	for i := 0; i <= len(query); i++ {
		if i == len(query) || query[i] == '&' {
			part := query[start:i]
			if len(part) > len(param)+1 && part[:len(param)+1] == param+"=" {
				return part[len(param)+1:]
			}
			start = i + 1
		}
	}
	return ""
}
