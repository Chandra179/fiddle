package oauth2

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testStateCookie = "oauth2_state"

func setupTestRouter() *http.ServeMux {
	return http.NewServeMux()
}

// newCallbackRequest builds a callback request; cookieState is sent as the state cookie when non-empty
func newCallbackRequest(t *testing.T, query, cookieState string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/callback?"+query, nil)
	require.NoError(t, err)
	if cookieState != "" {
		req.AddCookie(&http.Cookie{Name: testStateCookie, Value: cookieState})
	}
	return req
}

// findCookie returns the named cookie from the response, or nil
func findCookie(w *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, c := range w.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// assertStateCookieCleared verifies the callback expired the state cookie
func assertStateCookieCleared(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	c := findCookie(w, testStateCookie)
	require.NotNil(t, c, "state cookie should be cleared")
	assert.Empty(t, c.Value)
	assert.Negative(t, c.MaxAge)
}

func TestGoogleCallbackHandler_MissingParams(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return &CallbackInfo{}, nil
	}

	manager, err := NewManager(config)
	require.NoError(t, err)

	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, nil))

	// Test missing code
	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "state=test", ""))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing code")

	// Test missing state
	w = httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=test", ""))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing code") // Both are missing, so generic message

	// Test missing both
	w = httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "", ""))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGoogleCallbackHandler_InvalidState(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return &CallbackInfo{}, nil
	}

	manager, err := NewManager(config)
	require.NoError(t, err)

	// Register provider
	mockProvider := &MockProvider{name: "google", authURL: "https://example.com/auth"}
	manager.RegisterProvider(mockProvider)

	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, nil))

	// Cookie matches the query, but the state was never issued by the manager
	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=test&state=invalid", "invalid"))

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid or expired state")
	assertStateCookieCleared(t, w)
}

func TestGoogleCallbackHandler_StateCookie(t *testing.T) {
	tests := []struct {
		name        string
		cookieState func(state string) string
	}{
		{name: "missing cookie", cookieState: func(string) string { return "" }},
		{name: "mismatched cookie", cookieState: func(string) string { return "attacker-state" }},
		{name: "prefix of state", cookieState: func(state string) string { return state[:len(state)-1] }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := setupTestRouter()

			callbackCalled := false
			config := DefaultManagerConfig()
			config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
				callbackCalled = true
				return &CallbackInfo{SessionCookieName: "session", SessionID: "abc"}, nil
			}

			manager, err := NewManager(config)
			require.NoError(t, err)
			t.Cleanup(manager.Cleanup)
			manager.RegisterProvider(&MockProvider{name: "google", authURL: "https://example.com/auth"})

			_, state, err := manager.GetAuthURLWithState("google")
			require.NoError(t, err)

			router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, nil))

			w := httptest.NewRecorder()
			router.ServeHTTP(w, newCallbackRequest(t, "code=auth-code&state="+state, tt.cookieState(state)))

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Contains(t, w.Body.String(), "invalid or expired state")
			assert.False(t, callbackCalled, "callback handler must not run without a matching state cookie")
			assert.Nil(t, findCookie(w, "session"), "no session cookie on rejected callback")
			assertStateCookieCleared(t, w)
		})
	}
}

func TestGoogleCallbackHandler_Success(t *testing.T) {
	router := setupTestRouter()

	expectedCallbackInfo := &CallbackInfo{
		SessionCookieName: "test-session",
		UserID:            "user-123",
		SessionID:         "session-abc-xyz",
		CookieMaxAge:      3600,
	}

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return expectedCallbackInfo, nil
	}

	manager, err := NewManager(config)
	require.NoError(t, err)

	// Register mock provider
	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
	}
	manager.RegisterProvider(mockProvider)

	// Generate valid state
	authURL, _, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	handlerConfig := &HandlerConfig{
		SecureCookies:  true,
		CookiePath:     "/",
		RequestTimeout: 30 * time.Second,
	}

	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, handlerConfig))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=auth-code&state="+state, state))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "authenticated")
	assert.Contains(t, w.Body.String(), "user-123")

	// Verify session cookie was set and state cookie cleared
	cookies := w.Result().Cookies()
	require.Len(t, cookies, 2)
	session := findCookie(w, "test-session")
	require.NotNil(t, session)
	assert.Equal(t, "session-abc-xyz", session.Value)
	assert.True(t, session.HttpOnly)
	assert.True(t, session.Secure)
	assert.Equal(t, http.SameSiteStrictMode, session.SameSite)
	assert.Equal(t, "/", session.Path)
	assertStateCookieCleared(t, w)
}

func TestGoogleCallbackHandler_CallbackHandlerError(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return nil, errors.New("callback error")
	}

	manager, err := NewManager(config)
	require.NoError(t, err)

	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
	}
	manager.RegisterProvider(mockProvider)

	authURL, _, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, nil))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=auth-code&state="+state, state))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGoogleCallbackHandler_Timeout(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		// Simulate slow operation
		select {
		case <-time.After(500 * time.Millisecond):
			return &CallbackInfo{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	manager, err := NewManager(config)
	require.NoError(t, err)

	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
		handleCallback: func(ctx context.Context, code, state, nonce, codeVerifier string) (*UserInfo, *TokenSet, error) {
			// Simulate slow provider
			select {
			case <-time.After(500 * time.Millisecond):
				return &UserInfo{}, &TokenSet{}, nil
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		},
	}
	manager.RegisterProvider(mockProvider)

	authURL, _, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	// Set very short timeout
	handlerConfig := &HandlerConfig{
		RequestTimeout: 50 * time.Millisecond,
	}

	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, handlerConfig))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=auth-code&state="+state, state))

	// Provider honours ctx, so the timeout surfaces as an oauth2 callback failure
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "oauth2 callback failed")
}

func TestGoogleCallbackHandler_ZeroConfigUsesDefaults(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.Greater(t, time.Until(deadline), 20*time.Second, "zero RequestTimeout should default to 30s")
		return &CallbackInfo{SessionCookieName: "session", SessionID: "abc", CookieMaxAge: 3600}, nil
	}

	manager, err := NewManager(config)
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{name: "google", authURL: "https://example.com/auth"})

	_, state, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)

	// Zero-value config: RequestTimeout 0 must not expire the context immediately
	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, &HandlerConfig{}))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=auth-code&state="+state, state))

	assert.Equal(t, http.StatusOK, w.Code)
	session := findCookie(w, "session")
	require.NotNil(t, session)
	assert.Equal(t, "/", session.Path)
}

func TestDefaultHandlerConfig(t *testing.T) {
	config := DefaultHandlerConfig()
	assert.NotNil(t, config)
	assert.True(t, config.SecureCookies)
	assert.Equal(t, "/", config.CookiePath)
	assert.Equal(t, 30*time.Second, config.RequestTimeout)
	assert.Equal(t, "oauth2_state", config.StateCookieName)
	assert.Equal(t, 10*time.Minute, config.StateCookieMaxAge)
}

func TestHandlerConfig_WithDefaults(t *testing.T) {
	cfg := (&HandlerConfig{CookieDomain: "example.com"}).withDefaults()
	assert.False(t, cfg.SecureCookies, "explicit SecureCookies=false is kept")
	assert.Equal(t, "example.com", cfg.CookieDomain)
	assert.Equal(t, "/", cfg.CookiePath)
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout)
	assert.Equal(t, "oauth2_state", cfg.StateCookieName)
	assert.Equal(t, 10*time.Minute, cfg.StateCookieMaxAge)

	custom := &HandlerConfig{StateCookieName: "custom", CookiePath: "/auth", RequestTimeout: time.Second}
	cfg = custom.withDefaults()
	assert.Equal(t, "custom", cfg.StateCookieName)
	assert.Equal(t, "/auth", cfg.CookiePath)
	assert.Equal(t, time.Second, cfg.RequestTimeout)
	assert.Empty(t, custom.StateCookieMaxAge, "caller's config is not mutated")
}

func TestGoogleCallbackHandler_DefaultConfig(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return &CallbackInfo{
			SessionCookieName: "session",
			SessionID:         "abc",
			CookieMaxAge:      3600,
		}, nil
	}

	manager, err := NewManager(config)
	require.NoError(t, err)

	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
	}
	manager.RegisterProvider(mockProvider)

	authURL, _, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	// Pass nil config to use defaults
	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, nil))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=auth-code&state="+state, state))

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify cookie uses default secure settings
	session := findCookie(w, "session")
	require.NotNil(t, session)
	assert.True(t, session.HttpOnly)
	assert.True(t, session.Secure) // Default is secure
}

func TestGoogleLoginHandler_SetsStateCookieAndRedirects(t *testing.T) {
	router := setupTestRouter()

	manager, err := NewManager(&ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{name: "google", authURL: "https://example.com/auth"})

	router.HandleFunc("GET /login", GoogleLoginHandler(manager, nil))

	w := httptest.NewRecorder()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/login", nil)
	require.NoError(t, err)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusFound, w.Code)
	location := w.Header().Get("Location")
	assert.Contains(t, location, "https://example.com/auth?state=")

	c := findCookie(w, testStateCookie)
	require.NotNil(t, c)
	assert.Equal(t, extractStateFromURL(location), c.Value, "cookie must hold the state sent to the provider")
	assert.True(t, c.HttpOnly)
	assert.True(t, c.Secure)
	assert.Equal(t, http.SameSiteLaxMode, c.SameSite)
	assert.Equal(t, "/", c.Path)
	assert.Equal(t, 600, c.MaxAge)
}

func TestGoogleLoginHandler_FullFlowWithCallback(t *testing.T) {
	router := setupTestRouter()

	manager, err := NewManager(&ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{SessionCookieName: "session", SessionID: "abc", UserID: "u1"}, nil),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{name: "google", authURL: "https://example.com/auth"})

	handlerConfig := &HandlerConfig{SecureCookies: false, StateCookieName: "custom_state"}
	router.HandleFunc("GET /login", GoogleLoginHandler(manager, handlerConfig))
	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, handlerConfig))

	w := httptest.NewRecorder()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/login", nil)
	require.NoError(t, err)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusFound, w.Code)

	stateCookie := findCookie(w, "custom_state")
	require.NotNil(t, stateCookie)
	assert.False(t, stateCookie.Secure)
	state := extractStateFromURL(w.Header().Get("Location"))

	w = httptest.NewRecorder()
	req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, "/callback?code=auth-code&state="+state, nil)
	require.NoError(t, err)
	req.AddCookie(stateCookie)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	cleared := findCookie(w, "custom_state")
	require.NotNil(t, cleared)
	assert.Negative(t, cleared.MaxAge)
}

func TestGoogleLoginHandler_Errors(t *testing.T) {
	t.Run("provider not registered", func(t *testing.T) {
		router := setupTestRouter()
		manager, err := NewManager(&ManagerConfig{
			CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
		})
		require.NoError(t, err)
		t.Cleanup(manager.Cleanup)

		router.HandleFunc("GET /login", GoogleLoginHandler(manager, nil))

		w := httptest.NewRecorder()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/login", nil)
		require.NoError(t, err)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Nil(t, findCookie(w, testStateCookie))
	})

	t.Run("state store full", func(t *testing.T) {
		router := setupTestRouter()
		manager, err := NewManager(&ManagerConfig{
			CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
			MaxStateEntries: 1,
		})
		require.NoError(t, err)
		t.Cleanup(manager.Cleanup)
		manager.RegisterProvider(&MockProvider{name: "google", authURL: "https://example.com/auth"})

		router.HandleFunc("GET /login", GoogleLoginHandler(manager, nil))

		codes := make([]int, 2)
		for i := range codes {
			w := httptest.NewRecorder()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/login", nil)
			require.NoError(t, err)
			router.ServeHTTP(w, req)
			codes[i] = w.Code
		}

		assert.Equal(t, []int{http.StatusFound, http.StatusServiceUnavailable}, codes)
	})
}

func TestGoogleCallbackHandler_SessionCookieValueIsURLEscaped(t *testing.T) {
	const sessionID = "a+b/c=d e;f"

	manager, err := NewManager(&ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{SessionCookieName: "session", SessionID: sessionID, UserID: "u1"}, nil),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)
	manager.RegisterProvider(&MockProvider{name: "google", authURL: "https://example.com/auth"})

	_, state, err := manager.GetAuthURLWithState("google")
	require.NoError(t, err)

	router := setupTestRouter()
	router.HandleFunc("GET /callback", GoogleCallbackHandler(manager, nil))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, newCallbackRequest(t, "code=auth-code&state="+state, state))
	require.Equal(t, http.StatusOK, w.Code)

	session := findCookie(w, "session")
	require.NotNil(t, session)
	assert.NotEqual(t, sessionID, session.Value, "the wire value is escaped so it survives cookie syntax")
	got, err := url.QueryUnescape(session.Value)
	require.NoError(t, err)
	assert.Equal(t, sessionID, got)
}

func TestHandlers_ErrorResponsesAreJSON(t *testing.T) {
	manager, err := NewManager(&ManagerConfig{
		CallbackHandler: mockCallbackHandler(&CallbackInfo{}, nil),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)

	w := httptest.NewRecorder()
	GoogleCallbackHandler(manager, nil)(w, newCallbackRequest(t, "", ""))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"error":"missing code or state parameter"}`, w.Body.String())
}
