package oauth2

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"
)

// HandlerConfig contains configuration for OAuth2 handlers
type HandlerConfig struct {
	// SecureCookies sets the Secure flag on cookies (must be true in production)
	SecureCookies bool
	// CookieDomain sets the domain for cookies (empty for default)
	CookieDomain string
	// CookiePath sets the path for cookies
	CookiePath string
	// RequestTimeout is the timeout for OAuth2 operations
	RequestTimeout time.Duration
	// StateCookieName is the cookie binding the OAuth2 state to the browser
	// that started the login (login CSRF protection)
	StateCookieName string
	// StateCookieMaxAge is the lifetime of the state cookie; keep it close to
	// ManagerConfig.StateTimeout
	StateCookieMaxAge time.Duration
}

// DefaultHandlerConfig returns a secure default configuration
func DefaultHandlerConfig() *HandlerConfig {
	return &HandlerConfig{
		SecureCookies:     true,
		CookiePath:        "/",
		RequestTimeout:    30 * time.Second,
		StateCookieName:   "oauth2_state",
		StateCookieMaxAge: 10 * time.Minute,
	}
}

// withDefaults returns a copy of config with zero-valued fields set to defaults.
// SecureCookies is left as given since false is a meaningful choice.
func (config *HandlerConfig) withDefaults() *HandlerConfig {
	defaults := DefaultHandlerConfig()
	if config == nil {
		return defaults
	}

	cfg := *config
	cfg.CookiePath = cmp.Or(cfg.CookiePath, defaults.CookiePath)
	cfg.StateCookieName = cmp.Or(cfg.StateCookieName, defaults.StateCookieName)
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaults.RequestTimeout
	}
	if cfg.StateCookieMaxAge <= 0 {
		cfg.StateCookieMaxAge = defaults.StateCookieMaxAge
	}
	return &cfg
}

// writeJSON writes v as a JSON response with the given status
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// writeError writes {"error": msg} with the given status
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// setCookie sets an HTTP-only cookie using the path, domain and Secure flag
// from config. The value is URL-escaped, so arbitrary session IDs survive the
// round trip; read it back with url.QueryUnescape (gin's c.Cookie does).
// maxAge follows net/http: 0 omits Max-Age, negative deletes the cookie.
func setCookie(w http.ResponseWriter, config *HandlerConfig, name, value string, maxAge int, sameSite http.SameSite) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    url.QueryEscape(value),
		MaxAge:   maxAge,
		Path:     config.CookiePath,
		Domain:   config.CookieDomain,
		Secure:   config.SecureCookies,
		HttpOnly: true,
		SameSite: sameSite,
	})
}

// readCookie returns the unescaped value of the named cookie, and false when
// it is absent or malformed
func readCookie(r *http.Request, name string) (string, bool) {
	c, err := r.Cookie(name)
	if err != nil {
		return "", false
	}
	value, err := url.QueryUnescape(c.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

// GoogleLoginHandler starts the Google OAuth2 flow
// It stores the state in an HTTP-only cookie (checked by GoogleCallbackHandler)
// and redirects the browser to the provider
// The handler does not check the HTTP method; register it for GET. It is a
// plain net/http handler: mux.Handle("GET /auth/login/google", h), or with gin
// router.GET(path, gin.WrapF(h))
// @Summary Google OAuth2 login
// @Description Redirects to Google's authorization endpoint
// @Tags oauth2
// @Success 302 "Redirect to provider"
// @Failure 503 {object} map[string]string "Service Unavailable - too many pending logins"
// @Failure 500 {object} map[string]string "Internal Server Error"
// @Router /auth/login/google [get]
func GoogleLoginHandler(manager Manager, config *HandlerConfig) http.HandlerFunc {
	config = config.withDefaults()

	return func(w http.ResponseWriter, r *http.Request) {
		authURL, state, err := manager.GetAuthURLWithState("google")
		if err != nil {
			switch {
			case errors.Is(err, ErrProviderNotFound):
				writeError(w, http.StatusBadRequest, "provider not found")
			case errors.Is(err, ErrStateStoreFull):
				writeError(w, http.StatusServiceUnavailable, "too many pending logins, try again later")
			default:
				writeError(w, http.StatusInternalServerError, "failed to start login")
			}
			return
		}

		// Lax, not Strict: the cookie must be sent on the top-level
		// cross-site redirect back from the provider
		setCookie(w, config, config.StateCookieName, state, int(config.StateCookieMaxAge.Seconds()), http.SameSiteLaxMode)

		http.Redirect(w, r, authURL, http.StatusFound)
	}
}

// GoogleCallbackHandler handles Google OAuth2 callback
// The state query parameter must match the state cookie set by GoogleLoginHandler
// Note: This handler sets secure HTTP-only cookies for session management
// The session cookie value is URL-escaped (see setCookie)
// The handler does not check the HTTP method; register it for GET. It is a
// plain net/http handler: mux.Handle("GET /auth/callback/google", h), or with
// gin router.GET(path, gin.WrapF(h))
// @Summary Google OAuth2 callback
// @Description Handles Google OAuth2 callback and returns user info
// @Tags oauth2
// @Produce json
// @Param code query string true "OAuth2 code"
// @Param state query string true "OAuth2 state"
// @Success 200 {object} map[string]interface{} "Authenticated"
// @Failure 400 {object} map[string]string "Bad Request - missing parameters"
// @Failure 401 {object} map[string]string "Unauthorized - invalid state, state cookie, or code"
// @Failure 500 {object} map[string]string "Internal Server Error"
// @Router /auth/callback/google [get]
func GoogleCallbackHandler(manager Manager, config *HandlerConfig) http.HandlerFunc {
	config = config.withDefaults()

	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		code := query.Get("code")
		state := query.Get("state")

		if code == "" || state == "" {
			writeError(w, http.StatusBadRequest, "missing code or state parameter")
			return
		}

		// The state cookie is single-use: clear it whatever the outcome
		cookieState, hasCookie := readCookie(r, config.StateCookieName)
		setCookie(w, config, config.StateCookieName, "", -1, http.SameSiteLaxMode)

		// Bind the callback to the browser that started the login (login CSRF)
		if !hasCookie || subtle.ConstantTimeCompare([]byte(cookieState), []byte(state)) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid or expired state")
			return
		}

		// Create context with timeout to prevent hanging requests
		ctx, cancel := context.WithTimeout(r.Context(), config.RequestTimeout)
		defer cancel()

		info, err := manager.HandleCallback(ctx, "google", code, state)
		if err != nil {
			status := http.StatusInternalServerError
			errMsg := "authentication failed"

			// Determine appropriate status code based on error type
			switch {
			case errors.Is(err, ErrProviderNotFound):
				status = http.StatusBadRequest
				errMsg = "provider not found"
			case errors.Is(err, ErrInvalidState), errors.Is(err, ErrStateNotFound), errors.Is(err, ErrStateExpired):
				status = http.StatusUnauthorized
				errMsg = "invalid or expired state"
			case errors.Is(err, ErrOAuth2Callback):
				status = http.StatusUnauthorized
				errMsg = "oauth2 callback failed"
			}

			writeError(w, status, errMsg)
			return
		}

		// Set secure HTTP-only cookie (Secure comes from config, HttpOnly is always on)
		setCookie(w, config, info.SessionCookieName, info.SessionID, info.CookieMaxAge, http.SameSiteStrictMode)

		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "authenticated",
			"user_id": info.UserID,
		})
	}
}
