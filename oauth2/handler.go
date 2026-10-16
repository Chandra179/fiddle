package oauth2

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
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
	if cfg.CookiePath == "" {
		cfg.CookiePath = defaults.CookiePath
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaults.RequestTimeout
	}
	if cfg.StateCookieName == "" {
		cfg.StateCookieName = defaults.StateCookieName
	}
	if cfg.StateCookieMaxAge <= 0 {
		cfg.StateCookieMaxAge = defaults.StateCookieMaxAge
	}
	return &cfg
}

// GoogleLoginHandler starts the Google OAuth2 flow
// It stores the state in an HTTP-only cookie (checked by GoogleCallbackHandler)
// and redirects the browser to the provider
// @Summary Google OAuth2 login
// @Description Redirects to Google's authorization endpoint
// @Tags oauth2
// @Success 302 "Redirect to provider"
// @Failure 503 {object} map[string]string "Service Unavailable - too many pending logins"
// @Failure 500 {object} map[string]string "Internal Server Error"
// @Router /auth/login/google [get]
func GoogleLoginHandler(manager Manager, config *HandlerConfig) gin.HandlerFunc {
	config = config.withDefaults()

	return func(c *gin.Context) {
		authURL, state, err := manager.GetAuthURLWithState("google")
		if err != nil {
			switch {
			case errors.Is(err, ErrProviderNotFound):
				c.JSON(http.StatusBadRequest, gin.H{"error": "provider not found"})
			case errors.Is(err, ErrStateStoreFull):
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "too many pending logins, try again later"})
			default:
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start login"})
			}
			return
		}

		// Lax, not Strict: the cookie must be sent on the top-level
		// cross-site redirect back from the provider
		c.SetSameSite(http.SameSiteLaxMode)
		c.SetCookie(
			config.StateCookieName,
			state,
			int(config.StateCookieMaxAge.Seconds()),
			config.CookiePath,
			config.CookieDomain,
			config.SecureCookies,
			true, // HttpOnly
		)

		c.Redirect(http.StatusFound, authURL)
	}
}

// GoogleCallbackHandler handles Google OAuth2 callback
// The state query parameter must match the state cookie set by GoogleLoginHandler
// Note: This handler sets secure HTTP-only cookies for session management
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
func GoogleCallbackHandler(manager Manager, config *HandlerConfig) gin.HandlerFunc {
	config = config.withDefaults()

	return func(c *gin.Context) {
		code := c.Query("code")
		state := c.Query("state")

		if code == "" || state == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing code or state parameter"})
			return
		}

		// The state cookie is single-use: clear it whatever the outcome
		cookieState, cookieErr := c.Cookie(config.StateCookieName)
		c.SetSameSite(http.SameSiteLaxMode)
		c.SetCookie(config.StateCookieName, "", -1, config.CookiePath, config.CookieDomain, config.SecureCookies, true)

		// Bind the callback to the browser that started the login (login CSRF)
		if cookieErr != nil || subtle.ConstantTimeCompare([]byte(cookieState), []byte(state)) != 1 {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired state"})
			return
		}

		// Create context with timeout to prevent hanging requests
		ctx, cancel := context.WithTimeout(c.Request.Context(), config.RequestTimeout)
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

			c.JSON(status, gin.H{"error": errMsg})
			return
		}

		// Set secure HTTP-only cookie
		c.SetSameSite(http.SameSiteStrictMode)
		c.SetCookie(
			info.SessionCookieName,
			info.SessionID,
			info.CookieMaxAge,
			config.CookiePath,
			config.CookieDomain,
			config.SecureCookies, // Secure: from config
			true,                 // HttpOnly: always true
		)

		c.JSON(http.StatusOK, gin.H{
			"status":  "authenticated",
			"user_id": info.UserID,
		})
	}
}
