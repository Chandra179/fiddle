package oauth2

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestClaimBool_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		input   string
		want    bool
		wantErr bool
	}{
		{input: `{"v": true}`, want: true},
		{input: `{"v": false}`, want: false},
		{input: `{"v": "true"}`, want: true},
		{input: `{"v": "false"}`, want: false},
		{input: `{"v": null}`, want: false},
		{input: `{}`, want: false},
		{input: `{"v": "yes"}`, wantErr: true},
		{input: `{"v": 1}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var out struct {
				V claimBool `json:"v"`
			}
			err := json.Unmarshal([]byte(tt.input), &out)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, bool(out.V))
		})
	}
}

func newTestProvider(opts ...OIDCOption) *OIDCProvider {
	p := &OIDCProvider{
		providerName: "test",
		config: &oauth2.Config{
			ClientID:    "client-id",
			RedirectURL: "https://app.example.com/callback",
			Endpoint:    oauth2.Endpoint{AuthURL: "https://idp.example.com/auth"},
			Scopes:      []string{"openid"},
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func authParams(t *testing.T, p *OIDCProvider) url.Values {
	t.Helper()
	u, err := url.Parse(p.GetAuthURL("state-1", "nonce-1", "challenge-1"))
	require.NoError(t, err)
	return u.Query()
}

func TestOIDCProvider_GetAuthURL_Defaults(t *testing.T) {
	q := authParams(t, newTestProvider())

	assert.Equal(t, "state-1", q.Get("state"))
	assert.Equal(t, "nonce-1", q.Get("nonce"))
	assert.Equal(t, "challenge-1", q.Get("code_challenge"))
	assert.Equal(t, "S256", q.Get("code_challenge_method"))
	assert.False(t, q.Has("access_type"), "access_type is only sent when configured")
	assert.False(t, q.Has("prompt"), "the consent screen must not be forced by default")
}

func TestOIDCProvider_GetAuthURL_Options(t *testing.T) {
	t.Run("access type", func(t *testing.T) {
		q := authParams(t, newTestProvider(WithAccessType(AccessTypeOffline)))
		assert.Equal(t, "offline", q.Get("access_type"))
		assert.False(t, q.Has("prompt"))
	})

	t.Run("prompt", func(t *testing.T) {
		q := authParams(t, newTestProvider(WithPrompt(PromptConsent)))
		assert.Equal(t, "consent", q.Get("prompt"))
		assert.False(t, q.Has("access_type"))
	})

	t.Run("several prompt values are space-delimited", func(t *testing.T) {
		q := authParams(t, newTestProvider(WithPrompt(PromptSelectAccount, PromptConsent)))
		assert.Equal(t, "select_account consent", q.Get("prompt"))
	})

	t.Run("later options win", func(t *testing.T) {
		q := authParams(t, newTestProvider(WithAccessType(AccessTypeOffline), WithAccessType(""), WithPrompt(PromptLogin), WithPrompt()))
		assert.False(t, q.Has("access_type"))
		assert.False(t, q.Has("prompt"))
	})
}

func TestGoogleDefaults_OfflineWithoutForcedConsent(t *testing.T) {
	q := authParams(t, newTestProvider(googleOptions()...))
	assert.Equal(t, "offline", q.Get("access_type"), "refresh tokens need access_type=offline")
	assert.False(t, q.Has("prompt"), "must not force the consent screen on every login")

	q = authParams(t, newTestProvider(googleOptions(WithPrompt(PromptConsent), WithAccessType(AccessTypeOnline))...))
	assert.Equal(t, "online", q.Get("access_type"), "callers can override the Google defaults")
	assert.Equal(t, "consent", q.Get("prompt"))
}
