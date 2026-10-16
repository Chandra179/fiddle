package oauth2

import (
	"crypto/rand"
	"encoding/hex"

	"golang.org/x/oauth2"
)

// GenerateRandomString generates a cryptographically secure random string
func GenerateRandomString(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// GenerateCodeVerifier generates a PKCE code verifier (43 characters, RFC 7636).
// The error is always nil; it is kept for API compatibility.
func GenerateCodeVerifier() (string, error) {
	return oauth2.GenerateVerifier(), nil
}

// GenerateCodeChallenge generates a PKCE code challenge from verifier using S256 method
func GenerateCodeChallenge(verifier string) string {
	return oauth2.S256ChallengeFromVerifier(verifier)
}
