package oauth2

import (
	"crypto/rand"
	"encoding/hex"

	"golang.org/x/oauth2"
)

// GenerateRandomString returns length cryptographically secure random bytes,
// hex-encoded (so the result is 2*length characters long).
func GenerateRandomString(length int) string {
	b := make([]byte, length)
	_, _ = rand.Read(b) // never fails since Go 1.24
	return hex.EncodeToString(b)
}

// GenerateCodeVerifier generates a PKCE code verifier (43 characters, RFC 7636).
func GenerateCodeVerifier() string {
	return oauth2.GenerateVerifier()
}

// GenerateCodeChallenge generates a PKCE code challenge from verifier using S256 method
func GenerateCodeChallenge(verifier string) string {
	return oauth2.S256ChallengeFromVerifier(verifier)
}
