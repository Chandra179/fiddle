package cryptoutil

import "errors"

var (
	ErrInvalidCost    = errors.New("bcrypt: invalid cost")
	ErrMismatchedHash = errors.New("bcrypt: hash and password mismatch")
	// ErrPasswordTooLong is returned by Bcrypter.Hash for passwords longer than
	// 72 bytes, which bcrypt would otherwise silently truncate.
	ErrPasswordTooLong = errors.New("bcrypt: password length exceeds 72 bytes")
	ErrInvalidParams   = errors.New("scrypt: invalid parameters")
	ErrKeyDerivation   = errors.New("scrypt: key derivation failed")
)

// Bcrypter hashes and verifies passwords using bcrypt.
type Bcrypter interface {
	Hash(password string) (string, error)
	Compare(password, hash string) error
}

// BcryptConfig controls the bcrypt cost factor.
// Cost defaults to 10 if zero. Must be between 4 and 31.
type BcryptConfig struct {
	Cost int
}

// Scrypter derives cryptographic keys using scrypt.
type Scrypter interface {
	DeriveKey(password, salt []byte) ([]byte, error)
	// VerifyKey re-derives the key for password and salt and compares it with
	// expected in constant time. It returns ErrMismatchedHash on mismatch.
	VerifyKey(password, salt, expected []byte) error
}

// ScryptConfig configures the scrypt key derivation.
// Zero-valued fields are filled from DefaultScryptConfig.
type ScryptConfig struct {
	N      int
	R      int
	P      int
	KeyLen int
}

// DefaultScryptConfig returns a safe default configuration
// (N=32768, r=8, p=1, key length=32).
func DefaultScryptConfig() ScryptConfig {
	return ScryptConfig{
		N:      32768,
		R:      8,
		P:      1,
		KeyLen: 32,
	}
}

// IsMismatchedHash reports whether err indicates a password mismatch.
func IsMismatchedHash(err error) bool {
	return errors.Is(err, ErrMismatchedHash)
}
