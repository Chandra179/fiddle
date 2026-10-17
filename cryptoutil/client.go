package cryptoutil

import "errors"

var (
	ErrInvalidCost    = errors.New("bcrypt: invalid cost")
	ErrMismatchedHash = errors.New("bcrypt: hash and password mismatch")
	// ErrPasswordTooLong is returned by Bcrypter.Hash for passwords longer than
	// 72 bytes, which bcrypt would otherwise silently truncate.
	ErrPasswordTooLong = errors.New("bcrypt: password length exceeds 72 bytes")
	// ErrInvalidParams is returned by the constructors for out-of-range
	// scrypt or argon2id parameters.
	ErrInvalidParams = errors.New("cryptoutil: invalid parameters")
	ErrKeyDerivation = errors.New("scrypt: key derivation failed")
	// ErrInvalidHash is returned by PasswordHasher.Verify for an encoded hash
	// that is malformed, uses another algorithm, or carries parameters outside
	// the accepted bounds.
	ErrInvalidHash = errors.New("cryptoutil: invalid encoded hash")
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

// PasswordHasher hashes passwords into self-describing strings and verifies
// them later. The encoded form (PHC string format) stores the algorithm, its
// cost parameters, the salt and the derived key, so raising the cost later does
// not invalidate hashes that are already stored.
//
// Typical login flow: Verify the password; when it matches and NeedsRehash
// reports true, Hash the password again and store the new value.
type PasswordHasher interface {
	// Hash derives a key from password with a fresh random salt and returns the
	// encoded hash. Store this string as-is.
	Hash(password string) (string, error)
	// Verify checks password against encoded using the parameters stored in
	// encoded, not the hasher's own. It returns ErrMismatchedHash on mismatch
	// and ErrInvalidHash when encoded cannot be used.
	Verify(password, encoded string) error
	// NeedsRehash reports whether encoded was produced with parameters other
	// than the hasher's current ones (or cannot be parsed), meaning the hash
	// should be recomputed the next time the password is available.
	NeedsRehash(encoded string) bool
}

// Scrypter derives cryptographic keys using scrypt.
//
// DeriveKey and VerifyKey work on raw keys: the parameters are not stored with
// the key, so changing them invalidates keys derived earlier. For password
// storage use NewScryptHasher or NewArgon2idHasher, which store the parameters.
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

// DefaultScryptConfig returns the default configuration: the OWASP minimum
// (N=2^17, r=8, p=1, about 128 MiB of memory per derivation) with a 32-byte key.
func DefaultScryptConfig() ScryptConfig {
	return ScryptConfig{
		N:      1 << 17,
		R:      8,
		P:      1,
		KeyLen: 32,
	}
}

// Argon2idConfig configures the argon2id password hasher.
// Zero-valued fields are filled from DefaultArgon2idConfig.
type Argon2idConfig struct {
	// Memory is the memory cost in KiB.
	Memory uint32
	// Time is the number of passes over the memory.
	Time uint32
	// Threads is the degree of parallelism.
	Threads uint8
	// KeyLen is the length of the derived key in bytes.
	KeyLen uint32
}

// DefaultArgon2idConfig returns the second recommended option of RFC 9106
// section 4 (m=64 MiB, t=3, p=4) with a 32-byte key.
func DefaultArgon2idConfig() Argon2idConfig {
	return Argon2idConfig{
		Memory:  64 * 1024,
		Time:    3,
		Threads: 4,
		KeyLen:  32,
	}
}

// IsMismatchedHash reports whether err indicates a password mismatch.
func IsMismatchedHash(err error) bool {
	return errors.Is(err, ErrMismatchedHash)
}
