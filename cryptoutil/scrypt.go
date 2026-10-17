package cryptoutil

import (
	"cmp"
	"crypto/subtle"
	"fmt"

	"golang.org/x/crypto/scrypt"
)

// minKeyLen is the shortest derived key accepted; anything shorter is too weak
// to use as a password verifier or symmetric key.
const minKeyLen = 16

type scrypter struct {
	n, r, p, keyLen int
}

// NewScrypter creates a Scrypter with the given config.
// Zero-valued fields are filled from DefaultScryptConfig.
// Returns ErrInvalidParams if N is not a power of two > 1, r*p >= 2^30,
// or KeyLen < 16.
func NewScrypter(cfg ScryptConfig) (Scrypter, error) {
	return newScrypter(cfg)
}

func newScrypter(cfg ScryptConfig) (*scrypter, error) {
	def := DefaultScryptConfig()
	cfg.N = cmp.Or(cfg.N, def.N)
	cfg.R = cmp.Or(cfg.R, def.R)
	cfg.P = cmp.Or(cfg.P, def.P)
	cfg.KeyLen = cmp.Or(cfg.KeyLen, def.KeyLen)

	if cfg.N <= 1 || cfg.N&(cfg.N-1) != 0 {
		return nil, fmt.Errorf("%w: N must be a power of two > 1", ErrInvalidParams)
	}
	if cfg.R < 0 || cfg.P < 0 || uint64(cfg.R)*uint64(cfg.P) >= 1<<30 {
		return nil, fmt.Errorf("%w: r*p must be positive and < 2^30", ErrInvalidParams)
	}
	if cfg.KeyLen < minKeyLen {
		return nil, fmt.Errorf("%w: KeyLen must be >= %d", ErrInvalidParams, minKeyLen)
	}
	return &scrypter{n: cfg.N, r: cfg.R, p: cfg.P, keyLen: cfg.KeyLen}, nil
}

func (s *scrypter) DeriveKey(password, salt []byte) ([]byte, error) {
	key, err := scrypt.Key(password, salt, s.n, s.r, s.p, s.keyLen)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKeyDerivation, err)
	}
	return key, nil
}

func (s *scrypter) VerifyKey(password, salt, expected []byte) error {
	key, err := s.DeriveKey(password, salt)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(key, expected) != 1 {
		return ErrMismatchedHash
	}
	return nil
}
