package cryptoutil

import (
	"errors"

	"golang.org/x/crypto/bcrypt"
)

type bcrypter struct {
	cost int
}

// NewBcrypter creates a Bcrypter with the given config.
// If config.Cost is zero, it defaults to bcrypt.DefaultCost (10).
// Returns ErrInvalidCost if cost is outside [4, 31].
func NewBcrypter(cfg BcryptConfig) (Bcrypter, error) {
	cost := cfg.Cost
	if cost == 0 {
		cost = bcrypt.DefaultCost
	}
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		return nil, ErrInvalidCost
	}
	return &bcrypter{cost: cost}, nil
}

func (b *bcrypter) Hash(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), b.cost)
	if errors.Is(err, bcrypt.ErrPasswordTooLong) {
		return "", ErrPasswordTooLong
	}
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func (b *bcrypter) Compare(password, hash string) error {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
		return ErrMismatchedHash
	}
	return err
}
