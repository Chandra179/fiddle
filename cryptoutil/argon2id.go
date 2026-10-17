package cryptoutil

import (
	"cmp"
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Verify reads cost parameters from the stored hash, so it bounds them.
// NewArgon2idHasher applies the same limits.
const (
	maxArgon2Memory  = 1 << 19 // KiB: 512 MiB
	maxArgon2Time    = 10
	maxArgon2Threads = 16
	maxArgon2Work    = 1 << 21 // m*t in KiB-passes: 2 GiB-passes, 10x the default
	maxArgon2KeyLen  = maxEncodedKeyLen
)

// argon2CostTooHigh returns why (m, t, p) exceeds the limits above, or "".
func argon2CostTooHigh(m, t, p uint64) string {
	switch {
	case m > maxArgon2Memory:
		return fmt.Sprintf("memory must be <= %d KiB", maxArgon2Memory)
	case t > maxArgon2Time:
		return fmt.Sprintf("time must be <= %d", maxArgon2Time)
	case p > maxArgon2Threads:
		return fmt.Sprintf("threads must be <= %d", maxArgon2Threads)
	case m*t > maxArgon2Work:
		return fmt.Sprintf("memory*time must be <= %d", maxArgon2Work)
	}
	return ""
}

type argon2idHasher struct {
	memory  uint32
	time    uint32
	threads uint8
	keyLen  uint32
}

// NewArgon2idHasher creates a PasswordHasher that stores argon2id hashes as
// "$argon2id$v=19$m=65536,t=3,p=4$<salt>$<key>" (PHC string format). This is
// the recommended choice for new password storage.
//
// Zero-valued fields of cfg are filled from DefaultArgon2idConfig. It returns
// ErrInvalidParams when Memory < 8*Threads KiB, KeyLen is under 16 or over
// 1024, or the cost exceeds the limits Verify enforces.
func NewArgon2idHasher(cfg Argon2idConfig) (PasswordHasher, error) {
	def := DefaultArgon2idConfig()
	cfg.Memory = cmp.Or(cfg.Memory, def.Memory)
	cfg.Time = cmp.Or(cfg.Time, def.Time)
	cfg.Threads = cmp.Or(cfg.Threads, def.Threads)
	cfg.KeyLen = cmp.Or(cfg.KeyLen, def.KeyLen)

	if cfg.Memory < 8*uint32(cfg.Threads) {
		return nil, fmt.Errorf("%w: argon2id memory must be at least 8*Threads KiB", ErrInvalidParams)
	}
	if reason := argon2CostTooHigh(uint64(cfg.Memory), uint64(cfg.Time), uint64(cfg.Threads)); reason != "" {
		return nil, fmt.Errorf("%w: argon2id %s", ErrInvalidParams, reason)
	}
	if cfg.KeyLen < minKeyLen || cfg.KeyLen > maxArgon2KeyLen {
		return nil, fmt.Errorf("%w: KeyLen must be between %d and %d", ErrInvalidParams, minKeyLen, maxArgon2KeyLen)
	}
	return &argon2idHasher{memory: cfg.Memory, time: cfg.Time, threads: cfg.Threads, keyLen: cfg.KeyLen}, nil
}

func (h *argon2idHasher) Hash(password string) (string, error) {
	salt := make([]byte, saltLen)
	_, _ = rand.Read(salt) // never fails since Go 1.24
	key := argon2.IDKey([]byte(password), salt, h.time, h.memory, h.threads, h.keyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, h.memory, h.time, h.threads, b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}

func (h *argon2idHasher) Verify(password, encoded string) error {
	p, err := parseArgon2id(encoded)
	if err != nil {
		return err
	}
	key := argon2.IDKey([]byte(password), p.salt, p.time, p.memory, p.threads, uint32(len(p.key)))
	if subtle.ConstantTimeCompare(key, p.key) != 1 {
		return ErrMismatchedHash
	}
	return nil
}

func (h *argon2idHasher) NeedsRehash(encoded string) bool {
	p, err := parseArgon2id(encoded)
	if err != nil {
		return true
	}
	return p.memory != h.memory || p.time != h.time || p.threads != h.threads || uint32(len(p.key)) != h.keyLen
}

type argon2idEncoded struct {
	memory, time uint32
	threads      uint8
	salt, key    []byte
}

// parseArgon2id decodes and bounds-checks an encoded argon2id hash.
func parseArgon2id(encoded string) (argon2idEncoded, error) {
	var out argon2idEncoded
	// "" "argon2id" "v=19" "m=..,t=..,p=.." salt key
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return out, fmt.Errorf("%w: not an argon2id hash", ErrInvalidHash)
	}

	version, err := parseParams(parts[2], "v")
	if err != nil || version[0] != argon2.Version {
		return out, fmt.Errorf("%w: unsupported argon2 version", ErrInvalidHash)
	}

	params, err := parseParams(parts[3], "m", "t", "p")
	if err != nil {
		return out, err
	}
	m, t, p := params[0], params[1], params[2]
	if p < 1 || t < 1 || m < 8*p {
		return out, fmt.Errorf("%w: argon2id parameters out of range", ErrInvalidHash)
	}
	if reason := argon2CostTooHigh(uint64(m), uint64(t), uint64(p)); reason != "" {
		return out, fmt.Errorf("%w: argon2id %s", ErrInvalidHash, reason)
	}
	out.memory, out.time, out.threads = uint32(m), uint32(t), uint8(p)

	if out.salt, out.key, err = parseSaltAndKey(parts[4], parts[5]); err != nil {
		return argon2idEncoded{}, err
	}
	return out, nil
}
