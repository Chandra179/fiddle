package cryptoutil_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"cryptoutil"
)

func TestSHA256(t *testing.T) {
	got := cryptoutil.SHA256([]byte("hello"))
	want := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

func TestSHA512(t *testing.T) {
	got := cryptoutil.SHA512([]byte("hello"))
	want := "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

func TestSHA256Bytes(t *testing.T) {
	out := cryptoutil.SHA256Bytes([]byte("hello"))
	if len(out) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(out))
	}
}

func TestSHA512Bytes(t *testing.T) {
	out := cryptoutil.SHA512Bytes([]byte("hello"))
	if len(out) != 64 {
		t.Fatalf("expected 64 bytes, got %d", len(out))
	}
}

func TestBcrypt_HashAndCompare(t *testing.T) {
	b, err := cryptoutil.NewBcrypter(cryptoutil.BcryptConfig{Cost: 4})
	if err != nil {
		t.Fatal(err)
	}

	password := "supersecret123"
	hash, err := b.Hash(password)
	if err != nil {
		t.Fatal(err)
	}
	if hash == "" {
		t.Fatal("expected non-empty hash")
	}

	err = b.Compare(password, hash)
	if err != nil {
		t.Fatal("expected match, got:", err)
	}

	err = b.Compare("wrongpassword", hash)
	if !cryptoutil.IsMismatchedHash(err) {
		t.Fatal("expected mismatch error, got:", err)
	}
}

func TestBcrypt_InvalidCost(t *testing.T) {
	_, err := cryptoutil.NewBcrypter(cryptoutil.BcryptConfig{Cost: 1})
	if !errors.Is(err, cryptoutil.ErrInvalidCost) {
		t.Fatal("expected ErrInvalidCost, got:", err)
	}
}

func TestBcrypt_DefaultCost(t *testing.T) {
	b, err := cryptoutil.NewBcrypter(cryptoutil.BcryptConfig{})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := b.Hash("test")
	if err != nil {
		t.Fatal(err)
	}
	if hash == "" {
		t.Fatal("expected non-empty hash")
	}
}

func TestScrypt_DeriveKey(t *testing.T) {
	s, err := cryptoutil.NewScrypter(cryptoutil.ScryptConfig{
		N: 16384, R: 8, P: 1, KeyLen: 32,
	})
	if err != nil {
		t.Fatal(err)
	}

	key, err := s.DeriveKey([]byte("password"), []byte("somesalt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(key))
	}
}

func TestScrypt_DefaultConfig(t *testing.T) {
	s, err := cryptoutil.NewScrypter(cryptoutil.ScryptConfig{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := s.DeriveKey([]byte("password"), []byte("salt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Fatalf("expected 32 bytes from default config, got %d", len(key))
	}
}

func TestScrypt_InvalidN(t *testing.T) {
	_, err := cryptoutil.NewScrypter(cryptoutil.ScryptConfig{N: 3, R: 8, P: 1, KeyLen: 32})
	if err == nil {
		t.Fatal("expected error for N=3 (not a power of two)")
	}
}

func TestScrypt_Deterministic(t *testing.T) {
	s, err := cryptoutil.NewScrypter(cryptoutil.ScryptConfig{
		N: 16384, R: 8, P: 1, KeyLen: 16,
	})
	if err != nil {
		t.Fatal(err)
	}

	k1, _ := s.DeriveKey([]byte("password"), []byte("salt"))
	k2, _ := s.DeriveKey([]byte("password"), []byte("salt"))
	if string(k1) != string(k2) {
		t.Fatal("expected deterministic output")
	}
}

func TestBcrypt_PasswordTooLong(t *testing.T) {
	b, err := cryptoutil.NewBcrypter(cryptoutil.BcryptConfig{Cost: 4})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Hash(strings.Repeat("a", 73))
	if !errors.Is(err, cryptoutil.ErrPasswordTooLong) {
		t.Fatal("expected ErrPasswordTooLong, got:", err)
	}
}

func TestScrypt_PartialConfigUsesDefaults(t *testing.T) {
	// Only N set: R, P and KeyLen must fall back to defaults instead of
	// failing at DeriveKey time.
	s, err := cryptoutil.NewScrypter(cryptoutil.ScryptConfig{N: 1024})
	if err != nil {
		t.Fatal(err)
	}
	key, err := s.DeriveKey([]byte("password"), []byte("salt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Fatalf("expected default 32-byte key, got %d", len(key))
	}
}

func TestScrypt_InvalidParams(t *testing.T) {
	cases := []cryptoutil.ScryptConfig{
		{N: 1024, R: 1 << 15, P: 1 << 15},
		{N: 1024, KeyLen: 8},
		{N: 1024, R: -1},
	}
	for _, cfg := range cases {
		if _, err := cryptoutil.NewScrypter(cfg); !errors.Is(err, cryptoutil.ErrInvalidParams) {
			t.Fatalf("cfg %+v: expected ErrInvalidParams, got %v", cfg, err)
		}
	}
}

func TestScrypt_VerifyKey(t *testing.T) {
	s, err := cryptoutil.NewScrypter(cryptoutil.ScryptConfig{N: 1024})
	if err != nil {
		t.Fatal(err)
	}
	salt := []byte("somesalt")
	key, err := s.DeriveKey([]byte("password"), salt)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyKey([]byte("password"), salt, key); err != nil {
		t.Fatal("expected match, got:", err)
	}
	if err := s.VerifyKey([]byte("wrong"), salt, key); !cryptoutil.IsMismatchedHash(err) {
		t.Fatal("expected mismatch, got:", err)
	}
}

func TestScrypt_DefaultMeetsOWASPMinimum(t *testing.T) {
	cfg := cryptoutil.DefaultScryptConfig()
	if cfg.N < 1<<17 || cfg.R < 8 || cfg.P < 1 {
		t.Fatalf("default scrypt config %+v is below N=2^17, r=8, p=1", cfg)
	}
}

func TestScryptHasher_HashAndVerify(t *testing.T) {
	h, err := cryptoutil.NewScryptHasher(cryptoutil.ScryptConfig{N: 1024})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := h.Hash("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$scrypt$ln=10,r=8,p=1$") {
		t.Fatalf("unexpected encoding %q", hash)
	}
	if err := h.Verify("hunter2", hash); err != nil {
		t.Fatal("expected match, got:", err)
	}
	if err := h.Verify("hunter3", hash); !cryptoutil.IsMismatchedHash(err) {
		t.Fatal("expected mismatch, got:", err)
	}
	if h.NeedsRehash(hash) {
		t.Fatal("hash made with current params must not need a rehash")
	}

	other, err := h.Hash("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	if other == hash {
		t.Fatal("two hashes of the same password must differ (random salt)")
	}
}

// A hash stays verifiable after the hasher's parameters are raised, and is
// flagged for rehash. This is what makes raising the cost safe.
func TestScryptHasher_VerifyUsesStoredParams(t *testing.T) {
	old, err := cryptoutil.NewScryptHasher(cryptoutil.ScryptConfig{N: 1024})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := old.Hash("pw-123456")
	if err != nil {
		t.Fatal(err)
	}

	raised, err := cryptoutil.NewScryptHasher(cryptoutil.ScryptConfig{N: 2048, R: 8, P: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := raised.Verify("pw-123456", hash); err != nil {
		t.Fatal("hash made with old params must still verify, got:", err)
	}
	if !raised.NeedsRehash(hash) {
		t.Fatal("hash made with old params must need a rehash")
	}
}

func TestScryptHasher_Interop(t *testing.T) {
	// Produced independently with Python's hashlib.scrypt(b"password",
	// salt=b"somesalt", n=1024, r=8, p=1, dklen=32).
	const encoded = "$scrypt$ln=10,r=8,p=1$c29tZXNhbHQ$wdXoWEig5T693O7BJbufEPRk+qarG40BYOh1xe9tMAc"
	h, err := cryptoutil.NewScryptHasher(cryptoutil.ScryptConfig{N: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Verify("password", encoded); err != nil {
		t.Fatal("expected interop hash to verify, got:", err)
	}
}

func TestPasswordHasher_RejectsInvalidHash(t *testing.T) {
	sh, err := cryptoutil.NewScryptHasher(cryptoutil.ScryptConfig{N: 1024})
	if err != nil {
		t.Fatal(err)
	}
	ah, err := cryptoutil.NewArgon2idHasher(cryptoutil.Argon2idConfig{Memory: 64, Time: 1, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	shHash, _ := sh.Hash("pw")
	ahHash, _ := ah.Hash("pw")

	bad := []string{
		"",
		"plain",
		"$scrypt$ln=10,r=8,p=1$c29tZXNhbHQ", // missing key
		"$scrypt$ln=10,r=8$c29tZXNhbHQ$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y",     // missing p
		"$scrypt$ln=40,r=8,p=1$c29tZXNhbHQ$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y", // N too large
		"$scrypt$ln=30,r=8,p=1$c29tZXNhbHQ$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y", // memory bomb
		"$scrypt$ln=10,r=8,p=1$!!$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y",          // bad salt
		"$scrypt$ln=10,r=8,p=1$c29tZXNhbHQ$AAAA",                                        // key too short
		"$argon2id$v=19$m=64,t=1,p=1$c29tZXNhbHQ$AAAA",                                  // key too short
		"$argon2id$v=18$m=64,t=1,p=1$c29tZXNhbHQ$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y",
		"$argon2id$v=19$m=4294967295,t=1,p=1$c29tZXNhbHQ$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y",
		"$argon2id$v=19$m=64,t=999999,p=1$c29tZXNhbHQ$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y",
		"$argon2i$v=19$m=64,t=1,p=1$c29tZXNhbHQ$aSyoXBPjRBBQuaFB/uXSpKPYS4NhKkCcSONN32uUL0Y",
		shHash, // right format, wrong hasher, for the argon2id check below
		ahHash,
	}
	for _, enc := range bad[:len(bad)-2] {
		if err := sh.Verify("pw", enc); !errors.Is(err, cryptoutil.ErrInvalidHash) {
			t.Errorf("scrypt Verify(%q): expected ErrInvalidHash, got %v", enc, err)
		}
		if err := ah.Verify("pw", enc); !errors.Is(err, cryptoutil.ErrInvalidHash) {
			t.Errorf("argon2id Verify(%q): expected ErrInvalidHash, got %v", enc, err)
		}
		if !sh.NeedsRehash(enc) || !ah.NeedsRehash(enc) {
			t.Errorf("unusable hash %q must be flagged for rehash", enc)
		}
	}
	if err := ah.Verify("pw", shHash); !errors.Is(err, cryptoutil.ErrInvalidHash) {
		t.Errorf("argon2id must reject an scrypt hash, got %v", err)
	}
	if err := sh.Verify("pw", ahHash); !errors.Is(err, cryptoutil.ErrInvalidHash) {
		t.Errorf("scrypt must reject an argon2id hash, got %v", err)
	}
}

func TestArgon2idHasher_HashAndVerify(t *testing.T) {
	h, err := cryptoutil.NewArgon2idHasher(cryptoutil.Argon2idConfig{Memory: 64, Time: 1, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := h.Hash("correct horse")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$argon2id$v=19$m=64,t=1,p=1$") {
		t.Fatalf("unexpected encoding %q", hash)
	}
	if err := h.Verify("correct horse", hash); err != nil {
		t.Fatal("expected match, got:", err)
	}
	if err := h.Verify("battery staple", hash); !cryptoutil.IsMismatchedHash(err) {
		t.Fatal("expected mismatch, got:", err)
	}
	if h.NeedsRehash(hash) {
		t.Fatal("hash made with current params must not need a rehash")
	}

	raised, err := cryptoutil.NewArgon2idHasher(cryptoutil.Argon2idConfig{Memory: 128, Time: 2, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := raised.Verify("correct horse", hash); err != nil {
		t.Fatal("hash made with old params must still verify, got:", err)
	}
	if !raised.NeedsRehash(hash) {
		t.Fatal("hash made with old params must need a rehash")
	}
}

func TestArgon2idHasher_DefaultsAndInvalidParams(t *testing.T) {
	if _, err := cryptoutil.NewArgon2idHasher(cryptoutil.Argon2idConfig{}); err != nil {
		t.Fatal("zero config must use the defaults, got:", err)
	}
	cases := []cryptoutil.Argon2idConfig{
		{Memory: 4, Threads: 1},       // below 8*threads
		{Memory: 1 << 30},             // above cap
		{Time: 1 << 20},               // above cap
		{Memory: 64, KeyLen: 8},       // key too short
		{Memory: 64, KeyLen: 1 << 20}, // key too long
	}
	for _, cfg := range cases {
		if _, err := cryptoutil.NewArgon2idHasher(cfg); !errors.Is(err, cryptoutil.ErrInvalidParams) {
			t.Errorf("cfg %+v: expected ErrInvalidParams, got %v", cfg, err)
		}
	}
}

// A known-answer vector in PHC string format for argon2id (password "password",
// salt "somesalt", m=65536, t=2, p=1, 32-byte key). It was not produced by this
// package, so it pins the encoding (parameter order, unpadded base64, version
// field) to the format other implementations read and write.
func TestArgon2idHasher_KnownAnswerVector(t *testing.T) {
	const encoded = "$argon2id$v=19$m=65536,t=2,p=1$c29tZXNhbHQ$CTFhFdXPJO1aFaMaO6Mm5c8y7cJHAph8ArZWb2GRPPc"

	h, err := cryptoutil.NewArgon2idHasher(cryptoutil.Argon2idConfig{Memory: 65536, Time: 2, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Verify("password", encoded); err != nil {
		t.Fatal("reference vector must verify, got:", err)
	}
	if err := h.Verify("Password", encoded); !cryptoutil.IsMismatchedHash(err) {
		t.Fatal("wrong password must mismatch, got:", err)
	}
	if h.NeedsRehash(encoded) {
		t.Fatal("a hash with the hasher's own parameters must not need a rehash")
	}
}

// Verify reads its cost parameters from the stored hash, so they must be
// bounded in CPU as well as memory.
func TestPasswordHasher_RejectsExcessiveCost(t *testing.T) {
	const salt, key = "c29tZXNhbHQ", "wdXoWEig5T693O7BJbufEPRk+qarG40BYOh1xe9tMAc"
	sh, err := cryptoutil.NewScryptHasher(cryptoutil.ScryptConfig{N: 1024})
	if err != nil {
		t.Fatal(err)
	}
	ah, err := cryptoutil.NewArgon2idHasher(cryptoutil.Argon2idConfig{Memory: 64, Time: 1, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}

	hostile := map[string]string{
		"scrypt p=1024 (1024x the CPU)":   "$scrypt$ln=17,r=8,p=1024$" + salt + "$" + key,
		"scrypt p above the cap":          "$scrypt$ln=10,r=8,p=17$" + salt + "$" + key,
		"scrypt r above the cap":          "$scrypt$ln=10,r=33,p=1$" + salt + "$" + key,
		"scrypt work N*r*p above the cap": "$scrypt$ln=18,r=8,p=16$" + salt + "$" + key,
		"scrypt memory above the cap":     "$scrypt$ln=20,r=8,p=1$" + salt + "$" + key,
		"argon2id t above the cap":        "$argon2id$v=19$m=64,t=11,p=1$" + salt + "$" + key,
		"argon2id p above the cap":        "$argon2id$v=19$m=1024,t=1,p=17$" + salt + "$" + key,
		"argon2id memory above the cap":   "$argon2id$v=19$m=524289,t=1,p=1$" + salt + "$" + key,
		"argon2id 64 passes over 1 GiB":   "$argon2id$v=19$m=1048576,t=64,p=255$" + salt + "$" + key,
		"argon2id m*t above the cap":      "$argon2id$v=19$m=524288,t=5,p=1$" + salt + "$" + key,
	}
	for name, enc := range hostile {
		h := sh
		if strings.HasPrefix(enc, "$argon2id") {
			h = ah
		}
		start := time.Now()
		if err := h.Verify("pw", enc); !errors.Is(err, cryptoutil.ErrInvalidHash) {
			t.Errorf("%s: expected ErrInvalidHash, got %v", name, err)
		}
		if time.Since(start) > time.Second {
			t.Errorf("%s: must be rejected before any hashing work, took %v", name, time.Since(start))
		}
	}
}

// The constructors apply the limits Verify applies, so a hasher cannot write a
// hash it would refuse to verify; the defaults must be inside them.
func TestPasswordHashers_ConstructorsEnforceVerifyLimits(t *testing.T) {
	if _, err := cryptoutil.NewScryptHasher(cryptoutil.ScryptConfig{}); err != nil {
		t.Fatal("default scrypt params must be within the limits:", err)
	}
	if _, err := cryptoutil.NewArgon2idHasher(cryptoutil.Argon2idConfig{}); err != nil {
		t.Fatal("default argon2id params must be within the limits:", err)
	}

	for _, cfg := range []cryptoutil.ScryptConfig{
		{N: 1 << 17, R: 8, P: 17},
		{N: 1 << 17, R: 33},
		{N: 1 << 20, R: 8},
		{N: 1 << 18, R: 8, P: 16},
	} {
		if _, err := cryptoutil.NewScryptHasher(cfg); !errors.Is(err, cryptoutil.ErrInvalidParams) {
			t.Errorf("scrypt %+v: expected ErrInvalidParams, got %v", cfg, err)
		}
	}
	for _, cfg := range []cryptoutil.Argon2idConfig{
		{Memory: 64, Time: 11},
		{Memory: 1024, Threads: 17},
		{Memory: 524288, Time: 5},
		{Memory: 524289},
	} {
		if _, err := cryptoutil.NewArgon2idHasher(cfg); !errors.Is(err, cryptoutil.ErrInvalidParams) {
			t.Errorf("argon2id %+v: expected ErrInvalidParams, got %v", cfg, err)
		}
	}

	// The raw KDF stays unrestricted: its parameters are the caller's own.
	if _, err := cryptoutil.NewScrypter(cryptoutil.ScryptConfig{N: 1 << 18, R: 8, P: 16}); err != nil {
		t.Fatal("NewScrypter must not be limited:", err)
	}
}
