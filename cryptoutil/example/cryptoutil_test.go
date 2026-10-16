package cryptoutil_test

import (
	"errors"
	"strings"
	"testing"

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
