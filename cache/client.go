// Package cache defines a minimal key-value cache interface. Redis is the
// bundled implementation: use NewRedisCacheFromConfig to build one.
package cache

import (
	"context"
	"errors"
	"time"
)

// ErrCacheMiss is returned by Cache.Get when the key does not exist.
var ErrCacheMiss = errors.New("cache miss")

// ErrInvalidTTL is returned by Cache.SetNX when ttl is not positive. A lock
// without an expiry is never released if its holder crashes.
var ErrInvalidTTL = errors.New("ttl must be positive")

// IsCacheMiss reports whether err represents a cache miss.
// Use it instead of comparing errors to a backend's nil sentinel.
func IsCacheMiss(err error) bool {
	return errors.Is(err, ErrCacheMiss)
}

// Cache is a key-value store with per-key expiry. Redis implements it; see
// NewRedisCacheFromConfig. cacheaside works with any implementation.
type Cache interface {
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	// SetNX stores value only if key is absent and reports whether it did. ttl
	// must be positive, otherwise it returns ErrInvalidTTL.
	SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, key string) error
	// DelIfValue atomically deletes key only if its current value equals value.
	// Returns true if the key was deleted. Use to release a lock you own.
	DelIfValue(ctx context.Context, key, value string) (bool, error)
	Ping(ctx context.Context) error
	Close() error
}
