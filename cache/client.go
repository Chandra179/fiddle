package cache

import (
	"context"
	"errors"
	"time"
)

var (
	ErrCacheMiss    = errors.New("cache miss")
	ErrCacheTimeout = errors.New("cache operation timeout")
)

// IsCacheMiss reports whether err represents a cache miss.
// Use instead of comparing to redis.Nil directly.
func IsCacheMiss(err error) bool {
	return errors.Is(err, ErrCacheMiss)
}

type Cache interface {
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, key string) error
	// DelIfValue atomically deletes key only if its current value equals value.
	// Returns true if the key was deleted. Use to release a lock you own.
	DelIfValue(ctx context.Context, key, value string) (bool, error)
	Ping(ctx context.Context) error
	Close() error
}
