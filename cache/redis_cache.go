package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisCache implements Cache on a single Redis instance. It does not suit
// multi-datacenter setups, reads that must be strongly consistent (replication
// is asynchronous), or values over 512MB. It is unexported so callers depend
// only on the Cache interface.
type redisCache struct {
	client *redis.Client
}

// RedisConfig configures NewRedisCacheFromConfig. Zero values fall back to go-redis
// defaults (e.g. DialTimeout 5s, Read/WriteTimeout 3s, PoolSize 10*GOMAXPROCS).
type RedisConfig struct {
	Addr     string // host:port
	Username string // ACL user (Redis 6+); empty for default user
	Password string
	DB       int
	// TLSConfig enables TLS when non-nil.
	TLSConfig    *tls.Config
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

// NewRedisCacheFromConfig creates a Cache from cfg (auth, TLS, DB selection,
// pool and timeouts). It does not connect: a connection failure shows up on the
// first operation.
func NewRedisCacheFromConfig(cfg RedisConfig) Cache {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		TLSConfig:    cfg.TLSConfig,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
	})
	return &redisCache{client: rdb}
}

// NewRedisCache creates a Cache for the Redis at addr (host:port) with no auth
// and no TLS; use NewRedisCacheFromConfig for anything else. It does not
// connect: a connection failure shows up on the first operation.
func NewRedisCache(addr string) Cache {
	return NewRedisCacheFromConfig(RedisConfig{Addr: addr})
}

// Set stores value under key with ttl, overwriting any existing value. Use SetNX
// to write only if the key is absent.
func (r *redisCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

// Get returns the value for key, or ErrCacheMiss if it is missing. There is no
// bulk read: call it once per key.
func (r *redisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrCacheMiss
	}
	return val, err
}

// Del removes key. It is a no-op if the key is missing.
func (r *redisCache) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// delIfValueScript deletes KEYS[1] only if its value equals ARGV[1].
var delIfValueScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// DelIfValue atomically deletes key only if its value equals value (a Lua
// compare-and-delete) and reports whether it did. Use it to release a lock you
// own, so a lock re-acquired by someone else after yours expired stays intact.
func (r *redisCache) DelIfValue(ctx context.Context, key, value string) (bool, error) {
	n, err := delIfValueScript.Run(ctx, r.client, []string{key}, value).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// SetNX atomically sets key only if it does not exist and reports whether it
// did. For a lock, store a random token as the value and release it with
// DelIfValue, not Del. The TTL is not extended and the lock is not reentrant.
func (r *redisCache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, value, ttl).Result()
}

// Ping sends PING to Redis. A Redis that answers can still reject writes, for
// example when it is out of memory.
func (r *redisCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close closes the Redis client; later operations fail. Call it once: a second
// Close returns an error.
func (r *redisCache) Close() error {
	return r.client.Close()
}
