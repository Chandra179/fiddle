package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements Cache backed by a single Redis instance.
// Use for high-throughput key-value cache with TTL expiry.
// Not suitable for: multi-datacenter setups (no replication awareness),
// strongly-consistent reads (Redis async replication), or values >512MB.
type RedisCache struct {
	client *redis.Client
	addr   string
}

// RedisConfig configures a RedisCache. Zero values fall back to go-redis
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

// NewRedisCacheFromConfig creates a Cache from cfg. Use for auth, TLS,
// DB selection and pool/timeouts tuning.
// On connect failure the first op will error, not this call.
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
	return &RedisCache{client: rdb, addr: cfg.Addr}
}

// NewRedisCache connects to a Redis instance at addr (host:port) with
// defaults: no auth, no TLS. Use NewRedisCacheFromConfig for anything else.
// On connect failure the first op will error, not this call.
func NewRedisCache(addr string) Cache {
	return NewRedisCacheFromConfig(RedisConfig{Addr: addr})
}

// Addr returns the Redis server address this cache connects to.
func (r *RedisCache) Addr() string { return r.addr }

// Set stores value under key with ttl. Overwrites existing key.
// Use for: fresh writes, cache-warming, TTL refreshes.
// Limitation: no atomic existence check — use SetNX for that.
func (r *RedisCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

// Get retrieves value for key. Returns ErrCacheMiss if missing.
// Use for: cache reads, hot-path lookups.
// Limitation: no bulk/pipe — call N times for N keys.
func (r *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrCacheMiss
	}
	return val, err
}

// Del removes key. No-op if key missing.
// Use for: cache invalidation on write, manual eviction.
// Limitation: single-node — no cross-shard delete in cluster.
func (r *RedisCache) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// delIfValueScript deletes KEYS[1] only if its value equals ARGV[1].
var delIfValueScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// DelIfValue atomically deletes key only if its value equals value
// (compare-and-delete via Lua). Returns true if the key was deleted.
// Use for: releasing a lock you own (value = your random token), so an
// expired-and-reacquired lock held by someone else is left intact.
func (r *RedisCache) DelIfValue(ctx context.Context, key, value string) (bool, error) {
	n, err := delIfValueScript.Run(ctx, r.client, []string{key}, value).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// SetNX atomically sets key only if it does not exist. Returns true on success.
// Use for: distributed locks, deduplication, first-writer-wins.
// Limitation: not a reentrant lock — no TTL extension built in. Store a random
// token as value and release with DelIfValue, not Del.
func (r *RedisCache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	err := r.client.SetArgs(ctx, key, value, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Err()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Ping checks Redis connection liveness. Returns nil if reachable.
// Use for: startup health-check, readiness probe.
// Limitation: checks TCP only, not storage — a full Redis may still fail on ops.
func (r *RedisCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close shuts down the underlying Redis connection. Call once on app shutdown.
// Further ops will error. Not idempotent: a second Close returns an error.
func (r *RedisCache) Close() error {
	return r.client.Close()
}
