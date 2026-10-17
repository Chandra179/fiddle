package rabbitmq

import (
	"cmp"
	"crypto/tls"
	"errors"
	"math"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrURLRequired is returned when the RabbitMQ URL is not provided
var ErrURLRequired = errors.New("rabbitmq: URL is required")

// QueueType represents the type of RabbitMQ queue
type QueueType string

const (
	// QueueTypeQuorum uses Raft consensus for data safety (recommended for production)
	QueueTypeQuorum QueueType = "quorum"
	// QueueTypeClassic is the traditional queue type (use only for transient data)
	QueueTypeClassic QueueType = "classic"
)

// Default values for RabbitMQ configuration
const (
	DefaultChannelPoolSize     = 10
	DefaultPrefetchCount       = 10
	DefaultReconnectInitialSec = 1
	DefaultReconnectMaxSec     = 60
	DefaultMaxRetries          = 3
	// DefaultRetryBackoff is the base delay between retries when RetryEnabled
	// is false; it doubles with every retry, up to maxRetryDelay.
	DefaultRetryBackoff = time.Second
)

// Config holds RabbitMQ configuration
type Config struct {
	// Connection settings
	URL            string
	ConnectionName string
	// TLSConfig is used for amqps:// URLs (custom CA, client certificates).
	// nil uses the system roots with Go's default TLS settings.
	TLSConfig *tls.Config

	// Producer settings
	PublisherConfirms  bool
	ChannelPoolSize    int
	Mandatory          bool
	PersistentDelivery bool

	// Consumer settings
	PrefetchCount int
	AutoAck       bool // Should always be false in production

	// Queue settings
	//
	// QueueType decides whether durable queues declared through the
	// TopologyManager are quorum queues. Other queue properties (durability,
	// auto-delete, ...) are set per queue in QueueConfig.
	QueueType QueueType

	// Retry/DLX settings.
	//
	// RetryEnabled: reject a failed delivery without requeue, so the queue's
	// dead-letter exchange routes it to the TTL retry queue (see
	// TopologyManager.SetupDeadLetterExchange). Without a DLX it is dropped.
	//
	// Without RetryEnabled, the client waits RetryBackoff (doubling per retry),
	// then republishes the message with an incremented "x-retry-count" header
	// and acks the original. This bounds retries on any queue type. The wait
	// blocks the consume loop, which slows the consumer while a dependency
	// keeps failing. If the ack is lost the message is delivered twice.
	//
	// RetryBackoff: zero uses DefaultRetryBackoff, negative retries at once.
	// Ignored when RetryEnabled is true.
	//
	// MaxRetries: retries after the first failure. Zero uses DefaultMaxRetries,
	// negative means no retries.
	//
	// DeadLetterEnabled: after MaxRetries, publish the message to
	// "<queue>.parking-lot" and ack it; otherwise reject it.
	RetryEnabled      bool
	RetryBackoff      time.Duration
	MaxRetries        int
	DeadLetterEnabled bool

	// Reconnection settings
	ReconnectInitialInterval time.Duration
	ReconnectMaxInterval     time.Duration

	// Connection tuning (0 = use RabbitMQ defaults / server-negotiated)
	Heartbeat  time.Duration
	ChannelMax int
	FrameSize  int

	// OnReturn is called when a mandatory message cannot be routed.
	// Must be set before NewClient; not changed during the client lifetime.
	OnReturn func(amqp.Return)
}

// NewDefaultConfig returns a Config with default values applied
func NewDefaultConfig() *Config {
	return &Config{
		ConnectionName:           "go-service",
		PublisherConfirms:        false,
		ChannelPoolSize:          DefaultChannelPoolSize,
		Mandatory:                false,
		PersistentDelivery:       false,
		PrefetchCount:            DefaultPrefetchCount,
		AutoAck:                  false,
		QueueType:                QueueTypeQuorum,
		RetryEnabled:             false,
		RetryBackoff:             DefaultRetryBackoff,
		MaxRetries:               DefaultMaxRetries,
		DeadLetterEnabled:        false,
		ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
		ReconnectMaxInterval:     DefaultReconnectMaxSec * time.Second,
	}
}

// ApplyDefaults sets every zero-valued field that has a default in
// NewDefaultConfig: ConnectionName, ChannelPoolSize, PrefetchCount, QueueType,
// RetryBackoff, MaxRetries, ReconnectInitialInterval and ReconnectMaxInterval.
// For RetryBackoff and MaxRetries an explicit negative value is kept and means
// "none" (see Config); for the other fields a negative value is left alone so
// Validate rejects it. Because zero means "unset", an explicit zero cannot be
// expressed: MaxRetries: 0 becomes DefaultMaxRetries.
//
// Boolean fields cannot be told apart from unset ones and are never touched;
// start from NewDefaultConfig to get its boolean defaults. URL has no default.
func (c *Config) ApplyDefaults() {
	def := NewDefaultConfig()

	c.ConnectionName = cmp.Or(c.ConnectionName, def.ConnectionName)
	c.ChannelPoolSize = cmp.Or(c.ChannelPoolSize, def.ChannelPoolSize)
	c.PrefetchCount = cmp.Or(c.PrefetchCount, def.PrefetchCount)
	c.QueueType = cmp.Or(c.QueueType, def.QueueType)
	c.RetryBackoff = cmp.Or(c.RetryBackoff, def.RetryBackoff)
	c.MaxRetries = cmp.Or(c.MaxRetries, def.MaxRetries)
	c.ReconnectInitialInterval = cmp.Or(c.ReconnectInitialInterval, def.ReconnectInitialInterval)
	c.ReconnectMaxInterval = cmp.Or(c.ReconnectMaxInterval, def.ReconnectMaxInterval)
}

// Validate reports the first invalid field. It does not modify the config; call
// ApplyDefaults first to fill unset fields (NewClient does both).
func (c *Config) Validate() error {
	if c.URL == "" {
		return ErrURLRequired
	}

	if c.ConnectionName == "" {
		return errors.New("rabbitmq: connection name is required")
	}

	if c.ChannelPoolSize <= 0 {
		return errors.New("rabbitmq: channel pool size must be positive")
	}

	if c.PrefetchCount <= 0 {
		return errors.New("rabbitmq: prefetch count must be positive")
	}

	if c.QueueType == "" {
		return errors.New("rabbitmq: queue type is required")
	}

	if c.ReconnectInitialInterval <= 0 {
		return errors.New("rabbitmq: reconnect initial interval must be positive")
	}

	if c.ReconnectMaxInterval <= 0 {
		return errors.New("rabbitmq: reconnect max interval must be positive")
	}

	if c.ChannelMax < 0 || c.ChannelMax > math.MaxUint16 {
		return errors.New("rabbitmq: channel max must be between 0 and 65535")
	}

	return nil
}

// QueueConfig represents configuration for a specific queue
type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]any
}

// ExchangeConfig represents configuration for an exchange
type ExchangeConfig struct {
	Name       string
	Kind       string // direct, fanout, topic, headers
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]any
}

// BindingConfig represents a queue binding
type BindingConfig struct {
	QueueName  string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Args       map[string]any
}
