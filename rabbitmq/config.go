package rabbitmq

import (
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
	DefaultRetryTTLSec         = 30
	DefaultMaxRetries          = 3
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
	QueueType  QueueType
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool

	// Retry/DLX settings.
	//
	// RetryEnabled: a failed delivery is rejected without requeue so it flows
	// through the queue's dead-letter exchange into the TTL retry queue (see
	// TopologyManager.SetupDeadLetterExchange / SetupQueueWithDLX). Without a
	// DLX on the queue, rejected messages are dropped. When false, failed
	// deliveries are requeued immediately.
	//
	// DeadLetterEnabled: once MaxRetries is reached the message is published
	// to "<queue>.parking-lot" and acked. When false it is rejected instead.
	RetryEnabled      bool
	RetryTTL          time.Duration
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
		Durable:                  true,
		AutoDelete:               false,
		Exclusive:                false,
		NoWait:                   false,
		RetryEnabled:             false,
		RetryTTL:                 DefaultRetryTTLSec * time.Second,
		MaxRetries:               DefaultMaxRetries,
		DeadLetterEnabled:        false,
		ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
		ReconnectMaxInterval:     DefaultReconnectMaxSec * time.Second,
	}
}

// ApplyDefaults validates required config fields.
// Returns an error if any required field is not set.
// Note: All fields are required - values must be loaded from the YAML configuration.
func (c *Config) ApplyDefaults() error {
	if c.URL == "" {
		return ErrURLRequired
	}

	if c.ConnectionName == "" {
		return errors.New("rabbitmq: connection name is required")
	}

	if c.ChannelPoolSize <= 0 {
		return errors.New("rabbitmq: channel pool size is required")
	}

	if c.PrefetchCount <= 0 {
		return errors.New("rabbitmq: prefetch count is required")
	}

	if c.QueueType == "" {
		return errors.New("rabbitmq: queue type is required")
	}

	if c.RetryTTL <= 0 {
		return errors.New("rabbitmq: retry TTL is required")
	}

	if c.MaxRetries <= 0 {
		return errors.New("rabbitmq: max retries is required")
	}

	if c.ReconnectInitialInterval <= 0 {
		return errors.New("rabbitmq: reconnect initial interval is required")
	}

	if c.ReconnectMaxInterval <= 0 {
		return errors.New("rabbitmq: reconnect max interval is required")
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
