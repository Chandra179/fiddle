package kafka

import (
	"cmp"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

// saslMechanisms are the supported Config.SASLMechanism values.
var saslMechanisms = []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}

// defaultTLSConfig is used when TLS is enabled without an explicit config: TLS
// 1.2 or newer with the system roots.
func defaultTLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

// Config holds all configuration for Kafka client.
type Config struct {
	// Core settings
	Brokers []string
	Topics  []string
	GroupID string
	// Logger receives the client's own logs. franz-go's verbosity follows the
	// levels this logger enables: use a handler at slog.LevelDebug to see
	// franz-go debug logs.
	Logger *slog.Logger

	// Security - TLS
	TLSEnabled bool
	TLSConfig  *tls.Config // Default when nil and TLSEnabled: TLS 1.2+ with system roots

	// Security - SASL
	SASLMechanism string // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUsername  string
	SASLPassword  string
	// AllowPlaintextSASL permits SASL PLAIN without TLS, which sends credentials
	// in cleartext. Only intended for local development.
	AllowPlaintextSASL bool

	// Producer tuning
	RequiredAcks   RequiredAcks  // Default: AllISRAcks
	RecordRetries  int           // Default: 10
	RequestRetries int           // Default: 3
	BatchMaxBytes  int32         // Default: 1MB
	LingerDuration time.Duration // Default: 10ms

	// Consumer tuning
	FetchMaxWait      time.Duration // Default: 500ms
	FetchMinBytes     int32         // Default: 1
	FetchMaxBytes     int32         // Default: 50MB
	SessionTimeout    time.Duration // Default: 45s
	HeartbeatInterval time.Duration // Default: 3s
	RebalanceTimeout  time.Duration // Default: 60s

	// Connection settings
	RequestTimeoutOverhead time.Duration // Default: 10s
	ConnIdleTimeout        time.Duration // Default: 60s

	// Observability
	EnableOTel bool // Enable OpenTelemetry tracing
}

// RequiredAcks defines the acknowledgment level for produced messages.
type RequiredAcks int

const (
	// NoAck means the producer will not wait for any acknowledgment.
	NoAck RequiredAcks = 0
	// LeaderAck means the producer will wait for the leader to acknowledge.
	LeaderAck RequiredAcks = 1
	// AllISRAcks means the producer will wait for all in-sync replicas to acknowledge.
	AllISRAcks RequiredAcks = -1
)

// DefaultConfig returns a Config with production-ready defaults.
func DefaultConfig(brokers []string, groupID string) *Config {
	return &Config{
		Brokers: brokers,
		GroupID: groupID,
		Logger:  slog.Default(),

		// Producer defaults
		RequiredAcks:   AllISRAcks,
		RecordRetries:  10,
		RequestRetries: 3,
		BatchMaxBytes:  1_000_000, // 1MB
		LingerDuration: 10 * time.Millisecond,

		// Consumer defaults
		FetchMaxWait:      500 * time.Millisecond,
		FetchMinBytes:     1,
		FetchMaxBytes:     50 * 1024 * 1024, // 50MB
		SessionTimeout:    45 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		RebalanceTimeout:  60 * time.Second,

		// Connection defaults
		RequestTimeoutOverhead: 10 * time.Second,
		ConnIdleTimeout:        60 * time.Second,

		// Observability
		EnableOTel: true,
	}
}

// WithTopics sets the topics to consume.
func (c *Config) WithTopics(topics ...string) *Config {
	c.Topics = topics
	return c
}

// WithLogger sets a custom logger.
func (c *Config) WithLogger(logger *slog.Logger) *Config {
	c.Logger = logger
	return c
}

// WithTLS enables TLS with the provided configuration.
// If tlsConfig is nil, a default TLS config will be used.
func (c *Config) WithTLS(tlsConfig *tls.Config) *Config {
	c.TLSEnabled = true
	c.TLSConfig = cmp.Or(tlsConfig, defaultTLSConfig())
	return c
}

// WithSASLPlain configures SASL PLAIN authentication.
func (c *Config) WithSASLPlain(username, password string) *Config {
	c.SASLMechanism = "PLAIN"
	c.SASLUsername = username
	c.SASLPassword = password
	return c
}

// WithSASLScram configures SASL SCRAM authentication.
// mechanism should be "SCRAM-SHA-256" or "SCRAM-SHA-512".
func (c *Config) WithSASLScram(mechanism, username, password string) *Config {
	c.SASLMechanism = mechanism
	c.SASLUsername = username
	c.SASLPassword = password
	return c
}

// Validate ensures the configuration is valid for use.
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: at least one broker is required")
	}

	if err := c.validateSASL(); err != nil {
		return err
	}

	if c.RecordRetries < 0 {
		return errors.New("kafka: record retries must be non-negative")
	}

	if c.BatchMaxBytes <= 0 {
		return errors.New("kafka: batch max bytes must be positive")
	}

	if c.FetchMinBytes < 0 {
		return errors.New("kafka: fetch min bytes must be non-negative")
	}

	if c.FetchMaxBytes <= 0 {
		return errors.New("kafka: fetch max bytes must be positive")
	}

	return nil
}

// validateSASL checks the SASL settings; it accepts a config without SASL.
func (c *Config) validateSASL() error {
	if c.SASLMechanism == "" {
		return nil
	}
	if c.SASLUsername == "" || c.SASLPassword == "" {
		return errors.New("kafka: SASL username and password are required when SASL is enabled")
	}
	if !slices.Contains(saslMechanisms, c.SASLMechanism) {
		return fmt.Errorf("kafka: unsupported SASL mechanism: %s", c.SASLMechanism)
	}
	if c.SASLMechanism == "PLAIN" && !c.TLSEnabled && !c.AllowPlaintextSASL {
		return errors.New("kafka: SASL PLAIN requires TLS; enable TLS or set AllowPlaintextSASL")
	}
	return nil
}
