package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kotel"
)

// Sentinel errors for the kafka package.
var (
	ErrProducerClosed = errors.New("kafka: producer is closed")
	ErrConsumerClosed = errors.New("kafka: consumer is closed")
	ErrDLQPublish     = errors.New("kafka: failed to publish to DLQ")
)

// Producer produces messages to Kafka topics.
// Produce and SendMessage are asynchronous; SendToDLQ blocks until the broker
// acknowledges the record and returns the produce error, if any.
type Producer interface {
	Produce(ctx context.Context, msg *Message) error
	SendMessage(ctx context.Context, topic string, payload any) error
	SendToDLQ(ctx context.Context, originalTopic string, value []byte, key []byte, dlqErr error) error
}

// Consumer consumes messages from Kafka topics.
// Start blocks until the context is cancelled, the client is closed, or a fatal error occurs.
type Consumer interface {
	Start(ctx context.Context, handler func(*kgo.Record) error) error
}

// HealthChecker provides health check functionality for Kafka.
type HealthChecker interface {
	Check(ctx context.Context) error
	CheckWithMetadata(ctx context.Context) (*ClusterHealth, error)
	LivenessCheck(ctx context.Context) error
}

// NewKafkaClient creates a Kafka client with the provided configuration.
//
// Deprecated: Use NewKafkaClientFromConfig for production use.
func NewKafkaClient(slogInstance *slog.Logger, brokers []string, topics []string, groupID string) (*kgo.Client, error) {
	cfg := DefaultConfig(brokers, groupID).
		WithTopics(topics...).
		WithLogger(slogInstance)
	return NewKafkaClientFromConfig(context.Background(), cfg)
}

// NewKafkaClientFromConfig creates a Kafka client from a Config struct.
// This is the recommended way to create clients in production.
//
// When cfg.GroupID is set the client is created with kgo.BlockRebalanceOnPoll:
// callers that poll the client directly must call AllowRebalance after handling
// each poll. Consumer.Start does this for you.
func NewKafkaClientFromConfig(ctx context.Context, cfg *Config) (*kgo.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),

		// Producer settings
		kgo.RecordRetries(cfg.RecordRetries),
		kgo.RequestRetries(cfg.RequestRetries),
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			return time.Duration(tries) * 100 * time.Millisecond
		}),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.ProducerLinger(cfg.LingerDuration),

		// Connection settings
		kgo.RequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		kgo.ConnIdleTimeout(cfg.ConnIdleTimeout),

		// Observability
		kgo.WithLogger(&SlogShim{L: logger}),
	}

	// Set required acks
	switch cfg.RequiredAcks {
	case NoAck:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	case LeaderAck:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}

	// Consumer settings (only if GroupID is set)
	if cfg.GroupID != "" {
		opts = append(opts,
			kgo.ConsumerGroup(cfg.GroupID),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
			kgo.DisableAutoCommit(),
			kgo.FetchMaxWait(cfg.FetchMaxWait),
			kgo.FetchMinBytes(cfg.FetchMinBytes),
			kgo.FetchMaxBytes(cfg.FetchMaxBytes),
			kgo.SessionTimeout(cfg.SessionTimeout),
			kgo.HeartbeatInterval(cfg.HeartbeatInterval),
			kgo.RebalanceTimeout(cfg.RebalanceTimeout),
			kgo.BlockRebalanceOnPoll(),
		)
	}

	// Topics to consume
	if len(cfg.Topics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(cfg.Topics...))
	}

	// TLS configuration
	if cfg.TLSEnabled {
		tlsConfig := cfg.TLSConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	// SASL authentication
	if cfg.SASLMechanism != "" {
		saslOpt, err := buildSASLOpt(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SASL: %w", err)
		}
		opts = append(opts, saslOpt)
	}

	// OpenTelemetry hooks
	if cfg.EnableOTel {
		k := kotel.NewKotel()
		opts = append(opts, kgo.WithHooks(k.Hooks()...))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	// Ping the cluster to verify connectivity
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Kafka: %w", err)
	}

	return client, nil
}

func buildSASLOpt(cfg *Config) (kgo.Opt, error) {
	switch cfg.SASLMechanism {
	case "PLAIN":
		mechanism := plain.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsMechanism()
		return kgo.SASL(mechanism), nil

	case "SCRAM-SHA-256":
		mechanism := scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha256Mechanism()
		return kgo.SASL(mechanism), nil

	case "SCRAM-SHA-512":
		mechanism := scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha512Mechanism()
		return kgo.SASL(mechanism), nil

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASLMechanism)
	}
}

// SlogShim adapts *slog.Logger to the kgo.Logger interface.
type SlogShim struct {
	L *slog.Logger
}

func (s *SlogShim) Level() kgo.LogLevel {
	return kgo.LogLevelInfo
}

func (s *SlogShim) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var slogLevel slog.Level
	switch level {
	case kgo.LogLevelError:
		slogLevel = slog.LevelError
	case kgo.LogLevelWarn:
		slogLevel = slog.LevelWarn
	case kgo.LogLevelInfo:
		slogLevel = slog.LevelInfo
	case kgo.LogLevelDebug:
		slogLevel = slog.LevelDebug
	default:
		return
	}
	s.L.Log(context.Background(), slogLevel, msg, keyvals...)
}
