package kafka

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kotel"
)

// ErrDLQPublish is returned (wrapped) by Consumer.Start when a failed record
// could not be published to the dead letter topic.
var ErrDLQPublish = errors.New("kafka: failed to publish to DLQ")

// Producer produces messages to Kafka topics.
// Produce and SendMessage are asynchronous; SendToDLQ blocks until the broker
// acknowledges the record and returns the produce error, if any.
type Producer interface {
	Produce(ctx context.Context, msg *Message) error
	SendMessage(ctx context.Context, topic string, payload any) error
	SendToDLQ(ctx context.Context, originalTopic string, value []byte, key []byte, dlqErr error) error
}

// Handler processes one consumed record.
//
// ctx is cancelled when the context given to Consumer.Start is, so a blocking
// handler can stop on shutdown. With Config.EnableOTel it also carries the
// record's receive span, so spans started from it join the producer's trace.
//
// A non-nil error is retried (ConsumerOptions.MaxRetries) and then sent to the
// DLQ. The record's dlq_error header holds the error's class or type, not its
// message (see WithDLQClass and WithDLQErrorFormatter).
type Handler func(ctx context.Context, record *kgo.Record) error

// Consumer consumes messages from Kafka topics.
// Start blocks until the context is cancelled, the client is closed, or a fatal error occurs.
type Consumer interface {
	Start(ctx context.Context, handler Handler) error
}

// HealthChecker provides health check functionality for Kafka.
type HealthChecker interface {
	Check(ctx context.Context) error
	CheckWithMetadata(ctx context.Context) (*ClusterHealth, error)
	LivenessCheck(ctx context.Context) error
}

// NewKafkaClientFromConfig creates a Kafka client from a Config struct.
//
// When cfg.GroupID is set the client is created with kgo.BlockRebalanceOnPoll:
// callers that poll the client directly must call AllowRebalance after handling
// each poll. Consumer.Start does this for you.
func NewKafkaClientFromConfig(ctx context.Context, cfg *Config) (*kgo.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),

		// Connection settings
		kgo.RequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		kgo.ConnIdleTimeout(cfg.ConnIdleTimeout),

		// Observability
		kgo.WithLogger(&SlogShim{L: cmp.Or(cfg.Logger, slog.Default())}),
	}
	opts = append(opts, producerOpts(cfg)...)

	// Consumer settings (only if GroupID is set)
	if cfg.GroupID != "" {
		opts = append(opts, consumerOpts(cfg)...)
	}
	if len(cfg.Topics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(cfg.Topics...))
	}

	securityOpts, err := securityOpts(cfg)
	if err != nil {
		return nil, err
	}
	opts = append(opts, securityOpts...)

	// OpenTelemetry hooks
	if cfg.EnableOTel {
		opts = append(opts, kgo.WithHooks(kotel.NewKotel().Hooks()...))
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

// producerOpts returns the producer settings of cfg.
func producerOpts(cfg *Config) []kgo.Opt {
	acks := kgo.AllISRAcks()
	switch cfg.RequiredAcks {
	case NoAck:
		acks = kgo.NoAck()
	case LeaderAck:
		acks = kgo.LeaderAck()
	}

	return []kgo.Opt{
		kgo.RequiredAcks(acks),
		kgo.RecordRetries(cfg.RecordRetries),
		kgo.RequestRetries(cfg.RequestRetries),
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			return time.Duration(tries) * 100 * time.Millisecond
		}),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.ProducerLinger(cfg.LingerDuration),
	}
}

// consumerOpts returns the consumer-group settings of cfg.
func consumerOpts(cfg *Config) []kgo.Opt {
	return []kgo.Opt{
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
	}
}

// securityOpts returns the TLS and SASL settings of cfg.
func securityOpts(cfg *Config) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(cmp.Or(cfg.TLSConfig, defaultTLSConfig())))
	}
	if cfg.SASLMechanism != "" {
		saslOpt, err := buildSASLOpt(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SASL: %w", err)
		}
		opts = append(opts, saslOpt)
	}
	return opts, nil
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

// SlogShim adapts *slog.Logger to the kgo.Logger interface. kgo.Logger carries
// no context and franz-go logs from background goroutines unrelated to any
// request, so records are logged with context.Background().
type SlogShim struct {
	L *slog.Logger
}

// Level reports the most verbose level the underlying logger enables, so
// franz-go debug logs appear only when the slog handler accepts them. It is
// evaluated on every call, so a changing slog.LevelVar is followed.
func (s *SlogShim) Level() kgo.LogLevel {
	ctx := context.Background()
	switch {
	case s.L.Enabled(ctx, slog.LevelDebug):
		return kgo.LogLevelDebug
	case s.L.Enabled(ctx, slog.LevelInfo):
		return kgo.LogLevelInfo
	case s.L.Enabled(ctx, slog.LevelWarn):
		return kgo.LogLevelWarn
	case s.L.Enabled(ctx, slog.LevelError):
		return kgo.LogLevelError
	default:
		return kgo.LogLevelNone
	}
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
