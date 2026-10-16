package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message represents a message to be produced to Kafka.
type Message struct {
	Topic    string
	Payload  any
	Key      string
	Headers  map[string]string
	Callback func(*kgo.Record, error)
}

// producer implements the Producer interface.
type producer struct {
	client *kgo.Client
	logger *slog.Logger
}

// ProducerOption configures a producer.
type ProducerOption func(*producer)

// WithProducerLogger sets the logger for the producer.
func WithProducerLogger(logger *slog.Logger) ProducerOption {
	return func(p *producer) {
		p.logger = logger
	}
}

// NewProducer creates a new Producer with the given client and options.
func NewProducer(client *kgo.Client, opts ...ProducerOption) Producer {
	p := &producer{
		client: client,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Produce sends a message asynchronously.
func (p *producer) Produce(ctx context.Context, msg *Message) error {
	data, err := p.marshalPayload(msg.Payload)
	if err != nil {
		return err
	}

	record := buildRecord(msg, data)

	callback := msg.Callback
	if callback == nil {
		callback = func(r *kgo.Record, err error) {
			if err != nil {
				p.logger.Error("produce error",
					"topic", r.Topic,
					"error", err,
				)
			}
		}
	}

	p.client.Produce(ctx, record, callback)

	return nil
}

// SendMessage is a convenience method to send a message with just topic and payload.
func (p *producer) SendMessage(ctx context.Context, topic string, payload any) error {
	return p.Produce(ctx, &Message{Topic: topic, Payload: payload})
}

// SendToDLQ sends a message to the dead letter queue and waits for the broker
// to acknowledge it.
func (p *producer) SendToDLQ(ctx context.Context, originalTopic string, value []byte, key []byte, dlqErr error) error {
	dlqTopic := originalTopic + ".dlq"

	errMsg := "unknown error"
	if dlqErr != nil {
		errMsg = dlqErr.Error()
	}

	record := buildRecord(&Message{
		Topic: dlqTopic,
		Key:   string(key),
		Headers: map[string]string{
			"origin_topic":  originalTopic,
			"dlq_timestamp": time.Now().Format(time.RFC3339),
			"dlq_error":     errMsg,
		},
	}, value)

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce to %s: %w", dlqTopic, err)
	}
	return nil
}

// buildRecord converts msg into a record carrying the already-encoded value.
func buildRecord(msg *Message, value []byte) *kgo.Record {
	record := &kgo.Record{
		Topic: msg.Topic,
		Value: value,
	}
	// franz-go's default partitioner hashes any non-nil key, so an empty key
	// must stay nil or every keyless message lands on the same partition.
	if msg.Key != "" {
		record.Key = []byte(msg.Key)
	}
	if len(msg.Headers) > 0 {
		record.Headers = make([]kgo.RecordHeader, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}
	return record
}

func (p *producer) marshalPayload(payload any) ([]byte, error) {
	if b, ok := payload.([]byte); ok {
		return b, nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}
	return data, nil
}
