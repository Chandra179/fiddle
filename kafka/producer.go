package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

// dlqSuffix is appended to a topic's name to form its dead letter topic.
const dlqSuffix = ".dlq"

// dlqTopic returns the dead letter topic of topic.
func dlqTopic(topic string) string { return topic + dlqSuffix }

// maxDLQErrorLen bounds the dlq_error header written by DLQErrorMessage.
const maxDLQErrorLen = 512

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
	client         *kgo.Client
	logger         *slog.Logger
	dlqErrorFormat func(error) string
}

// ProducerOption configures a producer.
type ProducerOption func(*producer)

// WithProducerLogger sets the logger for the producer.
func WithProducerLogger(logger *slog.Logger) ProducerOption {
	return func(p *producer) {
		p.logger = logger
	}
}

// WithDLQErrorFormatter sets how the error that sent a record to the DLQ is
// rendered into the record's dlq_error header. Return "" to omit the header.
//
// The default is DLQErrorType, which never writes the error message (tag
// errors with WithDLQClass to give DLQ consumers something to triage on). Use
// DLQErrorMessage to include the (truncated) text, or your own function to
// redact it, but only when handler errors cannot carry secrets or PII: the
// header is readable by everyone who can read the DLQ topic.
func WithDLQErrorFormatter(format func(error) string) ProducerOption {
	return func(p *producer) {
		if format != nil {
			p.dlqErrorFormat = format
		}
	}
}

// dlqClassed is implemented by errors tagged with WithDLQClass.
type dlqClassed interface {
	error
	DLQClass() string
}

// classedError tags an error with a short, caller-chosen class for the DLQ.
type classedError struct {
	err   error
	class string
}

func (e *classedError) Error() string    { return e.err.Error() }
func (e *classedError) Unwrap() error    { return e.err }
func (e *classedError) DLQClass() string { return e.class }

// WithDLQClass tags err with a stable class such as "db_timeout" or
// "invalid_payload". When the record is dead-lettered, the default formatter
// (DLQErrorType) writes this class into dlq_error and nothing else, so DLQ
// consumers can triage failures without the error message ever leaving the
// service. Only the class is published: keep it free of request data. The
// returned error still behaves like err (message, errors.Is and errors.As). A
// nil err stays nil.
func WithDLQClass(err error, class string) error {
	if err == nil {
		return nil
	}
	return &classedError{err: err, class: class}
}

// DLQErrorType is the default dlq_error formatter. It never writes the error
// message, which handlers may fill with request data. It reports the class set
// with WithDLQClass if any error in the chain carries one, and otherwise the Go
// type of the root cause (for example "*pgconn.PgError"), found by following
// Unwrap.
func DLQErrorType(err error) string {
	if tagged, ok := errors.AsType[dlqClassed](err); ok {
		return tagged.DLQClass()
	}
	for {
		var next error
		// This loop is the unwrapping, so each layer is inspected directly.
		switch u := err.(type) { //nolint:errorlint
		case interface{ Unwrap() error }:
			next = u.Unwrap()
		case interface{ Unwrap() []error }:
			if errs := u.Unwrap(); len(errs) > 0 {
				next = errs[0]
			}
		}
		if next == nil {
			return fmt.Sprintf("%T", err)
		}
		err = next
	}
}

// DLQErrorMessage is a dlq_error formatter that writes err.Error(), cut to 512
// bytes without splitting a UTF-8 sequence. Opt in with
// WithDLQErrorFormatter(DLQErrorMessage) only when handler errors never carry
// secrets or PII.
func DLQErrorMessage(err error) string {
	msg := err.Error()
	if len(msg) <= maxDLQErrorLen {
		return msg
	}
	cut := maxDLQErrorLen
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut]
}

// NewProducer creates a new Producer with the given client and options.
func NewProducer(client *kgo.Client, opts ...ProducerOption) Producer {
	p := &producer{
		client:         client,
		logger:         slog.Default(),
		dlqErrorFormat: DLQErrorType,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Produce sends a message asynchronously.
func (p *producer) Produce(ctx context.Context, msg *Message) error {
	data, err := marshalPayload(msg.Payload)
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

	errMsg := "unknown error"
	if dlqErr != nil {
		errMsg = p.dlqErrorFormat(dlqErr)
	}

	headers := map[string]string{
		"origin_topic":  originalTopic,
		"dlq_timestamp": time.Now().Format(time.RFC3339),
	}
	if errMsg != "" {
		headers["dlq_error"] = errMsg
	}
	record := buildRecord(&Message{
		Topic:   dlqTopic(originalTopic),
		Key:     string(key),
		Headers: headers,
	}, value)

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce to %s: %w", record.Topic, err)
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

func marshalPayload(payload any) ([]byte, error) {
	if b, ok := payload.([]byte); ok {
		return b, nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}
	return data, nil
}
