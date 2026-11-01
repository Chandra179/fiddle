package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ErrPoisonMessage marks a message that can never be processed, such as a
// malformed payload. When a MessageHandler returns an error wrapping it the
// consumer skips the retries and settles the message as if MaxRetries was
// already reached: it is moved to "<queue>.parking-lot" when DeadLetterEnabled,
// and otherwise preserved while consumption stops.
var ErrPoisonMessage = errors.New("rabbitmq: poison message")

// MessageHandler is called for each consumed message.
//
// The payload is delivery.Body, exactly as published: the consumer neither
// decodes it nor assumes a content type. Use JSONHandler for typed JSON.
//
// Returning nil acknowledges the message. RetryPolicy classifies failures;
// unknown failures stop consumption after bounded in-place retries. An error
// wrapping ErrPoisonMessage is permanent and skips retries. Without a usable
// DLQ the original is preserved and consumption stops.
type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

// JSONHandler adapts a typed handler to a MessageHandler. The body is decoded
// into T with json.Unmarshal; a body that does not decode makes the message a
// poison message (ErrPoisonMessage), since redelivering it cannot help.
//
// Decode into a struct, or use json.Number fields, for numbers that must keep
// their precision: with T = any, JSON numbers become float64.
func JSONHandler[T any](fn func(ctx context.Context, msg T, delivery amqp.Delivery) error) MessageHandler {
	return func(ctx context.Context, d amqp.Delivery) error {
		var msg T
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			return fmt.Errorf("%w: decode JSON body: %w", ErrPoisonMessage, err)
		}
		return fn(ctx, msg, d)
	}
}

// Consumer handles message consumption with QoS and OTel tracing.
type Consumer struct {
	client     *Client
	logger     *slog.Logger
	tracer     trace.Tracer
	policy     RetryPolicy
	deadLetter *DeadLetterConfig
}

// NewConsumer creates a new Subscriber backed by client.
func NewConsumer(client *Client, logger *slog.Logger) Subscriber {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		client: client,
		logger: logger,
		tracer: otel.Tracer("rabbitmq-consumer"),
	}
}

// ConsumeOptions contains options for consuming messages.
type ConsumeOptions struct {
	RetryPolicy   *RetryPolicy
	DeadLetter    *DeadLetterConfig
	QueueName     string
	Consumer      string // Consumer tag
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Args          map[string]any
	AutoReconnect bool // When true, the consumer restarts after delivery channel closure.
}

// errDeliveryChannelClosed ends a consume loop whose delivery channel closed.
var errDeliveryChannelClosed = errors.New("delivery channel closed")

// StartConsumer starts consuming messages from a queue.
// If opts.AutoReconnect is true, it will automatically restart on delivery channel closure.
func (c *Consumer) StartConsumer(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	if c.client == nil || handler == nil {
		return errors.New("consumer requires a client and handler")
	}
	if opts.QueueName == "" {
		return errors.New("source queue is required")
	}
	if opts.AutoAck || c.client.config.AutoAck {
		return errors.New("reliable consumption requires manual acknowledgements")
	}
	local := *c
	local.policy, local.deadLetter = subscriptionPolicy(c.client.config, opts)
	if err := errors.Join(local.policy.validate(local.deadLetter != nil), local.deadLetter.validate(opts.QueueName)); err != nil {
		return err
	}
	if err := local.client.prepareDeadLetter(ctx, local.deadLetter); err != nil {
		return fmt.Errorf("prepare dead-letter queue: %w", err)
	}
	c = &local
	if opts.AutoReconnect {
		return c.startConsumerWithReconnect(ctx, opts, handler)
	}
	return c.startConsumeLoop(ctx, opts, handler)
}

// startConsumeLoop runs the core consume loop on a single channel.
func (c *Consumer) startConsumeLoop(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	ch, err := c.client.CreateConsumerChannel()
	if err != nil {
		return fmt.Errorf("failed to create consumer channel: %w", err)
	}

	defer func() { _ = ch.Close() }()
	// PrefetchCount was validated by NewClient and is positive.
	prefetchCount := c.client.config.PrefetchCount
	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	autoAck := c.client.config.AutoAck || opts.AutoAck

	deliveries, err := ch.Consume(
		opts.QueueName,
		opts.Consumer,
		autoAck,
		opts.Exclusive,
		opts.NoLocal,
		opts.NoWait,
		opts.Args,
	)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	cancelChan := ch.NotifyCancel(make(chan string, 1))

	c.logger.Info("started consuming",
		"queue", opts.QueueName,
		"consumer", opts.Consumer,
		"auto_ack", autoAck,
		"prefetch", prefetchCount,
	)

	for {
		select {
		case <-ctx.Done():
			_ = ch.Close()
			return ctx.Err()

		case reason, ok := <-cancelChan:
			_ = ch.Close()
			if !ok {
				return errDeliveryChannelClosed
			}
			c.logger.Warn("consumer cancelled by broker",
				"queue", opts.QueueName,
				"reason", reason,
			)
			return fmt.Errorf("consumer cancelled by broker: %s", reason)

		case delivery, ok := <-deliveries:
			if !ok {
				c.logger.Warn("delivery channel closed")
				_ = ch.Close()
				return errDeliveryChannelClosed
			}

			if err := c.processDelivery(ctx, delivery, handler, autoAck, opts.QueueName); err != nil {
				_ = ch.Close()
				return err
			}
		}
	}
}

// startConsumerWithReconnect wraps startConsumeLoop and restarts it after
// delivery channel closures, waiting for the client to reconnect.
func (c *Consumer) startConsumerWithReconnect(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	// Use a copy without AutoReconnect to avoid recursion
	innerOpts := opts
	innerOpts.AutoReconnect = false

	for {
		err := c.startConsumeLoop(ctx, innerOpts, handler)
		if err == nil || ctx.Err() != nil {
			return err
		}
		if !errors.Is(err, errDeliveryChannelClosed) && !errors.Is(err, ErrNotConnected) {
			return err
		}

		c.logger.Warn("consumer disconnected, waiting for reconnection",
			"queue", opts.QueueName,
			"error", err,
		)

		if err := c.waitForReconnect(ctx); err != nil {
			return err
		}

		c.logger.Info("client reconnected, restarting consumer", "queue", opts.QueueName)
	}
}

// waitForReconnect polls until the client is healthy or the context is cancelled.
func (c *Consumer) waitForReconnect(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.client.closeChan:
			return ErrClientClosed
		case <-ticker.C:
			if c.client.IsHealthy() {
				return nil
			}
		}
	}
}

// traceCarrier returns the string headers of a delivery, which is where trace
// context propagates.
func traceCarrier(headers amqp.Table) propagation.MapCarrier {
	carrier := make(propagation.MapCarrier, len(headers))
	for k, v := range headers {
		if str, ok := v.(string); ok {
			carrier[k] = str
		}
	}
	return carrier
}

// subscriptionPolicy copies per-subscription settings. Legacy fields only
// supply retry counts/delays; broker-side retry routing is no longer used.
func subscriptionPolicy(cfg *Config, opts ConsumeOptions) (RetryPolicy, *DeadLetterConfig) {
	policy := opts.RetryPolicy.normalized()
	if opts.RetryPolicy == nil {
		policy.MaxRetries = max(0, cfg.MaxRetries)
		if cfg.RetryBackoff > 0 {
			policy.InitialBackoff = cfg.RetryBackoff
			policy.MaxBackoff = max(policy.MaxBackoff, cfg.RetryBackoff)
		}
	}
	classify := policy.Classify
	policy.Classify = func(err error) FailureKind {
		if errors.Is(err, ErrPoisonMessage) {
			return PermanentFailure
		}
		if classify != nil {
			return classify(err)
		}
		return UnknownFailure
	}
	dlq := opts.DeadLetter
	if dlq == nil && cfg.DeadLetterEnabled {
		dlq = &DeadLetterConfig{}
	}
	return policy, dlq.normalized(opts.QueueName)
}

// processDelivery retries in place; an outage never republishes the input.
func (c *Consumer) processDelivery(ctx context.Context, delivery amqp.Delivery, handler MessageHandler, autoAck bool, queue string) error {
	if autoAck {
		return errors.New("reliable consumption requires manual acknowledgements")
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, traceCarrier(delivery.Headers))
	ctx, span := c.tracer.Start(ctx, "rabbitmq.consume")
	defer span.End()
	failure := c.policy.process(ctx, func(attemptCtx context.Context) error { return handler(attemptCtx, delivery) })
	if err := ctx.Err(); err != nil {
		return err
	}
	if failure.err != nil {
		span.RecordError(failure.err)
	}
	return settleProcessed(delivery, failure, func() error {
		if c.deadLetter == nil {
			return failure.paused()
		}
		return c.client.park(ctx, c.deadLetter, delivery, queue, failure)
	})
}

// settleProcessed acknowledges failures only after the replacement is confirmed
// and routed. Its caller closes the channel on error, preserving unacked input.
func settleProcessed(d amqp.Delivery, failure processingResult, publish func() error) error {
	if failure.err != nil {
		if !failure.park {
			return failure.paused()
		}
		if err := publish(); err != nil {
			return err
		}
	}
	if err := d.Ack(false); err != nil {
		return fmt.Errorf("acknowledge delivery: %w", err)
	}
	return nil
}

const parkingLotSuffix = ".parking-lot"

// publishingFrom rebuilds d as a Publishing: body, headers (including x-death
// history) and the properties that describe the message, Expiration included
// so a per-message TTL survives a retry. UserId is left out on purpose: the
// broker rejects a publish whose user-id differs from the connection's user,
// which would break retries when producer and consumer use different accounts.
func publishingFrom(d amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Headers:         d.Headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}

// ConsumeWithDefaults consumes queueName with manual acks and no auto-reconnect.
func (c *Consumer) ConsumeWithDefaults(ctx context.Context, queueName string, handler MessageHandler) error {
	return c.StartConsumer(ctx, ConsumeOptions{
		QueueName: queueName,
		Consumer:  "",
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}, handler)
}

// BatchHandler is called with a batch of deliveries; each payload is the
// delivery's Body, undecoded. Returning nil acks the whole batch. Returning an
// error applies the subscription RetryPolicy to the whole batch.
type BatchHandler func(ctx context.Context, deliveries []amqp.Delivery) error

// JSONBatchHandler adapts a typed handler to a BatchHandler. Every body is
// decoded into T with json.Unmarshal; msgs[i] belongs to deliveries[i].
//
// A body that does not decode fails the whole batch (the error wraps
// ErrPoisonMessage), and the batch is retried as one. To keep malformed
// messages out of batches in the first place, also give the BatchConsumer
// WithBatchValidator(ValidJSON).
func JSONBatchHandler[T any](fn func(ctx context.Context, msgs []T, deliveries []amqp.Delivery) error) BatchHandler {
	return func(ctx context.Context, deliveries []amqp.Delivery) error {
		msgs := make([]T, len(deliveries))
		for i, d := range deliveries {
			if err := json.Unmarshal(d.Body, &msgs[i]); err != nil {
				return fmt.Errorf("%w: decode JSON body of message %d: %w", ErrPoisonMessage, i, err)
			}
		}
		return fn(ctx, msgs, deliveries)
	}
}

// ValidJSON is a BatchConsumer validator (see WithBatchValidator) that accepts
// only deliveries whose body is valid JSON.
func ValidJSON(d amqp.Delivery) error {
	if !json.Valid(d.Body) {
		return fmt.Errorf("%w: body is not valid JSON", ErrPoisonMessage)
	}
	return nil
}

// BatchOption configures a BatchConsumer.
type BatchOption func(*BatchConsumer)

// WithBatchValidator sets a check run on every delivery before it joins a
// batch. A delivery that fails is settled on its own as a poison message
// (parked with DeadLetterConfig, otherwise preserved) and never reaches
// the handler, so one bad message cannot fail the batch it would sit in.
// Without a validator every delivery joins a batch.
func WithBatchValidator(validate func(amqp.Delivery) error) BatchOption {
	return func(bc *BatchConsumer) { bc.validate = validate }
}

// BatchConsumer owns its channel and settles each delivery independently, so
// a failed validator or transfer cannot be acknowledged by a later batch.
type BatchConsumer struct {
	client       *Client
	logger       *slog.Logger
	tracer       trace.Tracer
	batchSize    int
	flushTimeout time.Duration
	validate     func(amqp.Delivery) error
	policy       RetryPolicy
	deadLetter   *DeadLetterConfig
}

// NewBatchConsumer creates a batch consumer.
func NewBatchConsumer(client *Client, logger *slog.Logger, batchSize int, flushTimeout time.Duration, opts ...BatchOption) *BatchConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	if flushTimeout <= 0 {
		flushTimeout = time.Second
	}
	bc := &BatchConsumer{
		client:       client,
		logger:       logger,
		tracer:       otel.Tracer("rabbitmq-batch-consumer"),
		batchSize:    batchSize,
		flushTimeout: flushTimeout,
	}
	for _, opt := range opts {
		opt(bc)
	}
	return bc
}

// StartBatchConsumer consumes opts.QueueName in batches of the configured size,
// flushing a partial batch after the flush timeout. It returns when ctx ends or
// the delivery channel closes.
func (bc *BatchConsumer) StartBatchConsumer(ctx context.Context, opts ConsumeOptions, handler BatchHandler) error {
	if bc.client == nil || handler == nil || opts.QueueName == "" {
		return errors.New("batch consumer requires client, queue and handler")
	}
	if opts.AutoAck || bc.client.config.AutoAck {
		return errors.New("reliable consumption requires manual acknowledgements")
	}
	local := *bc
	local.policy, local.deadLetter = subscriptionPolicy(bc.client.config, opts)
	if err := errors.Join(local.policy.validate(local.deadLetter != nil), local.deadLetter.validate(opts.QueueName)); err != nil {
		return err
	}
	if err := local.client.prepareDeadLetter(ctx, local.deadLetter); err != nil {
		return fmt.Errorf("prepare dead-letter queue: %w", err)
	}
	bc = &local
	ch, deliveries, err := bc.consume(opts)
	if err != nil {
		return err
	}

	defer func() { _ = ch.Close() }()
	b := &batcher{bc: bc, queue: opts.QueueName, handler: handler}
	flushTimer := time.NewTimer(bc.flushTimeout)
	defer flushTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = ch.Close()
			return ctx.Err()

		case <-flushTimer.C:
			b.flushAndLog(ctx, "failed to flush batch on timeout")
			if b.err != nil {
				_ = ch.Close()
				return b.err
			}
			flushTimer.Reset(bc.flushTimeout)

		case delivery, ok := <-deliveries:
			if !ok {
				_ = ch.Close()
				return errDeliveryChannelClosed
			}
			if b.add(ctx, delivery) {
				flushTimer.Reset(bc.flushTimeout)
			}
			if b.err != nil {
				_ = ch.Close()
				return b.err
			}
		}
	}
}

// consume opens the batch consumer's channel and starts consuming opts.QueueName
// with manual acks.
func (bc *BatchConsumer) consume(opts ConsumeOptions) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := bc.client.CreateConsumerChannel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create consumer channel: %w", err)
	}

	// Prefetch below the batch size would cap every batch at the prefetch and
	// make each one wait for the flush timeout.
	prefetchCount := max(bc.client.config.PrefetchCount, bc.size())
	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	deliveries, err := ch.Consume(
		opts.QueueName,
		opts.Consumer,
		false, // never auto-ack for batch consumer
		opts.Exclusive,
		opts.NoLocal,
		opts.NoWait,
		opts.Args,
	)
	if err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("failed to start consuming: %w", err)
	}

	bc.logger.Info("batch consumer started",
		"queue", opts.QueueName,
		"batch_size", bc.size(),
		"flush_timeout", bc.flushTimeout,
	)
	return ch, deliveries, nil
}

// size is the number of deliveries that triggers a flush: at least one.
func (bc *BatchConsumer) size() int { return max(bc.batchSize, 1) }

// batcher accumulates the deliveries of one StartBatchConsumer run and settles
// them a batch at a time. It is used from the run's goroutine only.
type batcher struct {
	bc      *BatchConsumer
	queue   string
	handler BatchHandler
	pending []amqp.Delivery
	err     error
}

// add appends delivery to the batch unless it is invalid, flushing when the
// batch is full. It reports whether a flush happened.
func (b *batcher) add(ctx context.Context, delivery amqp.Delivery) (flushed bool) {
	if !b.accept(ctx, delivery) {
		return false
	}

	b.pending = append(b.pending, delivery)
	if len(b.pending) < b.bc.size() {
		return false
	}
	b.flushAndLog(ctx, "failed to flush full batch")
	return true
}

// accept reports whether delivery may join the batch. An invalid delivery is
// settled on its own as a poison message and never reaches the handler.
func (b *batcher) accept(ctx context.Context, delivery amqp.Delivery) bool {
	bc := b.bc

	msgCtx := otel.GetTextMapPropagator().Extract(ctx, traceCarrier(delivery.Headers))
	_, span := bc.tracer.Start(msgCtx, "rabbitmq.batch-consume")
	defer span.End()

	if bc.validate == nil {
		return true
	}
	err := bc.validate(delivery)
	if err == nil {
		return true
	}

	span.RecordError(err)
	bc.logger.Warn("invalid message, not batched", "queue", b.queue, "error", err)
	failure := processingResult{err: err, kind: PermanentFailure, attempts: 1, park: true}
	b.err = settleProcessed(delivery, failure, func() error {
		if bc.deadLetter == nil {
			return failure.paused()
		}
		return bc.client.park(ctx, bc.deadLetter, delivery, b.queue, failure)
	})
	return false
}

// flush hands the pending deliveries to the handler and settles them. On a
// handler error the retry policy is applied to each delivery.
func (b *batcher) flush(ctx context.Context) error {
	if len(b.pending) == 0 {
		return nil
	}
	batch := b.pending
	failure := b.bc.policy.process(ctx, func(attemptCtx context.Context) error { return b.handler(attemptCtx, batch) })
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, delivery := range batch {
		err := settleProcessed(delivery, failure, func() error {
			if b.bc.deadLetter == nil {
				return failure.paused()
			}
			return b.bc.client.park(ctx, b.bc.deadLetter, delivery, b.queue, failure)
		})
		if err != nil {
			return err
		}
	}
	b.pending = nil
	return nil
}

// flushAndLog flushes the batch and logs a failure under msg.
func (b *batcher) flushAndLog(ctx context.Context, msg string) {
	if err := b.flush(ctx); err != nil {
		b.err = err
		b.bc.logger.Error(msg, "error", err)
	}
}
