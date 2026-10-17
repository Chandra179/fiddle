package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
// and rejected otherwise.
var ErrPoisonMessage = errors.New("rabbitmq: poison message")

// MessageHandler is called for each consumed message.
//
// The payload is delivery.Body, exactly as published: the consumer neither
// decodes it nor assumes a content type. Use JSONHandler for typed JSON.
//
// Returning nil acks the message. Returning an error applies the retry policy
// of Config (RetryEnabled, MaxRetries, DeadLetterEnabled); an error wrapping
// ErrPoisonMessage skips the retries.
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
	client *Client
	logger *slog.Logger
	tracer trace.Tracer
}

// NewConsumer creates a new Subscriber backed by client.
func NewConsumer(client *Client, logger *slog.Logger) Subscriber {
	return &Consumer{
		client: client,
		logger: logger,
		tracer: otel.Tracer("rabbitmq-consumer"),
	}
}

// ConsumeOptions contains options for consuming messages.
type ConsumeOptions struct {
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
				return errors.New("consumer cancelled by broker")
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
				c.logger.Error("failed to process delivery",
					"error", err,
					"delivery_tag", delivery.DeliveryTag,
				)
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

		c.logger.Warn("consumer disconnected, waiting for reconnection",
			"queue", opts.QueueName,
			"error", err,
		)

		if !c.waitForReconnect(ctx) {
			return ctx.Err()
		}

		c.logger.Info("client reconnected, restarting consumer", "queue", opts.QueueName)
	}
}

// waitForReconnect polls until the client is healthy or the context is cancelled.
func (c *Consumer) waitForReconnect(ctx context.Context) bool {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if c.client.IsHealthy() {
				return true
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

// processDelivery processes a single delivery with tracing and ack management.
func (c *Consumer) processDelivery(ctx context.Context, delivery amqp.Delivery, handler MessageHandler, autoAck bool, queue string) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, traceCarrier(delivery.Headers))

	ctx, span := c.tracer.Start(ctx, "rabbitmq.consume")
	defer span.End()

	if err := handler(ctx, delivery); err != nil {
		span.RecordError(err)

		if !autoAck {
			c.settleFailure(ctx, delivery, queue, err)
		}
		return fmt.Errorf("handler error: %w", err)
	}

	if autoAck {
		return nil
	}
	if err := delivery.Ack(false); err != nil {
		c.logger.Error("failed to acknowledge message", "error", err)
		return fmt.Errorf("failed to ack: %w", err)
	}
	return nil
}

// settleFailure settles a delivery whose handler returned err.
func (c *Consumer) settleFailure(ctx context.Context, d amqp.Delivery, queue string, err error) {
	cfg := c.client.config
	if errors.Is(err, ErrPoisonMessage) {
		settleExhausted(ctx, cfg, c.logger, c.client.republish, d, queue, "poison message")
		return
	}
	if !c.waitBeforeRetry(ctx, queue, d) {
		return
	}
	handleFailedDelivery(ctx, cfg, c.logger, c.client.republish, d, queue)
}

// waitBeforeRetry waits out the retry backoff for d and reports whether to go
// on settling it. When ctx ends first the delivery is left unacked: closing the
// consumer's channel makes the broker redeliver it.
func (c *Consumer) waitBeforeRetry(ctx context.Context, queue string, ds ...amqp.Delivery) bool {
	if err := waitBeforeRetry(ctx, c.client.config, queue, ds...); err != nil {
		c.logger.Warn("retry wait interrupted, leaving message unacked", "queue", queue, "error", err)
		return false
	}
	return true
}

// parkingLotSuffix names the queue that receives messages which exhausted
// their retries (see TopologyManager.SetupDeadLetterExchange).
const parkingLotSuffix = ".parking-lot"

// retryCountHeader carries the number of failed attempts when RetryEnabled is
// false and the client republishes the message itself.
const retryCountHeader = "x-retry-count"

// republishFunc publishes a copy of d to queue through the default exchange.
type republishFunc func(ctx context.Context, queue string, d amqp.Delivery) error

// maxRetryDelay caps the backoff between counted retries.
const maxRetryDelay = 30 * time.Second

// retryDelay is the wait before the retry that follows retryCount failures:
// base, doubling with every failure, capped at maxRetryDelay. A non-positive
// base means no delay.
func retryDelay(base time.Duration, retryCount int) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for range retryCount {
		if d >= maxRetryDelay {
			break
		}
		d *= 2
	}
	return min(d, maxRetryDelay)
}

// waitBeforeRetry sleeps for the longest backoff any of ds needs before its
// counted retry. It returns at once when RetryEnabled (the retry queue's TTL is
// the delay), when the backoff is disabled, or when none of ds will be retried,
// and returns ctx's error if ctx ends first.
func waitBeforeRetry(ctx context.Context, cfg *Config, queue string, ds ...amqp.Delivery) error {
	if cfg.RetryEnabled {
		return nil
	}
	var delay time.Duration
	for _, d := range ds {
		if n := getRetryCount(d, queue); n < cfg.MaxRetries {
			delay = max(delay, retryDelay(cfg.RetryBackoff, n))
		}
	}
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// handleFailedDelivery settles a delivery whose handler failed:
//
//   - under MaxRetries with RetryEnabled: Reject without requeue (the queue's
//     DLX routes it through the TTL retry queue);
//   - under MaxRetries without RetryEnabled: republish a copy to the queue with
//     an incremented "x-retry-count" header and Ack the original (Nack with
//     requeue if that publish fails), so the attempts are counted on any queue
//     type;
//   - at MaxRetries: settleExhausted.
//
// At MaxRetries a reject would not reach the parking lot: the DLX routes by the
// main queue's name, which leads back to the retry queue.
func handleFailedDelivery(ctx context.Context, cfg *Config, logger *slog.Logger, republish republishFunc, d amqp.Delivery, queue string) {
	retryCount := getRetryCount(d, queue)

	if retryCount >= cfg.MaxRetries {
		settleExhausted(ctx, cfg, logger, republish, d, queue, "max retries exceeded")
		return
	}

	if cfg.RetryEnabled {
		if err := d.Reject(false); err != nil {
			logger.Error("failed to settle message for retry", "error", err)
		}
		logger.Debug("message scheduled for retry",
			"queue", queue,
			"retry_count", retryCount,
			"via_dlx", true,
		)
		return
	}

	next := d
	next.Headers = withRetryCount(d.Headers, retryCount+1)
	if err := republish(ctx, queue, next); err != nil {
		logger.Error("failed to republish message for retry, requeueing",
			"queue", queue,
			"retry_count", retryCount,
			"error", err,
		)
		if nackErr := d.Nack(false, true); nackErr != nil {
			logger.Error("failed to nack message", "error", nackErr)
		}
		return
	}
	if err := d.Ack(false); err != nil {
		logger.Error("failed to ack message republished for retry", "error", err)
	}
	logger.Debug("message scheduled for retry",
		"queue", queue,
		"retry_count", retryCount,
		"via_dlx", false,
	)
}

// settleExhausted settles a delivery that will not be retried again: it is
// published to "<queue>.parking-lot" and acked when DeadLetterEnabled (Nack
// with requeue if that publish fails), and rejected otherwise.
func settleExhausted(ctx context.Context, cfg *Config, logger *slog.Logger, republish republishFunc, d amqp.Delivery, queue, reason string) {
	retryCount := getRetryCount(d, queue)

	if !cfg.DeadLetterEnabled {
		if err := d.Reject(false); err != nil {
			logger.Error("failed to reject message", "error", err)
		}
		logger.Warn("message rejected ("+reason+")",
			"queue", queue,
			"retry_count", retryCount,
			"max_retries", cfg.MaxRetries,
		)
		return
	}

	parkingLot := queue + parkingLotSuffix
	if err := republish(ctx, parkingLot, d); err != nil {
		logger.Error("failed to move message to parking lot, requeueing",
			"queue", queue,
			"parking_lot", parkingLot,
			"error", err,
		)
		if nackErr := d.Nack(false, true); nackErr != nil {
			logger.Error("failed to nack message", "error", nackErr)
		}
		return
	}
	if err := d.Ack(false); err != nil {
		logger.Error("failed to ack message moved to parking lot", "error", err)
	}
	logger.Warn("message moved to parking lot ("+reason+")",
		"queue", queue,
		"parking_lot", parkingLot,
		"retry_count", retryCount,
		"max_retries", cfg.MaxRetries,
	)
}

// withRetryCount returns a copy of headers with the retry counter set to n.
// The broker-managed x-delivery-count is dropped: n already includes it.
func withRetryCount(headers amqp.Table, n int) amqp.Table {
	out := amqp.Table{}
	maps.Copy(out, headers)
	delete(out, "x-delivery-count")
	out[retryCountHeader] = int64(n)
	return out
}

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

// republish publishes d to queue via the default exchange, waiting for the
// broker confirm when publisher confirms are enabled. A message with a
// per-message TTL gets its full TTL again, since the broker counts from enqueue.
func (c *Client) republish(ctx context.Context, queue string, d amqp.Delivery) error {
	ch, err := c.GetPublisherChannel(ctx)
	if err != nil {
		return err
	}
	defer c.ReturnPublisherChannel(ch)

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, publishingFrom(d))
	if err != nil {
		return err
	}
	return waitConfirm(ctx, dc)
}

// getRetryCount returns how many times d has failed on queue: the largest of
// the x-death count for queue, x-retry-count and x-delivery-count. Taking the
// largest never under-counts a message that went through more than one retry
// path.
func getRetryCount(d amqp.Delivery, queue string) int {
	count := xDeathCount(d.Headers, queue)
	for _, header := range []string{retryCountHeader, "x-delivery-count"} {
		if n, ok := toInt(d.Headers[header]); ok {
			count = max(count, n)
		}
	}
	return count
}

// xDeathCount returns the "count" of the x-death entry the broker recorded for
// queue, or 0 when the message was never dead-lettered from it.
func xDeathCount(headers amqp.Table, queue string) int {
	entries, _ := headers["x-death"].([]any)
	count := 0
	for _, entry := range entries {
		table, ok := entry.(amqp.Table)
		if !ok {
			continue
		}
		if q, _ := table["queue"].(string); q != queue {
			continue
		}
		if n, ok := toInt(table["count"]); ok {
			count = max(count, n)
		}
	}
	return count
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int64:
		return int(n), true
	case int32:
		return int(n), true
	case int:
		return n, true
	default:
		return 0, false
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
// error applies the retry policy of Config to every delivery of the batch.
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
// (parking lot when DeadLetterEnabled, rejected otherwise) and never reaches
// the handler, so one bad message cannot fail the batch it would sit in.
// Without a validator every delivery joins a batch.
func WithBatchValidator(validate func(amqp.Delivery) error) BatchOption {
	return func(bc *BatchConsumer) { bc.validate = validate }
}

// BatchConsumer consumes in batches. It owns its channel and acks a whole batch
// at once.
type BatchConsumer struct {
	client       *Client
	logger       *slog.Logger
	tracer       trace.Tracer
	batchSize    int
	flushTimeout time.Duration
	validate     func(amqp.Delivery) error
}

// NewBatchConsumer creates a batch consumer.
func NewBatchConsumer(client *Client, logger *slog.Logger, batchSize int, flushTimeout time.Duration, opts ...BatchOption) *BatchConsumer {
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
	ch, deliveries, err := bc.consume(opts)
	if err != nil {
		return err
	}

	b := &batcher{bc: bc, queue: opts.QueueName, handler: handler}
	flushTimer := time.NewTimer(bc.flushTimeout)
	defer flushTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush the remaining batch before exiting; ctx is already
			// cancelled, so give the handler and acks a bounded detached context.
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bc.flushTimeout)
			b.flushAndLog(shutdownCtx, "failed to flush batch on shutdown")
			cancel()
			_ = ch.Close()
			return ctx.Err()

		case <-flushTimer.C:
			b.flushAndLog(ctx, "failed to flush batch on timeout")
			flushTimer.Reset(bc.flushTimeout)

		case delivery, ok := <-deliveries:
			if !ok {
				b.flushAndLog(ctx, "failed to flush batch on channel close")
				_ = ch.Close()
				return errDeliveryChannelClosed
			}
			if b.add(ctx, delivery) {
				flushTimer.Reset(bc.flushTimeout)
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
	settleExhausted(ctx, bc.client.config, bc.logger, bc.client.republish, delivery, b.queue, "invalid message")
	return false
}

// flush hands the pending deliveries to the handler and settles them. On a
// handler error the retry policy is applied to each delivery.
func (b *batcher) flush(ctx context.Context) error {
	if len(b.pending) == 0 {
		return nil
	}
	batch := b.pending
	b.pending = nil

	if err := b.handler(ctx, batch); err != nil {
		// One backoff for the whole batch, then the retry policy per message.
		if werr := waitBeforeRetry(ctx, b.bc.client.config, b.queue, batch...); werr != nil {
			b.bc.logger.Warn("retry wait interrupted, leaving batch unacked", "queue", b.queue, "error", werr)
			return fmt.Errorf("batch handler error: %w", err)
		}
		for _, d := range batch {
			handleFailedDelivery(ctx, b.bc.client.config, b.bc.logger, b.bc.client.republish, d, b.queue)
		}
		return fmt.Errorf("batch handler error: %w", err)
	}

	// This consumer owns the channel and settles deliveries in order, so a
	// single multiple-ack on the last tag acks the whole batch.
	if err := batch[len(batch)-1].Ack(true); err != nil {
		b.bc.logger.Error("failed to ack batch", "error", err)
	}
	return nil
}

// flushAndLog flushes the batch and logs a failure under msg.
func (b *batcher) flushAndLog(ctx context.Context, msg string) {
	if err := b.flush(ctx); err != nil {
		b.bc.logger.Error(msg, "error", err)
	}
}
