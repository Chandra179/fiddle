package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// MessageHandler is called for each consumed message.
type MessageHandler func(ctx context.Context, msg any, delivery amqp.Delivery) error

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

	prefetchCount := c.client.config.PrefetchCount
	if prefetchCount <= 0 {
		ch.Close()
		return fmt.Errorf("prefetchCount must be > 0")
	}

	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	autoAck := c.client.config.AutoAck
	if opts.AutoAck {
		autoAck = true
	}

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
		ch.Close()
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
			ch.Close()
			return ctx.Err()

		case reason, ok := <-cancelChan:
			ch.Close()
			if ok {
				c.logger.Warn("consumer cancelled by broker",
					"queue", opts.QueueName,
					"reason", reason,
				)
				return fmt.Errorf("consumer cancelled by broker: %s", reason)
			}
			return fmt.Errorf("consumer cancelled by broker")

		case delivery, ok := <-deliveries:
			if !ok {
				c.logger.Warn("delivery channel closed")
				ch.Close()
				return fmt.Errorf("delivery channel closed")
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

// processDelivery processes a single delivery with tracing and ack management.
func (c *Consumer) processDelivery(ctx context.Context, delivery amqp.Delivery, handler MessageHandler, autoAck bool, queue string) error {
	// Extract trace context from headers
	carrier := propagation.MapCarrier{}
	for k, v := range delivery.Headers {
		if strVal, ok := v.(string); ok {
			carrier[k] = strVal
		}
	}

	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	ctx, span := c.tracer.Start(ctx, "rabbitmq.consume",
		trace.WithAttributes(),
	)
	defer span.End()

	var msg any
	if err := json.Unmarshal(delivery.Body, &msg); err != nil {
		span.RecordError(err)

		if !autoAck {
			if rejectErr := delivery.Reject(false); rejectErr != nil {
				c.logger.Error("failed to reject message", "error", rejectErr)
			}
		}
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if err := handler(ctx, msg, delivery); err != nil {
		span.RecordError(err)

		if !autoAck {
			handleFailedDelivery(ctx, c.client, c.logger, delivery, queue)
		}
		return fmt.Errorf("handler error: %w", err)
	}

	if !autoAck {
		if err := delivery.Ack(false); err != nil {
			c.logger.Error("failed to acknowledge message", "error", err)
			return fmt.Errorf("failed to ack: %w", err)
		}
	}

	return nil
}

// parkingLotSuffix names the queue that receives messages which exhausted
// their retries (see TopologyManager.SetupDeadLetterExchange).
const parkingLotSuffix = ".parking-lot"

// handleFailedDelivery settles a delivery whose handler failed:
//
//   - under MaxRetries: Reject without requeue when RetryEnabled (the queue's
//     DLX routes it through the TTL retry queue), else Nack with requeue;
//   - at MaxRetries: publish a copy to "<queue>.parking-lot" and Ack when
//     DeadLetterEnabled (Nack with requeue if that publish fails), else Reject.
//
// Rejecting at MaxRetries is not enough on its own: the DLX routes by the main
// queue name, which leads back into the retry queue rather than the parking lot.
func handleFailedDelivery(ctx context.Context, client *Client, logger *slog.Logger, d amqp.Delivery, queue string) {
	cfg := client.config
	retryCount := getRetryCount(d, queue)

	if retryCount < cfg.MaxRetries {
		var err error
		if cfg.RetryEnabled {
			err = d.Reject(false)
		} else {
			err = d.Nack(false, true)
		}
		if err != nil {
			logger.Error("failed to settle message for retry", "error", err)
		}
		logger.Debug("message scheduled for retry",
			"queue", queue,
			"retry_count", retryCount,
			"via_dlx", cfg.RetryEnabled,
		)
		return
	}

	if !cfg.DeadLetterEnabled {
		if err := d.Reject(false); err != nil {
			logger.Error("failed to reject message", "error", err)
		}
		logger.Warn("message rejected (max retries exceeded)",
			"queue", queue,
			"retry_count", retryCount,
			"max_retries", cfg.MaxRetries,
		)
		return
	}

	parkingLot := queue + parkingLotSuffix
	if err := publishCopy(ctx, client, parkingLot, d); err != nil {
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
	logger.Warn("message moved to parking lot (max retries exceeded)",
		"queue", queue,
		"parking_lot", parkingLot,
		"retry_count", retryCount,
		"max_retries", cfg.MaxRetries,
	)
}

// publishCopy republishes d unchanged (body, properties and headers, including
// x-death history) to queue via the default exchange, waiting for the broker
// confirm when publisher confirms are enabled.
func publishCopy(ctx context.Context, client *Client, queue string, d amqp.Delivery) error {
	ch, err := client.GetPublisherChannel(ctx)
	if err != nil {
		return err
	}
	defer client.ReturnPublisherChannel(ch)

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
		Headers:         d.Headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	})
	if err != nil {
		return err
	}
	return waitConfirm(ctx, dc)
}

// getRetryCount returns how many times d has already failed on queue.
//
// It reads the "count" of the x-death entry for queue (incremented each time
// the message is dead-lettered from it), falling back to the x-delivery-count
// header that quorum queues set on redelivery after a requeue.
func getRetryCount(d amqp.Delivery, queue string) int {
	if xDeath, ok := d.Headers["x-death"].([]any); ok {
		for _, entry := range xDeath {
			table, ok := entry.(amqp.Table)
			if !ok {
				continue
			}
			if q, _ := table["queue"].(string); q != queue {
				continue
			}
			if n, ok := toInt(table["count"]); ok {
				return n
			}
		}
	}
	if n, ok := toInt(d.Headers["x-delivery-count"]); ok {
		return n
	}
	return 0
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

// ConsumeWithDefaults starts consuming with sensible defaults.
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

// BatchHandler is called with a batch of messages.
type BatchHandler func(ctx context.Context, messages []any, deliveries []amqp.Delivery) error

// BatchConsumer handles batch message consumption.
// It manages its own AMQP channel and ack lifecycle, avoiding double-ack issues.
type BatchConsumer struct {
	client       *Client
	logger       *slog.Logger
	tracer       trace.Tracer
	batchSize    int
	flushTimeout time.Duration
}

// NewBatchConsumer creates a batch consumer.
func NewBatchConsumer(client *Client, logger *slog.Logger, batchSize int, flushTimeout time.Duration) *BatchConsumer {
	return &BatchConsumer{
		client:       client,
		logger:       logger,
		tracer:       otel.Tracer("rabbitmq-batch-consumer"),
		batchSize:    batchSize,
		flushTimeout: flushTimeout,
	}
}

// StartBatchConsumer starts consuming messages in batches.
// It creates its own channel and handles acking per-batch to avoid double-ack.
func (bc *BatchConsumer) StartBatchConsumer(ctx context.Context, opts ConsumeOptions, handler BatchHandler) error {
	ch, err := bc.client.CreateConsumerChannel()
	if err != nil {
		return fmt.Errorf("failed to create consumer channel: %w", err)
	}

	batchSize := max(bc.batchSize, 1)

	// Prefetch below the batch size would cap every batch at the prefetch and
	// make each one wait for the flush timeout.
	prefetchCount := max(bc.client.config.PrefetchCount, batchSize)
	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
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
		ch.Close()
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	bc.logger.Info("batch consumer started",
		"queue", opts.QueueName,
		"batch_size", batchSize,
		"flush_timeout", bc.flushTimeout,
	)

	var batch []any
	var batchDeliveries []amqp.Delivery
	flushTimer := time.NewTimer(bc.flushTimeout)
	defer flushTimer.Stop()

	flush := func(ctx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		defer func() {
			batch = nil
			batchDeliveries = nil
		}()

		if err := handler(ctx, batch, batchDeliveries); err != nil {
			// Apply the retry policy per message so a failing batch cannot
			// hot-loop through immediate requeues.
			for _, d := range batchDeliveries {
				handleFailedDelivery(ctx, bc.client, bc.logger, d, opts.QueueName)
			}
			return fmt.Errorf("batch handler error: %w", err)
		}

		// This consumer owns the channel and settles deliveries in order, so a
		// single multiple-ack on the last tag acks the whole batch.
		if ackErr := batchDeliveries[len(batchDeliveries)-1].Ack(true); ackErr != nil {
			bc.logger.Error("failed to ack batch", "error", ackErr)
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// Flush remaining batch before exiting; ctx is already cancelled,
			// so give the handler and acks a bounded detached context.
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bc.flushTimeout)
			if flushErr := flush(shutdownCtx); flushErr != nil {
				bc.logger.Error("failed to flush batch on shutdown", "error", flushErr)
			}
			cancel()
			ch.Close()
			return ctx.Err()

		case <-flushTimer.C:
			if err := flush(ctx); err != nil {
				bc.logger.Error("failed to flush batch on timeout", "error", err)
			}
			flushTimer.Reset(bc.flushTimeout)

		case delivery, ok := <-deliveries:
			if !ok {
				if flushErr := flush(ctx); flushErr != nil {
					bc.logger.Error("failed to flush batch on channel close", "error", flushErr)
				}
				ch.Close()
				return fmt.Errorf("delivery channel closed")
			}

			// Extract trace context
			carrier := propagation.MapCarrier{}
			for k, v := range delivery.Headers {
				if strVal, ok := v.(string); ok {
					carrier[k] = strVal
				}
			}
			msgCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
			_, span := bc.tracer.Start(msgCtx, "rabbitmq.batch-consume")

			var msg any
			if err := json.Unmarshal(delivery.Body, &msg); err != nil {
				span.RecordError(err)
				span.End()
				// Reject poison pill without requeue
				if rejectErr := delivery.Reject(false); rejectErr != nil {
					bc.logger.Error("failed to reject poison message", "error", rejectErr)
				}
				continue
			}

			span.End()
			batch = append(batch, msg)
			batchDeliveries = append(batchDeliveries, delivery)

			if len(batch) >= batchSize {
				if err := flush(ctx); err != nil {
					bc.logger.Error("failed to flush full batch", "error", err)
				}
				flushTimer.Reset(bc.flushTimeout)
			}
		}
	}
}
