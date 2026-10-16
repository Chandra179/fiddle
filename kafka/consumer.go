package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CommitMode defines when offsets are committed.
type CommitMode int

// CommitRecord commits the offsets of successfully processed records once per poll.
// Ensures no message loss during shutdown - in-flight messages won't be committed
// and will be redelivered on restart (at-least-once delivery semantics).
const CommitRecord CommitMode = iota

// commitTimeout bounds the offset commit issued after each poll.
const commitTimeout = 10 * time.Second

// ConsumerOptions configures consumer behavior.
type ConsumerOptions struct {
	DLQProducer  Producer
	OnDLQPublish func(topic string, err error)
	MaxRetries   int
	RetryBackoff time.Duration
	Logger       *slog.Logger
}

// consumer implements the Consumer interface.
type consumer struct {
	client  *kgo.Client
	options ConsumerOptions
}

// NewConsumer creates a new Consumer with the given client and options.
func NewConsumer(client *kgo.Client, opts ...ConsumerOptions) Consumer {
	options := ConsumerOptions{
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
		Logger:       slog.Default(),
	}
	for _, o := range opts {
		if o.MaxRetries > 0 {
			options.MaxRetries = o.MaxRetries
		}
		if o.RetryBackoff > 0 {
			options.RetryBackoff = o.RetryBackoff
		}
		if o.Logger != nil {
			options.Logger = o.Logger
		}
		options.DLQProducer = o.DLQProducer
		options.OnDLQPublish = o.OnDLQPublish
	}
	return &consumer{client: client, options: options}
}

// Start begins consuming messages and processing them with the handler.
// It blocks until the context is cancelled or the client is closed.
//
// Offsets of processed records are committed once per poll. If a DLQ producer is
// configured and publishing a failed record to it fails, Start returns an error
// wrapping ErrDLQPublish without committing that record or any later one.
// The client has already fetched past those records, so close it (leaving the
// group) and create a new one; the uncommitted records are then redelivered.
func (c *consumer) Start(ctx context.Context, handler func(*kgo.Record) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		err := c.processFetches(ctx, fetches, handler)
		// The client is built with BlockRebalanceOnPoll, so rebalances wait until
		// the records of this poll are handled and committed.
		c.client.AllowRebalance()
		if err != nil {
			return err
		}
	}
}

// processFetches handles every record of one poll in order and commits the ones
// that were processed. It stops at the first record that must not be committed.
func (c *consumer) processFetches(ctx context.Context, fetches kgo.Fetches, handler func(*kgo.Record) error) error {
	logger := c.options.Logger

	// A fetch error on one partition must not drop records fetched from healthy ones.
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		logger.Error("fetch error", "topic", topic, "partition", partition, "error", err)
	})

	processed := make([]*kgo.Record, 0, fetches.NumRecords())
	var procErr error
	for iter := fetches.RecordIter(); !iter.Done(); {
		r := iter.Next()
		if procErr = ctx.Err(); procErr != nil {
			break
		}
		if procErr = c.handleRecord(ctx, r, handler); procErr != nil {
			break
		}
		processed = append(processed, r)
	}

	if len(processed) > 0 {
		// Commit even when ctx is cancelled so that a graceful shutdown does not
		// redeliver records that were already handled.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		defer cancel()
		if err := c.client.CommitRecords(commitCtx, processed...); err != nil {
			logger.Error("commit error", "error", err)
		}
	}

	return procErr
}

// handleRecord runs handler with retries and routes a final failure to the DLQ.
// A non-nil error means the record must not be committed.
func (c *consumer) handleRecord(ctx context.Context, r *kgo.Record, handler func(*kgo.Record) error) error {
	logger := c.options.Logger

	var lastErr error
	for attempt := 0; attempt <= c.options.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * c.options.RetryBackoff
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		if lastErr = handler(r); lastErr == nil {
			return nil
		}
		logger.Warn("handler error, retrying",
			"topic", r.Topic,
			"partition", r.Partition,
			"offset", r.Offset,
			"attempt", attempt+1,
			"max_retries", c.options.MaxRetries,
			"error", lastErr,
		)
	}

	if c.options.DLQProducer == nil {
		logger.Error("handler failed, no DLQ configured",
			"topic", r.Topic,
			"partition", r.Partition,
			"offset", r.Offset,
			"error", lastErr,
		)
		return nil
	}

	if err := c.options.DLQProducer.SendToDLQ(ctx, r.Topic, r.Value, r.Key, lastErr); err != nil {
		logger.Error("failed to send message to DLQ",
			"error", err,
			"topic", r.Topic,
			"partition", r.Partition,
			"offset", r.Offset,
			"dlq_error", lastErr,
		)
		return fmt.Errorf("%w: topic %s partition %d offset %d: %w", ErrDLQPublish, r.Topic, r.Partition, r.Offset, err)
	}

	logger.Error("message sent to DLQ",
		"topic", r.Topic,
		"partition", r.Partition,
		"offset", r.Offset,
		"dlq_topic", r.Topic+".dlq",
		"error", lastErr,
	)
	if c.options.OnDLQPublish != nil {
		c.options.OnDLQPublish(r.Topic, lastErr)
	}
	return nil
}

// StartConsumer starts consuming messages and processing them with the handler.
//
// Deprecated: Use NewConsumer and Consumer.Start instead.
func StartConsumer(ctx context.Context, client *kgo.Client, handler func(*kgo.Record) error, opts ...ConsumerOptions) error {
	c := NewConsumer(client, opts...)
	return c.Start(ctx, handler)
}

// StartConsumerWithDLQ is a convenience function to start a consumer with DLQ support.
//
// Deprecated: Use NewConsumer with ConsumerOptions.DLQProducer instead.
func StartConsumerWithDLQ(ctx context.Context, client *kgo.Client, handler func(*kgo.Record) error, dlqProducer Producer) error {
	return StartConsumer(ctx, client, handler, ConsumerOptions{
		DLQProducer: dlqProducer,
		MaxRetries:  3,
	})
}
