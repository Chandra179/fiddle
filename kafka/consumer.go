package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

// commitTimeout bounds the offset commit issued after each poll.
const commitTimeout = 10 * time.Second

// ConsumerOptions configures consumer behavior.
type ConsumerOptions struct {
	// RetryPolicy is optional. An explicit policy overrides legacy retry fields.
	RetryPolicy *RetryPolicy
	DeadLetter  *DeadLetterConfig
	// Deprecated: configure DeadLetter instead. Setting DLQProducer is rejected
	// before consumption because its topology and write guarantees are unknown.
	DLQProducer  Producer
	OnDLQPublish func(topic string, err error)
	MaxRetries   int
	RetryBackoff time.Duration
	Logger       *slog.Logger
}

// recordIO captures the broker operations that settle one poll.
type recordIO interface {
	CommitRecords(context.Context, ...*kgo.Record) error
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

// consumer implements the Consumer interface.
type consumer struct {
	client  *kgo.Client
	records recordIO
	options ConsumerOptions
	policy  RetryPolicy
	initErr error
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
		if o.RetryPolicy != nil {
			options.RetryPolicy = o.RetryPolicy
		}
		if o.DeadLetter != nil {
			options.DeadLetter = o.DeadLetter
		}
		options.DLQProducer = o.DLQProducer
		options.OnDLQPublish = o.OnDLQPublish
	}
	var topics []string
	if client != nil {
		topics, _ = client.OptValue(kgo.ConsumeTopics).([]string)
	}
	options.DeadLetter = options.DeadLetter.normalized(topics)
	policy := options.RetryPolicy.normalized()
	if options.RetryPolicy == nil {
		policy.MaxRetries = options.MaxRetries
		policy.InitialBackoff = options.RetryBackoff
		policy.MaxBackoff = max(policy.MaxBackoff, policy.InitialBackoff)
	}
	initErr := errors.Join(policy.validate(options.DeadLetter != nil), options.DeadLetter.validate(topics))
	if options.DLQProducer != nil {
		initErr = errors.Join(initErr, errors.New("DLQProducer is deprecated; configure DeadLetter instead"))
	}
	return &consumer{client: client, records: client, options: options, policy: policy, initErr: initErr}
}

// Start begins consuming messages and processing them with the handler.
// It blocks until the context is cancelled or the client is closed.
//
// Offsets of processed records are committed once per poll, so records still in
// flight at shutdown are redelivered on restart (at-least-once).
//
// If processing stops or publishing to the DLQ fails, Start returns an error
// without committing that record or any later one. DLQ failures wrap ErrDLQPublish. The
// client has already fetched past them, so close it and create a new one to have
// them redelivered.
func (c *consumer) Start(ctx context.Context, handler Handler) error {
	if c.initErr != nil {
		return c.initErr
	}
	if c.client == nil || handler == nil {
		return errors.New("consumer requires a client and handler")
	}
	if group, _ := c.client.OptValue(kgo.ConsumerGroup).(string); group == "" {
		return errors.New("reliable consumption requires a consumer group")
	} else {
		if disabled, _ := c.client.OptValue(kgo.DisableAutoCommit).(bool); !disabled {
			return errors.New("consumer requires DisableAutoCommit")
		}
		if blocked, _ := c.client.OptValue(kgo.BlockRebalanceOnPoll).(bool); !blocked {
			return errors.New("consumer requires BlockRebalanceOnPoll")
		}
	}
	if err := prepareDeadLetter(ctx, c.client, c.options.DeadLetter); err != nil {
		return fmt.Errorf("prepare dead-letter topic: %w", err)
	}
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
func (c *consumer) processFetches(ctx context.Context, fetches kgo.Fetches, handler Handler) error {
	logger := c.options.Logger

	// Process healthy partitions before returning fetch errors; never busy-loop
	// on a persistent broker/authorization failure.
	var fetchErr error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		fetchErr = errors.Join(fetchErr, fmt.Errorf("fetch topic %s partition %d: %w", topic, partition, err))
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
		if err := c.records.CommitRecords(commitCtx, processed...); err != nil {
			return errors.Join(procErr, fetchErr, fmt.Errorf("commit processed offsets: %w", err))
		}
	}

	return errors.Join(procErr, fetchErr)
}

// handleRecord leaves all settlement to processFetches. A non-nil error
// stops the poll at this record; neither it nor later records are committed.
func (c *consumer) handleRecord(ctx context.Context, r *kgo.Record, handler Handler) error {
	failure := c.policy.process(ctx, func(attemptCtx context.Context) error { return handler(recordContext(attemptCtx, r), r) })
	if err := ctx.Err(); err != nil {
		return err
	}
	if failure.err == nil {
		return nil
	}
	if !failure.park || c.options.DeadLetter == nil || c.options.DeadLetter.Topic == r.Topic {
		return fmt.Errorf("topic %s partition %d offset %d: %w", r.Topic, r.Partition, r.Offset, failure.paused())
	}
	publishCtx, cancel := context.WithTimeout(ctx, c.options.DeadLetter.Timeout)
	defer cancel()
	if err := c.records.ProduceSync(publishCtx, deadLetterRecord(c.options.DeadLetter, r, failure)).FirstErr(); err != nil {
		return fmt.Errorf("%w: topic %s partition %d offset %d: %w", ErrDLQPublish, r.Topic, r.Partition, r.Offset, errors.Join(failure.paused(), err))
	}
	if c.options.OnDLQPublish != nil {
		c.options.OnDLQPublish(r.Topic, failure.err)
	}
	return nil
}

// recordContext returns ctx enriched with the span that tracing hooks (kotel)
// stored on r, so work done by the handler is parented to the record's trace.
// Cancellation and deadlines still come from ctx alone.
func recordContext(ctx context.Context, r *kgo.Record) context.Context {
	if r.Context == nil {
		return ctx
	}
	if span := trace.SpanFromContext(r.Context); span.SpanContext().IsValid() {
		return trace.ContextWithSpan(ctx, span)
	}
	return ctx
}
