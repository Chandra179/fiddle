package rabbitmq

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Producer handles message publishing with confirms and OTel tracing.
type Producer struct {
	client *Client
	tracer trace.Tracer
}

// NewProducer creates a new Publisher backed by client.
func NewProducer(client *Client) Publisher {
	return &Producer{
		client: client,
		tracer: otel.Tracer("rabbitmq-producer"),
	}
}

// PublishOptions contains options for publishing a message.
// All fields are optional; zero values fall back to Config defaults
// or sensible behaviour (e.g. ContentType defaults to "application/json").
type PublishOptions struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool

	// AMQP message properties
	MessageId       string
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	Priority        uint8
	Type            string
	AppId           string
	UserId          string
	ContentType     string
	ContentEncoding string
	Timestamp       time.Time

	// DeliveryMode: 0 = use Config.PersistentDelivery, 1 = transient, 2 = persistent
	DeliveryMode uint8

	Headers map[string]any
}

// PublishMessage publishes a message to the specified exchange/routing key.
func (p *Producer) PublishMessage(ctx context.Context, opts PublishOptions, payload any) error {
	ctx, span := p.tracer.Start(ctx, "rabbitmq.publish")
	defer span.End()

	ch, err := p.acquireChannel(ctx)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer p.client.ReturnPublisherChannel(ch)

	dc, err := p.publishOn(ctx, ch, opts, payload)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if err := waitConfirm(ctx, dc); err != nil {
		span.RecordError(err)
		return err
	}

	p.client.logger.Debug("message published successfully",
		"exchange", opts.Exchange,
		"routing_key", opts.RoutingKey,
	)

	return nil
}

// acquireChannel takes a publisher channel from the pool. If the client is
// disconnected it waits ReconnectInitialInterval once for reconnection.
func (p *Producer) acquireChannel(ctx context.Context) (*amqp.Channel, error) {
	ch, err := p.client.GetPublisherChannel(ctx)
	if err == nil {
		return ch, nil
	}
	if !errors.Is(err, ErrNotConnected) {
		return nil, fmt.Errorf("failed to get publisher channel: %w", err)
	}

	timer := time.NewTimer(p.client.config.ReconnectInitialInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}

	ch, err = p.client.GetPublisherChannel(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get publisher channel after retry: %w", err)
	}
	return ch, nil
}

// publishOn marshals payload and publishes it on ch. When publisher confirms
// are enabled it returns the pending confirmation for the caller to wait on.
func (p *Producer) publishOn(ctx context.Context, ch *amqp.Channel, opts PublishOptions, payload any) (*amqp.DeferredConfirmation, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Prepare headers with OTel trace context
	headers := amqp.Table{}
	maps.Copy(headers, opts.Headers)

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		headers[k] = v
	}

	deliveryMode := amqp.Transient
	switch {
	case opts.DeliveryMode != 0:
		deliveryMode = opts.DeliveryMode
	case p.client.config.PersistentDelivery:
		deliveryMode = amqp.Persistent
	}

	mandatory := p.client.config.Mandatory || opts.Mandatory

	publishing := amqp.Publishing{
		ContentType:     cmp.Or(opts.ContentType, "application/json"),
		ContentEncoding: opts.ContentEncoding,
		Body:            body,
		DeliveryMode:    deliveryMode,
		Priority:        opts.Priority,
		MessageId:       opts.MessageId,
		CorrelationId:   opts.CorrelationId,
		ReplyTo:         opts.ReplyTo,
		Expiration:      opts.Expiration,
		Type:            opts.Type,
		AppId:           opts.AppId,
		UserId:          opts.UserId,
		Timestamp:       opts.Timestamp,
		Headers:         headers,
	}

	if !p.client.config.PublisherConfirms {
		if err := ch.PublishWithContext(ctx, opts.Exchange, opts.RoutingKey, mandatory, opts.Immediate, publishing); err != nil {
			return nil, fmt.Errorf("failed to publish message: %w", err)
		}
		return nil, nil
	}

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, opts.Exchange, opts.RoutingKey, mandatory, opts.Immediate, publishing)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
	return dc, nil
}

// waitConfirm blocks until the broker acks dc. A nil dc (confirms disabled)
// returns immediately.
func waitConfirm(ctx context.Context, dc *amqp.DeferredConfirmation) error {
	if dc == nil {
		return nil
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("message was nacked by broker")
	}
	return nil
}

// SendMessage is a convenience method for sending to a direct queue.
func (p *Producer) SendMessage(ctx context.Context, queueName string, payload any) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   "",
		RoutingKey: queueName,
	}, payload)
}

// SendMessageWithHeaders publishes a message with custom headers.
func (p *Producer) SendMessageWithHeaders(ctx context.Context, queueName string, payload any, headers map[string]any) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   "",
		RoutingKey: queueName,
		Headers:    headers,
	}, payload)
}

// SendToTopic publishes a message to a topic exchange.
func (p *Producer) SendToTopic(ctx context.Context, exchange, routingKey string, payload any) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   exchange,
		RoutingKey: routingKey,
	}, payload)
}

// SendToTopicWithHeaders publishes a message to a topic exchange with custom headers.
func (p *Producer) SendToTopicWithHeaders(ctx context.Context, exchange, routingKey string, payload any, headers map[string]any) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Headers:    headers,
	}, payload)
}

// PublishBatch publishes multiple messages on one channel. With publisher
// confirms enabled, all messages are sent before waiting for any confirm, so
// the batch costs one round trip instead of one per message.
func (p *Producer) PublishBatch(ctx context.Context, opts PublishOptions, payloads []any) error {
	ctx, span := p.tracer.Start(ctx, "rabbitmq.publish-batch")
	defer span.End()

	ch, err := p.acquireChannel(ctx)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer p.client.ReturnPublisherChannel(ch)

	confirms := make([]*amqp.DeferredConfirmation, 0, len(payloads))
	for i, payload := range payloads {
		dc, err := p.publishOn(ctx, ch, opts, payload)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to publish message %d: %w", i, err)
		}
		confirms = append(confirms, dc)
	}

	for i, dc := range confirms {
		if err := waitConfirm(ctx, dc); err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to confirm message %d: %w", i, err)
		}
	}
	return nil
}
