package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ProvisionMode uint8

const (
	UseExisting ProvisionMode = iota
	Declare
)

// DeadLetterConfig describes a per-subscription parking destination. A nil
// config disables parking. An empty Queue uses <source>.parking-lot, durable.
// Queue.Args accepts x-message-ttl, x-max-length-bytes and x-overflow; capacity
// limits must explicitly use reject-publish or drop-head (which deletes data).
// QueueType defaults to quorum. Exchange, when set, must be durable and direct;
// its binding uses RoutingKey (defaults to Queue.Name).
type DeadLetterConfig struct {
	Queue      QueueConfig
	QueueType  QueueType
	Exchange   *ExchangeConfig
	RoutingKey string
	Provision  ProvisionMode
	Timeout    time.Duration
	// ErrorFormatter is opt-in; by default no error message leaves the service.
	ErrorFormatter func(error) string
}

func (d *DeadLetterConfig) normalized(source string) *DeadLetterConfig {
	if d == nil {
		return nil
	}
	out := *d
	if out.Queue.Name == "" {
		out.Queue.Name = source + parkingLotSuffix
		out.Queue.Durable = true
	}
	out.Queue.Args = maps.Clone(d.Queue.Args)
	if out.Exchange != nil {
		e := *out.Exchange
		e.Args = maps.Clone(e.Args)
		out.Exchange = &e
	}
	if out.QueueType == "" {
		out.QueueType = QueueTypeQuorum
	}
	if out.RoutingKey == "" {
		out.RoutingKey = out.Queue.Name
	}
	if out.Timeout == 0 {
		out.Timeout = 10 * time.Second
	}
	return &out
}

func (d *DeadLetterConfig) validate(source string) error {
	if d == nil {
		return nil
	}
	if strings.TrimSpace(d.Queue.Name) == "" || d.Queue.Name == source || !d.Queue.Durable || d.Queue.AutoDelete || d.Queue.Exclusive || d.Queue.NoWait {
		return errors.New("dead-letter queue must be named, durable, distinct from source, and neither exclusive nor auto-delete/no-wait")
	}
	if d.QueueType != QueueTypeQuorum && d.QueueType != QueueTypeClassic {
		return errors.New("invalid dead-letter queue type")
	}
	if d.Provision > Declare || d.Timeout <= 0 {
		return errors.New("invalid dead-letter provisioning or timeout")
	}
	if d.Exchange != nil && (d.Exchange.Name == "" || d.Exchange.Kind != "direct" || !d.Exchange.Durable || d.Exchange.AutoDelete || d.Exchange.Internal || d.Exchange.NoWait) {
		return errors.New("dead-letter exchange must be named, durable and direct")
	}
	if d.Exchange == nil && d.RoutingKey != d.Queue.Name {
		return errors.New("default-exchange routing key must equal dead-letter queue name")
	}
	args := d.Queue.Args
	if _, length := args["x-max-length"]; length {
		if err := validateOverflow(args); err != nil {
			return err
		}
	}
	if _, bytes := args["x-max-length-bytes"]; bytes {
		if err := validateOverflow(args); err != nil {
			return err
		}
	}
	if kind, ok := args["x-queue-type"]; ok && kind != string(d.QueueType) {
		return errors.New("conflicting dead-letter queue type")
	}
	if _, loop := args["x-dead-letter-exchange"]; loop {
		return errors.New("parking queues must not automatically dead-letter messages again")
	}
	return nil
}

func validateOverflow(args map[string]any) error {
	if v := args["x-overflow"]; v != "reject-publish" && v != "drop-head" {
		return errors.New("bounded dead-letter queues require an explicit x-overflow policy")
	}
	return nil
}

func (c *Client) prepareDeadLetter(ctx context.Context, d *DeadLetterConfig) error {
	if d == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()
	return c.withTransferChannel(ctx, func(ch *amqp.Channel) error {
		args := maps.Clone(d.Queue.Args)
		if args == nil {
			args = map[string]any{}
		}
		args["x-queue-type"] = string(d.QueueType)
		q := d.Queue
		if d.Provision == UseExisting {
			if _, err := ch.QueueDeclarePassive(q.Name, q.Durable, false, false, false, args); err != nil {
				return err
			}
		}
		// The active idempotent declaration checks equivalence; a passive
		// declaration alone only checks existence in AMQP 0-9-1.
		if _, err := ch.QueueDeclare(q.Name, q.Durable, false, false, false, args); err != nil {
			return err
		}
		if d.Exchange == nil {
			return nil
		}
		e := d.Exchange
		if d.Provision == UseExisting {
			if err := ch.ExchangeDeclarePassive(e.Name, e.Kind, e.Durable, false, false, false, e.Args); err != nil {
				return err
			}
		}
		if err := declareExchangeOn(ch, *e); err != nil {
			return err
		}
		return ch.QueueBind(q.Name, d.RoutingKey, e.Name, false, nil)
	})
}

func (c *Client) park(ctx context.Context, cfg *DeadLetterConfig, d amqp.Delivery, source string, failure processingResult) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	message := publishingFrom(d)
	message.Headers = maps.Clone(message.Headers)
	if message.Headers == nil {
		message.Headers = amqp.Table{}
	}
	for key := range message.Headers {
		if strings.HasPrefix(key, "fiddle.dlq.") {
			delete(message.Headers, key)
		}
	}
	message.Headers["fiddle.dlq.source_queue"] = source
	message.Headers["fiddle.dlq.source_exchange"] = d.Exchange
	message.Headers["fiddle.dlq.source_routing_key"] = d.RoutingKey
	if count, ok := message.Headers["x-delivery-count"]; ok {
		message.Headers["fiddle.dlq.source_delivery_count"] = count
		delete(message.Headers, "x-delivery-count")
	}
	message.Headers["fiddle.dlq.message_id"] = d.MessageId
	message.Headers["fiddle.dlq.failure_kind"] = failure.kind.String()
	message.Headers["fiddle.dlq.attempts"] = int64(failure.attempts)
	message.Headers["fiddle.dlq.failed_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	if cfg.ErrorFormatter != nil {
		if detail := cfg.ErrorFormatter(failure.err); detail != "" {
			message.Headers["fiddle.dlq.error"] = detail
		}
	}
	// Source TTL must not expire a parked message before it can be inspected.
	if d.Expiration != "" {
		message.Headers["fiddle.dlq.source_expiration"] = d.Expiration
	}
	message.Expiration = ""
	exchange := ""
	if cfg.Exchange != nil {
		exchange = cfg.Exchange.Name
	}
	if err := c.transfer(ctx, exchange, cfg.RoutingKey, message); err != nil {
		return fmt.Errorf("dead-letter transfer to %s: %w", cfg.Queue.Name, errors.Join(failure.paused(), err))
	}
	return nil
}
