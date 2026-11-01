package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrUnroutable          = errors.New("rabbitmq: transfer was returned as unroutable")
	ErrTransferUnconfirmed = errors.New("rabbitmq: transfer was not confirmed")
)

// withTransferChannel isolates internal transfers from asynchronous public
// publishes. One connection/channel per operation also allows cancellation to
// close its socket: amqp091-go's publish context does not interrupt socket I/O.
// The semaphore bounds connections, waiters are bounded by the caller deadline.
func (c *Client) withTransferChannel(ctx context.Context, fn func(*amqp.Channel) error) error {
	limit := c.config.TransferTimeout
	if limit <= 0 {
		limit = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	if c.closing() {
		return ErrClientClosed
	}
	select {
	case c.transferSlots <- struct{}{}:
		defer func() { <-c.transferSlots }()
	case <-c.closeChan:
		return ErrClientClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	var socket net.Conn
	var stopIO func() bool
	defer func() {
		if stopIO != nil {
			stopIO()
		}
		if socket != nil {
			_ = socket.Close()
		}
	}()
	config := amqp.Config{
		TLSClientConfig: c.config.TLSConfig,
		Heartbeat:       c.config.Heartbeat,
		Properties:      amqp.Table{"connection_name": c.config.ConnectionName + "-transfer"},
		Dial: func(network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			socket = conn
			if deadline, ok := ctx.Deadline(); ok {
				if err := conn.SetDeadline(deadline); err != nil {
					_ = conn.Close()
					return nil, err
				}
			}
			stopIO = context.AfterFunc(ctx, func() { _ = conn.Close() })
			return conn, nil
		},
	}
	conn, err := amqp.DialConfig(c.config.URL, config)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	return fn(ch)
}

func (c *Client) transfer(ctx context.Context, exchange, key string, message amqp.Publishing) error {
	return c.withTransferChannel(ctx, func(ch *amqp.Channel) error {
		returned := ch.NotifyReturn(make(chan amqp.Return, 1))
		confirmed := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		if err := ch.Confirm(false); err != nil {
			return err
		}
		message.DeliveryMode = amqp.Persistent
		if err := ch.PublishWithContext(ctx, exchange, key, true, false, message); err != nil {
			return err
		}
		return awaitTransfer(ctx, returned, confirmed)
	})
}

func awaitTransfer(ctx context.Context, returned <-chan amqp.Return, confirmed <-chan amqp.Confirmation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r, ok := <-returned:
		if !ok {
			return ErrTransferUnconfirmed
		}
		return fmt.Errorf("%w: %d %s", ErrUnroutable, r.ReplyCode, r.ReplyText)
	case confirmation, ok := <-confirmed:
		if !ok || !confirmation.Ack || confirmation.DeliveryTag != 1 {
			return ErrTransferUnconfirmed
		}
		// RabbitMQ sends basic.return before basic.ack, and amqp091-go enqueues
		// them in wire order. Check the buffered return even if select chose ack.
		select {
		case r, ok := <-returned:
			if !ok {
				return ErrTransferUnconfirmed
			}
			return fmt.Errorf("%w: %d %s", ErrUnroutable, r.ReplyCode, r.ReplyText)
		default:
			return nil
		}
	}
}
