package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errPoolClosed is returned by get after the pool has been closed.
var errPoolClosed = errors.New("rabbitmq: channel pool is closed")

// channelFactory opens a new, fully configured publisher channel on the
// client's current connection.
type channelFactory func() (*amqp.Channel, error)

// channelPool manages a pool of pre-allocated AMQP channels for publishing.
// It is safe for concurrent use.
//
// Channels that are found closed (e.g. after a channel-level exception such as
// publishing to a missing exchange) are replaced through the factory, so the
// pool does not shrink over time.
type channelPool struct {
	channels chan *amqp.Channel
	size     int
	factory  channelFactory

	// mu guards closed and every send on channels, so a send can never race
	// with close(channels).
	mu     sync.Mutex
	closed bool
}

func newChannelPool(size int, factory channelFactory) *channelPool {
	return &channelPool{
		channels: make(chan *amqp.Channel, size),
		size:     size,
		factory:  factory,
	}
}

// fill populates the pool up to its size with channels from the factory.
func (p *channelPool) fill() error {
	for i := range p.size {
		ch, err := p.factory()
		if err != nil {
			return fmt.Errorf("failed to create channel %d: %w", i, err)
		}
		if !p.put(ch) {
			_ = ch.Close()
		}
	}
	return nil
}

// put offers ch to the pool without blocking. It reports false when the pool
// is closed or already full, in which case the caller owns ch.
func (p *channelPool) put(ch *amqp.Channel) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	select {
	case p.channels <- ch:
		return true
	default:
		return false
	}
}

// get retrieves a channel from the pool, respecting context cancellation.
// A closed channel taken from the pool is replaced with a fresh one.
func (p *channelPool) get(ctx context.Context) (*amqp.Channel, error) {
	select {
	case ch, ok := <-p.channels:
		if !ok {
			return nil, errPoolClosed
		}
		if ch != nil && !ch.IsClosed() {
			return ch, nil
		}
		fresh, err := p.factory()
		if err != nil {
			return nil, fmt.Errorf("replace closed channel: %w", err)
		}
		return fresh, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// returnCh returns a channel to the pool. A closed channel is replaced so the
// pool keeps its size; if no replacement can be opened (connection down), the
// slot is refilled by the next fill after reconnection.
func (p *channelPool) returnCh(ch *amqp.Channel) {
	if ch == nil {
		return
	}
	if ch.IsClosed() {
		fresh, err := p.factory()
		if err != nil {
			return
		}
		ch = fresh
	}
	if !p.put(ch) {
		_ = ch.Close()
	}
}

// drain removes and closes all channels currently in the pool.
func (p *channelPool) drain() {
	for {
		select {
		case ch, ok := <-p.channels:
			if !ok {
				return
			}
			if ch != nil && !ch.IsClosed() {
				_ = ch.Close()
			}
		default:
			return
		}
	}
}

// close permanently shuts down the pool and closes all remaining channels.
// After close, get returns an error and returnCh discards channels.
func (p *channelPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.channels)
	p.mu.Unlock()

	p.drain()
}
