package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrNotConnected is returned when an operation is attempted while the client is not connected.
var ErrNotConnected = errors.New("rabbitmq: client not connected")

// ErrClientClosed is returned when an operation races with Client.Close.
var ErrClientClosed = errors.New("rabbitmq: client closed")

// Publisher sends messages to RabbitMQ exchanges or queues.
type Publisher interface {
	PublishMessage(ctx context.Context, opts PublishOptions, payload any) error
	SendMessage(ctx context.Context, queueName string, payload any) error
	SendMessageWithHeaders(ctx context.Context, queueName string, payload any, headers map[string]any) error
	SendToTopic(ctx context.Context, exchange, routingKey string, payload any) error
	SendToTopicWithHeaders(ctx context.Context, exchange, routingKey string, payload any, headers map[string]any) error
	PublishBatch(ctx context.Context, opts PublishOptions, payloads []any) error
}

// Subscriber consumes messages from RabbitMQ queues.
type Subscriber interface {
	StartConsumer(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error
	ConsumeWithDefaults(ctx context.Context, queueName string, handler MessageHandler) error
}

// TopologyDeclarer declares queues, exchanges, and bindings.
type TopologyDeclarer interface {
	DeclareQueue(config QueueConfig) (*amqp.Queue, error)
	DeclareExchange(config ExchangeConfig) error
	BindQueue(config BindingConfig) error
	SetupDeadLetterExchange(mainQueueName, dlxName string, retryTTL int) error
	SetupQueueWithDLX(queueName, dlxName string) (*amqp.Queue, error)
}

// ConnectionState represents the current state of the connection.
type ConnectionState int32

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateClosing
)

// Client manages RabbitMQ connections with auto-reconnection.
type Client struct {
	config *Config
	logger *slog.Logger

	publisherConn *amqp.Connection
	consumerConn  *amqp.Connection

	publisherPool *channelPool

	state     atomic.Int32
	closeChan chan struct{}
	closeOnce sync.Once

	topology *TopologyCache

	// mu guards publisherConn and consumerConn.
	mu sync.RWMutex
}

// TopologyCache stores declared queues, exchanges, and bindings for reconnection.
type TopologyCache struct {
	queues    []QueueConfig
	exchanges []ExchangeConfig
	bindings  []BindingConfig
	mu        sync.RWMutex
}

// NewClient creates a new RabbitMQ client with auto-reconnection.
//
// Unset fields are filled with Config.ApplyDefaults and the result is checked
// with Config.Validate. The client works on a copy, so config is neither
// modified nor read again after NewClient returns.
func NewClient(config *Config, logger *slog.Logger) (*Client, error) {
	if config == nil {
		return nil, errors.New("rabbitmq: config nil")
	}

	cfg := *config
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("rabbitmq: %w", err)
	}
	config = &cfg

	if logger == nil {
		logger = slog.Default()
	}

	client := &Client{
		config:    config,
		logger:    logger,
		closeChan: make(chan struct{}),
		topology: &TopologyCache{
			queues:    make([]QueueConfig, 0),
			exchanges: make([]ExchangeConfig, 0),
			bindings:  make([]BindingConfig, 0),
		},
	}
	client.publisherPool = newChannelPool(config.ChannelPoolSize, client.newPublisherChannel)

	client.state.Store(int32(StateConnecting))

	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("rabbitmq: initial connection failed: %w", err)
	}

	return client, nil
}

// closing reports whether Close has been called.
func (c *Client) closing() bool {
	select {
	case <-c.closeChan:
		return true
	default:
		return false
	}
}

// connect establishes publisher and consumer connections and fills the channel pool.
// The caller must have moved the state to StateConnecting.
//
// Connections from a previous connect are closed here: when only one of the
// pair failed, the surviving one would otherwise leak together with its
// watcher goroutines.
func (c *Client) connect() error {
	pubConn, err := c.dial("publisher")
	if err != nil {
		return fmt.Errorf("publisher connection failed: %w", err)
	}

	consConn, err := c.dial("consumer")
	if err != nil {
		_ = pubConn.Close()
		return fmt.Errorf("consumer connection failed: %w", err)
	}

	// Install the new connections unless Close won the race. Close takes the
	// same lock, so it either sees (and closes) these connections or we see
	// that it ran.
	c.mu.Lock()
	if c.closing() {
		c.mu.Unlock()
		_ = pubConn.Close()
		_ = consConn.Close()
		return ErrClientClosed
	}
	oldPub, oldCons := c.publisherConn, c.consumerConn
	c.publisherConn = pubConn
	c.consumerConn = consConn
	c.mu.Unlock()

	// Their close watchers see they are no longer current and do not trigger
	// another reconnect.
	closeConn(oldPub)
	closeConn(oldCons)

	// Drain stale channels and fill pool from the new connection
	c.publisherPool.drain()
	if err := c.publisherPool.fill(); err != nil {
		_ = pubConn.Close()
		_ = consConn.Close()
		return fmt.Errorf("channel pool initialization failed: %w", err)
	}

	// Re-declare the topology before the client reports StateConnected, so
	// anything waiting on IsHealthy (such as a consumer re-subscribing) finds
	// its queues. A failure fails this attempt and is retried with backoff.
	if err := c.restoreTopology(); err != nil {
		_ = pubConn.Close()
		_ = consConn.Close()
		if c.closing() {
			return ErrClientClosed
		}
		return fmt.Errorf("topology restore failed: %w", err)
	}

	go c.handleConnectionClose(pubConn, "publisher")
	go c.handleConnectionClose(consConn, "consumer")
	go c.handleConnectionBlocked(pubConn, "publisher")
	go c.handleConnectionBlocked(consConn, "consumer")

	if !c.state.CompareAndSwap(int32(StateConnecting), int32(StateConnected)) {
		// Close ran while the pool was filling; it closes the connections.
		return ErrClientClosed
	}

	c.logger.Info("rabbitmq connected successfully",
		"connection_name", c.config.ConnectionName,
	)
	return nil
}

func closeConn(conn *amqp.Connection) {
	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}
}

// newPublisherChannel opens a channel on the current publisher connection with
// confirms and the return listener configured. It is the channel pool's factory.
func (c *Client) newPublisherChannel() (*amqp.Channel, error) {
	c.mu.RLock()
	conn := c.publisherConn
	c.mu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return nil, ErrNotConnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if c.config.PublisherConfirms {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("failed to enable confirms: %w", err)
		}
	}

	if onReturn := c.config.OnReturn; onReturn != nil {
		// The library closes returnChan when the channel closes, ending the goroutine.
		returnChan := ch.NotifyReturn(make(chan amqp.Return, 1))
		go func() {
			for r := range returnChan {
				onReturn(r)
			}
		}()
	}

	return ch, nil
}

// dial creates a new AMQP connection with a named connection type.
func (c *Client) dial(connType string) (*amqp.Connection, error) {
	config := amqp.Config{
		Locale: "en_US",
		Properties: amqp.Table{
			"connection_name": fmt.Sprintf("%s-%s", c.config.ConnectionName, connType),
		},
		TLSClientConfig: c.config.TLSConfig,
	}

	if c.config.Heartbeat > 0 {
		config.Heartbeat = c.config.Heartbeat
	}
	if c.config.ChannelMax > 0 {
		config.ChannelMax = uint16(c.config.ChannelMax) // bounded by ApplyDefaults
	}
	if c.config.FrameSize > 0 {
		config.FrameSize = c.config.FrameSize
	}

	return amqp.DialConfig(c.config.URL, config)
}

// isCurrent reports whether conn is one of the client's live connections.
func (c *Client) isCurrent(conn *amqp.Connection) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return conn == c.publisherConn || conn == c.consumerConn
}

// handleConnectionClose watches for a connection close event and triggers reconnection.
func (c *Client) handleConnectionClose(conn *amqp.Connection, connType string) {
	notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case <-c.closeChan:
		return
	case err := <-notifyClose:
		// select picks randomly when both are ready; re-check shutdown, and
		// ignore connections a reconnect already replaced on purpose.
		if c.closing() || !c.isCurrent(conn) {
			return
		}
		c.logger.Warn("rabbitmq connection closed",
			"connection_type", connType,
			"error", err,
		)
		c.triggerReconnect()
	}
}

// handleConnectionBlocked watches for broker resource alarms.
func (c *Client) handleConnectionBlocked(conn *amqp.Connection, connType string) {
	blocked := conn.NotifyBlocked(make(chan amqp.Blocking, 1))

	for {
		select {
		case <-c.closeChan:
			return
		case blocking, ok := <-blocked:
			if !ok {
				return
			}
			if !blocking.Active {
				c.logger.Info("rabbitmq connection unblocked",
					"connection_type", connType,
				)
				continue
			}
			c.logger.Warn("rabbitmq connection blocked",
				"connection_type", connType,
				"reason", blocking.Reason,
			)
		}
	}
}

// triggerReconnect starts a reconnect when the client is connected. In any
// other state a reconnect is already running or the client is closing.
func (c *Client) triggerReconnect() {
	if c.state.CompareAndSwap(int32(StateConnected), int32(StateConnecting)) {
		go c.reconnectWithBackoff()
	}
}

// reconnectWithBackoff handles reconnection with exponential backoff and jitter.
func (c *Client) reconnectWithBackoff() {
	backoff := c.config.ReconnectInitialInterval
	maxBackoff := c.config.ReconnectMaxInterval

	timer := time.NewTimer(jitter(backoff))
	defer timer.Stop()

	for {
		select {
		case <-c.closeChan:
			return
		case <-timer.C:
			c.logger.Info("attempting reconnection", "backoff", backoff)

			if err := c.connect(); err != nil {
				if errors.Is(err, ErrClientClosed) {
					return
				}
				backoff = min(backoff*2, maxBackoff)
				c.logger.Error("reconnection attempt failed", "error", err, "next_backoff", backoff)
				timer.Reset(jitter(backoff))
				continue
			}

			c.logger.Info("reconnection successful")
			return
		}
	}
}

// jitter returns a random duration in [d/2, d) so that many clients losing the
// same broker do not reconnect in lockstep.
func jitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half)
}

// GetPublisherChannel returns a channel from the publisher pool.
// The context controls how long the caller waits for a channel to become available.
func (c *Client) GetPublisherChannel(ctx context.Context) (*amqp.Channel, error) {
	if c.state.Load() != int32(StateConnected) {
		return nil, ErrNotConnected
	}

	return c.publisherPool.get(ctx)
}

// ReturnPublisherChannel returns a channel to the publisher pool.
func (c *Client) ReturnPublisherChannel(ch *amqp.Channel) {
	c.publisherPool.returnCh(ch)
}

// CreateConsumerChannel creates a new channel on the consumer connection.
// It returns ErrNotConnected unless the client is StateConnected.
func (c *Client) CreateConsumerChannel() (*amqp.Channel, error) {
	if c.state.Load() != int32(StateConnected) {
		return nil, ErrNotConnected
	}
	return c.openConsumerChannel()
}

// openConsumerChannel opens a channel on the current consumer connection
// whatever the client state. connect uses it to restore topology while the
// client is still StateConnecting.
func (c *Client) openConsumerChannel() (*amqp.Channel, error) {
	c.mu.RLock()
	conn := c.consumerConn
	c.mu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return nil, ErrNotConnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer channel: %w", err)
	}

	return ch, nil
}

// Close gracefully shuts down all connections and the channel pool.
// It is safe to call more than once; later calls are no-ops.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.state.Store(int32(StateClosing))
		close(c.closeChan)

		c.publisherPool.close()

		c.mu.Lock()
		defer c.mu.Unlock()

		closeConn(c.publisherConn)
		closeConn(c.consumerConn)

		c.logger.Info("rabbitmq client closed")
	})
	return nil
}

// IsHealthy returns true if the client is connected and healthy.
func (c *Client) IsHealthy() bool {
	return c.state.Load() == int32(StateConnected)
}

// GetState returns the current connection state.
func (c *Client) GetState() ConnectionState {
	return ConnectionState(c.state.Load())
}
