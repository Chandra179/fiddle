package rabbitmq

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TopologyManager handles queue/exchange/binding declarations
type TopologyManager struct {
	client *Client
}

// NewTopologyManager creates a new TopologyDeclarer backed by client.
func NewTopologyManager(client *Client) TopologyDeclarer {
	return &TopologyManager{client: client}
}

// declareQueueOn declares the queue on ch. It clones the caller's Args so the
// caller's map (and the cached config) is never mutated.
func (c *Client) declareQueueOn(ch *amqp.Channel, config QueueConfig) (amqp.Queue, error) {
	args := maps.Clone(config.Args)
	if args == nil {
		args = make(map[string]any)
	}

	// Add quorum queue argument if needed
	if c.config.QueueType == QueueTypeQuorum && config.Durable {
		args["x-queue-type"] = "quorum"
	}

	queue, err := ch.QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		args,
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("failed to declare queue %s: %w", config.Name, err)
	}
	return queue, nil
}

// declareExchangeOn declares the exchange on ch.
func declareExchangeOn(ch *amqp.Channel, config ExchangeConfig) error {
	err := ch.ExchangeDeclare(
		config.Name,
		config.Kind,
		config.Durable,
		config.AutoDelete,
		config.Internal,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange %s: %w", config.Name, err)
	}
	return nil
}

// bindQueueOn binds a queue to an exchange on ch.
func bindQueueOn(ch *amqp.Channel, config BindingConfig) error {
	err := ch.QueueBind(
		config.QueueName,
		config.RoutingKey,
		config.Exchange,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue %s to exchange %s: %w", config.QueueName, config.Exchange, err)
	}
	return nil
}

// DeclareQueue declares a queue with the given configuration
func (tm *TopologyManager) DeclareQueue(config QueueConfig) (*amqp.Queue, error) {
	ch, err := tm.client.CreateConsumerChannel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	queue, err := tm.client.declareQueueOn(ch, config)
	if err != nil {
		return nil, err
	}

	// Cache the topology
	tm.client.topology.mu.Lock()
	tm.client.topology.queues = upsert(tm.client.topology.queues, config, func(q QueueConfig) bool {
		return config.Name != "" && q.Name == config.Name
	})
	tm.client.topology.mu.Unlock()

	tm.client.logger.Info("queue declared", "queue", config.Name, "type", tm.client.config.QueueType)
	return &queue, nil
}

// DeclareExchange declares an exchange with the given configuration
func (tm *TopologyManager) DeclareExchange(config ExchangeConfig) error {
	ch, err := tm.client.CreateConsumerChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := declareExchangeOn(ch, config); err != nil {
		return err
	}

	// Cache the topology
	tm.client.topology.mu.Lock()
	tm.client.topology.exchanges = upsert(tm.client.topology.exchanges, config, func(e ExchangeConfig) bool {
		return e.Name == config.Name
	})
	tm.client.topology.mu.Unlock()

	tm.client.logger.Info("exchange declared", "exchange", config.Name, "type", config.Kind)
	return nil
}

// BindQueue binds a queue to an exchange
func (tm *TopologyManager) BindQueue(config BindingConfig) error {
	ch, err := tm.client.CreateConsumerChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := bindQueueOn(ch, config); err != nil {
		return err
	}

	// Cache the topology
	tm.client.topology.mu.Lock()
	tm.client.topology.bindings = upsert(tm.client.topology.bindings, config, func(b BindingConfig) bool {
		return b.QueueName == config.QueueName && b.Exchange == config.Exchange && b.RoutingKey == config.RoutingKey
	})
	tm.client.topology.mu.Unlock()

	tm.client.logger.Info("queue bound",
		"queue", config.QueueName,
		"exchange", config.Exchange,
		"routing_key", config.RoutingKey,
	)
	return nil
}

// upsert replaces the first element matching same with v, or appends v, so
// repeated declarations do not grow the topology cache.
func upsert[T any](s []T, v T, same func(T) bool) []T {
	if i := slices.IndexFunc(s, same); i >= 0 {
		s[i] = v
		return s
	}
	return append(s, v)
}

// restoreTopology re-declares all cached topology on the current connections.
// connect calls it while the client is still StateConnecting, before consumers
// that wait on IsHealthy are released.
//
// A declaration the broker rejects (for example PRECONDITION_FAILED because a
// queue exists with different arguments) is logged and skipped: retrying cannot
// fix a conflicting definition, and one bad entry must not keep the whole client
// unhealthy. Anything else, such as the connection dropping, is returned so the
// reconnect attempt fails and is retried. The cache is only read, never reset,
// so skipped entries are tried again on the next reconnect.
func (c *Client) restoreTopology() error {
	c.topology.mu.RLock()
	exchanges := slices.Clone(c.topology.exchanges)
	queues := slices.Clone(c.topology.queues)
	bindings := slices.Clone(c.topology.bindings)
	c.topology.mu.RUnlock()

	if len(exchanges)+len(queues)+len(bindings) == 0 {
		return nil
	}

	skipped := 0
	restore := func(what string, declare func(*amqp.Channel) error) error {
		ok, err := c.restoreOne(what, declare)
		if !ok {
			skipped++
		}
		return err
	}

	for _, exchange := range exchanges {
		if err := restore("exchange "+exchange.Name, func(ch *amqp.Channel) error {
			return declareExchangeOn(ch, exchange)
		}); err != nil {
			return err
		}
	}

	for _, queue := range queues {
		if err := restore("queue "+queue.Name, func(ch *amqp.Channel) error {
			_, err := c.declareQueueOn(ch, queue)
			return err
		}); err != nil {
			return err
		}
	}

	for _, binding := range bindings {
		if err := restore("binding "+binding.QueueName+" -> "+binding.Exchange, func(ch *amqp.Channel) error {
			return bindQueueOn(ch, binding)
		}); err != nil {
			return err
		}
	}

	c.logger.Info("topology restored",
		"exchanges", len(exchanges),
		"queues", len(queues),
		"bindings", len(bindings),
		"skipped", skipped,
	)
	return nil
}

// restoreOne runs one declaration on a channel of its own, since a rejected
// declaration closes its channel. It reports whether the entry was restored; a
// false result with a nil error means the broker rejected it and it was skipped.
func (c *Client) restoreOne(what string, declare func(*amqp.Channel) error) (bool, error) {
	ch, err := c.openConsumerChannel()
	if err != nil {
		return false, err
	}
	defer func() { _ = ch.Close() }()

	err = declare(ch)
	if err == nil {
		return true, nil
	}
	if isBrokerRejection(err) && !c.consumerConnClosed() {
		c.logger.Error("broker rejected topology entry, skipping it", "entry", what, "error", err)
		return false, nil
	}
	return false, err
}

// isBrokerRejection reports whether err is the broker refusing a single
// operation (a server-initiated channel exception), as opposed to a transport
// failure.
func isBrokerRejection(err error) bool {
	amqpErr, ok := errors.AsType[*amqp.Error](err)
	return ok && amqpErr.Server
}

// consumerConnClosed reports whether the consumer connection is gone. A broker
// that closes the whole connection also sets amqp.Error.Server, so this tells a
// rejected entry apart from a dead connection.
func (c *Client) consumerConnClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.consumerConn == nil || c.consumerConn.IsClosed()
}

// SetupDeadLetterExchange creates a DLX with retry and parking lot queues
func (tm *TopologyManager) SetupDeadLetterExchange(mainQueueName, dlxName string, retryTTL int) error {
	// Create the dead letter exchange
	if err := tm.DeclareExchange(ExchangeConfig{
		Name:    dlxName,
		Kind:    "topic",
		Durable: true,
	}); err != nil {
		return fmt.Errorf("failed to declare DLX: %w", err)
	}

	// Create retry queue
	retryQueueName := mainQueueName + ".retry"
	retryArgs := map[string]any{
		"x-message-ttl":             retryTTL,
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": mainQueueName,
	}

	if _, err := tm.DeclareQueue(QueueConfig{
		Name:    retryQueueName,
		Durable: true,
		Args:    retryArgs,
	}); err != nil {
		return fmt.Errorf("failed to declare retry queue: %w", err)
	}

	// Bind retry queue to DLX
	if err := tm.BindQueue(BindingConfig{
		QueueName:  retryQueueName,
		Exchange:   dlxName,
		RoutingKey: mainQueueName,
	}); err != nil {
		return fmt.Errorf("failed to bind retry queue: %w", err)
	}

	// Create parking lot queue (for messages that exceeded max retries)
	parkingLotName := mainQueueName + ".parking-lot"
	if _, err := tm.DeclareQueue(QueueConfig{
		Name:    parkingLotName,
		Durable: true,
	}); err != nil {
		return fmt.Errorf("failed to declare parking lot queue: %w", err)
	}

	// Bind parking lot queue to DLX with different routing
	if err := tm.BindQueue(BindingConfig{
		QueueName:  parkingLotName,
		Exchange:   dlxName,
		RoutingKey: mainQueueName + ".parking-lot",
	}); err != nil {
		return fmt.Errorf("failed to bind parking lot queue: %w", err)
	}

	tm.client.logger.Info("DLX setup complete",
		"main_queue", mainQueueName,
		"dlx", dlxName,
		"retry_ttl", retryTTL,
	)
	return nil
}

// SetupQueueWithDLX creates a main queue with DLX arguments
func (tm *TopologyManager) SetupQueueWithDLX(queueName, dlxName string) (*amqp.Queue, error) {
	args := map[string]any{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": queueName,
	}

	if tm.client.config.QueueType == QueueTypeQuorum {
		args["x-queue-type"] = "quorum"
	}

	queue, err := tm.DeclareQueue(QueueConfig{
		Name:    queueName,
		Durable: true,
		Args:    args,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to declare main queue with DLX: %w", err)
	}

	return queue, nil
}
