package rabbitmq

import (
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

// DeclareQueue declares a queue with the given configuration
func (tm *TopologyManager) DeclareQueue(config QueueConfig) (*amqp.Queue, error) {
	ch, err := tm.client.CreateConsumerChannel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}
	defer ch.Close()

	// Add quorum queue argument if needed. Clone so the caller's map (and
	// the cached config) is never mutated.
	args := maps.Clone(config.Args)
	if args == nil {
		args = make(map[string]any)
	}

	if tm.client.config.QueueType == QueueTypeQuorum && config.Durable {
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
		return nil, fmt.Errorf("failed to declare queue %s: %w", config.Name, err)
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
	defer ch.Close()

	err = ch.ExchangeDeclare(
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
	defer ch.Close()

	err = ch.QueueBind(
		config.QueueName,
		config.RoutingKey,
		config.Exchange,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue %s to exchange %s: %w", config.QueueName, config.Exchange, err)
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

// restoreTopology re-declares all cached topology after reconnection.
// It resets the cache before re-declaring so entries are not duplicated on each reconnect.
func (c *Client) restoreTopology() error {
	// Snapshot and reset cache atomically so DeclareQueue/Exchange/BindQueue
	// re-populate with exactly the same entries (no unbounded growth).
	c.topology.mu.Lock()
	exchanges := make([]ExchangeConfig, len(c.topology.exchanges))
	queues := make([]QueueConfig, len(c.topology.queues))
	bindings := make([]BindingConfig, len(c.topology.bindings))
	copy(exchanges, c.topology.exchanges)
	copy(queues, c.topology.queues)
	copy(bindings, c.topology.bindings)
	c.topology.queues = c.topology.queues[:0]
	c.topology.exchanges = c.topology.exchanges[:0]
	c.topology.bindings = c.topology.bindings[:0]
	c.topology.mu.Unlock()

	tm := NewTopologyManager(c)

	for _, exchange := range exchanges {
		if err := tm.DeclareExchange(exchange); err != nil {
			return err
		}
	}

	for _, queue := range queues {
		if _, err := tm.DeclareQueue(queue); err != nil {
			return err
		}
	}

	for _, binding := range bindings {
		if err := tm.BindQueue(binding); err != nil {
			return err
		}
	}

	c.logger.Info("topology restored successfully",
		"exchanges", len(exchanges),
		"queues", len(queues),
		"bindings", len(bindings),
	)
	return nil
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
