# rabbitmq

Go SDK for RabbitMQ with auto-reconnection, channel pooling, OTel tracing, and dead-letter support.

## Topology

```
┌─────────────────────────────────────────────────────┐
│                    rabbitmq package                   │
│                                                       │
│  ┌─────────────────────────────────────────────────┐ │
│  │  Connection Layer                                │ │
│  │  ┌──────────┐ ┌───────────┐ ┌────────────────┐ │ │
│  │  │  Client   │ │channelPool│ │ TopologyCache  │ │ │
│  │  │dual AMQP │ │N pub chan │ │ queues,exchs,  │ │ │
│  │  │conns +   │ │get/return │ │ bindings saved │ │ │
│  │  │state mach│ │drain/close│ │ for reconnect  │ │ │
│  │  └────┬─────┘ └─────┬─────┘ └───────┬────────┘ │ │
│  └───────┼─────────────┼───────────────┼───────────┘ │
│          │             │               │              │
│  ┌───────┴─────────────┴───────────────┴───────────┐ │
│  │  Messaging Layer                                 │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │ │
│  │  │ Producer │ │ Consumer │ │ BatchConsumer    │ │ │
│  │  │ publish  │ │ manual/  │ │ accumulate →     │ │ │
│  │  │ batch,   │ │ auto ack │ │ flush on size/   │ │ │
│  │  │ confirms │ │ retry/DLX│ │ timeout, batch   │ │ │
│  │  │ OTel inj │ │ OTel ext │ │ ack/nack         │ │ │
│  │  └────┬─────┘ └────┬────┘ └────────┬─────────┘ │ │
│  └───────┼──────────────┼──────────────┼───────────┘ │
│          │              │              │              │
│  ┌───────┴──────────────┴──────────────┴───────────┐ │
│  │  TopologyManager (implements TopologyDeclarer)  │ │
│  │  QueueDeclare → ExchangeDeclare → BindQueue     │ │
│  │  SetupDeadLetterExchange()                      │ │
│  │  Re-declares all cached topology on reconnect   │ │
│  └──────────────────────┬──────────────────────────┘ │
│                         │                             │
│  ┌──────────────────────┴──────────────────────────┐ │
│  │  Config: URL, conn, producer, consumer, retry,  │ │
│  │  reconnect settings, DLX flags                  │ │
│  └─────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘

            │ Dial / Channel
            ▼
┌───────────────────────┐
│   amqp091-go (AMQP)   │
└───────────┬───────────┘
            │ AMQP 0-9-1
            ▼
┌───────────────────────┐
│  RabbitMQ Broker      │
└───────────────────────┘
```

### Dead-Letter Exchange

```
main queue ──x-dead-letter-exchange──→ DLX (topic exchange)
       │                                   │
  Reject(false)                    routing key: orders
  (RetryEnabled)                         │
                                  orders.retry queue
                                  (TTL: 30s, dead-letters back to main)

after MaxRetries the consumer publishes a copy straight to the
orders.parking-lot queue and acks the original (no DLX involved)
```

### channelPool

RMQ conns have ~finite channel count. New channel per publish → exhaust. Pool recycles N fixed channels.

### Publisher Confirm

RMQ sends ACK/NACK back after broker handles msg. Without confirm: `Publish` returns success as soon as TCP buffer accepts bytes. Msg could be lost before broker routes it (exchange missing, queue full, disk write fail). Set `PublisherConfirms: true` in config.
## Connection State Machine

```mermaid
stateDiagram-v2
    [*] --> Disconnected : NewClient
    Disconnected --> Connecting : connect()
    Connecting --> Connected : both conns + pool ready
    Connected --> Connecting : connection lost
    Connecting --> Connecting : retry (backoff)
    Connected --> Closing : Close()
    Connecting --> Closing : Close()
    Disconnected --> Closing : Close()
    Closing --> [*]
```

## Reconnect Flow

```mermaid
sequenceDiagram
    participant App
    participant Client
    participant Pool
    participant Cache
    participant Broker as RabbitMQ

    Broker--)Client: connection close detected
    Client->>Pool: drain()
    Client->>Client: CAS Connected → Connecting
    loop backoff 1s → 2s → ... → 60s
        Client->>Broker: dial publisher conn
        Client->>Broker: dial consumer conn
        alt success
            Client->>Pool: fill(pubConn)
            Client->>Cache: snapshot + reset
            Client->>Cache: re-declare queues
            Client->>Cache: re-declare exchanges
            Client->>Cache: re-declare bindings
            Client->>Client: state = Connected
        else failure
            Client->>Client: backoff *= 2
        end
    end
```

On every reconnect the cached topology is re-declared **before** the client reports healthy, so consumers waiting on `IsHealthy` find their queues. A declaration the broker rejects (for example `PRECONDITION_FAILED` because the queue exists with different arguments) is logged and skipped, and the other entries are still restored; it is tried again on the next reconnect. A lost connection fails the attempt, which is retried with backoff.

## Dead-Letter Exchange Topology

```mermaid
graph LR
    subgraph Main
        MainQ["orders<br/><i>main queue</i>"]
    end

    subgraph DLX["orders.dlx<br/><i>topic exchange</i>"]
    end

    subgraph Retry["Retry"]
        RetryQ["orders.retry<br/><i>TTL: 30s<br/>DLX → orders</i>"]
    end

    subgraph DLQ["Dead Letter"]
        ParkingQ["orders.parking-lot<br/><i>failed messages</i>"]
    end

    MainQ -- "x-dead-letter-exchange" --> DLX
    MainQ -- "Reject(false)<br/>(RetryEnabled)" --> DLX
    RetryQ -- "TTL expires" --> MainQ
    DLX -- "routing key: orders" --> RetryQ
    MainQ -. "after MaxRetries:<br/>copy published, original acked" .-> ParkingQ

    style MainQ fill:#e8f5e9,stroke:#2e7d32
    style DLX fill:#fff3e0,stroke:#ef6c00
    style RetryQ fill:#e3f2fd,stroke:#1976d2
    style ParkingQ fill:#fce4ec,stroke:#c62828
```

## Message Lifecycle

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant Q as Queue
    participant C as Consumer
    participant DLX as DLX
    participant D as ParkingLot

    P->>B: PublishMessage("orders", payload)
    B->>Q: route
    Q->>C: deliver
    C->>C: handler(ctx, delivery)
    alt success
        C->>Q: Ack
    else handler error, retries < MaxRetries, RetryEnabled
        C->>Q: Reject(false)
        Q->>DLX: route via x-dead-letter-exchange
        Note over DLX: retry queue, back to Q after the retry queue TTL<br/>x-death count incremented
    else handler error, retries < MaxRetries, not RetryEnabled
        C->>Q: republish copy, x-retry-count + 1
        C->>Q: Ack original
    else handler error, retries >= MaxRetries (or ErrPoisonMessage)
        alt DeadLetterEnabled
            C->>D: publish copy to orders.parking-lot
            C->>Q: Ack original
        else
            C->>Q: Reject(false)
        end
    end
```

## Batch Consumer Flow

```mermaid
sequenceDiagram
    participant Q as Queue
    participant BC as BatchConsumer
    participant H as BatchHandler

    BC->>Q: Consume (QoS prefetch, autoAck=false)
    loop accumulating
        Q-->>BC: delivery
        opt WithBatchValidator set
            BC->>BC: validate(delivery)
            Note over BC: invalid: settled on its own<br/>(parking lot / reject), not batched
        end
        BC->>BC: append to batch[]
        alt batchSize reached
            BC->>BC: flush()
        else flushTimeout elapsed
            BC->>BC: flush()
        end
    end
    BC->>H: handler(ctx, deliveries)
    alt success
        H-->>BC: nil
        BC->>Q: multiple Ack on the last delivery
    else failure
        H-->>BC: error
        BC->>Q: retry policy applied per delivery
    end
    Q-->>BC: ctx cancelled
    BC->>BC: flush() remaining
    BC->>Q: close channel
```

## Public API

### Interfaces

| Interface | Purpose | Constructor |
|-----------|---------|-------------|
| `Publisher` | Send messages | `NewProducer(client *Client) Publisher` |
| `Subscriber` | Consume messages | `NewConsumer(client *Client, logger *slog.Logger) Subscriber` |
| `TopologyDeclarer` | Declare topology | `NewTopologyManager(client *Client) TopologyDeclarer` |

### Concrete types

| Type | Constructor | Notes |
|------|-------------|-------|
| `*Client` | `NewClient(config *Config, logger *slog.Logger) (*Client, error)` | Two AMQP conns (pub + con), channel pool, topology cache |
| `*BatchConsumer` | `NewBatchConsumer(client *Client, logger *slog.Logger, batchSize int, flushTimeout time.Duration, opts ...BatchOption) *BatchConsumer` | Owns its own channel; not part of `Subscriber` interface |
| `*Config` | `NewDefaultConfig() *Config` | Pre-populated defaults; caller must set `URL`. `NewClient` fills the other unset fields (`ApplyDefaults`) and checks the result (`Validate`) on a copy |

### Handlers

| Type | Signature | Notes |
|------|-----------|-------|
| `MessageHandler` | `func(ctx, delivery amqp.Delivery) error` | Raw payload in `delivery.Body`; any content type |
| `JSONHandler[T]` | `JSONHandler(func(ctx, msg T, delivery) error) MessageHandler` | Decodes the body into `T`; an undecodable body is a poison message |
| `BatchHandler` | `func(ctx, deliveries []amqp.Delivery) error` | Raw payloads |
| `JSONBatchHandler[T]` | `JSONBatchHandler(func(ctx, msgs []T, deliveries) error) BatchHandler` | `msgs[i]` belongs to `deliveries[i]` |
| `ValidJSON` | `func(amqp.Delivery) error` | For `WithBatchValidator`: keeps non-JSON bodies out of batches |

### Errors

| Sentinel | Meaning |
|----------|---------|
| `ErrNotConnected` | `rabbitmq: client not connected` |
| `ErrURLRequired` | `rabbitmq: URL is required` |
| `ErrPoisonMessage` | Wrap it in a handler error to skip the retries: the message is parked (`DeadLetterEnabled`) or rejected at once. `JSONHandler` does so for undecodable bodies |

### Health & State

| Method | Returns |
|--------|---------|
| `client.IsHealthy() bool` | `true` when `StateConnected` |
| `client.GetState() ConnectionState` | `StateDisconnected` / `StateConnecting` / `StateConnected` / `StateClosing` |

---

## Usage

### Basic

```go
logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

client, err := rabbitmq.NewClient(&rabbitmq.Config{
    URL:              "amqp://guest:guest@localhost:5672/",
    ConnectionName:   "my-service",
    PublisherConfirms: true,
    QueueType:         rabbitmq.QueueTypeQuorum,
    RetryEnabled:      true,
    MaxRetries:        3,
    DeadLetterEnabled: true,
}, logger)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Declare topology

```go
tm := rabbitmq.NewTopologyManager(client)

// Simple queue
tm.DeclareQueue(rabbitmq.QueueConfig{Name: "orders", Durable: true})

// Exchange + binding
tm.DeclareExchange(rabbitmq.ExchangeConfig{Name: "events", Kind: "topic", Durable: true})
tm.BindQueue(rabbitmq.BindingConfig{
    QueueName:  "orders",
    Exchange:   "events",
    RoutingKey: "order.*",
})

// Full DLX setup (creates orders.dlx exchange, orders.retry queue, orders.parking-lot queue)
tm.SetupDeadLetterExchange("orders", "orders.dlx", 30000)

// Queue with DLX pre-wired
tm.SetupQueueWithDLX("orders", "orders.dlx")
```

### Publish

```go
producer := rabbitmq.NewProducer(client)

// Simple publish to default exchange (routing key = queue name)
producer.SendMessage(ctx, "orders", orderPayload)

// Publish with custom headers
producer.SendMessageWithHeaders(ctx, "orders", payload, map[string]interface{}{
    "x-custom-header": "value",
})

// Publish to named exchange
producer.SendToTopic(ctx, "events", "order.created", eventPayload)
producer.SendToTopicWithHeaders(ctx, "events", "order.created", payload, headers)

// Full control
producer.PublishMessage(ctx, rabbitmq.PublishOptions{
    Exchange:    "events",
    RoutingKey:  "order.updated",
    DeliveryMode: 2, // persistent
    MessageId:   "msg-123",
    Headers:     map[string]interface{}{"region": "eu"},
}, payload)

// Batch publish (fails fast on first error)
producer.PublishBatch(ctx, rabbitmq.PublishOptions{
    Exchange:   "",
    RoutingKey: "orders",
}, []interface{}{payload1, payload2, payload3})
```

### Consume

```go
consumer := rabbitmq.NewConsumer(client, logger)

// Typed JSON: the body is decoded into Order. A body that does not decode is a
// poison message and is parked without retries.
handler := rabbitmq.JSONHandler(func(ctx context.Context, order Order, delivery amqp.Delivery) error {
    // Process order
    // Return nil → Ack
    // Return error → retry policy (see below)
    return nil
})

// Raw bytes: any content type, nothing is decoded for you.
raw := func(ctx context.Context, delivery amqp.Delivery) error {
    return process(delivery.ContentType, delivery.Body)
}

// Minimal (sensible defaults: manual ack, no auto-reconnect)
consumer.ConsumeWithDefaults(ctx, "orders", handler)

// Full control
consumer.StartConsumer(ctx, rabbitmq.ConsumeOptions{
    QueueName:     "orders",
    Consumer:      "worker-1",
    AutoReconnect: true, // restart on delivery channel close
}, handler)
```

A handler error is settled by the retry policy:

| Situation | What happens |
|-----------|--------------|
| `RetryEnabled`, attempts < `MaxRetries` | `Reject(false)`; the queue's DLX routes the message through the TTL retry queue (its TTL is the one given to `SetupDeadLetterExchange`) |
| not `RetryEnabled`, attempts < `MaxRetries` | The consumer waits `RetryBackoff` (doubling with each retry, capped at 30s), then republishes a copy with `x-retry-count` + 1 and acks the original. Works on classic and quorum queues. The wait is in the consume loop, so it also slows the consumer while a dependency keeps failing; a batch waits once for all its messages. The republished copy keeps the message properties, `Expiration` included (it gets its full TTL again), but not `UserId`, which the broker rejects when it differs from the connection's user |
| attempts >= `MaxRetries`, or error wraps `ErrPoisonMessage` | `DeadLetterEnabled`: copy published to `<queue>.parking-lot`, original acked. Otherwise `Reject(false)` |

The attempt count is the largest of the `x-death` count, `x-retry-count` and the quorum queue's `x-delivery-count`.

### Batch consume

```go
bc := rabbitmq.NewBatchConsumer(client, logger, 100, 5*time.Second,
    rabbitmq.WithBatchValidator(rabbitmq.ValidJSON), // optional: park non-JSON bodies on their own
)

batchHandler := rabbitmq.JSONBatchHandler(func(ctx context.Context, orders []Order, deliveries []amqp.Delivery) error {
    // Process batch
    // Return nil → one multiple-Ack for the whole batch
    // Return error → retry policy applied to every delivery
    return nil
})

bc.StartBatchConsumer(ctx, rabbitmq.ConsumeOptions{
    QueueName: "orders",
    Consumer:  "batch-worker",
}, batchHandler)
```

---

## Configuration Reference

### Config (top-level)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `URL` | `string` | **required** | AMQP connection string |
| `ConnectionName` | `string` | `"go-service"` | Identifies client in broker UI |
| `PublisherConfirms` | `bool` | `false` | Enable per-message publisher confirms |
| `ChannelPoolSize` | `int` | `10` | Number of pre-allocated publisher channels |
| `Mandatory` | `bool` | `false` | Broker returns unroutable messages via `OnReturn` |
| `PersistentDelivery` | `bool` | `false` | Default `DeliveryMode = 2` (overridable per-message) |
| `PrefetchCount` | `int` | `10` | Consumer QoS prefetch count |
| `AutoAck` | `bool` | `false` | Should always be `false` in production |
| `QueueType` | `QueueType` | `"quorum"` | `QueueTypeQuorum` or `QueueTypeClassic` |
| `RetryEnabled` | `bool` | `false` | Retry through the queue's DLX and TTL retry queue. When `false`, retries are republished with an `x-retry-count` header; see [Consume](#consume) |
| `RetryBackoff` | `time.Duration` | `1s` | Base delay between counted retries when `RetryEnabled` is `false`; doubles per retry, capped at 30s. Zero uses the default, negative retries at once |
| `MaxRetries` | `int` | `3` | Retries after the first failure (the count is the largest of `x-death`, `x-retry-count`, `x-delivery-count`). Zero means "unset" and uses the default, so write a negative value for no retries: failures then go straight to the parking lot (or are rejected) |
| `DeadLetterEnabled` | `bool` | `false` | Enable DLX routing for failed messages |
| `ReconnectInitialInterval` | `time.Duration` | `1s` | Exponential backoff start |
| `ReconnectMaxInterval` | `time.Duration` | `60s` | Exponential backoff cap |
| `Heartbeat` | `time.Duration` | `0` | `0` = server-negotiated |
| `ChannelMax` | `int` | `0` | `0` = server-negotiated |
| `FrameSize` | `int` | `0` | `0` = server-negotiated |
| `OnReturn` | `func(amqp.Return)` | `nil` | Callback for unroutable mandatory messages |

### QueueConfig

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Queue name |
| `Durable` | `bool` | Survive broker restart |
| `AutoDelete` | `bool` | Delete when last consumer disconnects |
| `Exclusive` | `bool` | Only accessible by declaring connection |
| `NoWait` | `bool` | Don't wait for broker confirmation |
| `Args` | `map[string]interface{}` | Custom arguments (e.g. `x-queue-type`) |

### ExchangeConfig

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Exchange name |
| `Kind` | `string` | `direct`, `fanout`, `topic`, `headers` |
| `Durable` | `bool` | Survive broker restart |
| `AutoDelete` | `bool` | Delete when no queues bound |
| `Internal` | `bool` | Internal exchange (no publisher access) |
| `NoWait` | `bool` | Don't wait for broker confirmation |
| `Args` | `map[string]interface{}` | Custom arguments |

### BindingConfig

| Field | Type | Description |
|-------|------|-------------|
| `QueueName` | `string` | Queue to bind |
| `Exchange` | `string` | Exchange to bind to |
| `RoutingKey` | `string` | Routing key pattern |
| `NoWait` | `bool` | Don't wait for broker confirmation |
| `Args` | `map[string]interface{}` | Custom arguments |

### PublishOptions

| Field | Type | Description |
|-------|------|-------------|
| `Exchange` | `string` | Exchange name (empty = default exchange) |
| `RoutingKey` | `string` | Routing key (or queue name for default exchange) |
| `Mandatory` | `bool` | Override Config.Mandatory |
| `Immediate` | `bool` | AMQP immediate flag |
| `MessageId` | `string` | App-level message identifier |
| `CorrelationId` | `string` | Request/reply correlation |
| `ReplyTo` | `string` | Reply queue address |
| `Expiration` | `string` | Per-message TTL string |
| `Priority` | `uint8` | Message priority (0-9) |
| `Type` | `string` | Message type name |
| `AppId` | `string` | Application identifier |
| `UserId` | `string` | User identifier |
| `ContentType` | `string` | MIME type (default: `"application/json"`) |
| `ContentEncoding` | `string` | Content encoding |
| `Timestamp` | `time.Time` | Message timestamp |
| `DeliveryMode` | `uint8` | `0` = use Config, `1` = transient, `2` = persistent |
| `Headers` | `map[string]interface{}` | Custom headers (OTel context auto-injected) |

### ConsumeOptions

| Field | Type | Description |
|-------|------|-------------|
| `QueueName` | `string` | Queue to consume from |
| `Consumer` | `string` | Consumer tag (name in broker UI) |
| `AutoAck` | `bool` | Override Config.AutoAck |
| `Exclusive` | `bool` | Exclusive consumer |
| `NoLocal` | `bool` | Don't receive own messages |
| `NoWait` | `bool` | Don't wait for broker confirmation |
| `Args` | `map[string]interface{}` | Custom arguments |
| `AutoReconnect` | `bool` | Restart consume loop after delivery channel close |

---

## Testing

```bash
cd rabbitmq && make test    # go test -count=1 -timeout 120s ./...
```
