# RabbitMQ

This module provides publishing, topology management, single-message and batch
consumers. Consumers use application classification, bounded in-place retries,
and confirmed transfers to preserve failed deliveries.

## Failure policy

Set `ConsumeOptions.RetryPolicy` once per subscription. Without it, legacy
`Config.MaxRetries` and positive `RetryBackoff` provide count/delay defaults;
`NewClient` fills defaults of three retries and a one-second initial backoff.
The total handler/retry budget defaults to ten seconds. An explicit policy uses
`MaxRetries: 0` as no retries. Handlers must honor the supplied context.

| Classification | Behavior |
| --- | --- |
| `UnknownFailure` (default) | Bounded retries, then stop and preserve; never automatically park |
| `RetryableFailure` | Bounded retries, then stop; optionally park with `OnExhausted: DeadLetter` |
| `PermanentFailure` | Skip retries; park if configured, otherwise stop and preserve |
| `DependencyUnavailable` | Stop immediately; recovery probes can gate restart |
| `ConfigurationFailure` | Stop immediately for operator intervention |

The caller supplies an optional `Classify func(error) FailureKind` using typed
errors or `errors.Is`. The SDK recognizes errors wrapping `ErrPoisonMessage` as
permanent. `JSONHandler` produces this error for malformed payloads; raw handlers
can accept any format. Cancellation leaves unfinished messages unacknowledged.
`ProcessingError` exposes the failure kind, attempt count and original wrapped
cause, and matches `ErrConsumerPaused`. Budgets apply to each consumer invocation;
supervisors must not restart unknown/configuration failures in tight loops.

Retries call the handler again on the same delivery. They do not republish it,
reject it into a retry queue, or repeatedly nack/requeue it. Any unhandled failure
returns from the consume loop and closes its channel so the broker can redeliver
unfinished messages. Manual acknowledgements are required. `AutoReconnect`
recovers transport disconnections; it does not restart failed processing.

## Configure the dead-letter destination

A nil `ConsumeOptions.DeadLetter` disables parking. To create a configured queue
and optional direct exchange/binding before consuming:

```go
options := rabbitmq.ConsumeOptions{
    QueueName: "cards",
    RetryPolicy: &rabbitmq.RetryPolicy{
        Classify: classifyApplicationError,
        MaxRetries: 3,
        OnExhausted: rabbitmq.DeadLetter,
    },
    DeadLetter: &rabbitmq.DeadLetterConfig{
        Provision: rabbitmq.Declare,
        Queue: rabbitmq.QueueConfig{
            Name: "cards.failed",
            Durable: true,
            Args: map[string]any{
                "x-max-length-bytes": int64(1 << 30),
                "x-overflow": "reject-publish",
                // Optional retention: "x-message-ttl": int64(604800000),
            },
        },
        QueueType: rabbitmq.QueueTypeQuorum,
        Exchange: &rabbitmq.ExchangeConfig{
            Name: "failed.events", Kind: "direct", Durable: true,
        },
        RoutingKey: "cards.failed",
        Timeout: 10 * time.Second,
    },
}
```

`UseExisting` first checks that configured objects exist. Both modes perform
idempotent declarations to verify argument equivalence and ensure the binding;
this requires declaration/binding permissions even for existing topology.
Existing incompatible definitions fail before subscription starts. Configured
queues must be durable, non-exclusive and not auto-delete. An empty configured
queue defaults to durable `<source>.parking-lot`; its type defaults to quorum.
An omitted exchange uses the default exchange and the destination queue name as
routing key. `OnExhausted: DeadLetter` requires a destination at startup and only
applies to explicitly retryable failures.

Retention and overflow are operational choices. Unspecified queue arguments use
broker defaults. Capacity limits require an explicit `x-overflow` of
`reject-publish` or `drop-head`; the latter deletes older parked messages. TTL
expires parked messages. Parking queues cannot configure an automatic DLX loop.

## Transfer guarantees and resource limits

Internal dead-letter writes always use publisher confirms, persistent delivery
and `mandatory=true`, regardless of normal publisher settings. A broker return,
nack, closed connection, or timeout prevents acknowledgement of the original.
Broker acknowledgements cannot mask preceding unroutable returns. Consumption
stops on transfer failure so it cannot create a requeue storm.

Each internal transfer uses an isolated connection/channel, with one publication
in flight. This makes return/confirm correlation unambiguous and allows a timeout
to close its socket even during a blocked publish. `ChannelPoolSize` bounds the
number of transfer connections per client, and `TransferTimeout` bounds each
operation (default ten seconds). This trades connection setup overhead on failure
paths for bounded I/O and isolation from asynchronous public publishing.

Original body and message properties are retained. Source TTL is recorded in
metadata and cleared on the parked copy; the DLQ's retention governs that copy.
Broker delivery-count headers are recorded and removed from the parked copy.
Reserved `fiddle.dlq.*` headers include the source queue/exchange/routing key,
message ID, classification, attempt count and failure time. Error text is omitted
unless `ErrorFormatter` is provided; return only sanitized details.

An exchange-based destination guarantees routing to at least one bound queue;
use the default exchange when the guarantee must refer to the exact named queue.
Delivery is at least once: a crash between transfer and original acknowledgement
can produce duplicates. Broker retention, overflow, and delivery limits still
apply. Redrive is explicit and should be idempotent.

Batch consumers use the same policy. A handler retry applies to the whole batch;
`WithBatchValidator(ValidJSON)` can keep malformed JSON out of batches. Every
message is settled individually so a later acknowledgement cannot include a
failed earlier validator or transfer. Shutdown preserves pending input for
redelivery rather than running new handler work with a detached context.

## Recovery and observing the cause

```go
err := rabbitmq.RunWithRecovery(ctx, rabbitmq.RecoveryPolicy{
    Probe: checkCardServiceHealth,
    Interval: 30 * time.Second,
    Timeout: 5 * time.Second,
}, func(ctx context.Context) error {
    client, err := rabbitmq.NewClient(cfg, logger)
    if err != nil { return err }
    defer client.Close()
    err = rabbitmq.NewConsumer(client, logger).StartConsumer(ctx, options, handleCard)
    // Record err here to observe the original cause while recovery probes run.
    return err
})
```

Only `DependencyUnavailable` triggers probes. The callback releases its consumer
before probing and creates a fresh one after a successful, timely health check.
No messages are consumed during the outage. Unknown/configuration, setup, and
transfer errors return for intervention. The outer context bounds recovery.
Probes must honor context cancellation and check the application dependency.

Normal publishing remains available through `NewProducer`, with public publisher
confirms and persistence controlled by `Config`. `NewTopologyManager` declares
queues, exchanges and bindings and caches topology for transport reconnection.

## Migration and tests

- Exhausted failures with parking disabled are preserved; they are no longer
  rejected and potentially discarded.
- `Config.RetryEnabled: true` is rejected. Replace broker retry loops with a
  subscription `RetryPolicy`; generic topology helpers remain available.
- `Config.DeadLetterEnabled` selects the conventional existing parking queue for
  compatibility. Prefer explicit per-subscription configuration. It does not
  automatically make unknown errors safe to dead-letter.
- `AutoAck` is rejected by reliable consumers.

Run `make check` in this module with Go 1.27.2 and the configured lint tools.
Unit tests cover policy budgets, outage recovery gating, settlement ordering,
batch preservation, and unroutable/ack races. An optional real broker test deletes
a parking queue after startup verification and checks original redelivery:

```sh
FIDDLE_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test -run TestIntegration -count=1 -timeout 60s
```

Use a disposable broker/vhost with queue declaration/deletion permissions. The
test is skipped when the variable is unset; Docker is not required by the suite.
