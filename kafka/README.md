# Kafka

Consumer processing failures are classified by the application. The SDK controls
retry timing, offset commits, and confirmed dead-letter publication.

## Processing policy

`ConsumerOptions.RetryPolicy` is optional. With no policy, the consumer allows
three retries with jittered exponential backoff under a ten-second total budget,
then returns an error wrapping `ErrConsumerPaused`. The failed record and later
unprocessed records are not committed. Earlier successful records may commit.

A non-nil policy uses `MaxRetries` literally: zero means no retries. Backoff and
elapsed-time fields default when zero. Handlers must honor their context.

| Classification | Behavior |
| --- | --- |
| `UnknownFailure` (default) | Bounded retries, then stop and preserve input; never automatically DLQ |
| `RetryableFailure` | Bounded retries, then stop; park only with explicit `OnExhausted: DeadLetter` |
| `PermanentFailure` | Skip retries; park if configured, otherwise stop and preserve input |
| `DependencyUnavailable` | Stop immediately; optional recovery probes gate a fresh consumer |
| `ConfigurationFailure` | Stop immediately for operator intervention |

Use `errors.Is` or typed errors to classify application failures. Do not classify
by matching error-message text. Shutdown cancellation bypasses classification and
leaves unfinished work uncommitted. `ProcessingError` exposes `Kind`, `Attempts`
and the original wrapped error; the returned error also names the source topic,
partition and offset. Retry budgets and attempt metadata apply per invocation;
supervisors must not repeatedly restart unknown/configuration failures.

## Dead-letter topic configuration

A nil `DeadLetter` disables parking. Example for an existing topic:

```go
options := kafka.ConsumerOptions{
    RetryPolicy: &kafka.RetryPolicy{
        Classify: classifyApplicationError,
        MaxRetries: 3,
        MaxElapsedTime: 10 * time.Second,
        OnExhausted: kafka.DeadLetter,
    },
    DeadLetter: &kafka.DeadLetterConfig{
        Topic: "cards.failed",
        Provision: kafka.UseExisting,
        Partitions: 3,
        ReplicationFactor: 3,
        Configs: map[string]string{
            "retention.ms": "604800000",
            "retention.bytes": "1073741824",
            "min.insync.replicas": "2",
        },
    },
}
```

`OnExhausted: DeadLetter` requires a configured destination, and only applies to
explicitly retryable errors. Unknown errors remain preserved even with a DLQ.

`UseExisting` checks topic existence and any specified partition, replication,
and topic-config requirements before polling. Zero partition/replication fields
in this mode mean no requirement. `Declare` creates the topic if absent and
requires explicit positive partitions/replication. Existing incompatible topics
cause startup failure rather than silent reconfiguration. Topic configuration
validation requires the relevant Kafka describe permissions. Provisioning needs
create permissions. If exactly one source topic is configured, an empty
configured destination name defaults to `<source>.dlq`.

The consumer's client must use `AllISRAcks` for DLQ writes and cannot be
transactional. Consumers require a group, must disable auto-commit and use
`BlockRebalanceOnPoll`; `NewKafkaClientFromConfig` configures those group options.
The SDK waits for synchronous publication before committing the original offset.
A failed publication returns `ErrDLQPublish` and preserves the input. Retention,
replication and ISR policy determine broker-side durability.

Payload, key and application headers are preserved. Reserved `fiddle.dlq.*`
headers hold source topic/partition/offset, classification, attempts and failure
time. Error metadata defaults to an error class/type; opt into sanitized details
with `DeadLetterConfig.ErrorFormatter`. A dead-letter topic must differ from
source topics. Redrive is explicit; the SDK never sends a DLQ back to its source.

## Recovery without consuming an outage backlog

`Start` returns on an outage, allowing the application to release the client.
Do not reuse a client that has fetched past a failed record. Use the same group
with a fresh client so broker-committed offsets determine redelivery.

```go
err := kafka.RunWithRecovery(ctx, kafka.RecoveryPolicy{
    Probe: checkCardServiceHealth, // application dependency, not broker health
    Interval: 30 * time.Second,
    Timeout: 5 * time.Second,
}, func(ctx context.Context) error {
    client, err := kafka.NewKafkaClientFromConfig(ctx, cfg)
    if err != nil { return err }
    defer client.Close()
    err = kafka.NewConsumer(client, options).Start(ctx, handleCard)
    // Record err here so the original cause is observable while probing.
    return err
})
```

Only a classified dependency outage triggers probes. No consumer exists while
probing; a successful, timely probe permits one fresh consumer run. Unknown,
configuration, setup, and DLQ transfer failures return to the caller. The outer
context bounds recovery; probes must honor their contexts.

Delivery is at least once. A crash after DLQ publication but before offset commit
can create duplicates. Handlers and redrive tools must be idempotent.

## Migration and verification

Previously, exhausted failures could be committed with no DLQ. They now stop.
`DLQProducer` is deprecated and rejected at startup; use `DeadLetterConfig` so the
SDK can verify topology, acknowledgements and metadata. The standalone
`Producer.SendToDLQ` convenience method also requires `AllISRAcks`.

Run `make check` from this module with Go 1.27.2 and the configured lint tools.
Unit tests use fake broker-operation interfaces. The optional broker test verifies
that a fresh consumer redelivers a failed record after earlier offsets commit:

```sh
FIDDLE_KAFKA_BROKERS=localhost:9092 go test -run TestIntegration -count=1 -timeout 60s
```

Use a disposable plaintext cluster: the test creates/deletes a uniquely named
topic; the unique consumer group follows broker offset retention. Without the
environment variable it is skipped. No
Docker orchestration is needed by the test suite.
