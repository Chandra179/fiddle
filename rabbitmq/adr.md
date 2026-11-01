# Processing failures and reliable transfers

Status: implemented; executable verification pending the declared Go toolchain.

The application classifies business failures through an optional policy. Retry
processing in place with bounded attempts/time and jittered exponential backoff.
Return on unknown/configuration failures, and stop immediately on a dependency
outage. An optional recovery runner probes the dependency with no consumer alive.
This avoids broker retry-copy loops and repeated calls to a failed dependency.

Park only permanent failures or explicitly configured retry exhaustion. Validate
per-subscription durable DLQ topology before consuming. Check publisher returns
as well as confirms before acknowledging the original. Settle batches per delivery
so a later multiple-ack cannot hide an earlier failure. Leave failed input unacked
and close the consumer channel instead of immediately requeueing into a hot loop.

Internal transfers use isolated, bounded-lifetime connections/channels. The
amqp091-go publish context does not interrupt blocked socket writes; cancellation
closes the dedicated socket. A semaphore bounds concurrent transfer connections.
One in-flight publication per channel eliminates return/confirm ambiguity, and
there are no unresolved notifications carried over to another transfer. This
adds connection setup cost on failure paths but leaves normal pooled publishing
behavior unchanged. A future pool must retain these cancellation and correlation
guarantees before replacing this implementation.

Durability, retention and capacity remain broker configuration choices; the SDK
requires durable destinations and explicit overflow semantics for bounded queues.
Using an exchange guarantees delivery to some matching queue, while the default
exchange identifies the exact named queue. AMQP cannot inspect queue argument
equivalence with passive declarations alone, so UseExisting also performs active
idempotent declarations and needs the corresponding permissions.

Delivery is at least once; applications must tolerate duplicates. Preserve body,
properties and failure metadata, reset source TTL/delivery count on parked copies,
and never automatically redrive a DLQ. Keep the same public policy contract as
Kafka without coupling the independent modules through a new shared dependency.
