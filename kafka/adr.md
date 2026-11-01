# Processing failures and dead-letter delivery

Status: implemented; executable verification pending the declared Go toolchain.

The application classifies business failures; the SDK owns retry budgets and
settlement. Unknown failures stop without committing. Dependency outages stop
immediately; an optional SDK recovery loop probes before constructing a fresh
consumer. Only permanent failures or explicitly configured retry exhaustion are
parked. This avoids moving an outage backlog into a DLQ.

Keep the existing Handler signature and add optional RetryPolicy/DeadLetterConfig.
Validate topic requirements before polling. Use the managed consumer's native
synchronous producer with AllISRAcks rather than an arbitrary DLQ producer whose
acknowledgement and metadata behavior cannot be verified. Reject automatic group
offset commits, which could advance past failures. Preserve the original cause
and record location for application observability.

Use franz-go/kadm's topic APIs for provisioning and validation. Existing topics
are not silently altered. Kafka partition/replication/retention settings remain
explicit operational choices. Do not add a shared policy module dependency to
this standalone multi-module repository; RabbitMQ has the same policy contract.

At-least-once semantics permit duplicate processing or parking after a crash.
Fresh-client recovery uses broker-committed offsets. Caller-owned resources must
be closed before entering the probe loop. Budgets are per run, and user callbacks
must respect context deadlines. Unknown errors never authorize deletion.
