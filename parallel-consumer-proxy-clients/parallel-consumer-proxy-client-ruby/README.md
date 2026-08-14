<!--
Copyright (C) 2026 Antony Stubbs and contributors
-->

# Parallel Consumer - Ruby client

Ordered concurrent consumption from Kafka, with one consumer, in Ruby. The Kafka work happens in a
sidecar process running Parallel Consumer itself; this library holds one gRPC stream to it, runs
your block on executor threads, and reports each record's outcome.

**Wave one.** Connect, configure, receive a dispatch wave, run the block, report, shut down
cleanly. Leases, heartbeats, the manifest reconnect, worker-death reporting, terminal outcomes, the
demo and RubyGems packaging are all deferred, and the module's testing-evidence record names them.
Not supported for external use yet.

## Using it

```ruby
require "parallel_consumer"

options = ParallelConsumer::ClientOptions.new(
  topics: ["orders"],
  max_concurrency: 64,
  kafka_properties: { "bootstrap.servers" => "localhost:9092", "group.id" => "orders" }
)

ParallelConsumer.open(options, sidecar: "/opt/pc/parallel-consumer-proxy") do |client|
  client.poll { |record| charge(record.key, record.value) }
  client.wait
end
```

The block is the user's function. Return normally and the record succeeded; raise and it failed,
with the exception's text riding back to you on the redelivery as `record.last_failure_reason`.
Return an `Outcome` when you want to say more:

```ruby
client.poll do |record|
  ParallelConsumer::Outcome.success(
    produce: [ParallelConsumer::OutboundRecord.new(topic: "orders.charged", key: record.key, value: "ok")]
  )
end
```

Workers never hold a Kafka producer: output rides back on the success report and the proxy produces
it. Keys and values are bytes, and `nil` means a null key or value - a tombstone is not an empty
value.

### `poll` does not block

`poll` starts consumption and returns; `wait` blocks. See the class documentation in
`lib/parallel_consumer/client.rb` for why - briefly, a blocking `poll` puts `close` out of reach of
the thread that called it, and every other language's client has the same non-blocking shape.

### Executors are threads

`Configured.executor_count` executors are Ruby threads, not processes. The full argument is in the
module documentation in `lib/parallel_consumer.rb`; the limitation it names is that on MRI a
**CPU-bound** block gets concurrency but not parallelism, and the answer to that is Ruby's usual
one - threads within a process, processes without: run several application processes in the same
consumer group, each with its own sidecar.

## Building and testing, locally

```bash
bundle install                # once
bundle exec rake              # rubocop, then the specs - the whole local loop
bundle exec rubocop           # the bug finder alone
bundle exec rspec             # the specs alone
```

`bundle exec rake` is what the Maven wrapper runs too, so `-Dpc.foreignClients` and a developer's
box run the same thing:

```bash
JAVA_HOME=... bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests   # the harness, first
JAVA_HOME=... bin/build.sh -pl :parallel-consumer-proxy-client-ruby -am -Dpc.foreignClients
```

The first command is not optional and Maven cannot do it for us: the conformance spec spawns the
JVM-side test-mode sidecar from the proxy module's **test** jar, and this module deliberately has
no Maven dependency on the proxy. Without it the spec fails naming that command, rather than
skipping.

### RuboCop is configured as a bug finder

`.rubocop.yml` leaves the `Lint/` and `Security/` departments at full strength and turns the style
departments down to what changes meaning. It has been proven to fail:
`Lint/UselessAssignment` and `Lint/UnreachableCode` were each triggered deliberately against this
configuration and watched go red before the defect was reverted.

### Regenerating the protobuf stubs

```bash
./scripts/generate-proto.sh   # or: bundle exec rake proto
```

The stubs under `lib/parallelconsumer/` are generated from the frozen schema and **committed** -
Ruby has no codegen step at install time. Re-running the script on an unchanged `.proto` must leave
`git status` clean.

## What this library will never do

- **Find the sidecar for you.** You supply an absolute path. It is never resolved through `PATH` or
  any directory an attacker could influence: this process hands the sidecar your Kafka credentials.
- **Log your `kafka_properties`.** Not at any level, not in an error, not through `inspect`.
- **Read configuration from a file, an environment variable or a flag.** Configuration is code, and
  it travels in the handshake and nowhere else.
