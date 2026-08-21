<!--
Copyright (C) 2026 Antony Stubbs and contributors
-->

# Parallel Consumer - Ruby client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

Ordered concurrent consumption from Kafka, with one consumer, in Ruby. The Kafka work happens in a
sidecar process running Parallel Consumer itself; this library holds one gRPC stream to it, runs
your block on executor threads, and reports each record's outcome.

**Wave one.** Connect, configure, receive dispatch waves, run the block, report per-record
outcomes, produce records back on success, shut down cleanly. Leases, heartbeats, the manifest
reconnect, worker-death reporting, terminal outcomes and RubyGems packaging are all absent -
**un-negotiated capabilities rather than half-built features**, since the client declares
`["dispatch"]` and nothing else. The module's testing-evidence record names them.

## Seeing it work

```bash
demo/run.sh          # needs Docker; Ruby and a JDK are optional
```

The same records through Ruby's own Kafka client (`rdkafka`) and through this library over a
sidecar, reporting each arm's throughput along with the records it processed and the distinct keys
it saw. [`demo/README.md`](demo/README.md) is its own documentation; the contract it
keeps, which every language's demo keeps, is
[`parallel-consumer-proxy/demo/README.md`](../../parallel-consumer-proxy/demo/README.md).

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
bundle exec rake syntax       # ruby -c over every source file - the parse check Maven's compile runs
bundle exec rubocop           # the bug finder alone
bundle exec rspec             # the specs alone
```

There is no clean command, because there is nothing to clean. Ruby compiles to nothing here, the
stubs under `lib/parallelconsumer/` are committed source, and the only thing a build leaves behind
is `vendor/bundle` - the installed bundle, which is this language's `~/.m2` and which `mvn clean`
has no more business deleting than it has emptying `~/.m2`. So `./mvnw clean` removes this module's
`target/` and nothing else, and `pom.xml` configures no clean filesets on purpose - the reasoning
is written out there, because an empty configuration and an unconsidered one look identical.
Measured again on this branch: a compile adds nothing to this directory but `vendor/`.

### In the Maven build

```bash
./mvnw compile -Dpc.foreignClients -pl :parallel-consumer-proxy-client-ruby -am   # scripts/build.sh
./mvnw package -Dpc.foreignClients -pl :parallel-consumer-proxy-client-ruby -am   # bundle exec rake
```

This module is `packaging: pom` with four `pc.foreign.*` properties naming those commands, and the
`foreign-clients` profile in the clients aggregator ([`../pom.xml`](../pom.xml)) binds them to
`compile` and `test` and decides whether the module is in the reactor at all. Nothing is bound to
`clean` in any language here - the pom says why that is forced rather than chosen.

- **`compile` runs `scripts/build.sh`: `bundle install`, then `rake syntax`.** Installing gems is
  not compiling, and while it was the whole of the phase `mvn compile` reported SUCCESS on a `lib/`
  file ending `this is not valid ruby @@@`. Re-verified from this branch: the same sabotage now
  fails the Maven build with MRI's own `SyntaxError`. The script's header owns why it is a script
  rather than two words in the pom, and the Rakefile's `syntax` task owns why `ruby -c` rather than
  RuboCop.
- **`test` runs `bundle exec rake`, whose default task is RuboCop then RSpec** - so the bug finder
  fails the Maven build on a developer's box, not only in the CI row that repeats it.
- **`package`, not `test`**: `spec/support/harness.rb` looks for the proxy module's test jar as a
  *file*, and `test` stops one phase short of producing one. The `ruby-e2e-harness` profile is what
  puts the proxy in the reactor at all, so the older instruction to build it by hand first is no
  longer a prerequisite - and that profile activates on `-Dpc.foreignClients`, *not* on
  `-P foreign-clients`, which activates the module without the engine behind it. That has its uses:
  `-P` leaves the engine out of the reactor - three modules instead of six, and no JDK 17 needed -
  which makes it the quicker loop when all you want is the gems installed and the sources parsed.
- **`-am` is not optional for `compile` or `test`.** `-pl` alone fails the enforcer's
  `ReactorModuleConvergence` with a message about parent modules, which reads as a broken pom;
  [`docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md`](../../docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md)
  owns that. `./mvnw clean -P foreign-clients -pl :parallel-consumer-proxy-client-ruby` still needs
  the profile - without it the module is not in the reactor at all - but needs no `-am`, the clean
  lifecycle never reaching `validate` where the enforcer is bound.
- **`vendor/bundle` must survive a clean**, and RuboCop's cache (`~/.cache/rubocop_cache`) is
  outside the module and shared with every checkout on the box. Neither is build output.

### The shared conformance suite

It drives this client's runner (`scripts/conformance-runner`) through the same scenarios as every
other language, asserting engine state Ruby cannot see:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=ruby
```

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

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md) own the
protocol; this file does not restate them.
