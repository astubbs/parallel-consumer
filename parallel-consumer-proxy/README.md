<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The proxy sidecar - Parallel Consumer for languages that are not the JVM

> **⚠️ EXPERIMENTAL - not for production use.** Nothing here is published to any package registry,
> the API may change without notice, and none of it has carried production traffic. Build it from
> this checkout, read it, test it - do not depend on it. Tracking: astubbs#242, upstream
> confluentinc#154.

A process that runs Parallel Consumer, holds the Kafka connection, and hands records to a worker in
another language over the frozen [v1 protocol](../parallel-consumer-proxy-protocol). The worker
returns verdicts; the engine decides everything else.

**The one thing the client deliberately does not mirror is ordering.** Shard selection, retry
scheduling and offset tracking all stay here. That is what keeps a client a facade rather than a
second implementation of Parallel Consumer in every language.

## The shape

```
Kafka ──► ParallelConsumer (this process) ──► gRPC stream ──► your worker, any language
              ▲                                                        │
              └──────────────── verdicts (succeeded / failed) ─────────┘
```

- `transport/` - `ProxyServer` and the two things that make loopback-only safe:
  `AuthorityAllowlistInterceptor` and `SingleConnectionGuard`. **The connection is the session**;
  one worker per sidecar is a design constraint, not a limitation waiting to be lifted.
- `config/` - `ConfigureHandler` and `OptionsMapper` turn a worker's handshake into
  `ParallelConsumerOptions`; `KafkaClientFactory` builds the clients.
- `engine/` - the seam itself. `ProxyProcessor` bridges Parallel Consumer's `ExternalEngine` to the
  wire; `DispatchWaveAssembler` decides what goes out; `InFlightRegistry` tracks what is unresolved;
  `RecordCodec` converts records; `LivenessLease`, `ReconnectWindow` and `ManifestReconciler` handle
  a worker that dies or reconnects.

## Two invariants worth knowing before you read the code

- **The in-flight ceiling counts *unresolved* records - queued plus executing - not queue depth.**
  Counting the queue lets a client exceed the ceiling by the size of its executor pool.
- **The thread that moves work must never be the thread that waits on work.** The transport thread
  never blocks; it is why core separates the poller from the control loop, and why a client whose
  processor blocks instead of awaiting deadlocks its own session.

## Security posture

Loopback-only, and that is what removes the need for authentication: only the spawning process can
reach it, and one application per sidecar means no multi-tenancy. Those constraints are load-bearing
- a shared multi-tenant server would discard all of them at once and is a different product.

## Writing a client

[`docs/client-authoring-guide.md`](docs/client-authoring-guide.md) **owns that** - the message
semantics, the dispatch-queue rules, and the conformance scenarios and what each asserts. The
[conformance suite's README](../parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/README.md)
owns the runner mechanics: how a language registers itself and what its runner must print.

## Exactly-once

Not reachable through the proxy today - `ExternalEngine` throws on transactional commit mode, which
is why the interaction model keeps produce on the engine side. Post-v6, with core changes sanctioned.

## Related

- [`parallel-consumer-proxy-protocol`](../parallel-consumer-proxy-protocol) - the wire contract and
  its freeze gate.
- [`parallel-consumer-proxy-clients`](../parallel-consumer-proxy-clients) - the client libraries and
  the shared conformance suite.
