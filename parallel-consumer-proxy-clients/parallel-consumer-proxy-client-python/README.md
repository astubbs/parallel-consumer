<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer for Python

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

Ordered concurrent Kafka consumption from a single consumer - by key or by partition - with the
user's function running in **worker processes**, so the GIL is not the ceiling. Kafka itself is
spoken by a sidecar proxy process this library starts and owns; nothing in the application process
holds a broker connection.

This is the flagship non-JVM client of the language-proxy work (astubbs#242). It is **wave one**:
the vertical slice below works end to end against the real protocol, and the sections marked *not
yet* are the following waves' - each is additive, and none of them changes what is here.

## What works today

```python
from parallel_consumer import ClientOptions, ParallelConsumerClient

options = ClientOptions(
    topics=["orders"],
    max_concurrency=64,
    kafka_properties={"bootstrap.servers": "localhost:9092", "group.id": "orders"},
)

def process(record):
    print(record.topic, record.partition, record.offset, record.value)
    # return nothing for success; raise to fail the record and have it redelivered

with ParallelConsumerClient(options, sidecar="/opt/pc/parallel-consumer-proxy") as client:
    client.poll(process)
    client.wait()
```

Connect, configure, receive a dispatch wave, run the user's function in a worker process, report
each record's outcome, drain and shut down cleanly.

## The surface

Every client library in every language mirrors the same small surface; the spelling below is
Python's.

| Type | What it is |
|---|---|
| `ClientOptions` | Connect-time configuration. Unset means "take the engine's default" - this library holds no defaults and guesses none. `kafka_properties` is credential-bearing and is never logged. |
| `ParallelConsumerClient` | `poll(processor)` starts consumption (at most once); `wait()` blocks until the session ends; `close()` drains. A context manager, because the sidecar and the workers are resources. |
| `InboundRecord` | The Kafka record plus the delivery state an in-process user function would see: `attempt`, `last_failure_at`, `last_failure_reason`. Keys and values are `bytes` or `None` - deserialization is your code's business. |
| `Outcome` | `Outcome.success(produce=[...])` or `Outcome.failure(reason)`. |
| `OutboundRecord` | A record to produce on success - the only sanctioned route for a worker's Kafka output. |

**The user's function may just be a function.** Return nothing and the record succeeded; raise and
it failed, with the exception's text as the reason, and Parallel Consumer redelivers it with the
attempt count and that reason attached. `Outcome` is for the cases those two cannot say: a success
that also produces records, or a failure you decided on rather than raised.

## How it is put together

```
your process
├── your function .......... runs in a worker process (a closure is fine on Linux)
├── admin .................. holds the ONE gRPC stream, owns the dispatch queue, reports outcomes
└── sidecar proxy .......... a child process; owns Kafka entirely
```

Three things about that shape are load-bearing rather than incidental:

* **The worker pool is forked before any gRPC channel exists.** gRPC Core does not support forking
  a process that holds an active channel, and the worker count only arrives in the handshake - so
  a launcher process is forked first, from an image that has never held a channel, and *it* forks
  the workers when the count arrives. `tests/test_fork_safety.py` asserts the ordering directly,
  because the failure it prevents is silent.
* **Importing this package starts nothing** - no channel, no process, no thread.
* **The sidecar binary is named by the application, absolutely.** Never resolved through `PATH`, a
  relative path, or any directory an attacker could influence: this process hands the sidecar its
  Kafka credentials. It is launched directly, never through a shell, because a shell wrapper holds
  the child's stdin and defeats the parent-death watch that stops a leaked JVM holding group
  membership.

Configuration travels in the connect-time handshake and nowhere else - nothing reaches the proxy
by argv, environment variable or file.

## Not yet - and what that means

Later waves add: the liveness lease and heartbeats, reconnect with a manifest, worker-death
reporting, terminal outcomes to a dead-letter topic, the demo container, PyPI packaging, and the
conformance scenarios beyond the four the harness serves today. **These are un-negotiated
capabilities, not half-built features.** The client declares exactly the one capability it
implements (`dispatch`) in its handshake, so the proxy holds it to that rather than to promises;
each wave adds its token alongside its duty.

## Working on it

```bash
make build        # install into .venv, with the dev tools
make test         # the suite, including the end-to-end test against the real sidecar
make lint         # ruff - the same check CI runs
make proto        # regenerate the stubs from the frozen proxy.proto
make proto-check  # regenerate and fail if the committed stubs have drifted
```

The generated stubs under `src/parallel_consumer/_generated/` are **committed deliberately**, so a
user installing this package needs neither `protoc` nor the schema file. `make proto-check` is what
stops the committed copy drifting from the contract.

`make test` spawns the real test-mode sidecar (`TestModeMain --mock`), which lives in the proxy
module's *test* jar and therefore needs a JVM classpath. Maven writes that classpath under
`target/`; running `make test` on its own drives the same Maven wiring to produce it. Under
`./mvnw ... -Dpc.foreignClients` - the CI matrix row's command - Maven has already written it.

**Maven runs none of this by default.** An ordinary `bin/build.sh -am` builds this module as an
empty skeleton and starts no Python interpreter; the `exec` bindings that call `make` are inactive
unless `-Dpc.foreignClients` is passed. That is what keeps the reactor buildable on a machine with
no Python client toolchain.

The shared cross-language conformance suite drives this client's runner
(`scripts/conformance_runner.py`) through the same scenarios as every other language, asserting
engine state Python cannot see:

```bash
./mvnw package -pl :parallel-consumer-proxy-client-python -am -Dpc.foreignClients       # the CI row
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=python
```

Depth on the protocol lives in
[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md).

## Two divergences worth knowing

Recorded here rather than in the shared client-authoring guide, which only the wave-sync resolver
edits:

* **A protocol violation cannot be answered with a status code.** The guide says to fail the stream
  with `FAILED_PRECONDITION` naming the count; a *client* cannot set a status on a stream it did
  not serve, so this one cancels the call and raises `ProtocolViolation` naming the count instead.
* **`Released` is sent only when the `shutdown` capability is negotiated.** On a session without
  it, the queue is discarded at shutdown and the proxy reclaims those records when the stream ends
  - sending a message outside the negotiated set would be this client's own violation.

## Also here

`poc/` is the preserved specification probe: a working client written from the protocol documents
alone, before this one existed, as evidence that the frozen protocol is implementable without
reading the proxy's Java. It is kept as evidence, not as source - it is not packaged, not linted,
and not maintained.
