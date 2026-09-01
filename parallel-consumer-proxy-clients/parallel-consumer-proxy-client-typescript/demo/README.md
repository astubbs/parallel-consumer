<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The TypeScript demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. Node 20+ and a JDK are optional: with both, the demo runs natively and starts its
broker in a container; without them it runs in a container too, and the broker is a compose
sibling. `run.sh` announces which it chose, and why, before it builds anything; **the demo itself
opens by naming the product**, in the banner every language prints, and then its effective
configuration.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file records only what is specific to TypeScript.

## The two arms

| arm | what it is |
|---|---|
| `AK core (kafkajs)` | [kafkajs](https://kafka.js.org), a TypeScript Kafka client, one record at a time |
| `pc-typescript-grpc (this client)` | this module's client library: it spawns the sidecar, receives records over a socket, runs the application's function, and reports outcomes back |

**Both halves of each label are load-bearing.** "AK core" is a *category* - the role an arm plays -
and every language fills it with a different library, so a reader cannot judge the comparison
without being told which one. The second arm names the client library because that library is the
thing being demonstrated.

On the second path **the application does no Kafka I/O at all** - the sidecar owns the consumer,
the producer, the group membership and the offsets. That is a claim about the *path*, not about
this process: the same process creates the topic, produces the backlog and runs the AK core arm
with kafkajs, because a comparison needs both sides.

Java's demo carries four more arms. Do not mirror them - the contract says why, and TypeScript has
nothing to compare a wrapper or a raw wire against.

**Both arms read the record's key and nothing else.** That is what fills the tables' `records` and
`keys` columns - the pair that shows the work happened rather than asserting it - and reading the
same amount on both paths is what keeps them comparable. Neither arm deserializes a value: that
would be work one arm does and the other does not.

## What is specific to TypeScript

### TypeScript has two serious Kafka clients, and this demo runs one of them

The contract asks a language with more than one serious client to say so, and to consider running
both as separate arms. TypeScript has two:

| client | what it is |
|---|---|
| [**kafkajs**](https://kafka.js.org) | pure JavaScript, no native build step. The one this demo runs. Its last release is 2.2.4, from 2022 |
| [**@confluentinc/kafka-javascript**](https://github.com/confluentinc/confluent-kafka-javascript) | a binding over librdkafka (the modern successor to `node-rdkafka`), actively released, and typically faster |

**The judgement: one arm, and it is kafkajs** - recorded here rather than left implicit, because the
choice materially changes the number and a reader asking "is this fast in *my* language" is really
asking about the client they already use.

- **Why kafkajs is the one that runs.** It installs from the registry with no compiler, no
  librdkafka and no platform-specific prebuild, on every architecture the demo's image is built for.
  The demo's promise is "needs Docker, nothing else"; a native binding puts a toolchain between a
  reader and the first table, and it is the AK core arm - the *baseline* - that would be carrying
  that risk.
- **Why not a third arm.** Two arms is the whole contract outside Java, and for a reason that
  applies here: a third arm would be a second *serial* row, comparing two Kafka clients with each
  other rather than the sidecar hop against anything. It would also put this demo's table out of
  step with the other ten, which is what `bin/ci-demo-conformance.sh` exists to catch.
- **What it costs, stated plainly.** kafkajs is the slower of the two, so this demo's AK core
  baseline is a conservative one and the ratio in the `vs AK core` column is correspondingly
  generous. A reader on `@confluentinc/kafka-javascript` should expect a *smaller* ratio, and the
  sidecar arm's own numbers to be unchanged - the sidecar runs no JavaScript Kafka client at all.
  **This demo has not measured that**, and the sentence above is reasoning, not a result.

### The simulated work is an awaited timer, and that is the sanctioned divergence

Every other language in the fan-out sleeps a worker thread. **Node is a single event loop**: a
blocking sleep stops the transport, the executors and the timers all at once, so the sidecar arm
would run exactly as serially as the AK core arm and the demo would be measuring nothing. The work
here is therefore

```ts
await new Promise((resolve) => setTimeout(resolve, delayMs));
```

That is not a weaker workload - it is the same workload in the wait this runtime actually has, and
it is what real Node work looks like, because a Node processor is overwhelmingly waiting on I/O.

**What it implies about the sidecar arm's concurrency, stated rather than left to be inferred:** the
parallelism is *promise concurrency on one event loop*. `Configured.executor_count` becomes that
many concurrent `await`s, not that many threads. A processor doing CPU work **synchronously** would
block the loop and this arm would collapse to serial - the client library's
[README](../README.md#concurrency-promise-concurrency-not-worker_threads) says so plainly, and this
demo does not pretend otherwise.

### The launcher starts the broker, not the demo

The Java demo starts a broker with Testcontainers when `--bootstrap` is absent. Here `run.sh` does
it, and the demo program refuses to run without an address. Two reasons:

- the demo container is **never** granted the host Docker socket, so the containerised path is
  always "an address was supplied" anyway - which makes broker-starting a property of the
  *launcher* rather than of the demo;
- the alternative was a 47 MB `@testcontainers/kafka` dependency in a package tree whose ordinary
  `npm ci` sits on the CI matrix's critical path, bought for one code path the container never
  takes.

The flags, the environment variables, the defaults, the fingerprint and the tables are unchanged.

### The demo is a separate npm package

`demo/package.json` is its own package that depends on the client library through `file:..`, which
npm resolves to a symlink. Two things follow, and both are deliberate:

- **the demo loads the library's built `dist/`** - the artifact a user would install - rather than
  reaching into its sources;
- **kafkajs is the demo's dependency and never the library's.** The whole point of the sidecar arm
  is that a foreign application needs no Kafka client library at all; a client library that pulled
  one in would contradict its own demo.

`npm ci` in the library must therefore run before `npm ci` here - `run.sh` and the `Dockerfile` both
do it in that order.

### The container carries two toolchains

Node runs the demo; a JVM runs the sidecar the client library spawns as a child process. The
`Dockerfile` builds the sidecar in a JDK stage, copies out a flat directory of jars, and lays a JRE
beside Node in the image a person actually runs. The sidecar is **not** a compose service: the
library spawns and supervises it, so a reader is never shown a deployment the product does not ask
for.

`PC_PROXY_SIDECAR_CLASSPATH` is how the classpath reaches the demo - the same variable the module's
end-to-end test harness already uses. It is deliberately not `PC_DEMO_SIDECAR_CLASSPATH`: `PC_DEMO_`
is the flag namespace, one variable per flag and no others, and a classpath is not a flag.

### Noise you should expect, and why it is not filtered

- **`SLF4J(W): No SLF4J providers were found`**, once per sidecar. `logback-classic` is `test` scope
  throughout this repository, so the sidecar a user deploys has no logging provider - and the demo
  runs it on exactly that classpath, on both entry points, rather than the flattered test one. It is
  recorded as a finding against the proxy module in
  [`docs/inflight/clients/typescript.md`](../../../docs/inflight/clients/typescript.md).
- **One kafkajs `The group coordinator is not available` error** while `__consumer_offsets` is being
  created. It retries and succeeds. On a cold broker under load it can escalate as far as
  `KafkaJSNumberOfRetriesExceeded: This is not the correct coordinator for this group` and a
  `Restarting the consumer in ...` line - observed on a container run, which still finished the
  backlog and exited 0, with the AK core arm's *elapsed* charged for the wait. Suppressing a broker
  error class to tidy a demo's output is how a real one gets hidden, so it stays visible.
- **`TimeoutNegativeWarning`** from kafkajs 2.2.4 on Node 25. Cosmetic; not seen on the Node 22 CI
  pins.

## Building and checking it

`run.sh` does all of this; by hand, from the client module's directory, it is:

```bash
npm ci && npm run compile              # the LIBRARY first - the file:.. link points at its dist/
cd demo && npm ci && npm run compile   # then this package
npm run clean                          # remove dist/
```

**`tsc` is this package's whole gate**, under the same `strict` plus `noUnusedLocals` /
`noImplicitReturns` settings the library uses. The library's type-aware eslint deliberately ignores
`demo/**`: it would need this package's dependencies installed to resolve kafkajs, and the CI
matrix row installs the library's only - so including it turned `eslint .` red in CI and green
locally, which is worse than not running it. `eslint.config.mjs` records the measurement.
