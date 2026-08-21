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
sibling. It announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file records only what is specific to TypeScript.

## The two arms

| arm | what it is |
|---|---|
| `AK core` | [kafkajs](https://kafka.js.org), TypeScript's own Kafka client, one record at a time |
| `typescript-grpc` | this module's client library: it spawns the sidecar, receives records over a socket, runs the application's function, and reports outcomes back |

On the second path **the application does no Kafka I/O at all** - the sidecar owns the consumer,
the producer, the group membership and the offsets. That is a claim about the *path*, not about
this process: the same process creates the topic, produces the backlog and runs the AK core arm
with kafkajs, because a comparison needs both sides.

Java's demo carries four more arms. Do not mirror them - the contract says why, and TypeScript has
nothing to compare a wrapper or a raw wire against.

## What is specific to TypeScript

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

## Building and checking it

```bash
cd demo && npm ci && npm run compile   # what run.sh does; needs `npm ci && npm run compile` above first
npm run clean                          # remove dist/
```

The library's `npm run lint` covers these files too - `eslint.config.mjs` lists `demo/tsconfig.json`
in its typed-lint projects, so the floating-promise rules that matter most to an async demo apply
here as well.
