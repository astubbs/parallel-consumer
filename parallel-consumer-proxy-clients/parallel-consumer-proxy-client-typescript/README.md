<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - TypeScript client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

Key-ordered concurrent Kafka consumption from Node, without raising partition counts. The
application's records are processed by a function it supplies; Kafka itself is owned by a sidecar
proxy process running the Java engine, which this library speaks to over one gRPC stream.

**Wave one.** Connect, `Configure`, receive a `Dispatch` wave, run the user's function, report the
outcome with the token echoed verbatim, produce records back on success, shut down cleanly - proven
end to end against the real test-mode sidecar. Not implemented, and **un-negotiated rather than
half-built**: leases and heartbeats, the manifest reconnect, worker-death reporting, terminal
outcomes, the `Shutdown` drain, the demo, and npm publishing. This client declares
`capabilities: ["dispatch"]`, so the proxy grants it nothing it does not perform.

The plan is `docs/plans/2026-08-14-001-feat-language-proxy-plan.md` (astubbs#242); what this wave
learned is in `docs/inflight/clients/typescript.md`.

## The surface

```ts
import { ParallelConsumerClient } from "@parallel-consumer/proxy-client";

const client = await ParallelConsumerClient.open({
  sidecar: { executable: "/absolute/path/to/the/proxy" },
  topics: ["orders"],
  kafkaProperties: { "bootstrap.servers": "localhost:9092", "group.id": "orders-app" },
});

client.poll(async (record) => {
  await handle(record.key, record.value);   // Buffers - deserialization is yours
});                                          // returning nothing is a success; throwing is a failure

await client.done();                         // the session's end, whenever it comes
await client.close();                        // or: await using client = await ...open(...)
```

- **`poll` does not block.** It starts consumption and returns; `done()` is the promise that settles
  when the session ends. A blocking `poll` would have to be awaited, which would stop the same
  `async` function ever calling `close()`, and nothing in JavaScript could interrupt it from
  elsewhere. There is one mechanism for the session's end, not two - no event emitter beside the
  promise.
- **Keys, values and offsets are honest.** Keys and values are `Buffer | null` (a tombstone is not
  an empty value); offsets and epochs are `bigint`, because 64-bit integers do not fit in `number`.
- **Options are a plain object**, and an omitted property means "take the proxy's default" - the
  same thing an absent field means on the wire.

## Concurrency: promise concurrency, not `worker_threads`

`Configured.executor_count` executors are `executor_count` concurrent async invocations on the one
event loop, not threads. The reasoning, in short:

- It keeps the user's function a **closure**. A `worker_thread` is a fresh isolate with no shared
  heap, so a function could only reach it as a module path or a source string - making this the one
  client in the fan-out whose processor cannot close over anything.
- It is what Node's concurrency is *for*: overlapping I/O, which is what the overwhelming majority
  of Node processors do.
- **The limit, stated plainly:** a processor that does CPU work *synchronously* blocks the event
  loop and therefore this client's transport. Offload CPU work to your own worker pool and `await`
  it - that composes with this model. A library-imposed worker-thread executor would force every
  user into module-path processors to serve the minority case, and can be added later as an option
  without touching the wire.

The transport never blocks on a processor regardless: dispatched records are queued synchronously
and the executors are separate async loops.

## Building, testing, and the local gate

Requires Node 20.11+ (CI pins 22.17.0) and, for the end-to-end test, a JDK 17.

```bash
npm ci                # dependencies, from the committed lockfile
npm run check         # THE LOCAL GATE: tsc --build (strict) then type-aware eslint
npm test              # the suite; the end-to-end test needs the sidecar classpath below
npm run proto         # regenerate src/generated/ from the frozen proxy.proto
npm run proto:check   # ...and fail if the committed stubs have drifted
npm run clean         # remove dist/ - every emitted file
```

`npm run clean` leaves `node_modules` standing on purpose: `mvn clean` deletes `target/` without
emptying `~/.m2`, and `node_modules` is this language's `~/.m2`. A clean that refetched 128
packages afterwards would not be one. Removing `dist/` is complete - the `.tsbuildinfo` lives
inside it - so the script is plain `rm` and needs no compiler, which is what lets it clean a
checkout that has never installed anything.

`./mvnw clean` removes the same directory, and does it without running `npm` - `pom.xml` lists it
as a `maven-clean-plugin` fileset, so cleaning needs no Node on the box. The two must agree: change
one, change the other.

`npm run check` is the bug-finding pass, and it is two halves that catch different things: `tsc`
under `strict` plus `noUnusedLocals`/`noImplicitReturns`, and ESLint's **type-aware** rules
(`recommendedTypeChecked`, with `no-floating-promises`, `no-misused-promises` and `require-await`
raised to errors). Untyped ESLint on TypeScript finds formatting; the typed rules find the silent
failure this codebase is shaped for, which is an unawaited promise whose rejection nobody sees.
`npm run lint` alone is exactly what the CI matrix row runs.

### The end-to-end test needs the proxy module built

The test spawns the real test-mode sidecar (`TestModeMain`, in the proxy module's **test** jar), so
it needs that jar and its classpath. This module deliberately has **no** Maven dependency on the
engine, so the dependency exists only inside the `typescript-e2e-harness` profile, which activates
with `-Dpc.foreignClients`; Maven then writes `target/sidecar-classpath.txt` where the test reads
it. From the repository root:

```bash
./mvnw test -pl :parallel-consumer-proxy-client-typescript -am -Dpc.foreignClients
```

That is the CI matrix row's exact command. To write only the classpath and then drive the suite from
`npm` - the shorter loop, and what the error below names - use:

```bash
./mvnw -pl :parallel-consumer-proxy-client-typescript -am -Dpc.foreignClients -DskipTests generate-test-resources
npm test
```

Running `npm test` on its own without that file fails with the command in its message rather than
skipping - a test that quietly does not run is not a passing test.

An ordinary build (`bin/build.sh -pl :parallel-consumer-proxy-client-typescript -am`, no
`-Dpc.foreignClients`) treats this module as an empty Maven skeleton and starts no Node at all.

### The shared conformance suite

It drives this client's runner (`test/conformance-runner.ts`) through the same scenarios as every
other language, asserting engine state this process cannot see:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=typescript
```

## Generated code

`src/generated/` is `protoc` output from the **frozen**
`parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto`, generated
by [ts-proto](https://github.com/stephenh/ts-proto) and **committed** - an npm consumer must not
need a `protoc`, and committing is what makes "regenerating produces no diff" checkable
(`npm run proto:check`). Do not edit those files, and never edit the `.proto`: it is the contract.

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md) own the
protocol; this file does not restate them.
