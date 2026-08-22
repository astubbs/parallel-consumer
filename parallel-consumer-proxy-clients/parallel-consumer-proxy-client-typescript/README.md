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
outcomes, the `Shutdown` drain, and npm publishing. This client declares
`capabilities: ["dispatch"]`, so the proxy grants it nothing it does not perform.

The plan is `docs/plans/2026-08-14-001-feat-language-proxy-plan.md` (astubbs#242); what this wave
learned is in `docs/inflight/clients/typescript.md`.

## See it work

```bash
demo/run.sh
```

The same records through kafkajs one record at a time, and through this library over a sidecar,
with the two throughput tables every language's demo prints. Needs Docker; Node and a JDK are
optional, because it will run itself in a container when they are missing.
[`demo/README.md`](demo/README.md) records what is specific to TypeScript - above all that the
simulated work is an **awaited timer**, since a blocking sleep on a single event loop would stop
the very concurrency the demo exists to show. The contract it keeps is
[`parallel-consumer-proxy/demo/README.md`](../../parallel-consumer-proxy/demo/README.md).

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

## Why this client stays on the sidecar, and does not vendor the engine

Some clients can link Parallel Consumer **into** the host process, as a GraalVM shared library, and
skip the sidecar entirely - Go and Python both do. **Node deliberately does not**, and the reasoning
is recorded here because "we did not get to it yet" and "we decided not to" look identical from
outside.

**It was tried, and it works.** A Node process can create the isolate, open a session and pull
frames. See [`ffi/probe_eventloop.mjs`](ffi/probe_eventloop.mjs).

**But the pull has to happen on a worker thread, and that is measured, not assumed.** The engine
does not push frames at you; something must call in and wait. Doing that on the main thread stops
the event loop completely - not slows it, stops it:

```
  baseline (no FFI call)   loop turned 152,860 times
  blocking on MAIN thread  loop turned       0 times
  blocking on WORKER       loop turned 149,106 times
```

**And that is where the case for embedding falls apart for Node specifically.** The whole point of
linking the engine in is to remove a hop. Here is what the two paths actually cost:

| | Path a frame takes |
|---|---|
| Sidecar (today) | socket -> libuv -> your code, on the main thread |
| Embedded | `pc_next` -> worker thread -> `postMessage` -> main thread -> your code |

libuv - the C library that *is* Node's event loop - watches network sockets **on the loop thread
itself**. A frame from the sidecar therefore lands exactly where your code runs, in one hop. The
embedded path removes that socket hop and adds a thread hop plus a structured-clone copy in its
place. **You would be trading a hop libuv is built for against one it is not.**

This last part is *reasoning, not measurement* - nobody has raced the two. It is recorded as the
current strategy rather than as a finding, and two things could change it:

- The copy is avoidable. Transferable `ArrayBuffer`s move ownership rather than copying, and
  `SharedArrayBuffer` skips the boundary entirely.
- The socket hop is already cheap over loopback or a Unix domain socket.

If someone measures it and the embedded path wins, this section is wrong and should be replaced by
the numbers.

**One hard constraint if anyone does try**, because the symptom does not point at the cause:
[koffi](https://koffi.dev) cannot call this library at all. It executes foreign calls on a stack it
allocates itself - its configurable `sync_stack_size` is the tell - while GraalVM derives its stack
guard zones from the calling thread's *real* stack. The result is a fatal `StackOverflowError`
inside `graal_create_isolate` whose own first suggested cause is "the wrong IsolateThread", which is
not the problem. Raising the stack to koffi's 16 MiB maximum changes nothing; size was never it. An
N-API addon calls straight down the thread's own stack and works - [`ffi/pc_addon.c`](ffi/pc_addon.c)
is about 150 lines and needs no node-gyp.

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

### In the Maven build

```bash
./mvnw compile -Dpc.foreignClients -pl :parallel-consumer-proxy-client-typescript -am   # npm run build
./mvnw test    -Dpc.foreignClients -pl :parallel-consumer-proxy-client-typescript -am   # npm test
```

This module is `packaging: pom` with four `pc.foreign.*` properties naming those npm scripts, and
the `foreign-clients` profile in the clients aggregator ([`../pom.xml`](../pom.xml)) binds them to
`compile` and `test` and decides whether the module is in the reactor at all. Nothing binds to
`clean` in any language here - the pom says why that is forced rather than chosen.

**What a Java engineer will find surprising:**

- **`compile` installs the dependencies first.** `npm run build` is `npm ci` then `tsc --build`: the
  exec binding calls one program with no shell, so there is nowhere else for the install to live,
  and `npm ci` from the committed lockfile is its reproducible form. It runs on every compile
  (measured: 128 packages, under a second warm), which is not a step a Maven build has.
- **Everything `tsc` emits lands in `dist/`** - `dist/src`, `dist/test`, and the incremental state
  at `dist/tsconfig.tsbuildinfo` - so one fileset is the whole answer and `clean` needs no pattern.
  Measured: after a build nothing outside `dist/` is new.
- **`node_modules` survives `clean`, deliberately** - see above; it is this language's `~/.m2`.
- **`-am` is not optional for `compile` or `test`.** `-pl` alone fails the enforcer's
  `ReactorModuleConvergence` with a message about parent modules, which reads as a broken pom;
  [`docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md`](../../docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md)
  owns that. `./mvnw clean -P foreign-clients -pl :parallel-consumer-proxy-client-typescript` still
  needs the profile - without it the module is not in the reactor at all - but needs no `-am`, the
  clean lifecycle never reaching `validate` where the enforcer is bound.
- **`-P foreign-clients` is not a synonym for `-Dpc.foreignClients` here.** It activates the module,
  but the `typescript-e2e-harness` profile below activates on the *property*, so under `-P` the
  classpath file is never written and the end-to-end test fails looking for it. That has its uses:
  `-P` leaves the engine out of the reactor - three modules instead of six, and no JDK 17 needed -
  which makes it the quicker loop when all you want is `tsc`.

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
