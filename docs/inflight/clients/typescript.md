# Client: TypeScript (astubbs#242)

Per-language working note for the TypeScript client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the TypeScript wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one landed.** Connect, `Configure`, one `Dispatch` wave, the user's function, the
report with the token echoed verbatim, and a clean client-initiated shutdown, proven by one
end-to-end test against the real test-mode sidecar. The module is at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript/`; its maturity and
testing-evidence deferrals are lifted, so its CI row now runs. Later waves: leases and heartbeats,
the manifest reconnect, worker death, terminal outcomes, the shutdown drain, npm publishing, and
the rest of the conformance suite.

**The demo landed in the demo wave** and lives at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript/demo/`. What it decided,
what it had to diverge on, and what it is still missing is the "The demo" section below.

## Decisions this wave took that a sync should confirm or overturn

- **Concurrency is PROMISE CONCURRENCY on the one event loop, not `worker_threads`.** This is the
  question the plan singles out for Node, so the argument matters more than the answer.
  `Configured.executor_count` becomes that many concurrent async invocations, each an executor loop
  taking FIFO from the dispatch queue.
  - *Why not worker threads:* a `worker_thread` is a fresh isolate with no shared heap, so a
    function can only reach it as a module path or a source string. That would make TypeScript the
    one client in the fan-out whose processor cannot be a closure, contradicting the reference
    surface's "a closure/lambda/callable - never an importable name". Python pays for closures with
    `fork`; Node has no equivalent, so the price would be the surface itself.
  - *Why it is the right default anyway:* Node's concurrency is for overlapping I/O, which is what
    the overwhelming majority of Node processors do. `executor_count` in-flight `await`s give real
    concurrency there.
  - *The honest limit, which is real and is written into the README rather than buried:* a processor
    doing CPU work **synchronously** blocks the event loop and therefore the transport - reports
    stop flowing, and once heartbeats exist they stop too, which expires leases and redelivers
    records the worker still holds. Node's own answer is for the application to offload CPU work and
    `await` it, which composes with this model. A worker-thread executor can be added later as an
    **option** without touching the wire, because `executor_count` is all the protocol knows.
  - Nothing tests the blocking case yet. It is recorded as a limitation on the module's
    testing-evidence fragment, not as a passing property.
- **`poll(processor)` does NOT block** - see the defect list below; the specification does not say,
  and this is the answer TypeScript chose. It starts consumption and returns; `done()` is a promise
  that settles when the session ends, and `close()` shuts down. One mechanism for the session's end,
  not two: there is deliberately no event emitter beside the promise, because a library offering
  both leaves every user guessing which one is authoritative. The client is also an
  `AsyncDisposable`, so `await using` works.
- **Three spellings of an outcome, one meaning**, resolved in one place (`applyProcessor`):
  returning nothing is a bare success, throwing or rejecting is a failure carrying the error's
  message, and returning an `Outcome` is for a success with records to produce or a failure with a
  reason but no exception. Go deliberately refused two spellings of failure; TypeScript accepts them
  because exceptions *are* the language's error idiom and a processor that lets one escape has said
  "failure" as clearly as a language can.
- **Only implemented capabilities are declared** - `capabilities: ["dispatch"]`, as a named constant
  listing what is implemented, so the set cannot fall out of step by omission. Same reasoning as the
  Go and Python waves, and the opposite of the Java reference client's empty list.
- **The token is echoed as the received object**, never rebuilt from parsed fields, so "opaque" is
  structural rather than a rule someone has to remember.
- **`Options` is a plain object literal, not a builder**, and an omitted property means "take the
  proxy's default" - which is what an absent field means on the wire, so the two agree with no
  translation table.
- **64-bit values are `bigint`** on the public surface (`record.offset`) and in the generated code
  (`Token.epoch`). A `number` truncates past 2^53, which is exactly what the golden corpus's
  deliberately beyond-int32 epoch exists to catch. The ergonomic cost is real - users compare
  against `0n`, not `0` - and it is the right trade.

## Specification defects, and what this client did about them

The first three were handed to this wave rather than rediscovered (a doc-fix session is repairing
them concurrently); they are recorded here because this module's behaviour depends on how they are
resolved. **Report only - a language wave does not edit the guide.**

1. **A gRPC client cannot fail a stream with `FAILED_PRECONDITION`.** Only a server sets a status.
   On queue overflow this client cancels the call and raises `ProtocolViolationError` naming the
   count the specification wanted named. Independently reached by the Go and Python waves, so it is
   a specification defect and not a language quirk.
2. **On a session without `shutdown`, there is no legal way to return queued records.** The guide's
   §5 has a closing client report every queued record `Released`; the capability rule forbids sending
   an outcome outside the negotiated set, and the harness negotiates only `["dispatch"]`. This client
   sends `Released` only when `shutdown` is negotiated, and otherwise discards the queue and lets the
   proxy reclaim it - and says so through the `onWarning` hook rather than silently.
3. **Whether `poll(processor)` blocks is unstated.** Answered above: it does not. If the reference
   surface means it to block, this client changes - but a blocking `poll` in TypeScript would have to
   be `await`ed, which would stop the same `async` function ever calling `close()`, and nothing in
   JavaScript could interrupt it from elsewhere. **This is the sync's to settle**; the doc-fix
   session asked each language for its answer, and this is TypeScript's.

Found by this wave:

4. **The `.proto` carries a placement option for eight languages and none for TypeScript.**
   `go_package`, `csharp_namespace`, `ruby_package`, `php_namespace`, `objc_class_prefix` and
   `swift_prefix` were added so no client has to supply placement on the command line. There is no
   equivalent file option for TypeScript - every TS generator takes its output layout from
   command-line options instead - so this module's `scripts/generate-proto.mjs` is the only record of
   where TypeScript's generated code lands and what it is called. That is not a schema gap to fix
   (the option does not exist), but the guide's codegen section should say so, or the next TS-shaped
   language (and a reviewer comparing modules) will look for the option and conclude it was forgotten.

## Harness observations

- **`protoc` from a normal install solved the Go wave's second finding for free.** Go had to borrow
  the `protoc` that the protocol module's `protobuf-maven-plugin` downloads and unzip the well-known
  types out of `protobuf-java`, because the Maven artifact is a bare executable with no `include/`.
  A standalone `protoc` (35.1 here) ships its own `include/google/protobuf/*.proto`, so
  `duration.proto` and `timestamp.proto` resolve with no include path at all. Worth writing into the
  guide as the preferred route, with the Maven-artifact procedure as the fallback for machines that
  cannot install one.
- **The mock ignores the subscription** (already recorded by the Go wave, confirmed here): the
  end-to-end test therefore asserts on the record's own content and its delivery count, never on the
  mere fact that a record arrived.
- **The plan defect the Go wave recorded is solved, not worked around.** `-am` cannot order a build
  this module has no dependency on, so the harness dependency lives in a profile gated on
  `-Dpc.foreignClients` (`typescript-e2e-harness` in the module's pom), exactly as the Python wave
  did it, and `maven-dependency-plugin` writes `target/sidecar-classpath.txt` where the test reads
  it. The CI matrix row's command works unchanged.

## Local bug-finding, and the proof it can fail

`npm run check` is the local gate, and it is deliberately two halves that catch different things:

```bash
npm ci && npm run check     # tsc --build (strict) then eslint with type-aware rules
npm run lint                # eslint alone - the CI matrix row's exact command
```

Versions, for the CI row to match: **Node 20.11+** (developed on 24.19, CI pins 22.17.0),
**typescript 5.9.3**, **eslint 10.8.1**, **typescript-eslint 8.67.0** (config
`recommendedTypeChecked`, with `no-floating-promises`, `no-misused-promises` and `require-await`
raised to errors), **ts-proto 2.12.0**. TypeScript is pinned to the 5.x line on purpose:
typescript-eslint 8.x declares `typescript >=4.8.4 <6.1.0`, so the 7.x line would silently lose the
type-aware rules, which are the half that finds bugs.

**It fails when it should**, established three ways rather than asserted:

- A deliberately planted floating promise (dropping the `await` in the client's `asyncDispose`) was
  reported as `no-floating-promises` **and** `require-await`, exit 1; reverting returned it to 0.
- It found two real defects in this wave's own code before any defect was planted: an unnecessary
  type assertion in the sidecar spawn, and a genuinely floating `test(...)` promise in the
  end-to-end test.
- `tsc --strict` caught a wrong assumption about `assert.throws`'s return type in the queue tests.

The end-to-end test's own negative control is recorded on the module's testing-evidence fragment:
throwing from the user function instead of returning produced three deliveries in the same quiet
period with the attempt count incrementing and the reason verbatim, and the test went red.

## Effort

ASM1 wanted a budget recorded **before** the unit started; none was, so R16 cannot be falsified
against one here either - recorded honestly rather than backfilled, which is the same hole the Go
wave reported. Actual wave-one effort, for whatever an uncalibrated point is worth: one agent
session, roughly two hours wall clock, ~1,100 lines of hand-written TypeScript across nine source
files, two test files and a generation script, on top of ~3,900 lines of generated stubs. The
end-to-end test passed on its first run against the real wire.

## The demo

`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript/demo/` - `run.sh`,
`Dockerfile`, `docker-compose.yml`, its own `README.md`, and five TypeScript sources under `src/`.
It keeps the contract in `parallel-consumer-proxy/demo/README.md`: the same seven flags with the
same defaults, one `PC_DEMO_` variable per flag with flags beating environment beating defaults,
the effective-configuration fingerprint printed before the run and **never** the bootstrap address,
the two tables in the same order, and no latency anywhere.

Two arms, which is the whole contract outside Java: **`AK core`** (kafkajs, one record at a time)
and **`typescript-grpc`** (this module's client library, which spawns the sidecar as a child
process). The big replay runs the sidecar arm only.

### The sanctioned divergence: the work is an awaited timer, and what that implies

The plan singles TypeScript out because a blocking sleep on a single event loop stops the
transport, the executors and the timers at once - the "parallel" arm would run exactly as serially
as the serial one. The work is therefore
`await new Promise(resolve => setTimeout(resolve, delayMs))`.

**What that implies about the sidecar arm, stated rather than left to be inferred:** the
parallelism it shows is *promise concurrency on one event loop*. `Configured.executor_count`
becomes that many concurrent `await`s, not that many threads, which is the wave-one concurrency
decision recorded above being exercised end to end for the first time. The demo therefore
demonstrates exactly the case this client is good at (overlapping waits) and says in its README
that a **synchronously** CPU-bound processor would block the loop and collapse the arm to serial.
Nothing in the demo tests that failure mode - it remains untested here as it was in wave one.

### Divergences that were NOT sanctioned, and why they were taken anyway

- **`run.sh` starts the broker; the demo program refuses to run without an address.** The Java demo
  starts one with Testcontainers when `--bootstrap` is absent. Two reasons: the containerised path
  is *always* "an address was supplied" anyway, because a demo container is never granted the host
  Docker socket - so broker-starting is a property of the launcher, not the demo; and the
  alternative was a **47 MB** `@testcontainers/kafka` dependency (measured on a clean install) in a
  package tree whose ordinary `npm ci` sits on the CI matrix's critical path, bought for one code
  path the container never takes. The flags, precedence, fingerprint and tables are unchanged, so
  this is invisible from outside - but it means `--bootstrap`'s documented "omit to start one" is
  the *script's* promise here, not the program's.
- **The demo is a separate npm package** (`demo/package.json`) that reaches the library through
  `file:..`. npm resolves that to a symlink (measured: `added 1 package in 294ms`, and it does not
  pack or reinstall the parent's dependencies), so the demo loads the library's built `dist/` - the
  artifact a user would install - and **kafkajs is the demo's dependency and never the library's**.
  A client library that pulled in a Kafka client would contradict the arm it exists to demonstrate.
- **`eslint.config.mjs` now lists `./demo/tsconfig.json` in its typed-lint projects.** Without it
  `eslint .` - the CI matrix row's exact command - reports every demo file as "not found in any of
  the provided project(s)" and goes red. Adding the project rather than ignoring the directory was
  the deliberate choice: `no-floating-promises` is exactly what an arm racing a countdown against a
  session's end needs, and it immediately caught an unused processor parameter.

### Measured, and what these numbers are NOT

Ten agents were running on this machine, so the brief for this wave was reduced to proving the
thing runs rather than measuring anything. **These are not throughput figures and must not be
quoted as any**: at 60-100 records the group join dominates every arm, and the honest reading is
"both arms completed and the tables printed".

| run | arm | records | elapsed | msg/s |
|---|---|---|---|---|
| native, `--records 100 --replay-factor 1 --partitions 4` | AK core | 100 | 1.4s | 70 |
| same | typescript-grpc | 100 | 0.7s | 140 |
| native, **no arguments**, `PC_DEMO_RECORDS=60 PC_DEMO_REPLAY_FACTOR=2 PC_DEMO_PARTITIONS=4` | AK core | 60 | 1.3s | 45 |
| same, small replay | typescript-grpc | 60 | 0.8s | 76 |
| same, big replay | typescript-grpc | 120 | 0.7s | 164 |
| native, `--records 80 --replay-factor 2 --partitions 4` | AK core | 80 | 3.7s | 21 |
| same, small replay | typescript-grpc | 80 | 1.2s | 68 |
| same, big replay | typescript-grpc | 160 | 1.1s | 148 |
| **container**, `--docker --records 60 --replay-factor 2 --partitions 4` | AK core | 60 | 4.4s | 13 |
| same, small replay | typescript-grpc | 60 | 1.5s | 40 |
| same, big replay | typescript-grpc | 120 | 3.1s | 38 |

Every one exited 0. The spread between two native runs at the same settings (AK core at 70 then 21
msg/s) is the load on the box, and it is the clearest possible evidence that none of these are
measurements. A real one wants an unloaded machine and the defaults.

### Three things the runs found, which no amount of reading would have

- **A KRaft broker started with `KAFKA_LISTENERS=...://0.0.0.0:...` exits 1 during preflight**, with
  `advertised.listeners cannot use the nonroutable meta-address 0.0.0.0`, because the CONTROLLER
  listener has no entry in `advertised.listeners` and is therefore taken from `listeners`. The
  compose file next door already binds to its service name for this reason and says so; the
  standalone `docker run` in `run.sh` had to learn it. Every listener now binds to
  `$BROKER_HOSTNAME`, and Docker still forwards the published port to a listener bound to the
  container's own address.
- **kafkajs 2.2.4 prints `TimeoutNegativeWarning` on Node 25.** Cosmetic - the run is unaffected -
  but kafkajs's last release is 2022 and this is the shape of problem an unmaintained client will
  keep producing. CI pins Node 22, where it was not seen. If it becomes more than noise, the
  replacement candidate is `@confluentinc/kafka-javascript`, which costs native prebuilds in the
  image. kafkajs also logs one `The group coordinator is not available` ERROR per run while
  `__consumer_offsets` is being created; it retries and succeeds, and it is left visible rather
  than filtered, because suppressing a broker error class to tidy a demo's output is how a real one
  gets hidden.
- **The sidecar the product SHIPS has no SLF4J provider**, and the demo is how that surfaced.
  `logback-classic` is `test` scope repository-wide, so a runtime-scoped classpath for
  `bz.stub.parallelconsumer.proxy.Main` prints `No SLF4J providers were found` and every `log.info`
  in the sidecar goes nowhere. This is a property of the proxy module, not of the demo, and fixing
  it is not a demo wave's call - but the demo was about to hide it: `build-classpath` with no
  `includeScope` writes *every* scope, so the native path was quietly flattered by test-scope
  logback while the container was not. `run.sh` now passes `-DincludeScope=runtime`, so both entry
  points run the sidecar on the classpath a user would get, and both print the warning. **For the
  proxy's owners:** a shipped sidecar that cannot log is a diagnosability problem well beyond this
  demo.

### Open, and NOT done by this wave

- **Nothing runs this demo in CI.** The contract's "both entry points are tested" section says a
  per-language demo inherits `bin/ci-demo-test.sh`, and `bin/**` was not this agent's to edit. The
  integrator has to wire the TypeScript entry points in, or the demo joins the ones that were
  shipped and never executed again.
- **There is no `ReferenceDemoIT` equivalent.** The Java demo has a test that calls its entry method
  and asserts what the arms did. Nothing here does; the evidence is the runs recorded above.
- **CHANGELOG and any cross-language index.** Not this agent's files.

The container path is **not** on this list: `demo/run.sh --docker` built the image and ran both
arms and both replays to exit 0, on a repository-context two-stage build (a JDK stage that
materialises a flat `/sidecar/lib`, then a Node image with a Temurin JRE copied in beside it). The
broker is a compose sibling and the demo container is given an address, never the Docker socket;
the sidecar is spawned as a child process inside the demo container and is not a compose service.

### Things in the shared contract that look wrong from here (recorded, not edited)

1. **`--bootstrap ADDR - an existing broker; omit to start one` is a promise only a language with a
   Testcontainers binding can keep in the program itself.** Java, Go, .NET and a few others have
   one; TypeScript's costs 47 MB and Rust's and C++'s are worse. The contract would be more honest
   phrased as "the entry point starts one", leaving where that happens to the language.
2. **The big replay's heading interpolates `total * delayMs / 1000` seconds**, which prints
   `would take 0s+` at any small volume. Faithfully mirrored, and it reads as a bug in every
   language that mirrors it. A floor, or a different phrasing under a second, would fix it once.
3. **The contract does not say what an arm is called.** Java's arms are `java-grpc` and friends and
   its table column is 14 characters wide; `typescript-grpc` is 15, so this demo widened the column.
   Harmless, but "same columns, same order" is stated as contract and column *width* is not - worth
   one sentence so the next language does not think it has broken something.
