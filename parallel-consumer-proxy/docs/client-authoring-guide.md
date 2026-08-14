# Parallel Consumer Proxy - Client-Authoring Guide

How to build a client library for the frozen v1 protocol, in any language. The wire contract itself lives
in [`protocol-specification.md`](protocol-specification.md) and the frozen
`parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto`; this guide is the
architecture, conventions and duties every client shares, and it **accumulates**: each fan-out wave's
resolved divergences are written back here by the wave's designated resolver, so language eleven inherits
what languages two through ten learned. Only the resolver edits this file between waves; language agents
record divergences in their own `docs/inflight/clients/<lang>.md`.

Author from the specification alone. Do not read the proxy's Java source - a question the specification
cannot answer is a specification defect to file (astubbs#242), not a reason to go around it.

## 0. Generating the stubs from the frozen schema

Numbered zero because it happens before anything else here, and because every later section is cited by
number from the per-language notes - the numbering below it does not move.

**Placement comes from the `.proto`, never from your command line.** The frozen file carries the placement
option for every language whose generator needs one: `java_package` / `java_multiple_files` /
`java_outer_classname`, `go_package`, `csharp_namespace`, `ruby_package`, `php_namespace`,
`objc_class_prefix`, `swift_prefix`. Languages whose generators derive placement from the proto package and
file path (Python, Rust, C++, TypeScript; Kotlin rides the `java_*` options) need nothing. Do **not** pass an
override - `--go_opt=M<file>=<path>` and its equivalents - even though it works: `buf breaking`'s `FILE`
category treats a change to one of these options as breaking, so the file is the one authority on where
generated code lands and an override is a second, silently divergent one. A language whose option is missing
is a specification defect to file (astubbs#242), not a command line to invent.

**Sourcing `protoc` and the well-known types.** The schema imports `google/protobuf/duration.proto` and
`google/protobuf/timestamp.proto`, so a generator needs both a `protoc` and an include path that resolves
those - "generate from the `.proto` by path" is not sufficient, and the Go wave lost time to it. Both tools
are in mise:

```bash
mise use -g protoc@latest buf@latest      # protoc 35.1, buf 1.72.0 at the time of writing
protoc -I <repo>/parallel-consumer-proxy-protocol/src/main/proto \
       --<lang>_out=<dir> parallelconsumer/proxy/v1/proxy.proto
```

The mise `protoc` ships its own `include/` directory, which `protoc` finds relative to its own binary, so the
well-known types resolve with **no second `-I`** - that command compiles the frozen schema as written. `buf`
bundles them the same way.

Without mise, the fallback is the one the Go wave built and it is worth knowing exists: the protocol module's
`protobuf-maven-plugin` downloads a `protoc` into the local Maven repository, but the Maven artifact is a bare
executable **without** `include/`, so the well-known types have to be unzipped out of the `protobuf-java` jar
beside it. Worked example:
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/scripts/generate-proto.sh`.

Whether the generated stubs are committed is your language's packaging question (Go has no codegen step at
`go get` time, so it commits them). Where they are committed, regenerating from an unchanged `.proto` must
leave `git status` clean - that is the check that the committed stubs are the schema's.

## 1. The architecture every client follows

One shape, ten languages (KTD3, KTD4):

```
application process
├── the user's function (a closure/lambda/callable - never an importable name the proxy learns)
├── client library
│   ├── admin        - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue,
│   │                  heartbeats, reconnects with the manifest, reports worker deaths
│   └── executors    - N workers (threads/processes/goroutines - your language's idiom),
│                      each: take record → run user function → report outcome with token echoed
└── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
```

- **The engine puppeteers; the client spawns.** `Configured.executor_count` says how many executors; the
  client creates them with its own mechanism. The proxy never learns what the user's function is.
- **The client is stateless per record.** The token rides from dispatch to report on the executor's call
  stack (or equivalent). No request map, no dedupe cache, no completion registry - a stateless client
  cannot have a state bug, and fencing is the proxy's job.
- **Workers never touch Kafka.** Output rides the success report's `produce` payload; the proxy produces.
- **The thinner client wins every close call.** With ten languages, anything that thickens the client is
  written, debugged and maintained ten times. When a design fork is finely balanced, take the branch with
  less client code and put the burden on the engine side.

### The API surface to mirror

The Java API module (`parallel-consumer-proxy-clients/.../parallel-consumer-proxy-client-java-api`) is the
reference definition of the wrapper surface. Mirror its *shape*, not its Java spelling - each surface must
pass an idiomatic review in its own language's terms (errors-as-values in Go, `Result` in Rust,
`async`/`await` where the language expects it):

- `ClientOptions` - topics/pattern, Kafka properties, max concurrency, ordering, the tunables. Carried to
  `Configure` unmodified.
- `ParallelConsumerClient.poll(processor)` - the poll-with-a-function shape; at most once per client.
- `RecordProcessor` - the user's function: takes an `InboundRecord` (topic, partition, offset, key/value
  bytes, attempt, last-failure time and reason), returns an `Outcome`.
- `Outcome` - success (optionally with `OutboundRecord`s to produce) or failure (with a reason). Languages
  with exceptions also translate a thrown exception to a failure outcome, exactly once, in one place.

Keys and values are **bytes** on this surface; deserialization belongs to the user's code.

### Does `poll(processor)` block? A per-language choice, with one thing it must preserve

**Unsettled across languages, deliberately, and not settleable by one wave.** Whether `poll` returns as soon
as the session is open or blocks for the session's life decides whether a language needs a `Done`/`Err`/await
surface at all, so the Go wave named it the sharpest hole in this guide - it chose *returns immediately*, with
`Done()` and `Err()`, because a blocking call interruptible only from another goroutine is not how Go
expresses a background service. That reasoning is idiomatic, not protocol: a language whose users expect
`await consumer.run()` should say so instead.

What the choice must preserve, whichever way it goes:

- **The caller can learn the session died, and why, without polling for it.** This is the real content of the
  question. A mid-session stream error, a cancelled call (§3.2), a sidecar exit and a completed drain all end
  the session; a surface where `poll` has already returned and nothing reports the end leaves the application
  believing it is still consuming. That exact defect is open on the Java reference client - a stream error
  parks every executor with no listener to tell the caller - so **the reference surface is not the authority
  here; do not mirror its silence.**
- **Nothing per-record changes.** The session's end is admin-level; executors still report per record with the
  token echoed, and the queue rules of §3 are unaffected by which shape you pick.
- **The client documents its own answer in its README**, in one sentence naming the shape (blocking, handle,
  future, channel, callback) and how a caller observes the end.

> **PLACEHOLDER - the settled cross-language shape.** Wave two answers this in five languages at once; the
> resolver fills this block from those answers (and, if they converge, promotes it from "per-language choice"
> to a rule). Until then a client picks its idiom and documents it - it is **not** free to leave the caller
> with no way to observe the session ending.

## 2. Spawning and finding the sidecar

- The application supplies the sidecar binary's location **explicitly** (an absolute path, or a
  package-embedded resource the library resolves to one). The library must **never** locate the binary
  through `PATH`, a relative lookup, or any directory an attacker could influence - the client hands this
  process the Kafka credentials, so which binary runs is security-relevant. (The full admission-hardening
  posture is post-v6; this rule is what every client does meanwhile.)
- Launch the binary **directly, never through a shell** - a shell wrapper holds the stdin write end and
  defeats the proxy's parent-death signal, leaking a JVM that still holds group membership.
- Keep the child's stdin pipe open and never write to it; read `port: <n>` from the first stdout line and
  connect to `127.0.0.1:<n>` plaintext.
- Pass **no configuration** by argv, environment or file. Configuration is code: it travels in
  `Configure`, from the user's own language.

## 3. The dispatch queue (normative, KTD39)

The gap between the proxy's in-flight ceiling and the client's executor count is a queue **inside the
client library**, and it is specified once, here, because everything about it is an ordering-or-liveness
decision ten authors would otherwise each invent. Conformance scenario:
`the-client-queue-hands-out-fifo-and-releases-on-shutdown` (see §7).

1. **The admin always reads the stream. It never applies backpressure by not reading.** The stream also
   carries `Drop`, `Shutdown` and reconnect traffic; an admin that stops reading to slow the proxy down
   head-of-line-blocks its own control plane. Read continuously; buffer.
2. **The queue's depth is `Configured.max_concurrency`** - the proxy's own in-flight ceiling, so in a
   correct system it can never overflow. Overflow is therefore a **protocol violation, not a load
   condition**: never drop records to make room, never grow unbounded. What a client may *do* about it is
   constrained by gRPC - **only the server side of a call sets a status, so a client cannot fail the stream
   with `FAILED_PRECONDITION`**, which is what this rule said until the Go and Python waves each hit it
   independently and resolved it the same way. The implementable form, which every client now follows:
   **cancel the call** (your language's cancel/abort on the streaming call - the proxy observes `CANCELLED`),
   and **raise a local protocol-violation error naming the count** - the depth, the negotiated
   `max_concurrency`, and the overflowing record's token - through whatever surface you chose for a dead
   session (§1). Name that error in your README. The count reaches nobody but the application: v1 has no
   client→proxy diagnostic message, so the proxy learns only that the stream was cancelled and takes its
   ordinary connection-loss path (specification, "Errors the proxy returns"). A diagnostic message would be an
   additive change with its own capability token, not something to improvise.
3. **Hand-out is FIFO** - by arrival, and within one `Dispatch` by record order. FIFO is not an ordering
   guarantee (shard ordering is the engine's); it is the one order every language expresses identically,
   which keeps ten clients comparable.
4. **A queued record is already leased, and that is fine.** The lease is extended by connection-level
   heartbeats, not by the record being worked on, so queue time cannot expire it. **Never withhold
   heartbeats because the queue is full.**
5. **At shutdown, release the queue when `shutdown` is negotiated - otherwise discard it.** Stop hand-out
   either way; let executing records finish and report normally; never run the queue, never invent a verdict
   for work you did not do. Which of the two applies is decided by `Configured.capabilities`, not by
   preference: `Released` is gated by the `shutdown` token (§4), so on a session without it - the test-mode
   harness is exactly this case, negotiating only `["dispatch"]` - sending `Released` would itself be the
   un-negotiated-message violation. **With `shutdown`:** report every queued record `Released`, which returns
   it to scheduling with its attempt count unchanged. **Without `shutdown`:** drop the queued records and
   report nothing for them; their offsets never committed, so the proxy returns them to scheduling on its own
   (specification, "Shutdown and drain"). The specification settles this so a client does not have to; both
   waves that met it resolved it this way before it was written down.
6. **On connection loss, discard the queue.** Queued records are held by no live worker, so they must not
   appear in the reconnect `Manifest` - discarding them is what keeps the manifest truthful. The proxy
   returns them to scheduling as unmanifested records; nothing is lost. (Executing records are the
   opposite: keep running, manifest them, report after reconnect.)
7. **A count reduction with records queued does not arise** - the executor count is fixed for the
   connection's life. Recorded so a future dynamic count knows the first question it must answer.

### Worked example

`max_concurrency = 3`, `executor_count = 2`, one wave of records A, B, C:

```
← Dispatch{[A, B, C]}          admin queues A, B, C (depth 3 = max_concurrency: fits exactly)
  executor-1 takes A (FIFO)    executor-2 takes B     C waits, leased, heartbeats covering it
→ Report{B, success}           executor-2 takes C     (out-of-order completion is normal)
← Shutdown{}                   nothing queued now, but suppose C were still queued:
→ Report{C, released}          - released, not run, not dropped
→ Report{A, success}           executing work finishes and reports normally
  admin half-closes
```

The `Shutdown` message only arrives on a session that negotiated `shutdown`, so the `released` report above is
legal by construction; the harness's `["dispatch"]`-only session reaches close the other way, by the
application closing the client, and drops what it has queued (§3.5, §5).

A fourth record arriving while A, B, C are all unresolved would overflow the queue - and is exactly the
proxy exceeding its own declared ceiling: cancel the call and raise the counted protocol violation (§3.2).
That is the conformance suite's negative control on this section.

## 4. Liveness duties

Every duty in this section and §5 is **gated by its capability token** (the specification's
capability-negotiation rules): heartbeats by `heartbeat`, the manifest reconnect and `Drop` by
`manifest`, `WorkerDied` by `worker-death`, the `Shutdown` drain by `shutdown`. A duty whose token is
not in `Configured.capabilities` does not exist on that session - do not send its messages, and do not
wait for its replies. The gated `Configured` fields (`heartbeat_interval`, `lease_duration`,
`reconnect_window`) are absent exactly when their token is un-negotiated, so there is no interval to
guess at.

- Send `Heartbeat` every `Configured.heartbeat_interval`, from the admin, unconditionally - independent of
  queue depth, executor business, or anything the workers are doing.
- Watch your own workers. When one dies, send `WorkerDied` naming the tokens it held - this is the primary
  reclaim path, faster than any timeout. A language whose workers are threads detects death differently
  from one whose workers are processes; the duty is the same.
- On connection loss: keep live workers running, discard the queue (§3.6), reconnect, and open the new
  stream with `Manifest` naming exactly the tokens live workers hold. Then treat `Configured` as the
  resumption signal and honour any `Drop` that follows (discard that worker's result; report nothing for
  it).
- Never build a per-record processing deadline. The lease is connection liveness, not a time budget for
  the user's function.

**Known gap, owed to the reconnect wave: nothing on the wire separates "connection lost, reconnect" from "the
sidecar has exited".** A clean drain, a parent-death exit and a genuinely transient loss all reach the client
as the same receive error - `EOF`, `UNAVAILABLE` or `CANCELLED` in some combination, language by language - so
a client that starts reconnecting after the sidecar is gone spins against a dead port for the whole
`reconnect_window`. It costs nothing today because no session negotiates `manifest`; it is a correctness
problem the moment one does. **Do not invent a mechanism for it here.** The wave that implements §4's
reconnect settles it, and the obvious candidate is out-of-band rather than on the wire - the client spawned
the sidecar and holds its process handle (§2), so the child's exit is observable without any protocol change -
but it is unproven, and the decision (including whether the reconnect loop must be bounded regardless) belongs
to that wave and lands in §9.

## 5. Shutdown duties

- **Proxy-initiated** (`Shutdown` arrives): §3.5's sequence, then half-close the stream. `Shutdown` only
  arrives when `shutdown` is negotiated, so its queued records go back as `Released`.
- **Client-initiated** (the application closes the client): same sequence unprompted - stop hand-out, deal
  with the queue per §3.5, wait for executing records to report - then half-close. The half-close is the
  signal; there is no shutdown-request message. **This is the path that reaches a session without
  `shutdown`**, where §3.5's discard branch applies: drop the queue silently, report only what your executors
  were actually running, half-close. The proxy returns the dropped records to scheduling with their attempt
  counts unchanged - it never committed their offsets, so nothing is lost and nothing is guessed
  (specification, "Shutdown and drain").
- After half-close, drain your executors and reap the sidecar process (it exits once drained and
  committed). Do not kill it while the stream is open - that turns a clean drain into a reconnect-window
  recovery for the next group member.

## 6. Credential hygiene (binding on every client)

`kafka_properties` carries credentials. The rules the proxy obeys bind the client library too:

- Never log the property map, any entry of it, or any message that embeds it (`Configure`'s natural
  to-string does!) - at any log level, including debug and trace.
- Never echo credentials back to the user in errors. Name the property key if needed, never the value.
- Never write credentials to argv, environment variables, or temp files on the way to the proxy - they
  travel the stream and nowhere else.
- Demos inherit all of this for own-cluster mode inputs: prompt without echo where the platform allows,
  and never print what was entered.

## 7. Conformance scenarios

Every client runs the same named scenarios against the same JVM-side harness (the test-mode sidecar,
`--mock`, seeds the scenario's records on a mock consumer and serves the real wire). The scenario name is
its identity everywhere: the harness CLI, each language's test names, and this list. A client is
conformant when every scenario below passes; a scenario name missing from a client's test suite is a
review finding.

### Running the harness

The harness is `bz.stub.parallelconsumer.proxy.testmode.TestModeMain`, deliberately shipped in the proxy
module's **test** jar (it must never reach a client package), so it runs on the proxy module's test
classpath: build with `bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests`, then
`java -cp <tests-jar>:<main-jar>:<test-scope deps> ...TestModeMain --mock --scenario <name>`. Run it with
no arguments for its usage text, which is authoritative for its flags. The conventions a test needs:

- **The scenario name is also the topic name.** `Configure` subscribes to the scenario name as its one
  topic; the seeded records arrive from it - but the subscription does not *select* them (see the fourth
  divergence below).
- **`kafka_properties` may be empty** - `--mock` builds mock Kafka clients and reads no properties. Real
  credentials never belong in a conformance test.
- **The harness is not the shipped sidecar, and it diverges from the lifecycle contract in four ways a
  test must absorb** (each is a harness limitation, not protocol): its stdout carries logging *before*
  the `port: <n>` line, so scan for the line rather than asserting it comes first (production spawn code
  still implements the specification's first-line contract); it serves sessions **until stdin EOF** - it
  does not exit after a client-initiated drain, so reap it by closing its stdin, never by waiting; it
  currently negotiates only `["dispatch"]`, so the liveness, manifest, terminal and shutdown duties are
  off by negotiation (their `Configured` fields absent) until the engine units land and their tokens are
  granted - which is the same statement as the "named now" table below; and **the mock ignores the
  subscription entirely**, seeding the scenario's records unconditionally on a mock consumer, from a record
  whose `topic` is the scenario name. Subscribing to the wrong topic - or to nothing at all - still passes
  every scenario, so **the subscription cannot be used as a negative control**, and a client that never
  populates `Configure.topics` will look conformant here while failing against a real broker. Found by the
  Go wave, which lost a control to it. Assert the subscription you sent against `Configured`'s echo instead.
- **The harness process carries no verdict channel**: it exits 0 whether or not the scenario's assertion
  held, and prints no result. Where an assertion names engine state a client cannot see (the committed
  offset in the first two scenarios), that assertion runs JVM-side; the client-side test asserts the
  wire-observable consequences - the dispatch arrives, a failure redelivers with `attempt` incremented
  and the reason verbatim, a success is followed by silence rather than redelivery. A per-scenario verdict
  the client test can read is U31's harness work, not something to go hunting for today.

Live now (the harness serves them today):

| Scenario | Asserts |
|---|---|
| `a-processed-record-advances-the-committed-offset` | The trivial baseline: one record in, processed once, offset advances past it |
| `an-unreported-record-holds-back-the-commit` | The negative control: a client that never reports leaves the offset uncommitted - proves the harness can fail |
| `a-failed-record-is-redelivered-with-its-failure-history` | Failure → redelivery with attempt=2 and the previous reason verbatim |
| `records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently` | Key ordering serializes per key, parallelizes across keys |

Named now, harness support lands with the engine units (a client author writes these tests against the
scenario names; they activate as the harness grows):

| Scenario | Asserts |
|---|---|
| `the-client-queue-hands-out-fifo-and-releases-on-shutdown` | §3 rule for rule, including the overflow negative control |
| `a-lost-connection-reconciles-by-manifest` | Keep-current / drop-superseded / return-unmanifested, attempt counts unchanged |
| `a-dead-workers-records-return-before-any-timeout` | `WorkerDied` reclaims immediately |
| `heartbeats-keep-a-slow-record-alive-and-their-absence-returns-it` | Lease extension and expiry, attempt count unchanged |
| `a-terminal-report-resolves-the-record-to-the-terminal-topic` | Terminal path: produced, offset advanced, never redispatched |
| `shutdown-drains-reported-work-and-releases-the-rest` | The two-sided drain contract of §5 |
| `the-executor-count-arrives-once-and-never-changes` | `executor_count` in `Configured`, and no `SetExecutorCount` ever - a negative control, not an absence |

## 8. The demo contract (identical in all eleven languages)

Every client ships a demo, and the demo ships as a container - someone who cannot build your language must
still be able to run your demo. The contract below is identical everywhere **because its whole value is
being identical**: a visitor moving from the Rust demo to the Python demo must relearn nothing. The
reference demo (Java) is the canonical rendering; mirror it.

### Modes, flags, environment, prompt

Three modes: `own-cluster` (the user's real Kafka), `broker` (a real Testcontainers broker - the honest
default), `mock` (no Docker, instant boot).

| Input | Flag | Environment variable |
|---|---|---|
| Mode | `--mode=own-cluster\|broker\|mock` | `PC_DEMO_MODE` |
| Bootstrap servers (own-cluster) | `--bootstrap-servers=<host:port,...>` | `PC_DEMO_BOOTSTRAP_SERVERS` |
| Topic (own-cluster) | `--topic=<name>` | `PC_DEMO_TOPIC` |
| Extra Kafka properties file (own-cluster, optional) | `--kafka-properties=<path>` | `PC_DEMO_KAFKA_PROPERTIES` |

Flags win over environment variables. On a TTY with no mode supplied, prompt - this wording exactly:

```
Where should the demo read from?
  1) your own Kafka cluster (you supply bootstrap servers and a topic)
  2) a real local broker in Docker (default)
  3) a mock, no Docker needed
Choose 1, 2 or 3 [2]:
```

Own-cluster inputs arrive the same way as the mode: prompted on a TTY when not supplied, flags/environment
otherwise. Credential hygiene (§6) applies in full - nothing the user supplies to reach their cluster is
ever logged or echoed.

**Non-TTY runs take mock, announced.** A demo that blocks on stdin in CI is the classic failure of this
shape, and one that silently pulls a broker image to die on a missing Docker socket is worse. The first
line of output states the choice and the reason:

```
No terminal detected: running in mock mode (set PC_DEMO_MODE to override).
```

### The serde extension point

Each demo carries this marker, rendered as your language's comment idiom, at the one place a user drops
their own deserializer:

```
PLACE SERDE SETUP IN YOUR LANGUAGE HERE
```

The default below it decodes keys and values as UTF-8 strings, falling back to a hex preview for bytes
that do not decode. It works untouched; the marker is a designed modification surface, because the demo
doubles as the user's starting template.

### Stats and sampling

The demo prints reading statistics and a **rate-limited sample** of message content, so a replay or
backlog renders as a sample rather than a scrolling wall:

- a stats line every 5 seconds: records consumed (total and /s), and per-key or per-partition spread as
  fits the language;
- sampled records at most 2 per second, each showing topic/partition/offset, the decoded key, and the
  decoded value truncated to one line;
- the sampling shape is decided once (in the reference demo) and mirrored; do not invent a fancier one.

A demo is an **application, not a client library**: its `--mode` flag and `PC_DEMO_*` variables are
ordinary application input, not proxy configuration - the no-flags/no-env rule of §2 governs what reaches
the *proxy*, and everything above reaches only the demo.

## 9. Wave-sync ledger

Resolved divergences land here, dated, with the losing alternatives named - so the next wave inherits
decisions, not debates.

**2026-08-14, from the Go and Python waves** (each found the first two independently, without seeing the
other's work, which is why they are treated as specification defects rather than local choices):

| Divergence | Settled as | Alternative rejected |
|---|---|---|
| Queue overflow (§3.2) | Cancel the call; raise a local, counted protocol-violation error | Failing the stream with `FAILED_PRECONDITION` - unimplementable in any language, since only a gRPC server sets a status |
| The queue at close on a session without `shutdown` (§3.5, §5) | Discard it and report nothing; the proxy reclaims | Sending `Released` regardless - it is the un-negotiated-message violation, and the harness session is exactly that case |
| Generated-code placement (§0) | Read from the `.proto`'s own options, one authority for all languages | Per-language `protoc` overrides, nine command lines that `buf breaking` would later pin anyway |
| Sourcing `protoc` and the well-known types (§0) | mise (`protoc@latest`, which ships `include/`) | Assuming a system `protoc`; the Maven-plus-unzip route survives as the no-mise fallback |

**Open, not settled here:** whether `poll(processor)` blocks (§1) - wave two answers it in five languages
simultaneously and the resolver fills that section's placeholder from those answers. Recording it as open is
the decision: settling it from one language's idiom is what the placeholder exists to prevent.
