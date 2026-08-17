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

### Does `poll(processor)` block? The shape is yours; the property is not

**Settled, by seven clients answering and not converging: the shape is each language's own, and no wave will
promote one to a rule.** Go returns immediately with `Done()`/`Err()`, because a blocking call interruptible
only from another goroutine is not how Go expresses a background service; TypeScript answers with `done()`,
Rust with `closed()`, Python and Ruby with `wait()`; C# returns a `Task` that *is* the session, so awaiting it
is the blocking reading and holding it is the background one, chosen per call site; Kotlin's `poll` suspends
for the session's life. Each is right in its own language and would read as foreign in the others, so pick the
idiom your users expect and document it. What follows is what every one of those answers has in common, and it
is normative.

- **The caller can learn the session ended, and why, without ending the client to find out.** This is the real
  content of the question. A mid-session stream error, a cancelled call (§3.2), a sidecar exit and a completed
  drain all end the session; a surface where `poll` has already returned and nothing reports the end leaves the
  application believing it is still consuming, which is the worst failure this surface can have. Both halves
  bind: *that* it ended, and *why*, from the same call. TypeScript's `done()` rejects with the cause and Ruby's
  `wait` raises it; Python and Rust deliver the *that* reliably but make you close or consume the client to
  learn the *why*, which is the half to improve, not to copy.
- **The JVM's answer is `CompletionStage<Void> sessionEnd()`** on `ParallelConsumerClient` - C#'s `Task` in
  Java's vocabulary - completing normally when the session ended cleanly (the application closed the client, or
  the engine completed the stream) and exceptionally with the cause when it did not. It is an accessor rather
  than `poll`'s return value because a session can die before or without a poll: a client that only connected
  still has an end to observe. Both Java transports implement it, including the in-process `java-direct` one,
  and the shared conformance suite has a case for it, so a transport that merely declares the method cannot
  pass. Mirror the property, not the `CompletionStage`.
- **Nothing per-record changes.** The session's end is admin-level; executors still report per record with the
  token echoed, and the queue rules of §3 are unaffected by which shape you pick.
- **The client documents its own answer in its README**, in one sentence naming the shape (blocking, handle,
  future, channel, callback) and how a caller observes the end - including the cause.

## 2. Spawning and finding the sidecar

- The application supplies the sidecar binary's location **explicitly** (an absolute path, or a
  package-embedded resource the library resolves to one). The library must **never** locate the binary
  through `PATH`, a relative lookup, or any directory an attacker could influence - the client hands this
  process the Kafka credentials, so which binary runs is security-relevant. (The full admission-hardening
  posture is post-v6; this rule is what every client does meanwhile.)
- Launch the binary **directly, never through a shell** - a shell wrapper holds the stdin write end and
  defeats the proxy's parent-death signal, leaking a JVM that still holds group membership.
- Keep the child's stdin pipe open and never write to it; read `port: <n>` from the first stdout line and
  connect to `127.0.0.1:<n>` plaintext. **Everything the child writes after that line is still yours to
  read** - §10 owns what happens to it, and not reading it hangs the sidecar.
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
2. **`Configured.max_concurrency` bounds the records this client has been dispatched and has not yet
   reported - queued *plus* executing - and never the queue structure's own length.** Handing a record to
   an executor moves it; it does not free its slot. Count unresolved records in one place, as a number
   the queue owns (`DispatchQueue.inFlight` in TypeScript, `_outstanding` in Python, `Shared::unresolved`
   in Rust, `@unresolved` in Ruby); a bounded channel or fixed array may stay as a second, structural
   statement of the same bound, but it must not be the thing that fires.

   **This is the rule of this section most often implemented wrongly, and the wording was the cause.** It
   read "the queue's depth is `Configured.max_concurrency`", with the counting basis stated only inside
   the worked example below - and three of the first five clients (Go, Rust, Ruby) bounded the queue.
   A client that bounds the queue **cannot detect overflow at all**, whatever its comments say: a record
   leaving the queue always makes room, so the condition the rest of this rule describes never arises.
   Implement from the sentence above; the worked example illustrates it and does not carry it.

   **Only a verdict frees a slot, and there are exactly four.** Every client has had to rediscover this
   list, so it is written down: a **report** is sent (success, failure or terminal - decrement after the
   send, not when the executor picks the record up); a record is reported **`Released`** at shutdown on a
   session that negotiated `shutdown` (§3.5); a record is **discarded** - queued records dropped at
   shutdown without `shutdown` (§3.5), the queue discarded on connection loss (§3.6), and a `Drop` for a
   record this client still holds (§4), for which it will report nothing and so nothing else will ever
   free it; or a worker's death is reported by **`WorkerDied`** (§4), which returns its records to
   scheduling without a report from this client. Put the decrement where an executor dying mid-record
   cannot skip it - your language's `finally`/`ensure`/`defer` - or the ceiling shrinks permanently, one
   slot per crash, and the client eventually declares a protocol violation against a correct proxy.

   **Overflow is therefore a protocol violation, not a load condition**: it is the proxy exceeding its own
   declared ceiling, so never drop records to make room and never grow unbounded. What a client may *do*
   about it is constrained by gRPC - **only the server side of a call sets a status, so a client cannot
   fail the stream with `FAILED_PRECONDITION`**, which is what this rule said until the Go and Python
   waves each hit it independently and resolved it the same way. The implementable form, which every
   client now follows: **cancel the call** (your language's cancel/abort on the streaming call - the proxy
   observes `CANCELLED`), and **raise a local protocol-violation error naming the counts** - the
   unresolved count, the negotiated `max_concurrency`, and the overflowing record's token - through
   whatever surface you chose for a dead session (§1). Name that error in your README. The counts reach
   nobody but the application: v1 has no client→proxy diagnostic message, so the proxy learns only that
   the stream was cancelled and takes its ordinary connection-loss path (specification, "Errors the proxy
   returns"). A diagnostic message would be an additive change with its own capability token, not
   something to improvise.

   **Rendering that token is not a breach of its opacity, and the specification requires it.** Opacity
   forbids *deriving* - parsing `record_id`, comparing epochs, branching on either (specification, "The
   epoch echo rule"); it says nothing about printing. So print the token's two fields **as they arrived**,
   the generated message's own rendering being the simplest correct answer - a `Token` carries no
   credentials, unlike the `Configure` message that §10.4 forbids rendering whole - and never assemble the
   message out of parts you interpreted. It is engine-generated identity, not user payload, so §10.5 does
   not reach it either. **No client does this yet**: all five render the counts and omit the token, which
   makes it the one part of this rule with zero implementations rather than a settled convention. Add it
   when you next touch that error's message.
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
← Dispatch{[A, B, C]}          admin admits A, B, C (3 unresolved = max_concurrency: fits exactly)
  executor-1 takes A (FIFO)    executor-2 takes B     C waits, leased, heartbeats covering it
                               ^ the queue now holds ONE record and the ceiling is still full
→ Report{B, success}           executor-2 takes C     (out-of-order completion is normal)
                               ^ B's report is what frees the first slot (§3.2)
← Shutdown{}                   nothing queued now, but suppose C were still queued:
→ Report{C, released}          - released, not run, not dropped
→ Report{A, success}           executing work finishes and reports normally
  admin half-closes
```

The `Shutdown` message only arrives on a session that negotiated `shutdown`, so the `released` report above is
legal by construction; the harness's `["dispatch"]`-only session reaches close the other way, by the
application closing the client, and drops what it has queued (§3.5, §5).

A fourth record D arriving while A, B and C are all **unresolved** would overflow - and is exactly the
proxy exceeding its own declared ceiling: cancel the call and raise the counted protocol violation (§3.2).
Read the trace again for *which* three are unresolved at that moment: A and B are executing and only C is
queued, so a client bounding its queue has two free slots and admits D without a murmur.

### The negative control, and the test shape that looks like it but is not

**The overflow control must fill the ceiling with records EXECUTING, not merely queued.** One `Dispatch` of
four records against a ceiling of three trips *any* bound (a bound on the queue's own length included), so
that test passes identically against a client carrying the defect rule 2 describes, and proves only that
some limit exists somewhere. **Rust shipped exactly that test, and it passed against the defect it was
named for**; the divergence was found later by reading five clients side by side, not by any suite.

The shape that discriminates is the worked example's:

1. dispatch records up to `max_concurrency` and let the executors take them, so the queue is short (or
   empty) while the ceiling is full;
2. report nothing - no success, no failure, no release;
3. dispatch one more record.

A client counting queued records accepts it and stays silent; a client counting unresolved records cancels
the call and raises. **Watch the test fail against a queue-length bound before you trust it** - both waves
that fixed this defect did, and it is the only evidence that the control controls anything. The
conformance scenario `the-client-queue-hands-out-fifo-and-releases-on-shutdown` (§7) carries this shape.

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

The rule is above; **§10.4 carries the part that is a client problem specifically** - the language mechanism
by which a configuration object logs itself without anyone asking it to.

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
  off by negotiation (their `Configured` fields absent) - which is the same statement as the "named now"
  table below. **Which side is holding each one back differs, and the difference decides what unblocks
  it**: the proxy already grants `dispatch, heartbeat, manifest, worker-death` (`ConfigureHandler`'s
  `PROXY_CAPABILITIES`), so liveness and manifest are un-negotiated because *no client declares or
  implements them yet* - not because the engine cannot answer; terminal and shutdown are the ones still
  waiting on their engine units. A scenario turning on a lease therefore needs client work, in every
  language, not an engine unit; and **the mock ignores the
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
| `the-client-queue-hands-out-fifo-and-releases-on-shutdown` | §3 rule for rule, including the overflow negative control - which overflows with records **executing**, not with one oversized wave (§3, "The negative control") |
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

**Settled 2026-08-15, once seven clients had answered:** whether `poll(processor)` blocks (§1) is **each
language's own shape**, and that is the settlement rather than a deferral - the seven answers did not
converge, and each is idiomatic where it was written. What binds every language is the property §1 states:
the caller can learn the session ended, and why, without ending the client to find out. *Alternative
rejected:* promoting one shape to the rule - Go's return-immediately, or C#'s `Task` that is the session -
which would read as foreign in half the fan-out for no gain the property does not already deliver.

**2026-08-15, the logging contract (§10)**, written after auditing all seven existing clients rather than
from first principles:

| Divergence | Settled as | Alternative rejected |
|---|---|---|
| The sidecar's stdout and stderr (§10.1) | Drained for the child's whole life, both of them; stderr reaches the application by default | Discarding stderr by default - the Go, Ruby and .NET clients each do, and a misconfigured broker then presents as an unexplained hang |
| One logging mechanism for all languages (§10.2) | Each language's own convention, named per language | An injectable interface everywhere - it is the right answer only where the ecosystem has no facade, and imposing it on Java or .NET would be a worse client in those languages |
| Whether the library may log unasked (§10.2) | Silent until the application configures a sink, by that language's mechanism for silence | Logging at a default level, which is how a library ends up writing to somebody else's stdout |
| Payload in log lines (§10.5) | Never, at any level - keys and values are user data | Debug-only payload logging, which is the same leak with a level attached to it |

**2026-08-15, the in-flight ceiling (§3.2)**, from the cross-client divergence review
(`docs/inflight/branch-client-divergence-review.md`, finding 2), which read all five non-JVM clients side
by side. **Three of five authors implemented this rule wrongly from the same text, so the text was the
defect** - the rule said "queue", the worked example said "unresolved", and the rule is what gets built:

| Divergence | Settled as | Alternative rejected |
|---|---|---|
| What `max_concurrency` counts (§3.2) | Records dispatched and not yet reported - queued plus executing - with the basis stated in the rule, not only in the example | Bounding the queue structure's length, as Go, Rust and Ruby each built: it cannot detect overflow at all, since hand-out makes room |
| What frees a slot (§3.2) | A verdict, and only a verdict: report, `Released`, discard, `WorkerDied` - named in the rule so no client works the list out again | Leaving it implied, which is how the decrement ends up on the executor's take in the next language |
| The token in the overflow error (§3.2) | Kept, and stated: render the token's fields as they arrived: opacity forbids deriving, not printing, and the specification's overflow contract requires the token | Dropping the requirement as a breach of opacity - it would put this guide at odds with a frozen specification over a misreading |
| The overflow negative control (§3, §7) | Fill the ceiling with records **executing**, report nothing, dispatch one more | One `Dispatch` larger than the ceiling - Rust shipped it, and it passed against the defect it was named for |

## 10. Logging (binding on every client)

Two channels, and confusing them is the mistake this section exists to prevent: **the sidecar's output**,
which the client owns because the client spawned it (§2), and **the client library's own logging**, which
belongs to the application that called it. The first is a liveness duty. The second is a politeness duty.
Seven clients reached seven different answers before this was written down, which is why it is here.

### 10.1 The sidecar's output is drained, always

**The client spawns the sidecar, so the client owns its stdout and stderr - and a pipe nobody reads fills
up and blocks the writer.** The sidecar then stops mid-log-line and never returns, which reaches the
application as a stalled consumer with no error, no exception and nothing in any log: the worst failure
shape available. It is not a slow leak either - an OS pipe buffer is 64 KiB on Linux, which a JVM at INFO
reaches in seconds under load.

- **Read both streams for the child's whole life, not just until the port line.** §2's `port: <n>` scan is
  the *start* of the read, never the end of it. The test-mode harness makes this easy to get wrong in the
  opposite direction - it logs *before* its port line, so the scan already tolerates chatter - but the
  lines that matter for this rule are the ones after it, which no test currently forces anyone to consume.
- **A stream you do not intend to read must not be a pipe.** Inheriting the parent's stderr, or pointing
  the child at the null device, is safe because there is no buffer to fill; opening a pipe and ignoring it
  is the bug. Closing the child's stderr descriptor outright is not a third option - the child then writes
  to a closed descriptor, and that descriptor number is free to be reused by the next file it opens.
- **stderr reaches the application by default.** Silencing a child process's diagnostics by default is how
  a misconfigured broker becomes an unexplained hang; the application may redirect or discard it, but it
  does not have to ask for it. Where the platform has a natural inherit (a parent stderr the child can
  share), that is the default; otherwise drain the pipe and write it out.
- **Keep a bounded tail for the diagnostic.** The last lines before a crash are the whole explanation, and
  a spawn that fails without them costs an afternoon. Retain a fixed number of recent stderr lines (30-40
  is what the clients that do this already keep) and put them in the error raised when the sidecar dies,
  fails to announce a port, or exits mid-session. Bounded, because an unbounded buffer of a chatty child's
  output is a leak of its own.
- **Forwarding the sidecar's stdout into the application's logs is a per-client choice, and the default is
  not to.** It is the proxy's own logging, already written to the proxy's own format; re-emitting it puts
  text this library did not compose into the application's log stream, at levels this library cannot
  interpret. Draining is mandatory; re-publishing is not.

### 10.2 The library logs through its ecosystem's facade, and says nothing until asked

**A library that writes to stdout or stderr unasked is badly behaved** - it corrupts programs whose stdout
is data, and it appears in logs whose format it does not know. So a client logs through whatever its
ecosystem's *applications* use to receive logs from libraries, and emits nothing until one is configured.

There is no cross-language mechanism here and inventing one would make every client worse in its own
language. The mechanism per language, with what it costs:

| Language | Log through | Silent until configured, because | The application plugs in by | Dependency added |
|---|---|---|---|---|
| Java | SLF4J `org.slf4j:slf4j-api` (Lombok's `@Slf4j` for the field) | with no binding on the classpath SLF4J 2.x no-ops after one startup notice | putting a binding (Logback, Log4j 2) on its own classpath | `slf4j-api` only - **a library never ships a binding** |
| Kotlin | the same `slf4j-api`, via `LoggerFactory.getLogger(...)` | as Java | as Java | as Java - **not** `kotlin-logging`, which is a second dependency for a wrapper this client does not need |
| Python | stdlib `logging`, `getLogger(__name__)` per module, **plus `logging.NullHandler()` on the package's top-level logger** | only *with* that handler: without it Python's `lastResort` prints WARNING and above straight to `sys.stderr`, unformatted | ordinary `logging` configuration on the `parallel_consumer` logger | none - stdlib |
| Go | stdlib `log/slog`: a `*slog.Logger` field on `Options` | the default is `slog.New(slog.DiscardHandler)`, whose `Enabled` is false at every level, so call sites cost nothing | setting `Options.Logger` | none - stdlib |
| Rust | the `log` crate's macros (`log::debug!`, `log::warn!`) | by construction - with no logger installed the facade is a no-op | installing its own logger (`env_logger`, or `tracing-subscriber` plus `tracing-log`); **the library never calls `set_logger`** | `log` 0.4, which has no required transitive dependencies |
| TypeScript | an injectable `Logger` interface on the options - **the ecosystem has no facade, and this is the answer, not a gap** | the field is absent by default and nothing is emitted | passing any object with the four methods: `pino`, `winston`, or bare `console` | none |
| .NET | `Microsoft.Extensions.Logging.Abstractions` - an `ILoggerFactory` on `ClientOptions`, `[LoggerMessage]` source-generated call sites | the default is `NullLoggerFactory.Instance` | setting `ClientOptions.LoggerFactory` | one abstractions package, no implementation |
| Ruby | stdlib `Logger`, duck-typed - anything answering `debug`/`info`/`warn`/`error` | the default is `Logger.new(IO::NULL)` | passing `logger:` | `logger` **must be declared in the gemspec**: it stops being a default gem in Ruby 3.5 |

**Three of these have moved recently enough that recall is not evidence** - check before copying an older
client:

- **Go's answer is `log/slog`, not logrus or zap.** `slog` entered the standard library in Go 1.21, which
  makes a third-party logging dependency in a thin client indefensible. `slog.DiscardHandler` needs Go
  1.24; below that the discard idiom is `slog.New(slog.NewTextHandler(io.Discard, nil))`, which is worse
  because its handler still evaluates. The Go client's `go.mod` already declares `go 1.25.0`.
- **Rust's answer is `log`, not `tracing`.** `tracing` is the richer instrumentation story and the heavier
  one; `log` is the direct SLF4J analogue, its own documentation tells libraries to link only against it,
  and `tracing-log` lets a `tracing` application capture it anyway - so choosing `log` excludes nobody.
- **Ruby's `logger` leaves the default gems in Ruby 3.5.** Requiring it without declaring it already warns
  on 3.4 and breaks on 3.5. The Ruby client has no gemspec at all today, so this lands with one.

### 10.3 Levels, and what is worth logging at all

The test is whether a line helps someone diagnose a *session*, not whether it narrates the library.
Per-record logging in a library processing at these rates is not diagnosis, it is a second workload.

- **INFO** - once-per-session facts: the sidecar's port, the connection opening, what capability
  negotiation actually granted (the effective `Configured` values, never what was asked for), and the
  session ending with its reason. That is roughly four lines for a healthy run, which is the target.
- **WARN** - the session is degraded but alive, or something was ignored: a sidecar that had to be killed
  because it did not exit on its lifecycle pipe closing, a message this client does not implement being
  dropped, a drain that timed out with work still unreported.
- **ERROR** - the session is over and the application needs to act: a protocol violation (with its counts -
  §3.2), a stream error, the sidecar exiting under a live session.
- **DEBUG** - the per-record and per-message level. Reachable, off by default, and still bound by §10.4 and
  §10.5: debug is not an exemption from either.
- **Redelivery and queue overflow are worth a line each; a normal dispatch is not.** Redelivery means the
  attempt count moved, which is the fact someone chasing a poison record needs. Overflow is a protocol
  violation and already fatal to the session (§3.2).

**A client that cannot yet tell its caller the session died must at minimum not be silent about it.** §1
requires the caller to be able to observe the session's end, and its cause, without ending the client - the
Java reference satisfies it through `sessionEnd()`, and Go does not yet (`Done()` never fires on a stream
error). Until a client's surface can report the death, it is logged at ERROR with its cause. That is a floor,
not a substitute: a log line the application must read to discover it has stopped consuming is a worse API
than an observable end, and §1's requirement stands.

### 10.4 Credentials: §6 is the rule, this is how it gets broken

**§6 owns the rule** - the property map, any entry of it, and any message embedding it are never logged, at
any level. What is client-specific is *how the leak happens*: nobody writes `log.info(kafkaProperties)`.
The leak is a **default renderer** on a configuration object, invoked by a log line that names the object
and looks harmless.

Every language has one, and they differ only in what they are called: a `record`'s compiler-generated
`ToString`, a `@dataclass`'s `__repr__`, a `derive(Debug)`, a Lombok `@ToString`, a `Struct#inspect`, a
`data class`'s `toString`. Each prints every field it has, including the credential-bearing map, into any
line that interpolates the object.

**So every client's options type carries an explicit, hand-written renderer that omits the property map**,
and prints its size instead of its contents - a count is a useful diagnostic and discloses nothing. This is
a rule about the type, not about the call sites: relying on call-site discipline means auditing every
future log line, while an options type that cannot render its own credentials is safe by construction. The
same applies to the generated `Configure` message, whose protobuf `toString` prints the map in full - it is
never logged whole, and a client that wants to log the configuration logs the fields it chose.

### 10.5 Beyond credentials: record keys and values are never logged

**Record keys and values are user payload - somebody's customer data - and they appear in no log line at
any level, including debug.** A client that logs payload at debug will eventually log someone's personal
data, in a system whose whole point is processing a lot of it quickly, and the person who turned debug on
to chase an unrelated problem will not know that is what they did.

The mechanism is §10.4's: an `InboundRecord` or `OutboundRecord` whose default rendering includes its
bytes will be logged by someone, so the record types carry hand-written renderers too. **Topic, partition,
offset and attempt count identify a record completely** for every diagnostic purpose, and none of them is
user data - that is the identity to log. A failure reason is worker-supplied text and may be logged, but
its length is the safe rendering when the reason is being echoed rather than reported.
