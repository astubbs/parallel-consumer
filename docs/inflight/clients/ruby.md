# Client: Ruby (astubbs#242)

Per-language working note for the Ruby client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Ruby wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Wave one landed:** connect, configure, one dispatched record through a worker thread, report,
clean drain. Leases, heartbeats, reconnect, worker death, terminal outcomes and RubyGems publishing
are all deferred and named in the module's testing-evidence `limitation`. The demo has since landed
- see below.

## Two decisions the specification does not make, taken here

The wave sync owns both; they are recorded as decisions with their arguments, not as preferences.

### Executors are THREADS, not processes - and this differs from Python deliberately

Python forked worker processes because its GIL made threads useless for the target workload. Ruby
has the same GVL and reached the opposite answer, on four grounds:

- **The workload this product exists for is IO-bound.** Keeping ordering while a slow external call
  runs is the whole differentiator, and Ruby releases the GVL around blocking IO. Threads deliver
  real concurrency for exactly that case.
- **A Ruby block cannot cross a process boundary.** `Proc` is not marshalable and Ruby has no
  `spawn`-style start method, so a worker-process design here can only be fork-and-inherit -
  which is unavailable on Windows and unsupported on JRuby and TruffleRuby. Choosing processes
  would narrow the library to MRI on Unix. Python at least had a `spawn` fallback with a stated
  semantic cost; Ruby has none. **This is the argument that decided it**, and it is a fact about
  Ruby rather than a preference.
- **The `grpc` gem declares a live stream fork-unsafe itself** - its bidirectional write loop is
  bracketed by `GRPC::Core.fork_unsafe_begin` / `fork_unsafe_end`. Python's launcher-process
  machinery exists to work around exactly that; threads make the constraint vanish rather than
  manageable.
- **The thinner client wins every close call**: no launcher, no IPC queues, no serialization
  boundary for records and outcomes.

**The cost, stated rather than engineered around:** on MRI a CPU-bound block gets concurrency but
not parallelism. The answer is Ruby's usual one - threads within a process, processes without: run
several application processes in the same consumer group, each with its own sidecar. On JRuby and
TruffleRuby the question does not arise, since neither has a GVL.

**Worth reading before C++ answers the same question**, and worth reading beside
`docs/inflight/clients/python.md`, which reached the other answer for reasons that hold there.

### `poll` does not block; `wait` does

The specification and the authoring guide are silent on whether `poll(processor)` blocks. Ruby's
`Client#poll` starts consumption and returns; `Client#wait(timeout = nil)` blocks; `Client#close`
performs the client-initiated shutdown, and `Client.open` takes an optional block that closes for
you, as `File.open` does.

Two reasons, in order of weight:

1. **A blocking `poll` puts `close` out of reach of the thread that called it.** Every user would
   then need a second thread or a signal handler just to shut down cleanly - and shutting down
   cleanly is the difference between a drained consumer group and a rebalance. The one call this
   library most needs users to make would be the awkward one.
2. **The reference surface is non-blocking**, and Java, Go and Python all return from `poll`. A
   blocking Ruby `poll` would be a shape divergence dressed as a translation.

Ruby loses nothing idiomatically: "start it, then wait for it" is already two calls here, with
`Thread#join` as the model.

## Two specification defects, both already confirmed by Python and Go

Neither was rediscovered independently - both were handed to this wave - so they are recorded as
confirmations rather than findings, and the third language agreeing is the useful part.

- **A client cannot answer a protocol violation with a status code.** The guide says to fail the
  stream with `FAILED_PRECONDITION` when the dispatch queue overflows. Only a gRPC *server* sets a
  status. Ruby cancels the call (the operation view from `return_op: true` is the only handle that
  can) and raises `ParallelConsumer::ProtocolViolation` naming the count.
- **`Released` on shutdown contradicts capability negotiation.** With only `dispatch` negotiated -
  every session the harness serves today - there is no legal message for a queued record. Ruby
  sends `Released` only when `shutdown` is negotiated and otherwise discards the queue for the
  proxy to reclaim.

## Fixed after wave one: the in-flight ceiling counted the wrong thing

The cross-client divergence review (`docs/inflight/branch-client-divergence-review.md`, finding 2)
found this by reading, and it held: **the ceiling bounds UNRESOLVED records - queued plus executing -
and `DispatchQueue#offer` bounded only the array (`@items.size >= @depth`).** A record handed to an
executor left the array, so hand-out made room. Replay the guide's worked example - ceiling 3, A B C
queued, two executors take A and B, D arrives - and `offer` accepted D: **the overflow the guide
names as the conformance suite's negative control could not fire.** Go and Rust had the same defect;
TypeScript (`DispatchQueue.inFlight`) and Python (`_outstanding`) already counted correctly, and
this fix converges on their semantics rather than inventing a third shape.

- **`@unresolved` is the admission control now**, and `DispatchQueue#unresolved` exposes it beside
  `#size`, which keeps its old meaning: queued only.
- **Only a verdict frees a slot**: `DispatchQueue#settle`, called from `Client#run_one`'s `ensure`
  so an executor dying mid-record cannot leave the ceiling permanently short. `#stop_handout`
  settles what it hands back, since a released-or-discarded record never gets a report.
- **`spec/dispatch_queue_spec.rb` is this module's first unit spec** - the end-to-end spec dispatches
  one record, so FIFO past the executor count and the overflow control were unreachable there. The
  two overflow examples were watched fail against the old array-depth check before the fix went in.

## Fixed after wave one: the sidecar was spawned with fd 2 CLOSED

The logging audit (`docs/inflight/branch-client-logging-contract.md`, authoring guide §10.1) found
it: `Process.spawn(..., err: :close)` does not discard the child's stderr, it starts the JVM with
**file descriptor 2 closed**. The next file the JVM opens can be handed fd 2 by the kernel, so
everything written to stderr afterwards lands in that file - and a sidecar that dies during startup
has nowhere to say why, leaving "the sidecar produced no 'port: <n>' line" as the whole diagnostic.
The contract is that a stream you will not read is **redirected, never closed**, and never an
undrained pipe either.

- **The default is now `:inherit`**, mapped to `Process.spawn`'s own `:err`, which is what Rust and
  TypeScript already do. `:null` (`File::NULL`) is the other named choice; an `IO` or a path passes
  through for a caller that will drain it.
- **`:close` is refused with an `ArgumentError` naming the descriptor**, rather than documented as
  wrong - a guard a caller can walk past is not a guard.
- **`spec/sidecar_stderr_spec.rb`** covers the refusal, the default redirect and an explicit
  destination; the first two were watched fail against the old default. The *consequence* of a
  closed fd 2 is deliberately not asserted: MRI reopens its own std descriptors at startup, so a
  Ruby fake sidecar cannot exhibit what a JVM does, and a test built on one would prove nothing.
- **Reaching the rest of §10.1 - a bounded tail of the sidecar's stderr kept for a crash
  diagnostic - is the later logging wave's job**, and is not done here.

## What Ruby hit that no other language will

- **`Thread::SizedQueue` is the wrong answer to the dispatch queue**, and it is the obvious one. Its
  `push` BLOCKS when full, and the only pusher is the thread reading the session stream - so using
  it would have head-of-line-blocked the control plane, which is the first rule of KTD39. The queue
  is hand-written with a `Mutex` and a `ConditionVariable` so that a full queue raises instead.
- **`Struct` silently overrode `Enumerable#partition`.** `InboundRecord` carries a `partition`
  field; as a `Struct` that shadows the enumerable method. RuboCop's `Lint/StructNewOverride` found
  it. Fixed by using `Data.define` (Ruby 3.2+), which includes no such module and is immutable -
  which a record handed to N threads wanted anyway.
- **`logger` left Ruby's default gems in 4.0.** `require "logger"` raises `LoadError` on 4.0.6 in a
  bundle that does not declare it. The library takes any object responding to `debug` instead,
  defaulting to none, which removes the dependency rather than pinning it.
- **The handshake reply and the first dispatch wave race.** The proxy may send a wave immediately
  behind `Configured`, so the session and the dispatch queue are built on the RECEIVING thread
  inside the `Configured` handler, not by the thread waiting on the handshake. Building them on the
  waiting thread leaves a window in which a wave arrives and finds no queue.
- **`send` is `Object#send`.** The method that puts a message on the stream is called `emit`.

## The demo, and the five decisions it took (astubbs#242, plan unit U35)

`demo/` in this module, keeping the contract in `parallel-consumer-proxy/demo/README.md`. Two arms -
`AK core` (the `rdkafka` gem, serial) and `ruby-grpc` (this module's client library over a sidecar it
spawns). The decisions below are the ones a later session would otherwise re-litigate; the ones that
are purely Ruby-facing live in `demo/README.md`, beside the code they explain.

- **`rdkafka`, not `ruby-kafka`, for the serial arm.** librdkafka behind FFI is what a Ruby
  application actually consumes Kafka with (Karafka is built on it); `ruby-kafka` has been archived
  by its authors since 2023. A comparison whose serial arm is an unmaintained gem flatters the
  sidecar for a reason that has nothing to do with Parallel Consumer. It ships **precompiled for
  linux-gnu**, so the demo image installs it without a C toolchain - the reason the container path
  is not slow to build for a native extension.
- **The blocking sleep was checked rather than inherited, and the contract's rule has since changed
  to match.** It is fine here for a reason confirmed against *this* client's own design: the
  executors are **threads**, and MRI releases the GVL around `sleep`. At the time the contract named
  nine languages as safe; four of them turned out not to be, and it now states a predicate instead -
  *is the client thread-per-record?* - citing Ruby as the one language that was checked rather than
  assumed. The lesson is not about Ruby: **a wave that inherits a per-language verdict inherits
  whatever the author of the list guessed**, and only re-deriving it against the client in front of
  you can tell the two apart.
- **The demo does not start a broker; `demo/run.sh` does.** The Java seed uses Testcontainers. Ruby
  has no equivalent this demo would rather depend on, so the entry point starts the *compose* broker
  - the same service the container path uses, one definition rather than two - and hands the address
  in. Natively that broker publishes a **host listener on 29092**, which is the one thing in the
  compose file the Java seed's does not have.
- **Two `PC_DEMO_` variables with no flag**, `PC_DEMO_SIDECAR_CLASSPATH` and `PC_DEMO_SIDECAR_JAVA`.
  The sidecar is a JVM program and Ruby cannot build one, so the launcher and classpath are computed
  by the entry point (Maven natively, baked in at image build) and handed over. They are plumbing,
  deliberately not dials: a flag would invite pointing the demo at an arbitrary binary, which is the
  decision `SidecarCommand` refuses to make on the library's side.
- **A native run needs two toolchains** - Ruby 3.2+ *and* a JDK - so `run.sh`'s auto-detection tests
  for both and says which one is missing. Only one of the ten languages avoids this; it is worth
  knowing before reading the mode line as a Ruby problem.

### The contract's environment precedence does not survive the seed's container path

Recorded rather than edited, since `parallel-consumer-proxy/demo/README.md` is the shared contract:
it promises that every flag has a `PC_DEMO_` variable and that **the environment beats the
defaults**. Compose forwards nothing it is not told to, and the Java seed's `docker-compose.yml`
declares only `PC_DEMO_BOOTSTRAP` and `PC_DEMO_ARGS` - so on the container path every other
`PC_DEMO_` variable is silently dropped, and that is the path a reader without the language's
toolchain always takes. This module's compose file forwards all of them explicitly and reads a blank
one as "not supplied". **Either the seed should do the same or the contract should say the
environment is a native-path feature**; an owner decision, and the same gap will be in whichever of
the ten demos transcribed the seed's compose file literally.

**SETTLED, the first way, in 9d3ee9390.** The seed's compose file now forwards every `PC_DEMO_`
variable explicitly, as do the other nine. The prediction held exactly: the gap was in **seven** of
the ten demos, all of them transcribed from the seed's compose file before it was fixed, and it was
`bin/ci-demo-conformance.sh` that found them on its first run rather than any of the seven agents.
Kept here rather than deleted because the shape is the durable lesson - a promise made by the
contract and kept only by the *native* entry point is invisible from the path a reader without the
toolchain always takes, and no per-language review would have caught it, because every one of the
seven was individually consistent with the seed it copied.

### `blocker-executor-count-formula.md` does not block Ruby

The identity executor-count function means `--concurrency 100` becomes 100 executors. In Python that
is 100 worker *processes*, which is why it blocks that demo; here they are threads, and the demo
observed the proxy granting exactly the number asked for (`--concurrency 4` -> 4 executor threads,
ceiling 4). Ruby therefore needs no cap invented in a demo. It does mean **the demo prints the
granted executor count**, which is the one place a reader can see the formula's effect.

## The reader-experience pass, and the one thing it breaks outside this module (astubbs#242)

The project owner ran **this** demo and was unimpressed by three things, all of which are now
clauses in `parallel-consumer-proxy/demo/README.md` under "The output a reader actually sees". This
module was the one they watched, so what follows is the fix rather than a transcription of someone
else's.

- **The first line named a dial, not the product.** It was `ruby-grpc: the proxy granted 100
  executor threads, ceiling 100`, which does not contain the words Parallel Consumer. The banner is
  now `Demo::BANNER` in `demo/demo.rb`, printed by `Demo.run` before anything else. **It is in the
  Ruby program and not in `demo/run.sh`**, because `docker compose up` in `demo/` is a documented
  entry point with no script in front of it - a banner in the wrapper would be missing from the way
  a reader with only Docker starts the demo. The consequence, stated because it is a real gap: on
  the **native** path `run.sh` still speaks first, with its mode line and "Building the sidecar...".
  The banner is the first thing the *demo* prints, not the first thing on the terminal.
- **The arms named a category, not a client.** `AK core` -> `AK core (rdkafka)` and `ruby-grpc` ->
  `ruby-grpc (this client)`. The labels are no longer reused as identifiers: `AK_CORE_GROUP` and
  `SIDECAR_GROUP` carry the consumer-group names, because a group id built from a label with a space
  and brackets in it is a thing to explain rather than to read.
- **Two new columns, `records` and `keys`**, appended after `vs AK core` rather than inserted before
  it - see the harness note below for why that position was not free. The AK core arm collects keys
  from `Rdkafka::Consumer::Message#key`; the sidecar arm collects them inside `Completion`, under
  the mutex that already guards the counter, since N executor threads reach it at once.

Observed at `--records 20 --concurrency 4 --partitions 2 --replay-factor 2`: `20 / 20` on both
small-replay arms and `40 / 40` on the big-replay sidecar arm, which is what the seeding predicts -
`key-#{index % 1000}` over 40 records is 40 distinct keys, and the two arms agreeing on both figures
is the point of having them.

### The contract's new labels are invisible to `bin/ci-demo-conformance.sh`, and it will not go red

**Reported, not fixed - `bin/**` is outside every language agent's file scope.** That script reduces
each demo's stdout to a skeleton and requires the skeletons to match. Two of its patterns cannot see
an arm row any more, and neither failure announces itself:

- its row matcher accepts an arm name of `[A-Za-z][A-Za-z0-9 _-]*` and every language's arm name now
  contains **parentheses**, so no `ROW` line is emitted at all;
- `normalise_arms` rewrites `ROW <lang>-grpc` to `ROW SIDECAR`, and the sidecar arm is now
  `ruby-grpc (this client)`.

The dangerous half is that the drift check still **passes**: all ten languages lose their `ROW`
lines identically, so the skeletons still match while no longer comparing the one thing the rows
were there for. The header assertion survives only because that pattern is unanchored -
`arm elapsed msg/s vs AK core` is still a prefix of the new header, which is the reason the two new
columns were appended rather than inserted, and the reason `vs AK core` kept its wording after the
arm it names was relabelled.

Whoever owns `bin/` needs to widen the row pattern to allow `(`, `)` and `.`, re-point
`normalise_arms` at the new sidecar label, and ideally assert the deterministic `records`/`keys`
pair the contract added them for - that is the assertion those columns exist to make possible, and
nothing uses it yet.

### `KAFKA_LOG4J_ROOT_LOGLEVEL: WARN` DOES NOT QUIETEN THIS BROKER - measured

**Reported, not fixed: broker log levels were explicitly placed out of scope for this pass.** It is
the first thing to fix before anyone runs a demo to be watched again, and the fix is one line in
each of the eleven compose files, so it wants doing once across the fan-out rather than eleven
times.

The measurement, from the verification run below (`--records 20 --concurrency 4 --partitions 2
--replay-factor 2`, `docker compose up`, the setting present in the compose file and the broker
container recreated from it): **1520 lines of combined output, of which 1465 were the broker, 1042
of those at INFO, and 41 were the demo's own.** The thing a reader came to watch is about one line
in thirty-seven.

The cause is in the image rather than in the compose file. The log4j template under
`/etc/confluent/docker/` in `confluentinc/cp-kafka:7.9.0` renders `KAFKA_LOG4J_ROOT_LOGLEVEL` into
`log4j.rootLogger` **and then unconditionally writes a block of explicit per-logger levels over the
top of it**:

```
log4j.rootLogger={{ env["KAFKA_LOG4J_ROOT_LOGLEVEL"] | default('INFO') }}, stdout
...
{% set loggers = { 'kafka': 'INFO', 'kafka.controller': 'TRACE',
                   'state.change.logger': 'TRACE', ... } -%}
```

A named logger beats the root logger in log4j, so `kafka.*` stays at INFO and the controller and
state-change loggers stay at **TRACE** whatever the root is set to. The root level only ever
governed the loggers *not* in that map, which is why the setting looks applied, changes the
rendered `log4j.properties`, and quietens almost nothing.

The variable that reaches those loggers is `KAFKA_LOG4J_LOGGERS`, which the same template merges
over the defaults (`parse_log4j_loggers`) - something of the shape
`KAFKA_LOG4J_LOGGERS: "kafka=WARN,kafka.controller=WARN,state.change.logger=WARN,org.apache.kafka=WARN"`
beside the existing root setting. Whoever applies it should **count the broker's INFO lines before
and after** rather than reading the rendered properties file: the current setting demonstrates that
a config change can be visible in the file and absent from the output.

### What was checked for noise in the demo's own output, and what was found

Separately from the broker, everything the demo process itself emits was read. **It is clean**: the
sidecar's stderr is inherited by the demo process and the JVM printed nothing, `rdkafka` printed
nothing, `grpc` printed nothing. The `puts` calls in `demo/lib/` are the fingerprint, the topic and
seeding lines, the two per-arm markers and the tables - nothing removable. One thing was wrong and
is fixed:

- **`(AK core is serial and would take 0s+)`.** The big-replay heading priced the arm it drops with
  integer-divided seconds, so at any volume under 500 record-milliseconds it told the reader the arm
  had been dropped to save no time at all - and the contract's own conformance volume is exactly
  such a volume. `Comparison#serial_cost` now prints the figure only when there is one worth
  quoting; "AK core is serial" is true at every volume. Verified both ways: at `--delay-ms 2` the
  clause is absent, and the control at `--delay-ms 100` - one term changed - prints "and would take
  4s+".

Two things a watcher might still notice, neither of them the demo's doing and so neither changed:

- **`docker compose up --build` shows the whole image build first**, including a four-minute Maven
  step and a screenful of `cp: warning: behavior of -n is non-portable`. That belongs to the
  Dockerfile's classpath copy, and it is only ever seen on a cold build.
- **The AK core arm prints its "starting" line and then nothing for as long as it runs** - about ten
  seconds at the demo's own defaults. That is silence rather than noise, and no clause of the
  contract asks for progress inside an arm; if one ever does, that arm is where it goes.

### Eleven demos, one host port: 29092 is not per-language

Every language's `docker-compose.yml` publishes the broker's host listener on **29092**, so two
demos running at once on one machine cannot both start. This pass hit it as
`Bind for 0.0.0.0:29092 failed: port is already allocated`, with the **Rust** demo's broker holding
it. The verification runs were completed by remapping this module's published port temporarily; the
compose file is unchanged.

It costs a reader nothing - they run one demo - so it is not obviously a defect to fix. It costs a
**fan-out** a great deal, and `bin/ci-demo-conformance.sh` runs the languages sequentially for what
may be this reason. Worth deciding once, for all eleven, rather than discovering per language.

The same run also exhausted Docker's disk (`no space left on device` while exporting a 1.38GB
image), with 33GB reclaimable in dangling images from the parallel builds. `docker image prune -f`
recovered 15GB without touching a tagged image, a running container or a build cache.

## Local verification, and what it is pinned to

From `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/`:

| Command | Runs |
|---|---|
| `bundle exec rake` | RuboCop, then the specs - the whole loop, and what Maven's test phase runs |
| `bundle exec rubocop` | The bug finder alone. **RuboCop 1.89.0**, pinned in the Gemfile |
| `bundle exec rspec` | The specs alone |
| `./scripts/generate-proto.sh` | Regenerates the committed stubs; a clean `git status` is the check |

RuboCop is configured as a **bug finder**: `Lint/` and `Security/` at full strength, style
departments turned down to what changes meaning. **Its ability to fail was measured, not assumed** -
an injected useless assignment (`Lint/UselessAssignment`) and an injected unreachable statement
(`Lint/UnreachableCode`) each turned it red before being reverted. One limitation found while doing
that: `Lint/ShadowedException` does not fire on a gem's own exception hierarchy, because the cop
reasons only about the standard library's - so an unreachable `rescue GRPC::Cancelled` after
`rescue StandardError` passes.

## The wave's code is not in the wave's commit

`git log -- parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby` points at
**d07198e01**, whose subject is the *TypeScript* client. Nothing is wrong with the content; the
attribution is. Parallel agents in this worktree share one git index, so this wave's `git add`
followed by a sibling's `git commit` handed the whole Ruby module to that commit. The trap is
already the compound ledger's item 10 in `docs/inflight/next-compound-engineering-ideas.md`,
recorded there from a smaller instance - one swallowed file deletion - a few minutes earlier. This
is the larger one, and it is what raises that entry from a tidiness point to a real risk: a commit
message that does not describe its diff is not recoverable by reading the log, which is exactly
where this repo keeps its reasoning, and where release notes are generated from.

The wave's own reasoning therefore lives in the commit that added this section, and the fix stands
as the ledger states it: **`git commit -- <paths>`, never `git add` then commit.**

## Owed to whoever picks this module up

### The demo's native path has never been executed

**The container path was run end to end and the native path was not.** What ran, on a machine shared
with nine other demo builds: `run.sh --docker` at `--records 20 --delay-ms 1 --concurrency 4
--partitions 2` with `--replay-factor 1` and again with `--replay-factor 2` (so the big-replay branch
and its footnote ran too); the **no-argument** invocation with the same values supplied through
`PC_DEMO_*`, which is what proved compose's environment forwarding; `--help` and an unknown flag
through both `run.sh` and the image's own entrypoint (exit 0 and exit 2). Both arms completed every
time and the process exited 0. **No throughput figure from those runs means anything** - twenty
records at 1ms is group-join cost, and the machine was carrying nine other agents.

The reader-experience pass added to that, and still not to the native path: the container path at
`--records 20 --concurrency 4 --partitions 2 --replay-factor 2` (the conformance harness's own
volume), through `docker compose up` on the image `run.sh --docker` built, exit 0 with both tables
and both new columns; then a second run at `--delay-ms 100` as the control for the big-replay
heading. RuboCop 1.89.0 was run in a `ruby:3.4-bookworm` container over the whole module and is
clean at 25 files. **No throughput figure from any of it means anything** - twenty records is
group-join cost, and ten other language agents were building on the same machine throughout.

The native path is unrun because **this machine ships Ruby 2.6.10 and has no other**, and the
library's floor is 3.2. Three things in it are therefore unexercised: the Maven build that computes
`PC_DEMO_SIDECAR_CLASSPATH`, the compose broker's **HOST listener on 29092**, and `bundle install`
building `rdkafka` from source on a platform with no precompiled gem (macOS). The first reader with
a modern Ruby should run `demo/run.sh --native` before trusting any of it.

Also never run: **the demo at its own defaults** (2000 records, replay factor 20), deliberately -
a full-scale measurement belongs on an unloaded machine.

### The demo has no CI coverage, and the contract says it should

`bin/ci-demo-test.sh` runs the Java demo through both entry points on every pull request, and the
contract is explicit that a per-language demo inherits that: "mirroring the flags and the tables is
not enough if nobody ever runs the container you shipped". That script is Java-only and outside this
wave's file scope, so **nothing runs this demo automatically**. Whoever owns `bin/` should decide
whether it grows a per-language loop or each language gets its own script - a decision that wants
making once for all ten rather than ten times.

### The demo image is 1.38GB

`ruby:3.4-bookworm` (1.0GB) plus a JRE plus the sidecar's jars. `-slim` would take most of it back
but removes the C toolchain that `rdkafka` needs where no precompiled gem exists, so it is a trade
rather than a saving. `docs/inflight/next-container-image-cache-and-size.md` is where that belongs
if it is worth doing across the fan-out.

- **The CI matrix row pins Ruby 3.4.4; every run behind this wave happened on 4.0.6.** The library
  targets 3.2 (`Thread::Queue#pop(timeout:)` is the binding floor) and nothing used is newer, but
  3.4 has not actually been exercised. The workflow has one writer and no wave edits it; whoever
  owns `.github/workflows/clients.yml` should decide whether to move the pin or leave it as the
  cross-version control it accidentally is. That row's scanner also installs `rubocop -v 1.69.2`,
  which does not match this module's pinned 1.89.0 and predates `TargetRubyVersion: 3.2` handling
  of some cops used here.
- **The overflow path has never run against the wire.** It cannot be provoked by a correct proxy,
  and the harness is correct. `spec/dispatch_queue_spec.rb` now covers the queue's own accounting,
  including the guide's worked overflow shape; what remains untested end to end is the client's
  answer to it - cancel the call, raise, report the count - until the conformance suite gains the
  negative control the guide names.
- **`src/docs/development/upstream-map.yaml` has no entry for this work** - outside the wave's file
  scope, the same gap the Python wave recorded.
