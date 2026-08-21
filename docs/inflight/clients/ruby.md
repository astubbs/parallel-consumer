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
- **The blocking sleep was checked rather than inherited.** The contract lists Ruby among the
  languages where a blocking sleep is fine, and names Python and TypeScript as the exceptions. It is
  fine here for a reason that had to be confirmed against *this* client's own design: the executors
  are **threads**, and MRI releases the GVL around `sleep`. Had this wave taken Python's answer -
  worker processes - the contract's own list would have been wrong for Ruby, so the list is right by
  agreement rather than by luck.
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

### `blocker-executor-count-formula.md` does not block Ruby

The identity executor-count function means `--concurrency 100` becomes 100 executors. In Python that
is 100 worker *processes*, which is why it blocks that demo; here they are threads, and the demo
observed the proxy granting exactly the number asked for (`--concurrency 4` -> 4 executor threads,
ceiling 4). Ruby therefore needs no cap invented in a demo. It does mean **the demo prints the
granted executor count**, which is the one place a reader can see the formula's effect.

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
