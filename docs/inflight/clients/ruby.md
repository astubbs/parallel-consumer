# Client: Ruby (astubbs#242)

Per-language working note for the Ruby client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Ruby wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Wave one landed:** connect, configure, one dispatched record through a worker thread, report,
clean drain. Leases, heartbeats, reconnect, worker death, terminal outcomes, the demo and RubyGems
publishing are all deferred and named in the module's testing-evidence `limitation`.

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

## Owed to whoever picks this module up

- **The CI matrix row pins Ruby 3.4.4; every run behind this wave happened on 4.0.6.** The library
  targets 3.2 (`Thread::Queue#pop(timeout:)` is the binding floor) and nothing used is newer, but
  3.4 has not actually been exercised. The workflow has one writer and no wave edits it; whoever
  owns `.github/workflows/clients.yml` should decide whether to move the pin or leave it as the
  cross-version control it accidentally is. That row's scanner also installs `rubocop -v 1.69.2`,
  which does not match this module's pinned 1.89.0 and predates `TargetRubyVersion: 3.2` handling
  of some cops used here.
- **The overflow path has never run.** It cannot be provoked by a correct proxy, and the harness is
  correct. It stays code-reviewed rather than tested until the conformance suite gains the
  negative control the guide names.
- **`src/docs/development/upstream-map.yaml` has no entry for this work** - outside the wave's file
  scope, the same gap the Python wave recorded.
