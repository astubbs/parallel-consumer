# Client: Rust (astubbs#242)

Per-language working note for the Rust client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Rust wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one landed.** Connect, `Configure`, one `Dispatch` wave, the user's function, the
report, and a clean client-initiated shutdown, proven by one end-to-end test against the test-mode
sidecar. The module is at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust/`; its maturity and
testing-evidence deferrals are lifted. Later waves: leases and heartbeats, the manifest reconnect,
worker death, terminal outcomes, the shutdown drain, the demo and its container, crates.io
publishing, and the rest of the conformance suite.

## The falsification result

**The premise held.** The client was written from `protocol-specification.md`,
`client-authoring-guide.md` and the frozen `.proto` alone. **No proxy Java source was read at any
point.** The one-record test passed on its first run against the real wire, and a negative control
(returning `Err` instead of `Ok`) produced the documented redelivery twice over, with `attempt`
incrementing to 2 and 3 and the reason text verbatim.

The Rust wave was also handed the Go wave's specification defects up front, so it did not
rediscover them; what is below is what remains after those.

## Rust's own answers, for the wave sync

- **The failure outcome is a returned value, and the type system enforces it.**
  `RecordProcessor::process` returns `Result<Outcome, ProcessingError>`; `Err` *is* the failure
  outcome and its text is the reason. There is no `Outcome::failure()` constructor, so a success
  and a failure cannot both be expressed - what other languages enforce by convention, Rust
  enforces structurally. `ProcessingError` converts from any `std::error::Error`, so `?` inside the
  user's function is the whole failure path. This matches Go's answer, which makes it two of two
  for errors-as-values in languages that have no exceptions.
- **A panic is a failure report, not a torn-down session.** Each invocation runs on its own task,
  and a `JoinError` carrying a panic becomes a failure outcome with the panic's message.
- **`poll` returns immediately** - see the open question below.
- **Blocking user code has its own entry point**, `blocking(...)`, which runs the function on the
  runtime's blocking pool. This is a Rust-specific hazard rather than a protocol one: the queue
  rules say the transport task must never block, and in an async runtime a blocking user function
  in an executor task can starve the transport task on the same threads. Other async languages
  (TypeScript, C#, Python's asyncio) should be asked the same question at the sync.
- **The processor is a trait with a blanket impl for async closures.** The trait is the primitive
  (so a language-without-closures shape is expressible, and a stateful named type needs no
  adapter); the blanket impl is the sugar. Both are exercised by a compile-time test, because the
  two are only compatible while Rust's coherence rules permit it - the test is what would notice
  if that changed.
- **Only implemented capabilities are declared**, as a constant naming what the code honours -
  the same choice Go made and the opposite of the Java reference client's empty list.
- **Generated code is a BUILD step, not committed output.** `cargo build` runs `build.rs` on every
  consumer's machine, so the bindings cannot drift from the schema and there is no
  regenerate-and-diff check to remember. The opposite of Go, for a reason that is a fact about each
  language rather than a preference: `go get` has no build step, so Go must commit its stubs.

## Specification questions still open

Each is a question a client author must answer and the frozen documents do not. **Report only - the
Rust wave does not edit the guide** (KTD23); the wave-sync resolver does.

### 1. Whether `poll(processor)` blocks is still unstated, and Rust answers "it does not"

The guide gives `ParallelConsumerClient.poll(processor)` and never says whether it returns once
processing has started or blocks for the session's life. Rust chose **returns immediately**, with
`closed()` awaiting the session's end, for a reason stronger than taste: `shutdown()` is an `async
fn` on the client, so a blocking `poll` would make the only route to a clean shutdown a second task
holding a clone. Go answered the same way. Two languages agreeing is not the same as the reference
surface saying so - the guide should say it.

### 2. The queue-overflow answer needs rewording for every gRPC language, not just Go

Already reported by the Go wave (a gRPC *client* cannot set `FAILED_PRECONDITION`); recorded here
only as a second data point, because `tonic` is a completely independent implementation and the
constraint is identical. This client cancels the call by dropping the stream and names the count in
its own error. Two of two languages means it is the protocol document that is wrong, not a Go
quirk.

### 3. The build-time toolchain question the guide still does not answer

The Go wave reported that no document says how a non-JVM client gets `protoc` and the well-known
type descriptors. Rust hits a sharper version of it: `protoc` resolved through a **mise shim**
cannot find its own `include/` directory, because the shim resolves to the mise launcher rather
than to protoc, so `google/protobuf/duration.proto` is "not found" even though a complete protoc
distribution is installed. `build.rs` therefore resolves the include directory itself - `$PROTOC_INCLUDE`,
then the binary's sibling `../include`, then the mise install tree, then `/usr/local/include`
and `/usr/include`. Every language whose codegen shells out to `protoc` will meet this on a machine
using a version manager, and there is still nothing in the guide to copy.

## Divergences from the guide this wave took deliberately

- **Queued records are dropped at shutdown, not `Released`.** The guide's §5 says to release them;
  `Released` is gated by the `shutdown` capability, which this client does not declare. The code
  says so at the point where the wave that implements the drain will add the branch. (The Go wave
  reported the same collision; both clients resolved it the same way.)
- **The port line is scanned for, not read as the first line**, per the guide's harness carve-out.

## Local gates, and the proof each one fires

Run from the module directory. **`cargo clippy --all-targets -- -D warnings` is the lint gate** -
`--all-targets` because it is the only way the tests and the build script are linted too, and
`-D warnings` because a warning nobody fails on is a warning nobody reads. It is also the module's
Maven *build* step (see the pom), so a CI row running the module runs the same command a developer
does.

Both gates were proven to fail rather than assumed:

- **Clippy**: `self.capabilities.len() == 0` in `src/session.rs` made it red (`clippy::len_zero`),
  and reverting made it green. It also caught two real defects in this wave's own first draft
  (a `format!` with no arguments, then the `concat!` that replaced it), which is how the proof
  started.
- **The end-to-end test**: returning `Err` from the user function instead of `Ok` produced three
  deliveries instead of one and turned the assertion red.
- **A clippy finding worth knowing**: `clippy::redundant_clone` is in the `nursery` group and does
  **not** fire by default, so a needless `.clone()` - the commonest waste in code that echoes
  protobuf messages around - passes the gate. Not worth turning the whole nursery group on; worth
  knowing before someone assumes the gate covers it.

## Effort, against ASM1

The budget was still not recorded before this unit started (the Go wave reported the same, and
nothing has changed), so R16 cannot be falsified against it. Recorded honestly rather than
backfilled. Wave-one effort: one agent session, roughly two hours wall clock, ~1,500 lines of
hand-written Rust across eight source files, a build script and two test files, on top of generated
bindings that are never written to disk in the source tree.

## Concurrent-agent hazard this wave hit

`./mvnw -pl :parallel-consumer-proxy dependency:build-classpath` - which the test harness uses to
locate the JVM-side sidecar's classpath - reads **every** module's pom, so it fails outright while
any sibling module in the reactor has a malformed pom. That is a normal transient state on a branch
several agents are editing at once, and it fails a test that has nothing to do with the module at
fault. The harness now falls back to `-f parallel-consumer-proxy/pom.xml`, which reads only that
module and its parents. Any other language wave writing the same helper wants the same fallback.
