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
worker death, terminal outcomes, the shutdown drain, crates.io publishing, and the rest of the
conformance suite.

**The demo landed too**, at `.../parallel-consumer-proxy-client-rust/demo/` - two arms, both
replays, both entry points run. What it found is in "What the demo wave found" below; what is still
open from it is in "Open from the demo wave".

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

## Fixed after wave one: the in-flight ceiling counted the wrong thing

The cross-client divergence review (`docs/inflight/branch-client-divergence-review.md`, finding 2)
found this by reading, and it held: **the ceiling bounds UNRESOLVED records - queued plus executing -
and this client bounded only the queued ones.** Admission was the bounded channel's own capacity
(`try_send` returning `Full`), so a record leaving the channel for an executor freed a slot. Replay
the guide's worked example - ceiling 3, A B C queued, two executors take A and B, D arrives - and
`try_send` succeeded: **the overflow the guide names as the conformance suite's negative control
could not fire.** Go and Ruby had the same defect; TypeScript (`DispatchQueue.inFlight`) and Python
(`_outstanding`) already counted correctly, and this fix converges on their semantics rather than
inventing a third shape.

- **`Shared::unresolved`, an `AtomicUsize`, is the admission control now**, checked in `enqueue`
  against `ceiling(max_concurrency)`; the bounded channel stays as the structural statement of the
  same invariant and can no longer be the thing that fires.
- **Only a verdict frees a slot**: `Shared::settle` is called after the report is sent in `run_one`,
  and for each record discarded by `shutdown`. Taking a record off the queue does not.
- **Read-then-add, not add-then-check**, because `enqueue` has one caller - the transport task - so
  admission is single-producer, and a concurrent `settle` can only free capacity. It also leaves the
  count honest on the violation path.
- **The old test could not have caught it**: it overflowed with a *wave* larger than the ceiling,
  which the channel bound alone already rejected. The two new tests use the guide's shape - records
  out with executors - and both were watched fail against the old admission check before the fix
  went in.

## Divergences from the guide this wave took deliberately

- **Queued records are dropped at shutdown, not `Released`.** The guide's §5 says to release them;
  `Released` is gated by the `shutdown` capability, which this client does not declare. The code
  says so at the point where the wave that implements the drain will add the branch. (The Go wave
  reported the same collision; both clients resolved it the same way.)
- **The port line is scanned for, not read as the first line**, per the guide's harness carve-out.

## What the demo wave found

The demo is a crate of its own at `.../parallel-consumer-proxy-client-rust/demo/`, beside the
client rather than inside it: it needs `rdkafka` for its AK core arm and for seeding, and the
client library must never need a Kafka client at all. A `src/bin/` target would have shared the
library's dependency list and quietly contradicted the claim the demo exists to make.

### The blocking-sleep carve-out needs a Rust footnote, and every async language should be asked

The contract's per-language rule exempts Python and TypeScript from a blocking sleep and names Rust
among the languages where one is fine. It is fine - and *where* it blocks decides whether the
engine's ceiling means anything, because this client's executors are tasks on an async runtime.

Measured with one term changed and everything else identical (12 cores, 40,000 records,
`--delay-ms 2`, `--concurrency 100`, same broker, same run of the demo):

| the user's function | msg/s |
|---|---|
| `blocking(\|r\| thread::sleep(..))` - the library's blocking-pool entry point | **10,341** |
| `async move { thread::sleep(..) }` - the same sleep, inside the executor task | **3,518** |

2.9x, and the low figure is about the core count divided by the delay - the predicted ceiling for a
runtime whose worker threads are all asleep. The prediction was stated before the run and held.

**This is not a Rust-only question.** C#, TypeScript and Python's asyncio all have a user function
running on a scheduler with a bounded worker set. The wave sync should decide whether the contract
says "a blocking sleep, through whatever the client library offers for blocking user code" rather
than "a blocking sleep is fine".

### The demo cannot see the sidecar's logs at all

The proxy module carries no logback configuration, so logback's default console appender writes to
**stdout** - and stdout is the lifecycle channel `sidecar.rs` drains and discards after the port
line. Setting `sidecar_stderr` does not bring them back; it governs the other stream. Observed, not
inferred: the sidecar prints several dozen start-up lines before its `port:` line when run by hand,
and none of them appear in any of the demo's runs.

So a Rust application whose sidecar fails to configure sees "the session failed" and no reason,
which is exactly the case `SidecarStderr::Inherit`'s doc comment says the default exists to
prevent. Two possible fixes, and the choice is not the demo's: give the proxy a logback
configuration that logs to stderr, or have the client forward drained stdout lines somewhere the
application can reach.

### The container needs two things the reference's does not, and both failed first

The demo's image carries **two** toolchains, because the application is Rust and the sidecar it
spawns is a Java program. Both extras were found by running it, not by reading:

- **`libprotobuf-dev`, not just `protobuf-compiler`.** Debian's `protobuf-compiler` ships the protoc
  binary only; the well-known types the frozen schema imports (`duration.proto`, `timestamp.proto`)
  live in `libprotobuf-dev`. Without it the build fails with "File not found" for those imports
  while a perfectly good protoc is installed - which reads as a broken schema. **Every non-JVM
  client that containerises its codegen will meet this**, and it is a third data point for the
  build-time-toolchain question already open above.
- **Nothing may look for `.git` inside the image.** The root `.dockerignore` excludes `.git`, so a
  walk up the tree to find the repository - which is how the demo locates its own compose file -
  cannot succeed there. The first version asked for it eagerly, which would have failed the
  container before its first Kafka call; only the branch that starts a broker natively needs it, so
  the resolution is now lazy. Caught by reading the `.dockerignore` against the code path, not by
  the container failing. Any language whose demo derives paths from the repository root has the
  same trap.

### `build.rs` picked a `protoc` it could not execute, in preference to a working one

Fixed on this branch. `protoc_from_maven_repository()` selected the Maven-downloaded protoc on
`is_file()` alone, and Maven stores that artifact `0644` - `protobuf-maven-plugin` chmods the copy
it extracts, never the one in `~/.m2`. Because that fallback is consulted *before* `PATH`, a
machine with a perfectly good protoc installed still failed, with a `Permission denied` naming a
path under `~/.m2` - which reads as a corrupt download. The candidate filter now requires the
executable bit, so an unexecutable copy falls through to `PATH` instead of being preferred over it.

**Where else the class could live, checked:** `parallel-consumer-proxy-client-go`'s
`scripts/generate-proto.sh` resolves the same `~/.m2/com/google/protobuf/protoc` directory the same
way - and it already `chmod +x`es what it finds, so it does not have the defect. It fixes it by
*mutating the user's local Maven repository*, which is the trade the Rust fix declines to make; the
two are worth reconciling at the wave sync, not unilaterally. No other client resolves that
directory (`grep -rn 'com/google/protobuf/protoc' parallel-consumer-proxy-clients/` finds only
those two).

## Open from the demo wave

- **No CI row runs this demo.** `bin/ci-demo-test.sh` runs the Java demo through both entry points
  on every pull request, and the contract says a per-language demo inherits that. Wiring it is
  outside this branch's ownership (it may not touch `bin/` or `.github/`), so it is open. Until it
  is wired, the Rust demo's container is exactly the kind of thing the contract warns about:
  shipped, and only ever run by the person who wrote it.
- **The demo crate is not in any Maven or cargo gate.** The module's pom runs
  `cargo clippy --all-targets -- -D warnings` and `cargo test` in the *client* crate's directory,
  which does not reach `demo/`. Both commands pass in `demo/` today, run by hand. The pom is not
  this branch's to edit either.

## The reader-experience polish, and the one thing it breaks

The demo now keeps the contract's rewritten output rules: the banner is the first thing printed,
each arm names the client that produced its row (`AK core (rdkafka)`, `rust-grpc (this client)`),
and both tables carry `records` and `keys` beside `msg/s`. Broker log levels were already set and
were not touched.

**`bin/ci-demo-conformance.sh` can no longer see a single arm row, and this is not Rust-specific.**
Observed, not predicted - its own `skeleton()` awk was run by hand over this demo's captured
output, and the result was `DIAL`s, two `TITLE`s, two `HEADER`s and **zero `ROW` lines**. Two
independent causes, either of which is sufficient:

- its arm-name character class is `[A-Za-z0-9 _-]*`, which excludes the parentheses the contract
  now requires in every arm label;
- its row pattern is anchored with `$` immediately after the ratio column, so any additional
  column - which is exactly what `records` and `keys` are - fails the match.

`normalise_arms` has the same problem one step later: `s/^ROW [a-z0-9]+-grpc$/ROW SIDECAR/` cannot
match `rust-grpc (this client)`. The failure is quiet rather than red: the skeleton is still
non-empty, so no language is *skipped*; the drift check simply stops comparing arm identity and
order, which is one of the two things it exists to compare. `bin/` is outside this branch's
ownership, so it is open here rather than fixed.

**The column order was chosen to keep the one regex that still works.** `records` and `keys` go
*after* `vs AK core`, because the `HEADER` pattern is not end-anchored and therefore still matches;
putting them anywhere else would have broken the header check as well as the row check. Eleven
languages picked their own order simultaneously, so **whoever reconciles this must confirm all
eleven agree** - the drift check cannot tell them apart until its row pattern is repaired, and a
demo whose columns are in a different order from its neighbours' would pass today.

## Two shared-machine hazards seen while verifying, neither of them the demo's fault

Both were hit on a box running eleven language agents at once, and both are worth knowing before
someone reads a failed demo run as a defect.

- **Ruby's and Rust's compose files publish the same host port, 29092.** Two native demos cannot run
  at once, and the second reports
  `Bind for 0.0.0.0:29092 failed: port is already allocated` from `docker compose up`. Python's file
  already parameterises its port (`${PC_DEMO_BROKER_PORT:-19095}`), which is the shape that fixes
  this; changing Rust's alone would only move the collision, so it is recorded rather than patched.
- **The Docker VM ran out of disk**, and the symptom names Kafka rather than the disk: the broker
  container exits 1 having logged
  `Formatting metadata directory /var/lib/kafka/data ... No space left on device`, and the demo
  reports only "starting the broker failed". The C++ demo's broker was in the same state at the same
  time. The host volume was at 99% with eleven image builds in flight. Nothing was pruned - that is
  shared state - beyond this demo's own compose project.

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
