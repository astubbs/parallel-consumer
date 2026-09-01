# Client: Go (astubbs#242)

Per-language working note for the Go client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Go wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one landed.** Connect, `Configure`, one `Dispatch` wave, the user's function, the
report, and a clean client-initiated shutdown, proven by one end-to-end test against the test-mode
sidecar. The module is at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/`; its maturity and
testing-evidence deferrals are lifted. Later waves: leases and heartbeats, the manifest reconnect,
worker death, terminal outcomes, the shutdown drain, packaging, and the rest of the conformance
suite.

**The demo wave has landed too** - `parallel-consumer-proxy-client-go/demo/`, both entry points run.
Its divergences from the contract, and what it found in the contract, are the last two sections of
this file.

## The falsification result

**The premise held.** The client was written from `protocol-specification.md`,
`client-authoring-guide.md` and the frozen `.proto` alone. **No proxy Java source was read at any
point** - the only JVM-side artifact consulted was `TestModeMain`'s own usage text, which the guide
names as authoritative for its flags. The one-record test passed on its first run against the real
wire, and a negative control (returning an error instead of succeeding) produced the documented
redelivery with `attempt` incremented and the reason verbatim.

That is the headline, and the list below is the price of it: the questions the documents could not
answer. **Do not release the seven-language fan-out until they are resolved into the guide** - that
is this unit's whole job as a gate, and every one of these costs nine more agents the same detour.

## Specification defects found (the unit's real output)

Each is a question a client author must answer and the frozen documents do not. **Report only - the
Go wave does not edit the guide** (KTD23); the wave-sync resolver does.

### 1. The frozen `.proto` has no `go_package`, and no non-Java language option at all

`proxy.proto` sets `java_package`, `java_multiple_files` and `java_outer_classname` and nothing
else. `protoc-gen-go` cannot generate without an import path, so a Go author must discover that the
mapping goes on the command line as `--go_opt=M<file>=<import path>` - which neither document
mentions. The same hole is waiting for C#, Ruby, PHP, Objective-C and Swift, each with its own
option name, so **nine of the ten remaining waves hit this**.

A file-level option is an *addition*, so the freeze permits adding `go_package` and its siblings -
but it is a decision with a capability-note obligation, not an edit to make casually. Either add
them, or write the per-language command-line mapping into the guide's codegen section. Doing
neither means ten authors each rediscover it.

### 2. No document says how a client gets the well-known type descriptors

`proxy.proto` imports `google/protobuf/duration.proto` and `timestamp.proto`. The guide says
non-JVM clients "generate from the `.proto` by path", and a path is not sufficient: the generator
also needs an include path for the well-known types, and it needs a `protoc` at all. On this
machine `protoc` is not installed and cannot be (system packages are managed elsewhere); the only
copy is the executable the protocol module's `protobuf-maven-plugin` downloads into the local Maven
repository, which ships **without** the `include/` directory the standalone release has.

What this wave did, and what the guide should either bless or replace: reuse that downloaded
`protoc`, and unzip the well-known types out of the `protobuf-java` jar beside it. Recorded in
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/scripts/generate-proto.sh`. Every
other language wave needs an answer to the same question and there is currently nothing to copy.

### 3. Whether `poll(processor)` blocks is unspecified, and it shapes the whole surface

The guide gives `ParallelConsumerClient.poll(processor)` - "the poll-with-a-function shape; at most
once per client" - and never says whether it returns once processing has started or blocks for the
session's life. That single answer decides whether a language needs a `Done`/`Err`/`await` surface
at all, so leaving it open guarantees divergence across ten clients that are supposed to be
comparable.

Go chose **returns immediately**, with `Done()` and `Err()` for the session's end, because a
blocking call that can only be interrupted by another goroutine is not how Go expresses a
background service. If the reference surface means it to block, say so and Go will change.

### 4. On a session without `shutdown`, there is no legal way to return queued records

The two documents give opposite answers, and the harness's session is exactly the case where they
collide:

- The guide's §5 says a client-initiated shutdown reports every queued record `Released`.
- The specification gates the `Released` outcome behind the `shutdown` capability, and the
  capability rule says a client "must not send a message type or outcome variant whose token is
  outside the negotiated set".

The harness negotiates only `["dispatch"]`, so a client closing with records queued may neither run
them, nor release them, nor keep them. Wave one drops them and relies on the proxy's connection-loss
path to return them - **inference, not specification**. The guide needs a sentence saying what a
client without `shutdown` does with its queue at close.

### 5. A gRPC *client* cannot fail a stream with `FAILED_PRECONDITION`

The specification says the client-side protocol violation - a `Dispatch` overflowing the queue past
`max_concurrency` - "is failed by the **client** with `FAILED_PRECONDITION` naming the count". In
gRPC only the *server* sets a status; a client can cancel the call, which the peer observes as
`CANCELLED`. The named status is unreachable from a Go client and, as far as this wave can tell,
from any gRPC client in any language - so the rule as written cannot be implemented anywhere.

Wave one cancels the call and reports the count in its own error. The guide should say that, or
define a client→proxy message for it.

### 6. Nothing distinguishes "connection lost, reconnect" from "the sidecar is gone"

Not exercised this wave (the `manifest` token is un-negotiated), but visible while reading for it.
The reconnect duty triggers on "connection loss", and a Go client sees a normal drain, a parent-death
exit and a genuine transient loss as the *same* `Recv` error shape - `io.EOF`, `UNAVAILABLE` or
`CANCELLED` in some combination. A client that starts a reconnect after the sidecar exited will spin
against a dead port for the whole reconnect window. The guide should name the observable that
separates them before any wave implements §4's reconnect.

## Fixed after wave one: the session could die and nobody could tell

The cross-client divergence review (`docs/inflight/branch-client-divergence-review.md`, finding 1)
found this by reading, and it held: **`closed` was closed in exactly one place, inside `closeOnce`
in `Close`.** `receive` on a stream error called `fail` and returned without touching it, so `Done()`
never fired, `Err()` - documented as "meaningful once Done is closed" - never became meaningful, and
every executor parked in a `select` on `stopHandout`/`queue` where neither case could ever be ready.
The only escape was an application that independently decided to `Close`. Go was the last client
where that was still true; the Java reference had the identical defect and answered it in
`061324e20` with `sessionEnd()`.

- **`endSession(cause)` is now the only place `closed` is closed**, reached from four paths: the
  stream faulting, the stream completing, an overflow this client refused, and `Close` finishing its
  shutdown. `stopOnce`/`endOnce` make it safe from more than one, which is the ordinary sequence -
  the stream faults, the application sees `Done`, then calls `Close` to reap the sidecar.
- **A completed drain ends the session too, with no cause.** `Err()` is `nil` for a clean end, so
  the two halves the guide's §1 asks for - *that* it ended and *why* - come from the one surface.
- **The cause is recorded before the close**, so anything that observed `Done` observes it. `failure`
  moved behind `failMu` for the same reason: `Err()` is read from the application's goroutine and
  written from the receive loop, and the channel close alone orders only the closing goroutine. The
  first cause wins; later errors are its consequences.
- **`Close` no longer waits on `closed`.** It cannot: `closed` now fires at the session's end, which
  can be long before, or entirely without, a `Close`. `sync.Once.Do` already blocks a second caller
  until the first completes, so nothing was needed in its place.
- **The estimate was wrong.** The review called it "a two-line fix - closing `c.closed` and
  `c.stopHandout` at the top of `receive()`'s error path". Those two lines *panic*: `Close` closes
  both channels too, and a session that faults and is then closed - the normal sequence - closes
  each twice. The real fix is the once-guards, the single end path, and the removal of `Close`'s
  wait on a channel that no longer means what it did.

## Fixed after wave one: the in-flight ceiling counted the wrong thing

Finding 2 of the same review, and the same defect Rust and Ruby were fixed for in `0f0c77491`:
**`max_concurrency` bounds UNRESOLVED records - queued plus executing - and this client bounded only
the queued ones.** Admission was the buffered channel's free space (`select` with a `default:`), so a
record leaving the queue for an executor freed a slot. Replay the guide's worked example - ceiling 3,
A B C queued, two executors take A and B, D arrives - and the channel had two free slots, so D was
admitted in silence: **the overflow this client documents at length could not fire in the case the
documentation describes.**

- **`unresolved`, an `atomic.Int64`, is the admission control now**, checked in `enqueue` against
  `session.MaxConcurrency`. The channel keeps its capacity as the structural statement of the same
  bound and can no longer be the thing that fires; its `default:` branch is unreachable while the
  count is right, and answers identically.
- **Only a verdict frees a slot.** `settle` is called by a `defer` in `runOne`, which runs after the
  report has been sent, and for each queued record `shutdown` discards - the records that will never
  get a report, so nothing else could ever free them. The `defer` is the point: a decrement written
  straight-line after the send is skipped by an executor that dies mid-record, and the ceiling then
  shrinks one slot per crash until a correct proxy is declared in violation.
- **Read-then-add, not add-then-check**, as Rust does and for the same reason: `enqueue` has one
  caller, the receive loop, so admission is single-producer, and a concurrent `settle` can only free
  capacity. It also leaves the count honest in the violation message.
- **The violation message names the unresolved count, the declared `max_concurrency` and the
  overflowing record's token**, rendered as it arrived. The guide asks for all three and records that
  no client had yet printed the token; this is the first that does. Printing is not a breach of
  opacity - deriving is - and a `Token` carries no credentials.
- **The two decrement points Go still does not have are the two it cannot**: a `Drop` for a record
  this client holds and `WorkerDied` are gated by capabilities this client does not declare, and the
  connection-loss discard (§3.6) arrives with the manifest reconnect. Whoever writes those waves owes
  a `settle` at each.

## Harness divergences the guide does not list

The guide names three ways the harness diverges from the lifecycle contract. There is a fourth, and
it cost a negative control:

- **The mock ignores the subscription.** Subscribing to a topic that is *not* the scenario name
  still delivers the scenario's seeded records, from a record whose `topic` field is the scenario
  name. The guide's "the scenario name is also the topic name ... the seeded records arrive from
  it" reads as though the subscription selects them; it does not, the mock seeds unconditionally. A
  client that never sets `topics` at all would pass every scenario, so this removes the obvious
  negative control and hides a real client bug class.

## Plan defects

- **The unit's own verification command cannot work.**
  `./mvnw -pl :parallel-consumer-proxy-client-go -am test` cannot build the conformance harness: the
  harness lives in `parallel-consumer-proxy`'s test jar, and this module deliberately has **no**
  Maven dependency on the engine, so `-am` does not pull it into the reactor. Something must build
  the proxy first (`bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests`). Wave one documents
  that in the module's pom and README and fails the test with that command in its message rather
  than skipping. Every non-JVM language has the same problem, so the CI matrix rows need it too.
- **ASM1's effort budget was not recorded before this unit started**, so R16 cannot be falsified
  against it - a number decided afterwards falsifies nothing, which is exactly what the plan warns
  about. Recorded honestly instead of backfilled. Actual wave-one effort, for whatever a single
  uncalibrated data point is worth: one agent session, roughly 90 minutes wall clock, ~700 lines of
  hand-written Go across five files plus a generation script, on top of ~2,300 lines of generated
  stubs. Record a real budget before Python and the seven-language wave, or the distribution R16
  needs will have a hole where its first point should be.

## Decisions this wave took that a sync should confirm or overturn

- **Errors are returned, not thrown.** `Processor` returns `(Outcome, error)`; a non-nil error *is*
  the failure outcome, its text is the reason. There is no `Fail()` constructor, because two ways to
  spell failure in a language with one is the un-idiomatic option. A panic is recovered into a
  failure report.
- **`Options` is a struct, not a builder**, and every zero value already means "take the proxy's
  default" - which is the wire's own convention, so the two agree without a translation table.
- **Only implemented capabilities are declared.** An empty list means the v1 baseline, and a partial
  client declaring it would entitle the proxy to send messages the client ignores. Wave one declares
  `["dispatch"]` and the wave that implements a duty adds its token. This was deliberately the
  *opposite* of the Java reference client, which declared nothing at all until `e955e3acd` adopted
  the same answer (`DISPATCH_CAPABILITY` in `WireMapping.toConfigure`) - the empty list reads on the
  wire as the whole v1 baseline, arming a lease-expiry redelivery loop the moment `heartbeat` is
  granted. Go's surface makes the safe answer the structural one: the declared set is a constant
  naming what is implemented, so it cannot fall out of step by omission.
- **The token is echoed as the received message**, never rebuilt from parsed fields, so "opaque" is
  structural rather than a rule someone has to remember.

## The demo wave: what it diverges on, and why

The demo is `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/demo/`, keeping
`parallel-consumer-proxy/demo/README.md`'s contract. Flags, environment variables, precedence,
defaults, the fingerprint-first rule, the never-print-the-bootstrap rule, the two tables and their
columns, and no latency anywhere: all mirrored exactly, and `options_test.go` derives the seven
environment-variable names from the flag list rather than trusting seven hand-written bindings.

Three divergences, all recorded in the demo's own README where a reader meets them:

- **The demo binary never starts a broker; `run.sh` does.** Java's `DemoBroker` falls back to
  Testcontainers when no `--bootstrap` arrives. Go has no comparable dependency in this repo, and
  adding one would put a Docker client library and its tree into a demo whose point is that the
  application needs no infrastructure. So natively `run.sh` starts a `cp-kafka` container on a
  random free port and removes it on exit, in the container the compose sibling is the broker, and
  the binary always receives an address. The *user-facing* contract is unchanged - omit
  `--bootstrap` and a broker appears.
- **`PC_DEMO_SIDECAR_CLASSPATH`**, which is plumbing rather than an eighth flag: no `--flag`, no
  default a user would set, and it disappears the day the sidecar ships as a binary.
- **The demo is a nested Go module.** Go's module graph propagates requirements to every consumer,
  so franz-go in the library's `go.mod` would hand a Kafka client library to applications whose
  whole reason for using the proxy is not needing one. The consequence: `go build ./...` and
  `go test ./...` in the parent do NOT descend into `demo/`, so the module's Maven-driven build
  never compiles it - `run.sh` and the Dockerfile do.

### Open follow-up: staticcheck does not cover the demo module

`scripts/analyse.sh` runs `go vet` over the demo and stops there. `go tool staticcheck` builds the
version pinned in the module it runs in, and `demo/go.mod` does not pin it; adding the `tool`
directive puts staticcheck's dependency tree into the module graph that `demo/Dockerfile` resolves,
and that image was verified without it. Small, and worth doing on an unloaded machine: add the
directive, `go mod tidy`, extend the script, then rebuild the demo image once to confirm the
container path still builds.

### Two things worth knowing before the next Go wave

- **`GOTOOLCHAIN=local` makes the client module unbuildable on an older Go.** The library declares
  `go 1.25.0` and uses `tool` directives, which Go 1.23 cannot even *parse* (`unknown block type:
  tool`) - so on a machine pinned to `local` with 1.23 installed, `go build` fails on the go.mod
  rather than on the code. `run.sh` probes the module with `go list -m`, retries once with
  `GOTOOLCHAIN=auto`, says out loud that it is doing so, and only then falls back to the container.
  Measured on the development machine, which is pinned that way.
- **Kafka refuses `0.0.0.0` as a KRaft controller listener's bind address**, not only as an
  advertised one: the format step aborts with "advertised.listeners cannot use the nonroutable
  meta-address" before the broker starts. `run.sh`'s native broker binds `CONTROLLER://localhost`
  for that reason - measured, not reasoned about. The compose sibling was already correct because
  it binds the service name.

## Contract defects the demo wave found (report only - do not edit the contract)

Recorded rather than fixed, per KTD23: the shared contract at `parallel-consumer-proxy/demo/README.md`
is the integrator's to change.

1. **"Omit `--bootstrap` to start one" assumes every language can start a broker, and most cannot
   cheaply.** The contract inherits Java's Testcontainers fallback without saying *who* starts the
   broker, and nine of the ten remaining languages will each have to decide it alone - some with a
   heavyweight dependency, some by moving it into `run.sh` as Go did, some perhaps not at all. The
   contract should say which layer owns it, or say explicitly that it is the language's choice as
   long as the user-facing promise holds.

2. **Every non-JVM demo container is a two-toolchain image, and the contract does not mention it.**
   "A reader with only Docker can run it" is satisfied, but only by shipping a JDK alongside the
   language's own runtime, because the sidecar is a JVM application spawned as a child of the
   running demo - it cannot be a build-stage artifact the way the demo binary can. Go's image is a
   `golang` build stage discarded into an `eclipse-temurin:17-jdk` runtime. Ten languages will each
   rediscover this; it belongs in the contract's container section beside the socket rule.

3. **The flag/environment table has no room for the sidecar's location, which every non-JVM demo
   needs.** Go added `PC_DEMO_SIDECAR_CLASSPATH` under the `PC_DEMO_` prefix the contract reserves
   for flags. An integrator diffing environment variables across languages will see an extra one
   and cannot tell from the contract whether it is a divergence or a shared necessity. It is the
   latter, in some spelling, for every language whose demo spawns the JVM sidecar.

4. **`bin/ci-demo-test.sh` runs the Java demo only.** The contract says both entry points are tested
   "on every pull request" and that "a per-language demo inherits this", but the script hard-codes
   the Java `run.sh` path and the Java arm list. Nothing yet runs a per-language demo's two entry
   points in CI, so ten demos can ship untested while the contract says otherwise. Out of this
   wave's ownership; named here because it is the gap the contract's own closing paragraph warns
   about.

5. **The big replay's table can be one row, and the `vs AK core*` column then compares the only arm
   against an arm that is not in the table.** That is what the contract asks for and it is not
   wrong, but it reads oddly, and it will read identically in every language except Java. Worth a
   sentence in the contract saying the single-row case is expected rather than a bug.

## The reader-experience wave: what the Go demo now prints

The contract's "The output a reader actually sees" section was rewritten from someone watching a
demo and finding it unimpressive. Applied here as four changes, all inside
`parallel-consumer-proxy-client-go/demo/`:

- **A banner, printed before anything else the demo says.** `main.go` holds it as a `const` copied
  verbatim from the contract rather than composed from parts, and `report_test.go` asserts its four
  lines, the 64-character rules, the words "Parallel Consumer", the fixed second line, and that it
  names *Go* - the one thing that differs per language and the one thing a copy-paste would leave
  pointing at the wrong demo.
- **Both arms name the client they ran**: `AK core (franz-go)` and `go-grpc (this client)`. The
  labels are still the row labels and the `=== arm starting ===` lines, so a reader sees the client
  named at every point the arm speaks.
- **`records` and `keys` columns**, from a small `keySet` both arms share. In the sidecar arm the
  key is recorded *before* the count is incremented, which is what makes the set safe to read the
  instant the count reaches the target - a counted record has already contributed its key. Covered
  by `go test -race`.
- **The broker log levels were already `WARN`** in `docker-compose.yml`, from the contract's own
  commit; nothing here touched them.

### The column ORDER is a judgement, and it is where eleven demos will drift

The contract fixes that the two columns exist and what they mean; it does not say where they go, and
eleven agents chose independently at the same moment. Go chose:

```
arm | records | keys | elapsed | msg/s | vs AK core
```

Reasoning, for whoever reconciles: the contract's own heading is "reports what it did, **not just**
how fast", so what it did precedes how fast; and `vs AK core` is derived from `msg/s`, so the three
speed columns stay adjacent rather than being split by two counters. **If the other ten chose
differently, Go is the one to move** - nothing here depends on the order but the format strings and
one test.

### `bin/ci-demo-conformance.sh` cannot parse this output yet

Not ours to fix (`bin/**` is outside this wave's ownership), and it is not a Go-specific problem -
it will be true of all eleven demos at once. Three of its regexes are written against the old shape:

- the `HEADER` line matches `arm ... elapsed ... msg/s ... vs AK core`, and `records` and `keys`
  now sit between `arm` and `elapsed`;
- the `ROW` line matches an arm name of `[A-Za-z][A-Za-z0-9 _-]*` followed directly by an elapsed
  figure - the parentheses in `AK core (franz-go)` fail the character class, and the two new
  integer columns fail the "elapsed comes next" anchor;
- `normalise_arms` maps `^ROW [a-z0-9]+-grpc$` to `ROW SIDECAR`, which no longer matches
  `go-grpc (this client)`.

The consequence is a *silent* one and worth naming: a demo whose skeleton comes out empty is
**SKIPPED, not failed** ("produced no recognisable fingerprint or table"), and a run in which every
language is skipped fails only on "no demo produced usable output". Whoever updates the script
should also make the new deterministic columns assertions in their own right - `records = 20` and
`keys = 20` for the standard invocation - since those two are the only figures in the table the
harness can hold every language to.

### Go has three serious Kafka clients: named in the README, one run

franz-go (pure Go, the one that runs), confluent-kafka-go (a cgo binding to librdkafka) and sarama
(pure Go). The contract asks for the disclosure and invites a second arm; the demo's README carries
the disclosure and does not add the arm.

- **confluent-kafka-go is ruled out on the container path**, not on preference. `demo/Dockerfile`
  builds with `CGO_ENABLED=0` precisely so the binary can be produced in a `golang` stage and run
  in the `eclipse-temurin` stage the spawned JVM sidecar needs. A cgo client makes that two
  runtimes or two images.
- **sarama is cheap and still not added**, and this is the part worth overturning later: the
  blocker is `bin/ci-demo-conformance.sh`, which requires every language's skeleton to be identical
  and exempts only Java for its extra arms. A third row in Go alone is permanent drift, so the
  harness would fail a demo for doing what the contract invited. **The right sequence is: give the
  harness a way to say "this language legitimately carries an extra arm", then add sarama.** Filed
  here rather than acted on because the harness is outside this wave's ownership.

### Two smaller things

- **The banner is the first line of the DEMO's output, not of the terminal, natively.** `run.sh`
  prints its mode line and its build progress first. That is build chatter rather than the demo
  introducing itself as something else, and duplicating the banner in the script would have meant
  printing it twice in one run. Recorded in case the integrator wants it in the script instead.
- **`keys` is `min(records, 1000)` in every language**, because all eleven seed `key-{n % 1000}` -
  checked across the other ten demos' broker files rather than assumed. That is what makes the
  column comparable, and it also means the column stops distinguishing anything above 1000 records:
  at the default 2000 both arms report 1,000, which is correct and says less than it looks like.

### What was actually run, and what it is worth

`demo/run.sh --records 20 --concurrency 4 --partitions 2 --replay-factor 2`, natively, exit 0. The
shape is right: banner, then the fingerprint, then two tables; both arms named with their client;
`records` 20 and `keys` 20 on the small replay and 40/40 on the big one, which is what the seeding
predicts; no bootstrap address and no latency anywhere.

**No throughput figure from this run means anything and none is quoted.** Eleven language agents
were building and running demos on this machine at once at load averages between 40 and 100, and
the serial arm took 10.3s for twenty records with a 2ms delay - about eighty times the work implied
by its own dial. The deterministic columns are exactly why that run is still evidence: they are the
half of the table that a loaded machine cannot distort.

**The first two attempts died on the environment, not on the demo**, and it is worth knowing the
signature. The broker container exited immediately with
`java.nio.file.FileSystemException: /var/lib/kafka/data/__cluster_metadata-0: No space left on
device` - the Docker VM's disk, exhausted by eleven agents' images and broker volumes at once. What
the demo *shows* is `the broker did not become ready within 60s`, followed by the broker's own log,
which is `run.sh` doing the right thing; the space message is only visible because it prints that
log. Several other languages' brokers (`pc-cpp-demo-broker-1`, `pc-rust-demo-broker-1`) were dying
the same way in the same minutes. It cleared on its own as other agents' runs finished. Nothing was
pruned: reclaiming 30GB of shared images would have cost the other ten agents a full rebuild.

**The conformance harness's blindness to the new shape was verified, not inferred.** The
`skeleton()` function was lifted verbatim out of `bin/ci-demo-conformance.sh` and run against a
sample of the new output: it emits every `DIAL` and the `TITLE`, and **no `HEADER` and no `ROW` at
all**. A skeleton with dials but no rows is non-empty, so the script does not even take its
"produced no recognisable fingerprint or table" skip - it compares dial lists between languages and
reports agreement, having silently stopped checking the arms. That is worse than the skip, and it is
the reason this is written down rather than left for CI to find.

### The container path ran too, and the same two columns came out identical

`demo/run.sh --docker` at the same dials, exit 0. Same banner, same arm labels, and - the point of
the exercise - **`records` 20 / `keys` 20 and 40/40, exactly as in the native run**, while the
speed columns disagreed wildly: the sidecar arm was 8.1x the serial arm natively and 0.5x it in the
container, on the same machine an hour apart. That is not a result about containers; it is eleven
agents' load, and it is precisely the reason the deterministic pair was added.

### VERIFIED: "The broker is quiet" does not hold, and it is not the compose file's fault

The container run's own log: **883 levelled lines from the broker against 42 from the demo** - 826
INFO, 56 TRACE, 1 WARN. The tables are buried exactly as the contract's commit says they must not
be, with `KAFKA_LOG4J_ROOT_LOGLEVEL: WARN` and `KAFKA_TOOLS_LOG4J_LOGLEVEL: WARN` correctly set.

The cause is in the image, read out of `confluentinc/cp-kafka:7.9.0` rather than guessed.
`/etc/confluent/docker/log4j.properties.template` renders the root logger from the environment
variable - and then unconditionally emits a hardcoded per-package block that **overrides it**:

```
{% set loggers = { 'kafka': 'INFO', 'kafka.controller': 'TRACE',
                   'state.change.logger': 'TRACE', 'kafka.log.LogCleaner': 'INFO', ... } %}
```

A per-logger level beats the root level in log4j, so `KAFKA_LOG4J_ROOT_LOGLEVEL` cannot silence any
package under `kafka`. The top emitters in the captured run line up exactly: `kafka.coordinator.group`
341 lines, `state.change.logger` 233, `kafka.log.UnifiedLog$` 92, `kafka.log.LogManager` 63,
`kafka.cluster.Partition` 60.

**The lever that would work is `KAFKA_LOG4J_LOGGERS`**, which the same template parses over those
defaults - something like
`KAFKA_LOG4J_LOGGERS: "kafka=WARN,kafka.controller=WARN,state.change.logger=WARN,kafka.log.LogCleaner=WARN"`.
Not applied here: this wave was told explicitly not to touch broker log levels, and the change
belongs in all eleven compose files at once rather than in Go's alone. `KAFKA_TOOLS_LOG4J_LOGLEVEL`
is harmless but redundant - the tools template already defaults to WARN.
