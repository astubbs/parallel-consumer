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
worker death, terminal outcomes, the shutdown drain, the demo and its container, packaging, and the
rest of the conformance suite.

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
  `["dispatch"]` and the wave that implements a duty adds its token. This is deliberately the
  *opposite* of the Java reference client, which declares nothing at all - the first finding in
  [`docs/inflight/parked-proxy-review-findings.md`](../parked-proxy-review-findings.md), which
  predicts the lease-expiry redelivery loop that arms the moment `heartbeat` is granted. Go's
  surface makes the safe answer the structural one: the declared set is a constant naming what is
  implemented, so it cannot fall out of step by omission.
- **The token is echoed as the received message**, never rebuilt from parsed fields, so "opaque" is
  structural rather than a rule someone has to remember.
