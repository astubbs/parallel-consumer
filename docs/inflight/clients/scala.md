# Client: Scala (astubbs#242)

Per-language working note for the Scala client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything this wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: the first wave landed, wrapping `java-grpc`.** Connect, `Configure`, a `Dispatch` wave, the
user's function, the report with the token echoed verbatim, and a clean client-initiated shutdown -
proven by two end-to-end tests against the real test-mode sidecar. The module is at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-scala/`; its maturity and
testing-evidence deferrals are lifted and it has a CI row. **The demo and its container landed on
`demos/scala`** - see "The demo" below. Later waves: leases and heartbeats, the manifest reconnect,
worker death, terminal outcomes, the `Shutdown` drain, and publishing - **most of which this client
inherits rather than implements.**

## The decisions this wave was left to make

- **Scala 2.13, not 3.x.** Decided on who the client is for. Kafka-adjacent Scala still runs 2.13 -
  `kafka-streams-scala` publishes `_2.13`, Spark's Scala API is 2.12/2.13, Flink's is 2.12 - and a
  Scala 3 application can depend on a 2.13 library while a 2.13 application cannot depend on a Scala
  3 one. So 2.13 serves both populations and 3.x serves one. The cost is named rather than hidden:
  no Scala 3 syntax and no `enum`, neither of which anything in this surface wants. No cross-build,
  deliberately - two artefacts to test, publish and keep in step is real recurring work for an
  experimental client that already reaches Scala 3 users.
- **`scala.concurrent.Future`, not cats-effect `IO` or ZIO `Task`.** Both wrap a `Future` in one call
  (`IO.fromFuture`, `ZIO.fromFuture`), so a `Future` surface excludes neither; either alternative
  excludes the other's users and adds a large dependency to a client whose argument is thinness. It
  is also what the standard library already bridges to the transport's `CompletionStage`
  (`scala.jdk.FutureConverters`), so the translation costs nothing.
- **`poll` returns `Future[Unit]` for the session, and that future *is* the transport's
  `sessionEnd()`.** Guide §1 leaves the shape to each language and makes the property normative: the
  caller learns that the session ended and why, without closing the client. **This is the one place
  Scala went further than Kotlin rather than mirroring it** - Kotlin's `poll` still returns only on
  `close` or cancellation, because nothing there joins its own `ended` to the transport's. Here they
  are the same future by construction, so there is no wiring left to forget.
- **JUnit 5 and Truth rather than ScalaTest or MUnit.** The whole repository asserts this way, the
  surefire lane already runs it, and a second test framework in one module would be a second thing to
  keep working. The one Scala-specific cost is `ScalaTruth`, a small forwarder: Scala's overload
  resolution takes a `scala.Boolean` to Truth's `Object` overload rather than boxing it, so
  `assertThat(x).isFalse()` does not compile without it.

## What is inherited from `java-grpc`, and what is implemented here

Checked against the transport's source rather than assumed, because "inherited" is exactly the kind
of claim that is comfortable and wrong.

**Inherited** (no code in this module): the single bidirectional stream and the `Configure`-first
handshake; the `dispatch` capability declaration (`WireMapping.toConfigure`); the dispatch queue
whose depth is the negotiated `max_concurrency`, with overflow failing the stream as a protocol
violation naming the count; FIFO hand-out; the executor count from `Configured`, never revised; the
verbatim token echo with no per-record state; the shutdown order (stop hand-out, let executing
records report, then half-close); and the session end with its cause.

**Implemented here**: the Scala spelling of the surface (`Bridge.scala` is the whole of it), the
`Future`-to-`CompletionStage` bridge, the "no verdict" primitive, and **the sidecar spawn**
(`Sidecar.scala`). The spawn should not be here - it belongs to the Java lifecycle unit, and until
that lands every JVM client writes it again; Kotlin's `Sidecar.kt` is the same file in another
language, and deleting both is a later wave's easy win.

## The conformance runner exists; the registry entry does not

`scripts/conformance-runner` implements the contract in the conformance module's README: the five
flags, the three exit statuses, one observation line per delivery, all four behaviour tokens, the
fixed literals and the 3-second `report-nothing` hold.

**It is not registered in `LanguageRunners.java`, so no scenario has been proven red by the shared
suite.** That file was being edited by other waves concurrently and a lost update there is silent, so
this wave reports what it needs rather than landing it. **Two edits, and the second is not optional:**

```java
/** Scala: a JVM client, so its "binary" is the wrapper over `java` and a classpath the build wrote. */
public static LanguageRunner scala() {
    var module = module("scala");
    return new LanguageRunner("scala", module, List.of(),
            module.resolve("scripts").resolve("conformance-runner"));
}
```

...plus `scala()` in `all()`; and **a test-scope dependency on `parallel-consumer-proxy-client-scala`
in the conformance module's pom**, which is what puts this module in that module's reactor so
`-am` compiles the runner and writes its classpath file before a scenario starts.

**The empty build command is the point, and it is the JVM convention the Kotlin runner arrived at
rather than this wave's invention**: every foreign language's entry shells out to its own toolchain,
but a JVM client's toolchain *is* the Maven build already running, and a nested `mvn` would rewrite
the class directories of the very JVM executing the suite. This module's
`conformance-classpath` execution therefore sits in the default build rather than in a profile, so an
ordinary `test-compile` leaves the wrapper runnable.

Whoever lands it should then run
`./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=scala` and prove
each scenario red, which this wave could not.

What *was* proven, by hand, against the same test-mode sidecar the suite spawns: all four behaviours
produce the right observation lines and exit `0` (`fail-then-succeed` shows `attempt=2` carrying
`conformance-prescribed-failure` back verbatim; `report-nothing` holds ~3s and reports nothing;
`hold-first-until-second` releases on the second delivery without deadlocking), a prescription that
cannot be carried out exits `1`, a missing sidecar exits `1`, and every usage error exits `2`.

**The registry wants an executable path, not a classpath**, which for a JVM client means the wrapper
every interpreted language already has plus a resolved classpath - `dependency:build-classpath`
writes it into `target/conformance-classpath.txt`, and the wrapper fails loudly naming the build
command when it is absent.

## An end-to-end test that could not detect the defect it was named for

Worth reading before writing the next client's tests, and **it is a live finding for the Kotlin
client, whose end-to-end test is the same shape**.

The obvious first test - run the `a-processed-record-advances-the-committed-offset` scenario, report
success, assert no redelivery follows - **passes against a client that reports nothing at all**.
Measured, not reasoned about: a client sabotaged to hand every record an uncompleted stage left it
green (25 seconds instead of 5, which is the drain budget, and passed). The engine holds an unreported
record rather than redelivering it while the session lives, so silence is what a correct client and a
mute one both produce.

The instrument that works is a **redelivery**: it can only happen because a report arrived, was
applied, and moved the attempt count. So this module runs
`a-failed-record-is-redelivered-with-its-failure-history` as a second end-to-end test, and that is
the one that goes red. `OneRecordThroughTheSidecarTest` records the measurement at the point of use.

Kotlin's `OneRecordThroughTheSidecarTest` asserts the same silence and therefore has the same blind
spot; its owner should add the redelivery scenario. This note is the pointer - the Kotlin note is not
this wave's to edit.

## Static analysis: the compiler

`scripts/analyse.sh`, which is what the CI row runs. The flags are `-Xlint -Wunused -Werror` (plus
`-deprecation -feature`), declared once in the module's pom so there is no local/CI skew to guard
against - unlike detekt, a compiler flag cannot be passed from a workflow without the build agreeing
to it.

scapegoat and WartRemover were rejected on the "only if mature, and it must run locally" filter every
other client applied: both are compiler plugins that must be published for the exact Scala patch
version in use, so gating on one makes a Scala bump wait on someone else's release. Scalafix is a
rewriter whose useful rule set here is mostly formatting.

**Proven able to fail**: an unused private method and an unused import each turned the build red, and
removing them turned it green. `-Wvalue-discard` is on for main sources and off for tests, where
discarding a value is often the assertion.

## For whoever owns the CI row

This wave added the row itself, which is the one exception to `clients.yml`'s "this file has one
writer and it already ran": the module was seeded *after* those rows were written, so there was no
row to un-defer. Two facts the row's owner may want:

- The row's command (`test -pl :<module> -am -Dpc.foreignClients`) **needs the harness-lane stanza in
  this module's pom** - the same `ServiceConfigurationError: Provider ... MyRunListener not found`
  the Kotlin wave hit, for the same reason. The `scala-e2e-harness` profile carries it.
- `scripts/analyse.sh` **`cd`s to the repo root** before running Maven rather than passing
  `-f <root pom>` alone: the root project's `asciidoc-template` step resolves
  `src/docs/README_TEMPLATE.adoc` against the *process's* working directory, so a build started from
  a module directory fails on a file that never moved. The row runs `scanner-cmd` with the module as
  its working directory, so any JVM client whose recipe shells out to Maven hits this.

## The demo

`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-scala/demo/` - `run.sh`, `Dockerfile`,
`docker-compose.yml`, a `README.md` recording what is Scala's own, a `logback.xml`, and the sources
under `demo/src/main/scala`. It keeps the contract in `parallel-consumer-proxy/demo/README.md`
exactly: the seven flags with the same defaults, the same `PC_DEMO_*` variables with flags beating
environment beating defaults, the banner first and the effective-configuration fingerprint straight
after it, never carrying the bootstrap address, the two tables in the same order with the same
columns, and no latency anywhere.

**Two arms - `AK core (KafkaConsumer)` and `scala-grpc (this client)` - and the four extra ones the
Java seed carries are absent on purpose.** Scala is a JVM language, so it *could* run `pc-core`, `java-direct` and the rest against
the same broker in the same process, which is exactly why the temptation had to be named and
declined: the contract's value is that a reader who has run one language's demo has run them all, and
a six-row Scala table beside a two-row Ruby table would not be that. `scala-grpc` goes through
`ParallelConsumerClient`, not the wire - nothing in the demo's sources names a protobuf message, a
channel or a token.

### The demo is behind a Maven profile, for the reason the harness lane is

The sidecar arm has to hand its child process a classpath carrying `parallel-consumer-proxy`.
Declared unconditionally that is a permanent reactor edge to the engine, and `bin/build.sh` opens
with `clean` - which would delete the sidecar jar every other language's conformance test spawns. So
the demo's sources are a **test source root added only by the `scala-demo` profile**
(`-Dpc.scalaDemo`, passed by `run.sh` and by the Dockerfile), and its dependencies live there too.

**Verified with a control arm rather than asserted**, because "the edge is only in the profile" is
exactly the kind of claim that is comfortable and wrong. One term changed, everything else identical,
and the reactor listing is what flips:

| command | reactor | `parallel-consumer-proxy` |
|---|---|---|
| `./mvnw -pl :parallel-consumer-proxy-client-scala -am validate` | 8 modules | **absent** |
| the same, plus `-Dpc.scalaDemo` | 9 modules | present, as "Language Proxy" |

Read the reactor listing, not the build result: **that command currently fails on this machine for an
unrelated reason**, and a reader who greps for `BUILD SUCCESS` will think the check is broken.
`validate` produces no artifacts, so in a reactor whose snapshots are not installed locally
`parallel-consumer-proxy-client-java-grpc` cannot resolve `parallel-consumer-proxy-client-java-api`
and Maven has a cached Central miss for it. Confirmed to have nothing to do with Scala: the identical
failure reproduces on `./mvnw -pl :parallel-consumer-proxy-client-java-grpc -am validate`, which
never mentions this module. `-U`, or any lane that reaches `package`, does not have it.

This is the third thing in this module arranged that way, after `scala-e2e-harness` and the
conformance runner's classpath. Any JVM client whose demo spawns a real sidecar will need the same
shape, and it is worth stating once rather than each wave rediscovering it.

### Three findings that are not Scala's, recorded rather than fixed

The first two apply to **every JVM demo, the Java seed included**, and neither is this branch's to
edit.

- **With no logback configuration on the classpath, logback's fallback is root at `DEBUG`.** A
  fifty-record run of this demo emitted over four thousand lines of Netty frame dumps, docker-java
  HTTP headers and Kafka client configuration, with the two tables buried in the middle. Measured on
  the first run of this demo, before `demo/logback.xml` existed. The Java seed's demo module carries
  no logging configuration either, so it will do the same; this branch fixed only its own, by pointing
  `-Dlogback.configurationFile` at a file in `demo/` rather than shipping a resource, because
  `target/test-classes` is also what `scripts/conformance-runner` runs from and the demo's logging
  preferences must not silently become the shared suite's.
- **Every Kafka client logs its full effective configuration at `INFO` when constructed, and
  `bootstrap.servers` is in it.** The contract's rule that a demo never prints the bootstrap address
  exists because own-cluster mode puts a user's real broker there - and the demo's own fingerprint
  honours it, while the client's dump prints it anyway, several times a run. `org.apache.kafka` is at
  `WARN` in this demo's logging configuration for that reason and not for noise. **Whoever owns the
  Java seed should check the same thing.** *Settled since:* the contract now says the rule binds the
  whole run rather than only the fingerprint block, and `bin/ci-demo-conformance.sh` enforces it by
  grepping every line the demo prints. So this is closed as a contract question and stays open only
  as something each language's logging configuration has to keep honouring.
- **`SidecarCommand` requires an absolute path to an executable, and a JVM sidecar is a jar**, so
  every JVM caller writes "this JVM's `java` plus a `-cp` argument" again. That is now in three places
  in this module's orbit: `OneRecordThroughTheSidecarTest`, the Java seed's `SidecarProcess`, and this
  demo. It reinforces the note above that spawning belongs to the Java lifecycle unit rather than to
  each client.

### The reader-experience wave: banner, arm labels, and the two deterministic columns

Applied from the rewritten contract, after someone watched a demo run and found it unimpressive.
Four things changed in this module, and two of them cost a decision worth recording.

- **The banner is printed by the demo, not by `run.sh`.** It has to appear on the container path too,
  where `run.sh` is not in the picture, so the application owns it - and printing it in both places
  would show it twice on the native path. `println` rather than the logger: a timestamp and a logger
  name in front of the rule line would put a configuration line first again, which is the exact
  failure the banner exists to fix.
- **Which forced the launcher's own output to standard error.** `Mode: native - a JDK toolchain is
  present` is a configuration line, and it was the first thing on standard output. So was the Maven
  build's: even at `-q` it prints the copyright check's summary and one `Jabel: initialized` per
  compiled module, and on the first run of this change **four `Jabel` lines and two copyright lines
  sat above the banner**. Both now go to standard error. Nothing is suppressed - a failing build still
  explains itself and `set -e` still stops the script - and standard output now opens with the banner
  on both paths. Any JVM demo whose `run.sh` shells out to Maven has this, because the build's
  chatter is not the demo's output.
- **The fingerprint moved ahead of the broker.** It used to be the first thing `runFor` did, which put
  it *after* `DemoBroker.resolve` had pulled and started a Kafka image - so a reader waited the better
  part of a minute before learning what settings the numbers were produced with. `main` now prints it
  before resolving, which is also the contract's stated order: banner, configuration, run.
- **Arm labels name the client**, so `AK core (KafkaConsumer)` and `scala-grpc (this client)`. The
  group ids are unaffected: they were always built from separate literals rather than from these
  constants, which is lucky rather than designed - a label carrying spaces and parentheses into a
  `group.id` would have been an unpleasant way to find that out.

**Scala's answer to "does this language have more than one serious client" is unusual and is recorded
in the demo's README**, as the contract asks. Alpakka/Pekko Kafka, FS2 Kafka, ZIO Kafka and
`kafka-streams-scala` are what a Scala team actually uses, and every one of them **wraps
`org.apache.kafka.clients.consumer.KafkaConsumer`** rather than speaking the protocol. So unlike Go or
Ruby there is no second independent client to run as a second arm - a second row would measure a
streaming façade's own scheduling on top of the same consumer, which is a different question.

#### The column order was chosen without coordination, and the integrator has to reconcile it

Eleven agents applied this contract at once, in eleven worktrees, and the contract fixes *which*
columns exist without fixing where the two new ones go. This module chose:

```
arm | records | keys | elapsed | msg/s | vs AK core
```

`records` and `keys` come immediately after the arm because they are what shows the work happened,
and the contract's own argument for them is that the table should demonstrate the run rather than
assert it. **If another language put them at the end, one of the two has to move** - the contract's
"same columns, same order" is the binding clause and neither choice is more correct.

**`bin/ci-demo-conformance.sh` cannot read the new table, and it does not fail - it goes quiet.**
That file is not this branch's to edit, and this was measured rather than reasoned about: its
`skeleton` function was run verbatim over this demo's captured output, and what came back was six
`DIAL` lines and two `TITLE` lines and **nothing else**. No `HEADER`, no `ROW`.

That is the dangerous shape. The skeleton is not empty, so the script's own
`[ ! -s "$lang.skel" ]` guard does not skip the language; the absolute assertions all still pass;
and the drift check happily compares eleven skeletons that no longer say anything about the tables.
**A green run would mean the table columns and the arm order had stopped being checked at all** -
which is precisely the "narrowed run that reads like a full one" the script's own header says it
exists to prevent.

Two of its `awk` patterns were written against the old shape:

- its `HEADER` pattern matches `arm elapsed msg/s vs AK core` as a prefix, which no longer describes
  the header row under this column order. Note that a language which *appended* the two columns
  instead would still match it - so the header line alone cannot be relied on to reveal the drift;
- its `ROW` pattern is `^ <name> <elapsed>s <rate> <ratio>$` with the name constrained to
  `[A-Za-z][A-Za-z0-9 _-]*`. **The arm labels alone break it** - `AK core (KafkaConsumer)` contains
  parentheses - and it is anchored at the end, so trailing columns break it too. Its
  `normalise_arms` sed, `^ROW [a-z0-9]+-grpc$`, does not match `scala-grpc (this client)` either.

None of that is fixable from a language branch without eleven conflicting edits to one shared file.
Whoever integrates the fan-out should update those three patterns once, against whatever column order
is settled on, and then re-run the drift check - which is the point at which any disagreement between
the eleven becomes visible rather than theoretical. **Worth adding while they are in there: a
`ROW`-count or `HEADER`-present assertion**, so that a skeleton which has quietly stopped recognising
the tables fails instead of comparing clean.

#### One blemish observed and deliberately not fixed here

The big replay's heading computes the serial arm's cost as `total * delayMs / 1000` in integer
seconds, so at the conformance harness's own volume it reads **"AK core is serial and would take
0s+"** - which is both untrue and the sort of thing that makes a demo look unfinished. It is
pre-existing, it is in the shared table title rather than in anything this wave touched, and the
wording is almost certainly identical in the other ten languages. Changing it unilaterally would
introduce exactly the cross-language prose drift the contract exists to prevent, so it is reported
rather than fixed.

### What was run, and what was not

Run natively on macOS, under heavy concurrent load from the parallel client fan-out - load average
above 100 on twelve cores at the time of the second run - so **no throughput figure from these runs
means anything and none is recorded here**. What they prove is that the machinery works:

- `demo/run.sh --records 20 --replay-factor 2 --partitions 2 --concurrency 8` - both arms completed,
  both tables rendered, exit 0, 59 lines of output end to end. The `--replay-factor 2` was chosen over
  1 deliberately: at 1 the big replay is skipped, and the second table's rendering path would never
  have executed at all.
- `demo/run.sh` with **no arguments at all**, configured entirely through `PC_DEMO_RECORDS`,
  `PC_DEMO_REPLAY_FACTOR`, `PC_DEMO_PARTITIONS` and `PC_DEMO_CONCURRENCY`. This is the case that has
  broken before - `bash 3.2` under `set -u` treats an empty array expansion as an unbound variable -
  and it proves the environment layer at the same time.
- `--help` and a misspelled flag both reach the usage text; the misspelled flag exits 2.

Re-run twice after the reader-experience wave, as
`demo/run.sh --records 20 --concurrency 4 --partitions 2 --replay-factor 2` - the flags
`bin/ci-demo-conformance.sh` uses. Exit 0 both times, both tables rendered, and:

- standard output opens with the banner, with nothing above it;
- both arms report **20 records over 20 keys** in the small replay and the sidecar arm **40 over 40**
  in the big one, identical across the two runs while every timing moved - which is the determinism
  claim actually observed rather than asserted;
- the two absolute assertions the conformance harness makes hold on the captured output: no
  `bootstrap.servers` anywhere in the demo's own lines, and no latency, percentile, `p95` or `p99`.

**The container path was not run**, and that is the one gap. `Dockerfile` and `docker-compose.yml` are
written and mirror the Java seed's, including the two rules that are not negotiable - the broker is a
compose sibling and the host Docker socket is never mounted, and the sidecar is a child process
rather than a compose service - but building the image means building the whole reactor inside it,
and the machine was running ten agents. The contract is explicit that "a demo with one tested entry
point has an untested entry point", so **this is unproven, not proven-elsewhere.**

**It is also not wired into `bin/ci-demo-test.sh`**, which runs the Java demo through both entry
points on every pull request. That file is shared across the fan-out and this branch does not own it;
whoever integrates the eleven demos should add the Scala row, and the container path should be
considered untested until that row has run once.
