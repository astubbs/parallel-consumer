<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The shared conformance suite, and the runner contract every language implements

One Java test module drives every language's client through the same scenarios, in parallel, and asserts the
same things about each. This file is the contract the other side of that implements: what a **conformance
runner** is, what its command line looks like, what it prints, and how a new language registers itself.

It lives here rather than in
[`parallel-consumer-proxy/docs/client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md)
because that guide is owned by the protocol work and was being edited concurrently. The guide **owns the
scenario names and what each asserts** (its §7); this file owns the **runner mechanics**, and where the two
disagree about a scenario's meaning, the guide wins.

Tracking: astubbs#242, upstream confluentinc#154.

---

## 1. Why the assertions are in Java and the runners are dumb

A conformance runner **asserts nothing**. It connects, does what the scenario prescribes, and exits.
Everything about what "correct" means - offset frontiers, ordering, redelivery, attempt counts - stays in
`ConformanceScenarios.java`, on the JVM, in one place, for every language.

Three reasons, and they compound:

- **Ten definitions of correct is no definition at all.** If each client asserted for itself, two of them
  disagreeing would leave nobody able to say which was wrong. Written once, agreement between clients is
  evidence; written eleven times, it is coincidence.
- **A runner that could decide would decide in its own favour.** Not maliciously - by drift. The prescribed
  behaviour is a closed token set (§3) precisely so a runner cannot quietly do something adjacent to what was
  asked and still pass.
- **A sharpened assertion sharpens for everyone.** Tightening one scenario tightens it for eleven clients in
  one commit, with no per-language work at all.

The consequence for a client author is the good one: **adding a language is one runner and one registry
entry.** There is no test to port.

## 2. What this suite is for, and what belongs elsewhere

Three layers, each tested where it is cheapest and most precise. **Only the middle one belongs here.**

| Layer | Where it is tested | Examples |
|---|---|---|
| Engine internals | The JVM's own unit and integration suites | The offset-map codec, shard selection, the commit machinery, queueing |
| **The client/sidecar contract** | **This suite** | Handshake and capability negotiation, dispatch, per-record outcomes, redelivery with attempt counts, token echo, the client dispatch queue, close semantics |
| Language-idiom hazards | Each client's own tests | Ruby's `SizedQueue` blocking the transport thread, Kotlin's `CancellationException` becoming a failure, TypeScript's floating promises, Rust blocking inside an executor |

**The filter, when you are deciding whether a scenario belongs here:**

> **Could a conforming client fail this scenario while the engine is correct?**

If no - if the only way it goes red is an engine bug - it belongs in the JVM suite, not here. *"The offset
encoding round-trips"* fails the filter. *"A record failed once is redelivered with attempt 2 and the previous
reason verbatim"* passes it, because a client can absolutely get that wrong.

Do not re-test Parallel Consumer through seven language clients. It is already tested thoroughly, precisely
and cheaply from inside the JVM, and re-asserting it out here buys nothing while costing seven implementations
and seven maintenance burdens.

**The trap worth naming:** the harness makes deep engine state easy to reach, and it is tempting to assert it
*because* it is easy. Resist. Every such assertion makes the suite slower, more brittle, and more likely to go
red for a reason no client author can act on.

### This does not replace a client's own tests

The shared suite proves every client behaves **identically** on the protocol - that is exactly why it is the
most reliable evidence available, and exactly why it is blind to everything inside a client process. A
client's own suite is where its idiom-specific traps get pinned, and every example in the third row above is a
real defect found in this fan-out that no protocol scenario could have expressed. **Both layers are
load-bearing.** A language gains a conformance runner *beside* its own tests; it never trades them for one.

## 3. The runner contract

A conformance runner is a small standalone program in the client's own language, built from that language's
own module. It uses the client library exactly as an application would.

### Command line

```
<runner> --scenario <name> --behaviour <token> --sidecar <abs-path> --expect-dispatches <n> --timeout-seconds <n>
```

| Flag | Meaning |
|---|---|
| `--scenario <name>` | The scenario's stable kebab-case name. **It is also the topic name** - the client subscribes to it, and the harness seeds the scenario's records on the topic it is named after. |
| `--behaviour <token>` | What to do with each delivery. One of the closed set below; anything else is a usage error. |
| `--sidecar <abs-path>` | The sidecar command to spawn, absolute. Passed straight to the client library's spawn option; a runner never chooses its own fixture. |
| `--expect-dispatches <n>` | How many deliveries the scenario prescribes before the runner is finished. |
| `--timeout-seconds <n>` | The runner's whole wall-clock budget. |

All five are required. Every language spells them the same way, including the British `--behaviour`.

### Exit statuses - the verdict channel

| Status | Meaning |
|---|---|
| `0` | The prescribed behaviour completed. The suite's own assertions then decide whether the scenario passed. |
| `1` | The runner could not do what was prescribed: it failed to connect or handshake, or the budget elapsed first. |
| `2` | Usage: a missing flag, a non-absolute sidecar path, an unknown behaviour token. |

**There is no results file, no report message, and no per-language codegen for test data.** That was
considered and rejected: carrying test results over a wire is the whole wire problem again, multiplied by ten
languages, to say something an exit status already says. Everything the suite knows about engine state it
reads from the engine it is hosting, in its own JVM.

### stdout: one observation line per delivery

```
dispatch key=<key> offset=<n> attempt=<n> reason=<last-failure-reason>
```

- Printed at the moment of delivery, before the behaviour acts on it. Keys and reasons are UTF-8 text;
  `reason=` is **last** and takes the rest of the line, because it is worker-supplied and may contain spaces.
  `reason=` is empty on a first delivery.
- These are **observations, never verdicts**. The runner reports what arrived; the suite decides what it
  means. That is what keeps the assertions in one language while the runners stay dumb.
- Anything else on stdout is ignored, so a spawned sidecar's logging on the same stream is harmless. A line
  that *starts* with `dispatch ` but does not parse is a contract violation and fails loudly - silently
  dropping it would let a runner pass by printing nothing readable.
- Diagnostics go to **stderr**, which the suite captures and attaches to any failure message.

### Behaviour tokens (the closed set)

| Token | Prescribed behaviour |
|---|---|
| `succeed` | Report every delivery as a success, with no produced output. |
| `report-nothing` | Take the delivery and **never** report an outcome for it. Then hold the session open for the fixed hold below, and exit `0` without a clean close - a worker that vanished mid-record. |
| `fail-then-succeed` | On `attempt == 1`, report a failure whose reason is exactly `conformance-prescribed-failure`. On any later attempt, report success. |
| `hold-first-until-second` | Do not report the **first** delivery until a **second** one arrives; then succeed both and everything after. If no second arrives within the budget, fail that record and exit `1`. |

Adding a behaviour is a change to this table, to `RunnerBehaviour.java`, and to every runner - deliberately
expensive, because the alternative is runners inventing logic that makes an assertion mean nothing.

### Fixed literals every runner hard-codes

These are contract, not runner judgement. A runner free to pick its own would make the suite's convergence
budgets mean something different in each language.

| Constant | Value | Why |
|---|---|---|
| Failure reason | `conformance-prescribed-failure` | The suite asserts the redelivery carries it back **verbatim**, so it cannot be a message a runner composes. |
| Commit interval | `100ms` | Set on the session. The engine's production default is 5s; scenarios must converge at unit-test speed. |
| Retry delay | `50ms` | Same reason; the production default is 1s. |
| `report-nothing` hold | `3s` | See below - it is what makes the negative control a control. |
| Max concurrency | `--expect-dispatches` | So a scenario prescribing a **held** record cannot deadlock on an executor count smaller than its own shape. |

**The `report-nothing` hold is not cosmetic, and it was found the hard way.** Without it, the runner exits the
instant the record arrives - and a *sabotaged* runner that wrongly reported success has its report killed in
flight by the process exit. The suite then sees an unadvanced offset either way, and the negative control
passes for a broken client. Measured, not reasoned about: reporting success from this behaviour left the suite
**green** until the hold existed. Any new language must implement it.

**"Without a clean close" is about the RECORD, not about the shutdown**, and the five-language wave is what
forced the distinction. Go abandons its session because its workers are goroutines that die with the process;
Python's workers are separate *processes*, and a runner that abandoned them left a blocked interpreter behind
for every negative control it ran - holding the stdout the suite was still reading, so the transcript came back
empty from a runner that had printed perfectly well. Python therefore shuts its session down instead, with a
drain short enough that the held record is never reported. **What the contract requires is that no outcome for
that record ever reaches the engine, and that the session stays alive for the hold first.** How a language then
disposes of its own workers is its own business - and one that leaks a worker has failed a rule of its own, not
this one.

## 4. How a scenario prescribes behaviour

A scenario is one Java value with three halves, in `ConformanceScenarios.java`:

1. **What the engine seeds** - taken from the engine-side `HarnessScenario`, which already owns it.
2. **What the client is prescribed to do** - a `RunnerBehaviour` token plus an expected delivery count.
3. **What must then be true** - an assertion with the harness and the transcript in hand.

The runner receives (2) on its command line and nothing else. It cannot see (1) or (3), which is the point:
the prescription is complete enough to run, and too narrow to game.

**One definition, many bindings.** Those three halves are written once and executed once per *binding*: the
engine driven by a plain Java function (`CoreBinding`), the JVM clients driven as objects
(`JvmClientBindings` - see §6), then each foreign runner. The same assertion executing
many times is the goal; the same assertion being *written* many times is what `ConformanceBinding` exists to
prevent.

**Core is the control arm, and it is why it runs in every selection.** Every other binding puts a client, a
protocol and a language runtime between the scenario and the engine, so a red run has three suspects and the
client is always the first one looked at. A scenario red against a plain Java function is a **wrong scenario**
- there is nothing else left for it to be. It earned its place on the day it was written: the redelivery
scenario went red against core because the binding read the failure reason off the Throwable core recorded,
which is the wrapper's message rather than the user's - the same unwrap the engine's own `RecordCodec` does
before it puts the reason on the wire. That was a bug in the *binding*, found in seconds, that would have read
as "the client mangles the reason" in any language it appeared in.

Wired today, all four passing for every binding:

| Scenario | Behaviour | Deliveries | Asserted |
|---|---|---|---|
| `a-processed-record-advances-the-committed-offset` | `succeed` | 1 | One delivery, `attempt=1`, and the committed offset advances past it |
| `an-unreported-record-holds-back-the-commit` | `report-nothing` | 1 | The record **reached** a client (arrival sync), and the offset never advances past it |
| `a-failed-record-is-redelivered-with-its-failure-history` | `fail-then-succeed` | 2 | Redelivery of the same offset with `attempt=2` and the reason verbatim; then the offset advances |
| `records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently` | `hold-first-until-second` | 3 | While one record is held, the client accepts and runs a delivery on the **other** key, and the same key's next record does not arrive until the held one is reported |

The last one is a client test, not a shard-selection test: what it catches is a client whose admin loop
head-of-line-blocks, whose queue hands out wrongly, or which reports a record whose function has not returned.
Its instrument is the **hold**, not the transcript - removing the hold leaves every one of its assertions
still true, because the engine dispatches both shards in one wave regardless. The sabotage that proves it red
is a client that can only run one record at a time (a mutex around the whole processor - the shape of Ruby's
`SizedQueue` and Rust's blocking-in-an-executor defects): it deadlocks, exceeds its budget, and exits `1`.

## 5. How the mechanism works

```
   the suite's JVM                                    the runner's process
  ┌────────────────────────────────┐                 ┌────────────────────────┐
  │ ProxyHarness                   │                 │ conformance runner     │
  │  ├─ MockConsumer/MockProducer  │                 │  └─ the client library │
  │  ├─ the engine                 │◄──── gRPC ──────┤     (used as an app    │
  │  └─ a gRPC server on :ephemeral│                 │      would use it)     │
  │                                │                 └───────────┬────────────┘
  │  asserts: committed offset,    │   spawn (stdout: port,      │ spawns
  │  produced records, transcript  │   stdin: lifeline)          ▼
  └────────────────────────────────┘                 ┌────────────────────────┐
                                                     │ sidecar shim (4 lines) │
                                                     └────────────────────────┘
```

**The engine is in the suite's own JVM.** That is the only way to read engine state without inventing a
results protocol.

**The `--sidecar` the runner spawns is a four-line shim** that prints `port: <n>` and then holds its stdin as
the parent-death lifeline. That is the client libraries' real, specified spawn-and-reap path, exercised
rather than routed around. A `--port` flag would have been simpler for the suite and would have meant adding
a connect-to-an-existing-proxy option to eleven client APIs for a test's convenience - a surface decision that
belongs to the protocol specification and the authoring guide, not here. If that option is ever added for its
own reasons, this shim is the thing to delete.

**No Docker, no broker.** The whole suite runs in the ordinary unit-test lane in seconds.

**Against a real broker later**, only the assertion side moves: the committed offset comes from the Kafka
**Admin API** (`listConsumerGroupOffsets`) and produced records from a verification consumer, in place of the
mock consumer's commit history. Nothing in this contract changes with it.

### Parallel by construction

Every run owns its own engine, its own ephemeral loopback port, its own shim and its own child process, so
there is nothing for two of them to share. The matrix is concurrent by default
(`src/test/resources/junit-platform.properties`), and `LanguagesRunInParallelTest` proves it: two languages
driven at once, one passing and one deliberately broken, judged separately, with the overlap **measured** from
the test's own clock rather than assumed from a config file.

The first thing that broke when parallelism was switched on is worth knowing: the shim filename was keyed on
language and scenario, so two tests driving the same language through the same scenario overwrote each other's
shim and pointed both clients at one engine - the loser failed its handshake against the single-connection
guard. Shim filenames now carry the port.

### Absence must never read as agreement

A language whose toolchain is missing, whose runner fails to build, or whose binary is not where the registry
says, **fails**. It is never skipped. Of everything that can go wrong with a suite driving eleven languages,
a language quietly not running is the one most likely to survive to a release: nothing goes red, the run is
fast, and the report says every scenario passed.

Two negative controls hold the suite to this (`AbsentAndBrokenRunnersFailTest`): a runner that is not there,
and a runner that is there and exits non-zero. Both must fail with a message naming what was wrong.

The one sanctioned way to run fewer languages is explicit and visible on the command line:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=go
```

A name that is not registered fails rather than selecting nothing, because a typo that ran nothing would read
as a pass. **The property is singular and its name is fixed** - the `clients` workflow's per-language matrix
rows are written against it, and the earlier plural spelling is rejected outright rather than ignored, since
ignoring it would select every binding and fail in a row that installed one toolchain.

**The core binding is in every selection** and cannot be selected away. Naming a language runs that language
*and* the engine beside it, because "is this scenario wrong?" is an answer worth having in the same job as
the client that went red rather than hours later in another one. `-Dpc.conformance.language=core` is the way
to run the control arm alone - no toolchain, a few seconds.

## 6. Adding the next language

1. **Write the runner** in the client's own module, using the client library as an application would. Go's
   lives at `parallel-consumer-proxy-client-go/cmd/conformance-runner/main.go` and is the reference. Implement
   §3 exactly: the five flags, the three exit statuses, the observation line, all four behaviour tokens, and
   the fixed literals - including the `report-nothing` hold.
2. **Add one registry entry** in `LanguageRunners.java`: the language's name, its module directory, the
   command that builds the runner, and where the binary lands. Copy whichever entry is closest in shape. The
   registry checks a path is *executable*, so an interpreted language keeps a two-line wrapper
   (`scripts/conformance-runner`) beside its runner rather than a registry entry that names an interpreter.
3. **Run it**: `./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=<lang>`.
4. **Prove each scenario can fail.** Make the runner do the wrong thing - report success where silence was
   prescribed, change the failure reason, stop holding - watch the suite go red with a message that names
   what was wrong, and revert. A scenario that cannot fail is worthless, and this repository has seven
   recorded instances of checks that passed without ever having run.
5. **Record the evidence** in `docs/data/testing-evidence.d/<artifact>.yaml`, saying what is covered and what
   is not.

Nothing else in this module changes. The scenarios, the assertions and the driver are already language-blind -
the only thing the driver knows about Go is a path in a registry entry.

### A JVM client is a binding, not a subprocess - and Kotlin is the exception that shows why

Three clients live on the JVM, and two of them are driven as **objects** rather than as child processes:
`java-direct` and `java-grpc` are registered in `JvmClientBindings.java`, not in `LanguageRunners.java`.
That is a reading of what a runner is *for* rather than an exemption from it. A runner exists to do three
things a test cannot do from inside this JVM: use the client library as an application would, cross a process
boundary, and exercise the library's own **sidecar spawn**. A JVM client needs no process boundary for the
first - the suite can hold the very object an application holds - and neither Java transport has a spawn to
exercise: `GrpcParallelConsumerClient` connects to a port it is given, by design, because spawning belongs to
the lifecycle unit. Wrapping one in a subprocess would have meant *writing* the spawn the library does not
have, and then testing that.

What is not relaxed is the prescription. `PrescribedRun.java` is the runner contract carried out in this JVM
- the four behaviour tokens, the fixed failure literal, one observation per delivery, an exit status as the
verdict - and it is the same code the control arm runs, so no assertion can be written to suit an in-process
binding. `java-direct` is the most interesting binding in the set for the same reason it is the least
ceremonious: its wire is a function call, so a scenario that passes for `java-grpc` and fails there is a
claim about the shared API rather than about a stream.

**Kotlin is a spawned runner**, and that is what keeps the spawn path covered: it owns a sidecar spawn
(`Sidecar.kt`), so its runner is a real child process like every other language's. Two things differ from an
interpreted language, and both live in one file each:

- **The executable is a wrapper that resolves a classpath**, because a JVM client's "binary" is a JVM plus a
  classpath. `scripts/conformance-runner` reads `target/conformance-classpath.txt`, written by the module's
  own `dependency:build-classpath` execution, and prefers `$JAVA_HOME/bin/java` - this repository's JDK 17 is
  installed by a version manager and deliberately off `PATH`.
- **Its registry entry carries no build command**, because its toolchain is the Maven build already running.
  The conformance module test-depends on the Kotlin module precisely so the reactor compiles the runner and
  writes its classpath file before a scenario starts; a nested `mvn` would rewrite the class directories of
  the JVM executing this suite, while it ran. The wrapper still fails loudly when the classpath file is
  absent, so a module nobody built does not read as one that passed.

## 7. Running it

```bash
# the suite, every binding, all wired scenarios
./mvnw test -pl :parallel-consumer-proxy-conformance -am

# one language, plus the core control arm that always runs beside it
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=go

# the control arm alone: the engine, a plain Java function, no toolchain, seconds
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=core

# the JVM clients, which need no foreign toolchain either - Kotlin's runner is built by this reactor
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=java-direct,java-grpc,kotlin
```

`-am` is required: the module's parents must be in the reactor, and the engine and harness are built from
source. It needs no Docker and no broker.
