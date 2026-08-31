<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The shared conformance suite: one definition of correct, and core as the control arm

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

One Java test module holds the scenarios every client is driven through and the assertions about
each, so that **"correct" is defined once for every language rather than once per language**. This
file is what the other side of that implements.

Tracking: astubbs#242, upstream confluentinc#154.

---

## 1. Why the assertions live here and the clients stay dumb

A client under conformance **asserts nothing**. It connects, does what the scenario prescribes, and
reports what happened. Everything about what correct means - offset frontiers, ordering,
redelivery, attempt counts, the in-flight ceiling - stays in `ConformanceScenarios.java`, on the
JVM, in one place.

Three reasons, and they compound:

- **Ten definitions of correct is no definition at all.** If each client asserted for itself, two of
  them disagreeing would leave nobody able to say which was wrong. Written once, agreement between
  clients is evidence; written eleven times, it is coincidence.
- **A client that could decide would decide in its own favour.** Not maliciously - by drift. The
  prescribed behaviour is a closed token set (§3) precisely so a client cannot quietly do something
  adjacent to what was asked and still pass.
- **A sharpened assertion sharpens for everyone.** Tightening one scenario tightens it for every
  client in one commit, with no per-client work at all.

The consequence for a client author is the good one: **adding a language is one runner and one
registry entry.** There is no test to port.

## 2. What is wired today, and what is not

| Binding | State | Why |
|---|---|---|
| `core` | **runs, in every selection** | Parallel Consumer driven by a plain Java function. The control arm - see §4 |
| `java-direct` | **runs** | The shared client API bound in-process to core, with no protocol underneath |
| `java-grpc` | **deferred** | Its wire needs a sidecar with an engine behind it, and the sidecar on `master` hosts none - it refuses every session `UNIMPLEMENTED`. `TheEngineArrivingMustBringTheGrpcBindingTest` fails the build if the engine arrives and this binding does not |
| the ten foreign clients | **deferred** | Their runners, the registry that locates them, the sidecar shim and the process driver are the next extraction out of astubbs#293 |

Nothing about a deferred binding is stubbed. A stand-in for the engine would make agreement between
bindings a statement about the stand-in, which is the one thing this suite may never be.

## 3. What this suite is for, and what belongs elsewhere

Three layers, each tested where it is cheapest and most precise. **Only the middle one belongs
here.**

| Layer | Where it is tested | Examples |
|---|---|---|
| Engine internals | The JVM's own unit and integration suites | The offset-map codec, shard selection, the commit machinery, queueing |
| **The client/sidecar contract** | **This suite** | Dispatch, per-record outcomes, redelivery with attempt counts, the in-flight ceiling, close semantics |
| Language-idiom hazards | Each client's own tests | A blocking queue on the transport thread, a swallowed cancellation, a floating promise |

**The filter, when you are deciding whether a scenario belongs here:**

> **Could a conforming client fail this scenario while the engine is correct?**

If no - if the only way it goes red is an engine bug - it belongs in the JVM suite, not here. *"The
offset encoding round-trips"* fails the filter. *"A record failed once is redelivered with attempt 2
and the previous reason verbatim"* passes it, because a client can absolutely get that wrong.

**The trap worth naming:** the harness makes deep engine state easy to reach, and it is tempting to
assert it *because* it is easy. Resist. Every such assertion makes the suite slower, more brittle,
and more likely to go red for a reason no client author can act on.

### This does not replace a client's own tests

The shared suite proves every client behaves **identically** on the protocol - that is exactly why
it is the most reliable evidence available, and exactly why it is blind to everything inside a
client process. A client gains a conformance binding *beside* its own tests; it never trades them
for one.

## 4. The prescription, and why core is one of the rows

A scenario is one Java value with three halves, in `ConformanceScenarios.java`:

1. **What the engine seeds** - a `HarnessScenario`, which owns the records and their keys.
2. **What the client is prescribed to do** - a `RunnerBehaviour` token plus an expected delivery
   count and an in-flight ceiling.
3. **What must then be true** - an assertion with the harness and the transcript in hand.

The client receives (2) and nothing else. It cannot see (1) or (3), which is the point: the
prescription is complete enough to run, and too narrow to game.

**One definition, many bindings.** Those three halves are written once and executed once per
*binding*. The same assertion executing many times is the goal; the same assertion being *written*
many times is what `ConformanceBinding` exists to prevent.

**Core is the control arm, and it is why it runs in every selection.** Every other binding puts a
client - and eventually a protocol and a language runtime - between the scenario and the engine, so
a red run has several suspects and the client is always the first one looked at. A scenario red
against a plain Java function is a **wrong scenario**; there is nothing else left for it to be. It
earned its place on the day it was written: the redelivery scenario went red against core because
the binding read the failure reason off the `Throwable` core recorded, which is the wrapper's
message rather than the user's - the same unwrap the engine's own serializer does before putting
the reason on the wire. That was a bug in the *binding*, found in seconds, that would have read as
"the client mangles the reason" in any language it appeared in.

Wired today, all five passing for both bindings:

| Scenario | Behaviour | Deliveries | Ceiling | Asserted |
|---|---|---|---|---|
| `a-processed-record-advances-the-committed-offset` | `succeed` | 1 | 1 | One delivery, `attempt=1`, and the committed offset advances past it |
| `an-unreported-record-holds-back-the-commit` | `report-nothing` | 1 | 1 | The record **reached** a client (arrival sync), and the offset never advances past it |
| `a-failed-record-is-redelivered-with-its-failure-history` | `fail-then-succeed` | 2 | 2 | Redelivery of the same offset with `attempt=2` and the reason verbatim; then the offset advances |
| `records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently` | `hold-first-until-second` | 3 | 3 | While one record is held, the client accepts and runs a delivery on the **other** key, and the same key's next record does not arrive until the held one is reported |
| `the-in-flight-ceiling-bounds-unresolved-records` | `hold-until-ceiling-full` | 6 | **2** | Six records on six distinct keys, and the client held **exactly** two unresolved at any instant - never three - while every one of them was eventually delivered and committed |

The fourth is a client test, not a shard-selection test: what it catches is a client whose admin
loop head-of-line-blocks, whose queue hands out wrongly, or which reports a record whose function
has not returned. Its instrument is the **hold**, not the transcript - removing the hold leaves
every one of its assertions still true, because the engine dispatches both shards in one wave
regardless.

**The fifth is the only one that constrains how many records may be outstanding at once**, and it is
the first scenario whose ceiling is smaller than its own record count. The keys are distinct so that
key ordering is not what limits concurrency: seeded on one key, a small observed concurrency would
prove only that the shard serialized them, and the scenario would pass for a client respecting no
ceiling at all. Both halves of its assertion are load-bearing - **at most the ceiling** is the
product's claim, and **every record eventually delivered** is what stops a client satisfying it by
never asking for work.

**What it deliberately cannot see, so nobody reads more into a green run.** A client bounding its
*queue* rather than its *unresolved* records is invisible from out here while the engine is correct,
because a correct engine never over-dispatches and so never offers the record that would expose it.
What this catches is the consequence reaching the engine: a ceiling the client mis-declares, an
executor pool wider than the ceiling, or a record resolved before its function returned. The
counting itself stays a matter for each client's own tests.

## 5. The observation channel

Every binding produces the same two lines per record, whether it prints them to stdout as a
subprocess or appends them in this JVM:

```
dispatch key=<key> offset=<n> attempt=<n> reason=<last-failure-reason>
settled  key=<key> offset=<n> attempt=<n> reason=<reason-this-client-reported>
```

- The `dispatch` line is recorded at the moment of delivery, **before** the behaviour acts on it.
  The `settled` line is recorded the moment the behaviour has decided that record's outcome, which
  is when the record stops being unresolved. `reason=` is **last** and takes the rest of the line,
  because it is worker-supplied and may contain spaces.
- **`report-nothing` produces no `settled` line, ever.** By prescription it never resolves its
  record, and the absence is the observation.
- These are **observations, never verdicts.** The client reports what happened; the suite decides
  what it means.
- **The pair is how the suite sees overlap, and no clock is involved.** A dispatch opens a record's
  unresolved window and its settled line closes it, so the running difference between the two
  counts, read in line order, is how many records the client was holding at that instant - which is
  what the in-flight ceiling bounds. `TranscriptOverlapTest` pins that computation on its own.

The verdict channel is an exit status: `0` the prescription completed, `1` it could not be carried
out, `2` the caller asked for something the contract does not define. There is deliberately no
results file and no report message - carrying test results over a wire is the whole wire problem
again, to say something an exit status already says. Everything the suite knows about engine state
it reads from the engine it is hosting, in its own JVM.

`RunnerContract.java` is the same contract in the form the suite can hold itself to, so a drift
between it and this file shows up as a failing scenario rather than as documentation nobody re-read.

## 6. Absence must never read as agreement

A binding that cannot run **fails**. It is never skipped. Of everything that can go wrong with a
suite driving many clients, a client quietly not running is the one most likely to survive to a
release: nothing goes red, the run is fast, and the report says every scenario passed.

The one sanctioned way to run fewer bindings is explicit and visible on the command line:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=java-direct
```

A name that is not registered **fails** rather than selecting nothing, because a typo that ran
nothing would read as a pass - `SelectorMatchingNothingFailsTest` is the negative control on that,
and it includes the deferred `java-grpc` name so a row naming it cannot report green having run only
the control arm.

**The core binding is in every selection** and cannot be selected away. Naming a client runs that
client *and* the engine beside it, because "is this scenario wrong?" is an answer worth having in
the same job as the client that went red rather than hours later in another one.
`-Dpc.conformance.language=core` is the way to run the control arm alone.

## 7. Running it

```bash
# the suite, every wired binding, all wired scenarios
./mvnw test -pl :parallel-consumer-proxy-conformance -am

# the control arm alone: the engine, a plain Java function, seconds
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=core
```

`-am` is required: the module's parents must be in the reactor, and core and the client are built
from source. It needs no Docker and no broker - the harness drives Parallel Consumer over mock Kafka
clients, so the whole suite runs in the ordinary unit-test lane in seconds.

**Against a real broker later**, only the assertion side moves: the committed offset comes from the
Kafka **Admin API** (`listConsumerGroupOffsets`) and produced records from a verification consumer,
in place of the mock consumer's commit history. Nothing on the client side of the contract changes
with it.
