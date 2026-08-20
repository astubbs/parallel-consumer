# Branch: the classic comparison demo, rescued and ported per language

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

Branch `feats/classic-vertx-demo`, stacked on `feats/proxy-requirements` (astubbs#293, astubbs#242),
because the per-language demos need the proxy client modules and those exist only there. The rescue
half needs nothing from that branch; the port half needs all of it.

**This file is the session ledger for the design conversation.** It is written up front, before the
work, because the decisions below were made in conversation and would otherwise live only in a chat
history. Every decision records who made it. Open questions stay open in writing rather than being
silently resolved by whoever picks this up.

## Say "AK core", never bare "core" (owner, 2026-08-21)

Both senses are already in this file, one decision apart, meaning opposite things - so the word is
settled here and recorded in [`CONCEPTS.md`](../../CONCEPTS.md):

- **AK core** - the Apache Kafka client, `KafkaConsumer`. The serial arm. Decision 8 calls it
  *vanilla/native*; decision 4 calls it *the native client* for Java.
- **`parallel-consumer-core`** - the module, as in decision 10's "build the core/sleep one".

`ComparisonDemo` names the lane `AK_CORE`. Bare "core" means neither.

## Why this exists at all

The driver is a **v6 experimental release of the proxy clients**. They are close to ready; what is
missing is the owner's own confidence that they work outside the test suite. The demo is the
confidence instrument, and only secondarily the marketing artifact:

- **(a) Prove to ourselves they work** - watch a client run against a real broker, outside a test.
- **(b) Show new users they work** - in the environment they actually use.

(a) is the gating one for v6 and it is the cheaper of the two. It is also the one blocked on the
sidecar entry point.

## The artifact being reproduced

The project's most-linked image is the asciinema cast at <https://asciinema.org/a/404299>, embedded
in `src/docs/README_TEMPLATE.adoc`. The code behind it is `Demo.java`, at
`parallel-consumer-vertx/src/test-integration/java/.../vertx/integrationTests/Demo.java`, on branch
`origin/presentation` at `ffda9c6a3` (2021-05-05).

**It has never been on master.** Not deleted - never merged. `origin/presentation` is 3 ahead / 631
behind and untouched since 2021. Nobody can reproduce that cast today.

What it actually does, from reading the source and the cast rather than from memory of it:

- WireMock (Jetty) is the **server**; `pc.vertxHttpReqInfo(...)` - PC's non-blocking Vert.x HTTP
  client - is the PC arm. The vanilla arm uses a blocking `simplehttp` Apache client, one GET per
  record, serial. Vert.x is the client, not the server; getting this backwards inverts the whole
  explanation of the result.
- `simulatedDelayMs = 2`, slept inside WireMock's request listener. This is the delay knob, already
  present.
- 5,000 records, then a 350,000 backlog for the long PC run. `maxConcurrency = 100`. Ordering
  `UNORDERED`. Keys all unique, so keys are inert.
- Vanilla ~333 msg/s; PC ~27,201 msg/s.
- Runs the arms **sequentially**, and via `mvn exec:java -Dexec.classpathScope=test` on a class that
  is a JUnit test with a `main` bolted on. Extends `BrokerIntegrationTest`, so a Testcontainers
  broker.

Helpers all still exist on master under the renamed packages - `ProgressBarUtils`, `RateLimiter`,
`BrokerIntegrationTest` - and `me.tongfei:progressbar` is in root `dependencyManagement`. The rescue
is a port, not a rewrite.

### The arithmetic that decides the new demo's design

353,625 records in 13s = 27,201 msg/s. At the 2ms service time that is **54 records in flight**;
attributing 3ms per record for HTTP overhead it is **82**. `maxConcurrency` was **100**.

**PC was running at its configured ceiling, not at a threading limit.** 100 threads each sleeping
2ms delivers the same number. Vert.x was never load-bearing for this workload - it only starts to
matter above roughly a thousand concurrent, where a thread per in-flight record stops being free.

This is why the new demo can drop both the HTTP server and Vert.x and still reproduce the headline.
Recorded here because it is the single fact the new design rests on, and it is cheap to re-derive
wrongly.

## Decisions (owner, in conversation, 2026-08-20)

1. **Scope is one demo per language, not the eleven-language grid.** The grid comes later; this feeds
   it.
2. **This feeds U35 and jumps its queue.** U35 is the plan's demo unit (R72/R73/R75-R77): a demo that
   consumes and displays - running stats, a rate-limited sample of message content, three modes
   (own-cluster / Testcontainers broker / mock), and a marked `PLACE SERDE SETUP IN YOUR LANGUAGE
   HERE` block. One arm, no comparison, no dials. This work is the comparison demo; U35 builds on it.
   ("Reading demo" was a term coined in conversation to name the contrast. It is not the plan's
   word and should not leak into the plan.)
3. **Arms: foreign-over-PC vs that language's own native client** (its librdkafka wrapper, or a
   native-protocol client where the language has one). Not against the Java client.
4. **For Java, core is the native client and the sidecar is the foreign one** - "a dialect of Java".
   This makes Java the one place where the sidecar hop can be priced with engine, workload, broker
   and host held identical.
5. **Simulated work is a sleep. No HTTP server.** Deliberately simple.
6. **Sequential or concurrent is the user's choice**, a flag, so they can compare and draw their own
   conclusion. Sequential also reproduces the original cast's shape.
7. **Same topic, two group ids**, so both arms process identical records.
8. **Four lanes: native/vanilla, PC-UNORDERED, PC-KEY, PC-PARTITION.** All combinations. Note the
   pairing that falls out: vanilla is inherently partition-ordered and serial, so PC-PARTITION is the
   apples-to-apples lane and PC-UNORDERED is the ceiling. This is the README chart's own structure.
9. **Knobs:** key-set size as a percentage of records, user-function delay, percentage of failures,
   max concurrency.
10. **Keep the Vert.x demo** - rescue it, do not throw it away - **and build the core/sleep one as
    well.** Both.
11. **One branch**, named for the classic Vert.x demo.
12. **Demos live under each module**: `<client-module>/demo/` per language (e.g.
    `.../parallel-consumer-proxy-client-swift/demo/`), with the reference at
    `parallel-consumer-proxy/demo/`. This is already exactly U35's stated fileset.
13. **Docker: one image per language, several container configurations** as one-click presets for the
    different modes.
14. **`origin/presentation` gets archived as a tag, then the branch is deleted.** The branch name
    pollutes the namespace; the code should survive. Tag under an `archive/` prefix. Do this only
    after the rescue is pushed.
15. **Stacked PR** off `feats/proxy-requirements`.
16. **The concurrency dial is always capped** - a demo must choose a number; there is no "let it
    run". (An earlier framing offered "cap it or let it run and document the degradation"; the second
    option does not exist. Correction recorded so it is not re-proposed.)

## Sequencing

The sidecar has **no production entry point** - the only `main` is `TestModeMain` in
`parallel-consumer-proxy/src/test`, and it boots the engine with `MockConsumer`/`MockProducer`. The
real lifecycle is plan unit U10, unlanded. The proxy also has no `src/test-integration`, so nothing
in it has ever run against a real broker.

A comparison demo needs a real broker by construction - a native-client arm has nothing to read from
a mock. So:

- **U10 comes first** for anything proxy-side. Confirmed by the owner.
- **The rescue needs neither U10 nor the proxy** and can proceed immediately, in parallel.

## The rescue, specifically

- First commit lands `Demo.java` **verbatim** as `ffda9c6a3` wrote it, preserving original authorship
  and date. The port to today's packages and APIs is a later commit, so the diff shows what five
  years cost.
- **Copyright:** the file carries no header at all today. The convention is not a bare year - its own
  siblings in that directory carry:
  ```
  /*-
   * Copyright (C) 2020-2022 Confluent, Inc.
   * Modifications Copyright (C) 2026 Antony Stubbs and contributors
   */
  ```
  Written in 2021 at Confluent, so that exact block is what it gets - added in the **port** commit,
  not the verbatim one.

### State of the rescue, and what running it under Maven cost

The rescue commits landed the demo **without ever running it under Maven** - it had only been run
from a hand-built classpath. Running the documented command found four things, in the order they
bite. All four are fixed; the last one had already reached a required test.

1. The build died at `validate`. The copyright check judged `Demo.java` fork-original - it is
   Confluent-authored, but no fork-point path holds it, because its branch was never merged. The
   scanner now has a third provenance table, `RECOVERED_FROM_UPSTREAM_BRANCH`, and unlike the
   extraction list it **verifies** the claim against the origin commit rather than trusting it.
2. `-pl parallel-consumer-vertx` alone fails the enforcer's ReactorModuleConvergence rule. The
   working command needs `-am`, plus `-Dfailsafe.failIfNoSpecifiedTests=false` (not
   `-DfailIfNoTests`, which is surefire's spelling and does not stop failsafe failing the modules
   `-am` drags in).
3. `setupWireMock()` was called only from `main()`, so the JUnit entry point the previous commit
   introduced ran with a null stub. It is `@BeforeEach` now, and there is an `@AfterEach` that
   closes the stub, the engine and the consumer - the hang-on-failure the previous commit flagged.
   `System.exit(0)` is gone from the test body: under Maven that runs inside the failsafe fork and
   would report as a crashed VM however well the demo went.
4. **`VertxConcurrencyIT` was broken by the shared-helper refactor and nobody saw.** Collapsing the
   produce loops onto `KafkaClientUtils#produceMessages` reached `PCModuleTestEnv`, whose
   `MutableClock` comes from a **test-scoped** dependency of core - and test scope is not
   transitive, so consuming core's tests jar does not bring it. Both callers died on
   `NoClassDefFoundError` at runtime having compiled cleanly, and `VertxConcurrencyIT` is in the
   required lane. `threeten-extra` is now managed in the parent and declared by the vertx module.

**Re-measured from the Maven entry point, 2026-08-21** (idle host, ~85% CPU idle; Time Machine was
running, so treat these as soft):

|                | 2021 cast | port, from `main()` | now, via Maven |
|----------------|-----------|---------------------|----------------|
| vanilla        | 333 msg/s | 263 msg/s           | 250 msg/s      |
| PC (350k)      | 27,201    | 20,588              | ~19,400        |
| **ratio**      | **81.7x** | **78.2x**           | **~77.8x**     |

The ratio is what the README actually claims, and it holds. Absolute throughput is lower on a laptop
also running Docker, as it was for the earlier port.

## The core demo's first real numbers, and what they expose

`ComparisonDemo` at its defaults - 5,000 records, 2ms, `maxConcurrency` 100, 10 partitions:

| lane | elapsed | msg/s | vs vanilla |
|---|---|---|---|
| VANILLA | 15s | 325 | 1.0x |
| PC_UNORDERED | 1s | 4,229 | 13.0x |
| PC_KEY | 1s | 4,426 | 13.6x |
| PC_PARTITION | 2s | 1,712 | 5.3x |

**That table was one volume, and it understated by six.** Superseded by the two-replay design below;
kept here because the number it produced is what exposed the problem.

### Resolved: two replays, and decision 7 stands (owner, 2026-08-21)

I put this up as a choice - honour decision 7 and publish an understated ratio, or break it for a
real number. **That framing was wrong.** The classic does both, and decision 7 is never violated,
because the big run is not a comparison at all:

- **Small replay** - every lane, identical records, one topic, a group id each. Decision 7 to the
  letter. This is the honest side-by-side.
- **Big replay** - the same topic grown to a real backlog, dial-bound lanes only. Answers a
  different question: what the engine sustains once start-up stops dominating.

Both are printed. Neither is averaged into a figure true of nothing. At the classic's own settings
(5,000 compared, 350,000 replayed, 2ms, 100 concurrency, 10 partitions):

| lane | small replay | big replay |
|---|---|---|
| AK_CORE | 338 msg/s (1.0x) | excluded |
| PC_UNORDERED | 4,268 (12.6x) | 31,909 (**94.1x**) |
| PC_KEY | 4,527 (13.4x) | 24,593 (72.6x) |
| PC_PARTITION | 1,823 (5.4x) | excluded |

The ~80x the 2021 cast quoted comes straight back out, from a sleep and no HTTP server.

**Which lanes the big replay drops is one rule, not two exceptions:** a lane joins only if it can
use the concurrency dial. AK core is capped at one. **Partition ordering is capped at the partition
count** - one in-flight record per partition - so at ten partitions it performs in the AK core
client's class however high the dial goes. Measured at ~370 msg/s when an earlier version tried to
replay it: fifteen minutes for the backlog the unordered lane clears in ten seconds. Expressed as
the property, so raising `demo.partitions` past `demo.maxConcurrency` readmits that lane on merit.

That attempt also found a defect worth keeping in mind for any future deadline here: it divided the
ideal time by the *configured* concurrency, which no ordered lane can reach, so PC_PARTITION got a
deadline ten times tighter than its own best case and was reported as stalled. A deadline must be
derived from what a lane can achieve - the number of independent ordering units, or the dial,
whichever is lower.

### Logging was on for all of these, and it does not matter (controlled, 2026-08-21)

Asked whether logging had been turned off - it had not. The default test config runs the root logger
and `bz.stub.parallelconsumer` at INFO to a console appender, and the progress bar emits through the
demo's own logger, so every number above was taken with all of it on. Tested rather than argued,
three arms of `ComparisonDemo` at identical settings:

| logging | per-lane rates | failsafe elapsed |
|---|---|---|
| full (engine INFO + progress bar) | 325 / 4,229 / 4,426 / 1,712 | 25.64s |
| engine off, progress bar on | 320 / 4,235 / 4,434 / 1,731 | 25.38s |
| everything off | (no report - it is logged) | 26.78s |

Under 1.5% between arms, no monotonic relationship to log volume, and the fully silent run was the
*slowest*. Logging is not a confound here; the lanes are start-up bound, which is the same finding
the volume section above reaches from the other direction.

**A trap worth not repeating:** the first instrumentation check compared `grep -c 'ProgressBar'`
between arms and read 29 -> 0 as proof the progress bar had been silenced. It had not. The quiet
config also changed the log PATTERN, so the thread name stopped being printed and the grep was
counting the pattern, not the logging. Verify a silenced logger by a MESSAGE string it emits
(`Close complete`), never by anything the pattern contributes - and prefer a measure the logging
config cannot touch at all, which is why the table above ends in failsafe's own elapsed time.

**This collides with decision 7** ("same topic, two group ids, so both arms process identical
records"), which the classic demo did not honour and could not have. Resolving it needs the owner:
either the parallel lanes get a larger backlog than the serial one and "identical records" becomes
"identical workload definition", or the demo keeps one volume and publishes a ratio it knows to be
understated by roughly 6x. Recorded rather than decided, because decision 7 was made deliberately.

## Constraints inherited from elsewhere - do not re-derive

- The **per-language sleep rule**: the simulated delay must use the language's non-occupying wait
  where the language has one. Python's client runs worker **processes** (100 sleeping processes is
  not free the way 100 sleeping threads is); TypeScript is a **single event loop** (a blocking sleep
  there is fatal, it must be an awaited timer). Go, Ruby, Java, Kotlin, Scala, Rust, Swift, C#, C++
  are fine either way. Specify once in the reference, mirror per language, per KTD40.
- The **fairness charter** in `parked-testing-as-a-feature-for-the-clients.md`: no
  language-vs-language ranking, publish the case we expect to lose, report the sidecar hop rather
  than engineering it out.
- **Coordinated omission**, agreed across both tracks: at user-chosen delays the workload must be
  driven open-loop at a fixed arrival rate, with latency measured from intended send time.
- **Effective-config fingerprint on both arms.** Proxy `UNSPECIFIED` defaults diverge from core
  defaults, so the arms are not comparable without it. No absolute-number framing.
- **A demo container is never granted the host Docker socket** (U35). Broker mode reaches a compose
  sibling.
- The owner's corrected framing is the comparison in
  [`parked-perf-against-native-kafka-clients.md`](parked-perf-against-native-kafka-clients.md),
  which is marked post-v6 and low priority and gated behind the shared suites existing. This work
  un-parks it as the demo's headline. That doc's cautions still apply, and one of its rules needs a
  demo-shaped carve-out written down: it says native clients are test-scope only and must never reach
  a client's runtime classpath, but a demo container necessarily ships librdkafka.
- The classic workload is **already the fair one**. That doc warns a naive throughput race flatters
  the native client, because "consume as fast as possible with no ordering requirement" is its home
  turf. The classic demo is not that - it is "every record needs an external call of duration D",
  which is exactly where serial consumption genuinely loses. Protect that property when adding dials;
  the 0ms/no-ordering corner is the native client's home turf and the UI should label it, not hide it.

## Open questions

- **Where the Java comparison demo lives.** It needs core *and* the sidecar. The rescued Vert.x demo
  is in `parallel-consumer-vertx`; decision 12 puts per-language demos under their client module.
  Java's comparison spans both. **Still open** - the core/sleep demo below settled only its own half.

### Settled while building the core/sleep demo (agent, 2026-08-21)

The owner said "proceed with the sleep core version" rather than answering these individually, so
they were decided in the work and are recorded here with the reasoning, as the decisions above are.
Overturn any of them by editing here, not by arguing with the code.

- **The core/sleep demo lives in `parallel-consumer-core/src/test-integration`**, as
  `ComparisonDemo`, beside the Vert.x demo's own module placement. This settles the core-only half
  of the open question above and *not* the Java-spanning-the-sidecar half, which is still blocked on
  U10 and stays open. Decision 12's `<client-module>/demo/` layout governs the per-language proxy
  demos; nothing in it reaches core.
- **The concurrency cap is 1000** (decision 16 demanded a number). Chosen because it is where this
  demo's own model stops being honest, which is the only defensible place for it: simulated work is
  a blocking sleep, so in-flight records are threads, and the thread-per-record cost starts showing
  up in the reported number above roughly a thousand. That is the same line this file's arithmetic
  section already identified as where Vert.x starts to earn its keep - so the cap is also the point
  at which the sibling demo becomes the one worth running. Requests above it are capped and the run
  says so rather than silently obeying.
- **The keyspace dial does not change the ordering mode**, and decision 8 dissolved the question as
  predicted. Running all four lanes makes the dial orthogonal: it sets the shard count under
  `PC_KEY` and is inert everywhere else. The report labels where it bites instead of leaving a
  reader to assume it bites everywhere.
- **The demo reports throughput and deliberately reports no latency.** Every record is on the topic
  before any lane starts, so the workload is closed-loop and per-record timings would be flattered
  by exactly the amount a lane fell behind. The coordinated-omission constraint both perf tracks
  agreed requires open-loop arrival at a fixed rate, measured from intended send time; this demo
  does not do that, so it does not publish the number rather than publishing a wrong one. Anything
  wanting latency needs the what-if machine, not this.
- **No `main` method.** The Vert.x demo has one and paid for it twice - a hand-built classpath hid a
  real dependency, and the `System.exit` it needs reports as a crashed VM when it runs inside a
  failsafe fork. Maven owns the classpath.

## Open thread: the branch-archaeology gap this uncovered

Raised by the owner on finding that `origin/presentation` is in no ledger: *"How did we miss this in
the sweeps? What else are we missing?"*

**Investigated, and it is not this branch's to carry.** The findings live in
`next-fork-branch-archaeology.md` on `docs/fork-branch-archaeology`, cut from **master** rather than
stacked here - the audit is a repo-wide concern with nothing to do with the language proxy, and
stacking it would gate a master-level finding behind an unrelated feature PR. The headline: 109
pre-2026 branches on `origin` are named in no document, so `presentation` is one of a class rather
than a one-off, and every existing audit was seeded from a tracker instead of from the set of refs.

## Related tracks - whose toes this steps on

This work sits across four tracks that were designed separately. Read these before changing scope;
each one has already decided something this could contradict.

- **[`next-polyglot-demo-app.md`](next-polyglot-demo-app.md)** - owns the demo app, UI, live loop and
  marketing narrative. Its ideas 21/23/25 are the ranked directions; **idea 23, the
  bring-your-own-topic what-if machine, is this demo's control surface dial for dial** and is the
  owner's own direction. This work is a narrower, earlier cut of it.
- **[`next-perf-comparison-matrix.md`](next-perf-comparison-matrix.md)** - owns workload definitions,
  measurement semantics and the blessed-numbers pipeline. The boundary with the demo track is
  **already agreed and recorded in both files**. It names this exact ask: "the classic README intro
  performance test running in each proxy language - the double-click demo wanted before astubbs#293
  merges". Its constraint on us: the scenario definitions parameterise delay, ordering, failure
  percentage and concurrency so both tracks run **one definition**, not two.
- **[`parked-perf-against-native-kafka-clients.md`](parked-perf-against-native-kafka-clients.md)** -
  is the owner's corrected framing for the arms (foreign-over-PC vs that language's own native
  client). Marked post-v6 and low priority, gated behind the shared suites existing. **This work
  un-parks it as the headline**, so its cautions bind here.
- **[`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md)**
  - the fairness charter both perf tracks answer to.
- **[`parked-demo-gallery.md`](parked-demo-gallery.md)** - the retired R74 hosted gallery. States that
  "the per-language demo containers and the shared demo contract (R72, R73, R75-R77) land with the
  plan", which is the thing this work is jumping the queue on. Nothing here builds hosting.
- **[`branch-language-proxy.md`](branch-language-proxy.md)** and
  **[`pr-293-handoff.md`](pr-293-handoff.md)** - the parent branch's own ledger and handoff. The
  handoff's "known-shallow, deliberately" list is why U10 blocks the proxy-side demos.
- **U35** in [`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`](../plans/2026-08-14-001-feat-language-proxy-plan.md)
  - the demo unit whose fileset this adopts, and whose KTD40 identical-UX contract caps how much
  per-language divergence the control surface may have.
