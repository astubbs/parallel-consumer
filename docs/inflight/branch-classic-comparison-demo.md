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

**Outstanding: the timed re-measurement.** The numbers in commit `ef698d1c9` (263 msg/s vanilla,
20,588 msg/s PC, 78.2x) were taken before these fixes, from `main()`. They should be re-taken from
the Maven entry point before any of them are quoted anywhere a reader sees. That run needs an idle
machine - it was deferred here because a throughput bisect was using the same host, and a perf
number taken beside one is worthless in both directions.

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
  Java's comparison spans both.
- **What the concurrency dial's cap actually is.** Above roughly 1000 the blocking-sleep model stops
  scaling, so the cap is a real number that has to be chosen and stated.
- **Whether the keyspace dial changes the default ordering mode.** The dial only means anything under
  `KEY`; the classic is `UNORDERED`. Decision 8 runs all four lanes, which may dissolve this - confirm.

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
