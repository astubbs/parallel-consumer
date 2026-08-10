# Release 0.6.0.0

**Tracking issue: astubbs#197.** That issue is the linkable handle - from PRs, from mirrors, from upstream
comments. This file is the detail behind it. Keep them in step: if a blocker is resolved here, tick it
there.

Not yet released: the pom is `0.6.0.0-SNAPSHOT`, there is no `v0.6.0.0` tag, and the changelog section
is written. Release = strip `-SNAPSHOT` and merge to `master`; `publish.yml` runs after CI succeeds,
deploys via the `maven-central` profile, tags `v<version>` and cuts a GitHub release (AGENTS.md →
*Releasing*).

**No longer blocked by the quarantine guard** - astubbs#80 emptied the registry when it merged, so
`release.yml`'s "no release while tests are quarantined" gate now passes.

## Bugs found while triaging the upstream mirrors (2026-08-05)

None of these has an issue of its own - they were found by reading code to diagnose something else.
The first is a wrong *statement* in an artefact the release itself publishes, so it should not
survive the release.

1. **`CHANGELOG.adoc` says the Kafka client "stays on 3.9.1"; `pom.xml:121` says `3.9.2`.** The
   release notes for an unreleased release are factually wrong. Decide which is intended and make
   them agree.
2. **`PCModule:127-129` builds `new DynamicLoadFactor(staticLoadFactor, staticLoadFactor)`** when
   `messageBufferSize` is set, so `isMaxReached()` is true from startup and
   `AbstractParallelEoSStreamProcessor:1130` logs *"Max loading factor steps reached"* at WARN on
   every control-loop pass. Anyone following the README's buffer-tuning advice gets permanent log
   noise reporting a non-problem. Related: astubbs#155.
3. **MDC context is not propagated into the worker pool.** PC sets its own `pcId` and `offset` keys
   but never captures the caller's context map at submit time (no `copyOfContextMap` anywhere), so a
   caller's `trace_id` is lost crossing into the worker threads and the vert.x event loop. Raised by
   a user in the `confluentinc#907` thread (astubbs#195).

## Marked for 0.6.0.0

- **astubbs#196** - fixes astubbs#167 (confluentinc#622): the README's retry-delay example used `multiplier = 0.5`, so
  `Math.pow(multiplier, attempts)` *halved* the wait on each failed attempt instead of growing it.
  Reported upstream 2023-08-08 and patched there by confluentinc#864 - but only in the generated
  `README.adoc`, not the `CoreApp.java` it is generated from, so upstream's source still reads `0.5`
  and the patch reverts on the next regeneration. This fork inherited that patch and lost it exactly
  that way. Fixed in `CoreApp.java`, since the README embeds that snippet by asciidoc include, and
  `README.adoc` regenerated.

## Release gate: no disabled tests

**0.6.0.0 does not ship while any test is disabled.** Tests currently carrying `@Disabled`:
`VertxTest`, `ParallelEoSStreamProcessorTest` (two), and `MultiInstanceRebalanceTest`. All four
predate the fork - they were added in 2021 and 2022, before this repo had a rule against muting or the
`@Quarantined` mechanism that replaced it - so this is inherited debt rather than a rule being broken.
One is not a muted test at all: `VertxTest.handleHttpResponseCodes`'s entire body is
`assertThat(true).isFalse()`, a stub that was never written.

Each needs the same decision: fix it, delete it, or quarantine it with a diagnosis under
`@Quarantined`. Quarantining is not a way to satisfy this gate on its own, because a release is
separately blocked while the quarantine registry is non-empty - so a quarantined test defers the
problem to the same gate by another route. AGENTS.md already gives the reasoning for why muting is the
wrong answer: it "loses the signal - a 'known flake' can be a real product bug".

This is a gate rather than a documentation task. The testing-as-a-product material asserts flake
discipline, and a reader who greps for `@Disabled` inside a minute of reading it is the exact reader
that material is written for.

## Say plainly that the experimental modules cannot affect plain PC

If 0.6.0.0 ships new experimental modules - the Kafka Streams one (astubbs#255) and the Connect one -
the release notes, README and any announcement must make it **obvious to an existing PC user that
these have no bearing on their usage**. The failure mode to design against is someone reading
"parallel-consumer now patches Kafka Streams internals at build time" and concluding *my plain PC
setup just got riskier*. Nothing about that inference is true, and left uncorrected it costs the
release trust it has not spent.

The claim is provable, not reassurance - state it as a fact with its reason:

- The experimental modules are **leaves**. They depend on `parallel-consumer-core`; nothing depends
  on them. Core's pom does not reference them, and adding them changed no shipped code in any
  existing module - verified on the spike branch against `origin/master`.
- The patched Apache Kafka classes ship **only inside the experimental module's own jar**. A user who
  does not depend on that artifact never has them on the classpath.
- The experimental seam **cannot be reached** from the core, vert.x or reactor modules. It is not a
  flag those modules read.

So: **depending on the experimental artifact is the entire opt-in.** Not depending on it is a
complete opt-out, requiring no configuration and no action.

Two things to get right in the wording:

- **Per-module maturity, not global.** The project ships a stable 0.6.0.0 *and* an alpha module at the
  same time; that is a normal state, not a contradiction. Do not let one alpha module downgrade how
  the release describes itself.
- **Do not overcorrect into burying it.** The alpha is promotional material worth having - it should
  be easy to find and try. The goal is an accurate blast radius, not a quiet one.

## Promotional material wanted

Each of these is an idea rather than a commitment, and each has its own note:

- **The test suite presented as a product**, including what has been tested and fixed since the last
  upstream release - the strongest available answer to "this fork just added features". See
  `next-testing-suite-as-product-docs.md`.
- **A per-module maturity table**, and correcting what `<1.0` implies: it reserves the *API surface*,
  not reliability. See `next-module-maturity-table.md`.
- **A living roadmap** of high-level themes, anchored on stated 1.0 exit criteria rather than a date.
  See `next-living-roadmap.md`.
- **Per-PR feature documentation**, so features stop landing undocumented. See
  `next-per-pr-docs-and-feature-index.md`.

## Do at release: one sweep over the upstream mirrors

All 78 upstream issues are mirrored here (astubbs#44, astubbs#117-astubbs#195), and each carries a **Fork status** section
written on 2026-08-05. Roughly **11 of them name `0.6.0.0`** in the future tense - "ships in", "is set
to ship", "answered by the fork" - and those statements only become true when the release goes out.

At release, in one pass:

- **Reword the ~11 that name `0.6.0.0`** from future to past tense, and give the actual coordinate
  (`bz.stub.parallelconsumer:parallel-consumer-core:0.6.0.0`) so a reader can act on it rather than
  being told a release exists.
- **Close astubbs#186 (`confluentinc#880`), astubbs#188 (`confluentinc#885`) and astubbs#195 (`confluentinc#907`)** with a pointer
  to the release. All three are "is this maintained / where do I get a version that works with
  kafka-clients 3.9.1", which the release answers outright.
- **Post one new comment** on `confluentinc#880`, `#885` and `#907` upstream announcing it. This is the
  deliberate exception to "one backlink comment per issue, never a second": a shipped artefact is
  actionable, which is the bar astubbs#114 sets for a second notification. Everything else gets a silent
  edit, since editing a comment notifies nobody.

**Do not point anyone at astubbs#197 before the release.** It is tempting to answer the "is this still
maintained?" threads with a link to the release tracker, but a tracking issue is not an artefact -
sending someone a checklist when they asked where to get a jar is worse than saying nothing. The
answer to those threads is the coordinate, and it only exists once this ships.

Bounded and one-off. It is listed here rather than in the mirroring plan because the release is the
trigger, and this file is what gets read when the release is cut.

Find them with: `gh issue list -R astubbs/parallel-consumer --label upstream-mirror --search "0.6.0.0"`.
