# Release 0.6.0.0

**Tracking issue: astubbs#197.** That issue is the linkable handle - from PRs, from mirrors, from upstream
comments. This file is the detail behind it. Keep them in step: if a blocker is resolved here, tick it
there.

Not yet released: the pom is `0.6.0.0-SNAPSHOT`, there is no `v0.6.0.0` tag, and the changelog section
is written. Release = strip `-SNAPSHOT` and merge to `master`; `publish.yml` runs after CI succeeds,
deploys via the `maven-central` profile, tags `v<version>` and cuts a GitHub release
([`docs/releasing.md`](../releasing.md)).

**No longer blocked by the quarantine guard** - astubbs#80 emptied the registry when it merged, so
`release.yml`'s "no release while tests are quarantined" gate now passes.

## Bugs found while triaging the upstream mirrors (2026-08-05)

None of these has an issue of its own - they were found by reading code to diagnose something else.

1. **`PCModule#initDynamicLoadFactor` builds `new DynamicLoadFactor(staticLoadFactor, staticLoadFactor)`** when
   `messageBufferSize` is set, so `isMaxReached()` is true from startup and
   `AbstractParallelEoSStreamProcessor` logs *"Max loading factor steps reached"* at WARN on
   every control-loop pass. Anyone following the README's buffer-tuning advice gets permanent log
   noise reporting a non-problem. Related: astubbs#155.
2. **MDC context is not propagated into the worker pool.** PC sets its own `pcId` and `offset` keys
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

## Breaking changes that have already landed

Two, both from astubbs#296 (`fix(core) astubbs#209`, commit `79a7b6c62`, whose body carries the full
reasoning). They are written down here because `CHANGELOG.adoc`'s `=== Breaking` section is
regenerated from the commit log when the tag is cut - until then the log is the only record, and a
release note is not something to discover by reading commits.

These are **not** items from [`docs/refactoring.md`](../refactoring.md)'s *Breaking changes queued for
next major version*. That section is the queue of removals still to be done; these two are done. What
they share is the gate: that section explains why it is currently OPEN, which is what let them land
now rather than wait.

Both narrow a `protected`/subclass surface on `internal/AbstractParallelEoSStreamProcessor.java`. **A
user of `ParallelStreamProcessor` is not affected, and neither is a subclass that leaves both alone** -
the population is people extending the internal controller. Say that plainly in the notes: an
unqualified "breaking" on a stability release will cost more upgrade hesitancy than these two are
worth.

- **A subclass overriding `setupWorkerPool` must now return a pool whose `RejectedExecutionHandler` is
  a `ThreadPoolExecutor.AbortPolicy`** (a subclass of `AbortPolicy` counts - the requirement is the
  throw). Anything else, and construction throws `IllegalArgumentException` with a message naming the
  handler it got. Previously such a pool was accepted and **silently lost records**: with a
  non-throwing handler `submit()` returns a `Future` that never completes, so the containers stay in
  flight, `numberRecordsOutForProcessing` is inflated for the life of the instance, and their offsets
  are never committed - no exception, nothing in the logs. **What to do:** build the pool with
  `AbortPolicy`, or delegate to `super.setupWorkerPool` and adjust the pool it returns; if the pool
  then rejects work, its queue is too small for the configured `maxConcurrency`, which is now visible
  rather than absorbed. In-repo subclasses are unaffected - `VertxParallelEoSStreamProcessor` and
  `ExternalEngine` both `return super.setupWorkerPool(1)`. The named guard this release's condition
  asks for is `requireRejectionIsVisible`, pinned by `AbstractParallelEoSStreamProcessorConfigurationTest`
  (`aPoolThatSilentlyDiscardsRejectedWorkIsRefusedAtSetup` and its three siblings, plus
  `theDefaultPoolIsAcceptedUnchanged` against over-reach).

- **`setState` is no longer callable from outside its own package.** The `private State state` field
  carried a bare Lombok `@Setter`, so `setState` was public and reachable from any subclass - including
  the cross-module `VertxParallelEoSStreamProcessor`, `MutinyProcessor` and `ReactorProcessor`. It is
  now `@Setter(AccessLevel.PACKAGE)`, because this is the controller's own state machine and a subclass
  driving it arbitrarily is the shape of bug astubbs#296 exists to prevent. **Who is affected: only
  code that actually called it**, and nothing outside this package's own tests did, on either side of
  the fork. There is deliberately no replacement for the write. A `protected getState` arrives in the
  same change - new, not narrowed, since the field had no getter at all before - so a subclass that
  only wanted to know whether it is still running is better off than it was.

At release, when the changelog section is regenerated, check both survived into `=== Breaking`:
generation reads the commit log, so they are only as findable as those commit bodies. The rename side
of that same check is in [`release-0600-blockers.md`](release-0600-blockers.md).

## Public API change landing with astubbs#204: the commit give-up exception

Not a breaking change to a *subclass* surface like the two above - this one is visible to every user
of `PERIODIC_CONSUMER_SYNC`, so it needs its own line in the notes.

**`ConsumerManager.commitSync` no longer rethrows Kafka's bare `TimeoutException` /
`SaslAuthenticationException` when a commit exhausts its budget.** It throws
`OffsetCommitBudgetExceededException` (new, in the public `bz.stub.parallelconsumer` package,
extending `ParallelConsumerException`) with the broker's exception as the **cause**. Anyone catching
the Kafka type directly around PC's failure surface - `getFailureCause()`, or a supervisor wrapping
PC - stops matching, and must catch the PC type or unwrap `getCause()`.

Why it earns the break on a stability release: the bare Kafka exception can say a commit timed out
but not *which of PC's options bounded it*, what that option's relationship to the consumer's own
timeouts is, or what to do about it. That gap is the whole subject of astubbs#177 /
confluentinc#833 - two reporters, neither of whom could tell from the message where to look. The new
message names the budget that ran out, its configured value, the knob to raise, and - when only one
attempt was made - that `offsetCommitTimeout` is below the consumer's `default.api.timeout.ms`
(**60000ms** by default, verified in kafka-clients 3.9.2) so no retry was reachable at all.

Two behaviour changes ship alongside it and belong in the same note, because a reader meeting one
will ask about the others:

- **`offsetCommitTimeout` now bounds the whole commit, not each attempt.** It was captured inside the
  retry loop, so every attempt reset it and the loop could retry forever. This makes PC give up where
  it previously hung. Matches Kafka's own two-level model, where `default.api.timeout.ms` bounds a
  call *including* its retries.
- **`saslAuthenticationRetryTimeout` is measured from the first SASL failure**, not from the start of
  the commit call, so a slow commit no longer spends an unrelated option's budget.

## Release gate: no disabled tests

**0.6.0.0 does not ship while any test is disabled.** Tests currently carrying `@Disabled`:
`VertxTest`, `ParallelEoSStreamProcessorTest` (two), and `MultiInstanceRebalanceTest`. All four
predate the fork - they were added in 2021 and 2022, before this repo had a rule against muting or the
`@Quarantined` mechanism that replaced it - so this is inherited debt rather than a rule being broken.
One is not a muted test at all: `VertxTest.handleHttpResponseCodes`'s entire body is
`assertThat(true).isFalse()`, a stub that was never written.

Each needs the same decision: fix it, delete it, or quarantine it with a diagnosis under
`@Quarantined`. Quarantining does not satisfy this gate on its own, because a release is separately
blocked while the quarantine registry is non-empty - so it defers the same gate by another route.
AGENTS.md already gives the reasoning for why muting is the wrong answer: it "loses the signal - a
'known flake' can be a real product bug".

This matters to the release rather than only to the codebase, because `docs/data/testing-evidence.yaml`
asserts flake discipline as evidence, and a reader who greps for `@Disabled` a minute later is exactly
the reader that data is written for.

## This release is a stability release, and that is the point

0.6.0.0 clears the known-defect scope and makes the evidence for that conclusion inspectable. Resolving
known defects, and improving the machinery that finds and proves them, is a first-class outcome here
rather than background maintenance behind a feature release.

**Release condition.** 0.6.0.0 is cut only when every known **critical** defect in scope is resolved
and each resolution has a named guard that passes. Critical is the line, deliberately: claiming "all
known defects" would be a promise this project cannot keep, and non-critical defects are not claimed
to be fixed. Unknown defects remain possible and must not be denied.
Evidence is in `docs/data/testing-evidence.yaml`; the checks to run are in
`docs/data/module-maturity.yaml` under `release_validation`. If a check fails, amend the claim rather
than waive the item.

**Deliberately not in this release:** virtual threads, micro-batching and the dead letter queue. These
are new capabilities rather than known-defect exceptions, so deferring them does not weaken the gate.
They carry horizons in `docs/data/roadmap.yaml`.

**Not a 1.0 attempt.** Expect one or two more major 0.x releases first. What 1.0 waits on is in the
roadmap data.

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

All four now exist as data. Each keeps its planning note, which holds the reasoning the data does not:

- **The test suite presented as evidence** - `docs/data/testing-evidence.yaml`. The strongest available
  answer to "this fork just added features". Reasoning: `docs/plans/2026-08-10-001-docs-testing-evidence-plan.md`.
- **Per-module maturity** - `docs/data/module-maturity.yaml`, with reliability and API stability as
  independent axes. Note that "pre-1.0 reserves the API surface, not reliability" is false: 1.0 also
  waits on functionality still intended for the release, and on confidence reaching beyond the defects
  already known. Reasoning: `docs/plans/2026-08-10-002-docs-module-maturity-plan.md`.
- **A roadmap** of big-picture entries anchored on stated 1.0 exit criteria rather than dates -
  `docs/data/roadmap.yaml`. Reasoning: `docs/plans/2026-08-10-003-docs-roadmap-plan.md`.
- **A record per feature** - `docs/features/`, so capabilities stop being undocumented. Reasoning:
  `docs/plans/2026-08-10-004-docs-feature-catalogue-plan.md`.

What does not exist yet is the rendered documentation an agent generates from all of it.

## Do at release: one sweep over the upstream mirrors

The upstream issues are mirrored here, labelled `upstream-mirror`, and each carries a **Fork status** section
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
