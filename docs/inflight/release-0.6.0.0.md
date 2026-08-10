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

## The experimental modules do not gate this release

**Settled 2026-08-11.** Neither the Kafka Streams module (astubbs#255) nor the Kafka Connect one
(astubbs#240) blocks 0.6.0.0, and neither needs to reach an MVP first. This reverses the working
assumption that the release waits for them.

The reasoning is a trade, stated plainly. 0.6.0.0 is a stability release and its content is the
known-defect backlog - which is ready. Holding it for exploratory work would park a queue of finished
bug fixes behind unfinished experiments, delaying the thing that is done for the thing that is not.
Both directions of that trade are bad. **Whatever state the experimental modules are in when the
release is cut is what ships.**

Three consequences worth being explicit about, because each one silently reintroduces the pressure if
left unstated:

- **There is no MVP bar.** Do not scope experimental work by "what must be in for v6". Scope it by what
  is demonstrably true, ship that, and describe the boundary honestly. For the Streams module the
  interesting claim - that a Kafka Streams topology gets per-key concurrency inside a single partition
  at all - is already evidenced: Kafka's own test suites pass with the seam off, crash safety is proven
  with a red-then-green test, and the head-of-line measurement is published with the control that shows
  where it does *not* help. Completeness is not what makes that land. Raw and real beats polished and
  narrow.
- **Merging and publishing are separate gates.** Merging an experimental module to master is cheap to
  reverse: these are leaf modules, nothing depends on them, and removal is one `git rm` plus a pom line.
  Publishing is not reversible once anyone depends on the coordinate. So merge freely and publish
  deliberately - and only once a module does something interesting enough to be worth someone taking a
  dependency on it. Until then, keep it in the reactor and out of the deploy set. **The publish decision
  is expected before v6, but it is a decision, not a formality.**
- **The honesty obligation gets larger, not smaller.** Shipping something raw is only defensible while
  the limits are stated where a user will actually meet them. The Streams module's README leads with the
  alpha status and points at its live shortcomings list rather than duplicating it, and known-broken APIs
  are being physically refused rather than documented - see astubbs#255. That refusal work is what makes
  "ships in whatever state it is in" a reasonable position instead of a reckless one.

See `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md` for the Streams side of this decision.

## Bugs found while triaging the upstream mirrors (2026-08-05)

None of these has an issue of its own - they were found by reading code to diagnose something else.
The first two are wrong *statements* in artefacts the release itself publishes, so they should not
survive the release.

1. **`CHANGELOG.adoc` says the Kafka client "stays on 3.9.1"; `pom.xml:121` says `3.9.2`.** The
   release notes for an unreleased release are factually wrong. Decide which is intended and make
   them agree.
2. **The README's Roadmap sends readers to the *upstream* tracker** and refers to this repo in the
   third person: *"have a look at the confluentinc/parallel-consumer GitHub issues, and clone
   Antony's fork"* (`src/docs/README_TEMPLATE.adoc:1011-1012`). That is pre-fork text, it is the
   section someone reads to answer "is this maintained?" (astubbs#195), and it is now doubly wrong because
   all 78 upstream issues are mirrored *here*. Edit the template, not `README.adoc`.
3. **`PCModule:127-129` builds `new DynamicLoadFactor(staticLoadFactor, staticLoadFactor)`** when
   `messageBufferSize` is set, so `isMaxReached()` is true from startup and
   `AbstractParallelEoSStreamProcessor:1130` logs *"Max loading factor steps reached"* at WARN on
   every control-loop pass. Anyone following the README's buffer-tuning advice gets permanent log
   noise reporting a non-problem. Related: astubbs#155.
4. **MDC context is not propagated into the worker pool.** PC sets its own `pcId` and `offset` keys
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
