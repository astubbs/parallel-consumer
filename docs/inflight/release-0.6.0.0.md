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
5. ~~**`OffsetEncoding` throws on an unknown magic byte *before* `invalidOffsetMetadataPolicy` is
   consulted.**~~ **FIXED** while debugging astubbs#118 (confluentinc#326). `OffsetEncoding:66`
   `throw new RuntimeException("Unexpected magic: " + magic)` was reached from
   `EncodedOffsetPair.unwrap()`, while the policy was only honoured later at `EncodedOffsetPair:123-130`.
   Two symptoms, one defect: a future PC encoding killed an older reader regardless of policy
   (forward-compatibility), and metadata left by a *previous non-PC owner* of the consumer group killed
   PC on assignment (the reported bug - the bare `RuntimeException` slipped past the
   `catch (OffsetDecodingError)` recovery in `loadPartitionStateForAssignment` and escaped the rebalance
   listener, which Kafka turns into a fatal "User rebalance callback throws an error").
   `OffsetEncoding.decode` now throws `OffsetDecodingError`, so both land in the existing recovery path:
   log, drop the offset map, resume from the committed offset. The policy now governs only
   recognisably-Kafka-Streams metadata. `OffsetMapCodecManager.errorPolicy` was also de-staticed - it had
   made the policy JVM-global, so with two PC instances the last one constructed set it for both.
   Same treatment applied to the one remaining unchecked escape on that path: `EncodedOffsetPair`'s
   `default ->` branch threw `UnsupportedOperationException` for an encoding that *has* a registered magic
   byte but no decoder - currently the `ByteArray` pair, which `OffsetSimultaneousEncoder` no longer emits.
   Covered by `ForeignOffsetMetadataOnAssignmentTest`.

## Marked for 0.6.0.0

- **astubbs#196** - fixes astubbs#167 (confluentinc#622): the README's retry-delay example used `multiplier = 0.5`, so
  `Math.pow(multiplier, attempts)` *halved* the wait on each failed attempt instead of growing it.
  Reported upstream 2023-08-08, never fixed there, inherited by the fork. Fixed in `CoreApp.java`,
  since the README embeds that snippet by asciidoc include, and `README.adoc` regenerated.

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
