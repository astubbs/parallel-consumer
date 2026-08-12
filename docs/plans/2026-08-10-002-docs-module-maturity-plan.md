# A per-module maturity table, and what "not 1.0" actually means

**Status:** the data exists as `docs/data/module-maturity.yaml`, and the README carries the sentence that below 1.0 does not mean unproven. What shipped is data plus a sentence rather than the table this plan originally asked for, because reliability turned out not to vary across the shipped modules - it varies only at the boundary with the experiments.
**Migrated:** 2026-08-10, from `docs/inflight/`. The reasoning below is why the artefact has the shape it does.

Wanted for 0.6.0.0. Two problems it solves at once: the release ships a stable core *and* an alpha
experiment, and the version number implies something about readiness that is no longer accurate.

## The tension to resolve honestly

- The project is `<1.0`, which conventionally signals that it is not ready for production use.
- A substantial number of people already run it in production, successfully, and have done for years.
- The position taken at 0.6.0.0 is that **every known critical defect has been addressed**, under
  considerably more - and more rigorous - testing than before, and that the project is **ready for
  production use as far as reliability is concerned**.
- What actually remains before 1.0 is refactoring and a set of contained follow-up items. None of it
  is reliability work.

The resolution is not to soften any of those. It is that **a single version number is being asked to
carry two independent facts**, and it cannot:

| Axis | 0.6.0.0 position |
|---|---|
| **Reliability** - will it lose or double-process your data, stall, or leak | The claim being made. Known critical issues fixed, and demonstrably tested |
| **API stability** - will your code still compile next release | Not yet settled. This is what `<1.0` is really reserving |

"Pre-1.0 refers to the API surface, not to reliability" is quotable and false, and it survived several
rounds of review because of the first property. 1.0 waits on three things: a settled API surface, the
functionality still intended for the release, and confidence reaching beyond the defects already known.
Say all three. Note also that the README carries no `<1.0` badge to correct - it made no readiness
claim at all, so the artefact is additive and what it corrects is silence.

Do not oversell what testing proves. It shows the known failure modes are covered and no longer
reproduce - not that no unknown ones exist. The claim to make is **"all known critical issues"**, with
a pointer to how they were found and proven, which is the testing-as-a-product section
(`docs/plans/2026-08-10-001-docs-testing-evidence-plan.md`).

## The claim is a release gate, so write the docs assuming it holds

"All known critical defects are fixed" is not a description of the source tree today - it is **the
condition on which 0.6.0.0 ships**. The release does not go out until it is true, which is what makes
it safe to write the documentation as though it is.

That is deliberate, and it is the cheaper of the two available designs. The alternative - a separate
readiness tracker, kept strictly in step with reality by every PR - is one more artefact requiring
per-PR maintenance, and artefacts maintained that way drift the moment attention moves. Assuming the
gate holds, and checking once at release, costs almost nothing and fails safe: if the gate has not
been met when the release is cut, the documentation is amended to match reality rather than the
reverse.

**The check to run at release**, so that this is verified rather than assumed:

- **The original `confluentinc#857` deadlock** (`bug-857-family.md`). Two of the three defects behind
  that symptom have landed - astubbs#100 and astubbs#80 - but the deadlock itself,
  `synchronized(commitCommand)` contended between the poll thread in `onPartitionsRevoked` and the
  control thread in `commitOffsetsThatAreReady`, is still open. The fix exists in **astubbs#29**
  (`ReentrantLock.tryLock()`) and needs a rebase and retarget. It is not theoretical:
  `RebalanceEoSDeadlockTest` failed once during a 20-run stress hunt. Landing astubbs#29 also unblocks
  the deferred proof that thread-parallel integration tests are safe again - currently sidestepped by
  forking per broker, which avoids the deadlock rather than demonstrating its absence.
- **Lesser items worth a glance against the final wording**: astubbs#155's persistent "Max loading
  factor steps reached" warning for anyone following the README's own tuning advice, and MDC context
  not being captured at submit time. Neither is critical; both are the kind of thing a reader
  encounters early.

If any of these is still open at release, amend the wording rather than the standard - naming a
known-open defect costs far less credibility than a claim a reader can falsify.

## The table

Per module, because maturity is per module - one alpha experiment must not downgrade how the core
describes itself, and the core's stability must not be borrowed by the alpha.

Suggested columns, kept to what a reader can act on: module, status (stable / beta / alpha
experiment), reliability confidence, API stability, and a one-line "use this if". Candidate rows:
core, vertx, reactor, mutiny, examples, and the Streams and Connect experiments.

The experimental rows must also carry the blast-radius point - depending on the artifact is the whole
opt-in, and these modules cannot affect plain PC usage (see `release-0.6.0.0.md`).

`README_TEMPLATE.adoc` already has a `=== Java Version per Module` table; this belongs
near it, and that table needs the new modules added anyway. `README.adoc` is generated - edit the
template.

## Include the engineering system, not just the code

The CI and analysis apparatus is part of the maturity claim and is currently invisible to readers:
static and dynamic analysis, the test lanes, mutation testing, and automated PR review coverage. For
the target audience this is often *more* persuasive than a test count, because it speaks to what will
catch the next defect rather than the last one. Detail belongs in the testing section; the table
should reference it rather than duplicate it.

## Why 1.0 is nearer than the backlog implies

Worth stating once, briefly, and without resting any weight on it. The outstanding pre-1.0 items are
refactoring and follow-up work, and the rate at which work of that kind can now be completed has
changed materially. A backlog that would once have implied years now implies considerably less.

Two cautions on how this is used. It explains the *schedule*, not the *quality* - it is not evidence
of reliability and must not be positioned as though it were. And it is an assertion about capacity
rather than a verifiable fact, so it belongs as a short aside rather than as an argument the reader
is asked to accept.

## Remaining

A rendered table earns its place once the experiment modules land and that boundary exists to draw. Before then every row would say the same thing.
