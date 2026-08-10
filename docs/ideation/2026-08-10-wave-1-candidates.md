---
date: 2026-08-10
topic: wave-1-candidates
focus: testing-as-product, living roadmap, per-PR feature index
mode: repo-grounded
---

# Wave 1 candidates

Scaled run: repo grounding plus external prior art, then one ideation pass per idea, then correction. No frame fleet and no adversarial verifier, on the wave 0 finding that the fleet's most convergent idea was also its most wrong, and that the corrections which mattered came from review rather than from more agents.

Raw candidates awaiting correction. Nothing here is settled.

---

## T: the test suite as a product

**Correction to the brief, made by the agent.** All four `@Disabled` annotations date from 2021 and 2022 - `VertxTest` and both `ParallelEoSStreamProcessorTest` ones in July 2021, `MultiInstanceRebalanceTest` in February 2022 - which is upstream-era, before the fork's rule against muting or the `@Quarantined` mechanism existed. One is not a muted test at all: `VertxTest.handleHttpResponseCodes`'s entire body is `assertThat(true).isFalse()`, a stub never written. This is inherited debt the new rule has not been applied backwards to, not a rule being broken today.

### Strongest

- **The enemy is the test that passes without testing.** Organise the whole document around one claim: a suite's failure mode is not missing tests but green tests that assert nothing. Three unrelated pieces of machinery then become three attacks on one enemy - the vacuous-await diagnosis, the negative-control rule, and the mutation lane. Basis: the write-up title and AGENTS.md's "An assertion nobody has seen fail is decoration" are two independently authored artefacts converging on the same claim.
- **The control arm, with its numbers, as the centrepiece.** One worked experiment carries the credibility for everything else: an identical delay injected after a lock release failed every run, the same delay before the release passed every run, against a roughly one-in-six baseline. AGENTS.md's own wording notes the control arm "is what ruled out 'it is just slower under load', which every previous look at that flake had concluded."
- **Name the disabled tests, with their dates, in the published document.** A four-row table: test, year muted, what it is. The date column is the point, because it shows the rule is newer than the debt. A sceptic runs that grep within a minute of reading a document that boasts about flake discipline, so getting there first converts the most damaging finding into the proof of honesty.
- **Refuse to state a test count.** Make it an explicit stated policy, with the reason: counts are the numbers a suite can inflate without improving. The project already owns the stronger ground, having an artefact class built around tests that passed vacuously.

### Also raised

Open with one table of lanes, commands, and whether each gates a merge. Generate the defect section from `docs/solutions/` frontmatter rather than writing it, since defect, proof and guard are already machine-readable there. Publish the quarantine rules plus the check's literal output rather than describing the system. Publish the tooling's own limits (a flaky test silently aborts the mutation run; the chaos job selects zero tests while its subject is quarantined). Note that the chaos probes are calibrated against a defect that really happened, red on pre-fix compositions and green on fixed. Close with the reproducer rather than a summary, inviting the reader to falsify. Cut ArchUnit, module breadth, and the SpotBugs line, with the argument made rather than assumed. One correct sentence on 1.0.

---

## R: the living roadmap

**The contradiction that has to be resolved before publishing.** `STRATEGY.md` says Performance is "the main track", but the work actually queued near-term is reliability, refactoring, docs and the experiments, and the open-issue distribution agrees. The moment the roadmap publishes an order, that contradiction is public. Two honest resolutions: the roadmap explains that the main track runs last because you cannot tune what you cannot measure or trust, or STRATEGY's wording changes from "main" to something about where long-term value sits.

**A live defect found in passing.** The README's roadmap sentence says the upstream mirror covers "all 78 of them". The real figure is now higher. A mechanical count in reader-facing prose that nothing keeps current, sitting in the exact sentence that has to be edited anyway.

### Strongest

- **The roadmap is the time axis over STRATEGY's tracks, and inherits rather than restates them.** STRATEGY answers why these four areas and gives no ordering at all. Themes carry a track tag rather than living under a track heading, because real themes serve more than one. Test: if someone edits STRATEGY's track descriptions, does any roadmap sentence become wrong? If yes, it is duplicating.
- **Rule on the four taxonomies and demote one.** Tracks, `area/*` labels, ledger prefixes and roadmap themes all claim to organise the same work. The area labels are the redundant layer - three of them are the tracks under different names. Re-cut them to match track names so the roadmap can link a track to a live label query.
- **Horizons are the release-train labels that already exist**, not invented now/next/later buckets. A theme's horizon becomes a fact about the tracker rather than an opinion in a document, and moving a theme means relabelling its issues. It also keeps the no-dates rule structurally rather than by discipline.
- **Every theme carries a clickable anchor; unanchored ones go below a line.** Each theme names one artefact proving it exists: a label query, a PR, a branch, a written spec. This is the standard the maturity work already set - do not assert what the tree contradicts.
- **State 1.0 as three gates, each with its evidence type, and say which one cannot be checked by reading.** The API gate has an unbuilt mechanism to point at, the functionality gate is the theme list, and the confidence gate is deliberately not inspectable - its honest indicator is STRATEGY's hand-counted production deployments.

### Also raised

Define a theme by a test rather than a size, and cap the count with one-in-one-out. A "not on the roadmap, and why" section given equal weight, populated from decisions already made. Publish the measurement-before-optimisation dependency edge, which STRATEGY hands over by conceding its central metrics are not emitted today. A freshness contract stamping the release at which it was last reviewed, so it degrades safely without anyone remembering. Make it load-bearing internally by having the release-notes step read it, and check it rather than generate it. Restructure the README paragraph into three doors - changelog for what shipped, roadmap for where this is going, tracker for what is happening now.

---

## F: the per-PR feature index

Its inclusion in the release is still unresolved, and the candidates were generated without assuming it.

### Strongest

- **Define user-visible as "the public surface moved", and let the diff pull the trigger.** Stop asking authors to judge. The gate reads the diff for the public configuration and API surface; everything else never trips it, so N/A is not needed and cannot be abused. `changelog-ref-gate.js` already picked this shape for its own scoping problem and wrote the reasoning into its source.
- **Make the page cite the code it documents, and check both directions.** Front matter carries the option names, classes or meters the page covers; a script walks it both ways exactly as the quarantine registry does. Existence checks catch missing docs; nothing else catches wrong docs, which is the commoner and worse failure.
- **A coverage baseline that may only shrink.** Do not backfill the existing surface and do not exempt it. Commit a baseline of today's undocumented capabilities, fail if it grows, pass if it shrinks, delete the file when it empties. It answers "the directory is empty at release" without a documentation sprint.
- **Required questions, not a required length.** Fixed headings that are the user's questions - what it does, when to reach for it and when not to, how to configure it, what to watch out for. Nobody can write a release note under "when to reach for it"; the question refuses the answer. Solves the empty-file and voice problems with one mechanism.

### Also raised

A voice linter banning change-time language, since voice is the only requirement in the proposal with no enforcement attached. Reframe user-visible as "could a user plausibly file an issue about this", which is a judgement a contributor can make at eleven at night. State the collision test every future front-matter field must pass, since numeric ordering prefixes would bring the conflict back in a new costume. Decide the index question deliberately: committed is safe only when generated and CI-checked, which is how the TODO index went from cautionary case to working pattern. Split or retire the "Docs updated" checklist box once a machine-derived gate covers end users. Seed the directory by extracting README topic sections that are already feature pages, replacing each with a pointer so the two corpora cannot diverge. Scaffold the file and make the CI failure message the entire documentation, because the deferral argument turns on contributor cost and that is a property of the failure path. Ship with a stated kill condition, which makes approving it a smaller commitment while its inclusion is unresolved.
