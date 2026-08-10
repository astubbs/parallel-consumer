# How to run the CE cycle over the 0.6.0.0 documentation ideas

The four promotional and documentation ideas for 0.6.0.0 are specs, not artefacts. This is the plan
for turning them into artefacts: which agents run, in what order, what they share, and what each one
owns. Nothing here has been executed. Written on `docs/v6-release-ideas`, to be run from Monday.

The four ideas, referred to below by letter:

| | Note | Deliverable |
|---|---|---|
| **T** | `next-testing-suite-as-product-docs.md` | A published section presenting the test suite as a product |
| **M** | `next-module-maturity-table.md` | A per-module maturity table in `src/docs/README_TEMPLATE.adoc` |
| **R** | `next-living-roadmap.md` | A themes-level roadmap in markdown, plus the 1.0 exit criteria |
| **F** | `next-feature-index.md` | A machine-readable index of the product features, backfilled, seeding the docs site |

## They are not four independent tracks

The instinct to fan out is right, but only one of them is genuinely independent. The notes
cross-reference each other, and three of those references are load-bearing:

- **T feeds M.** M's readiness claim rests on a pointer to how defects were found and proven, which is
  T. M's own note says the engineering-system detail belongs in T and that the table should reference
  it rather than duplicate it. Two agents writing that content independently produces duplication that
  only surfaces at convergence.
- **M feeds R.** R anchors on a stated definition of 1.0, and M is where the definition comes from.
  Wave 0 has now settled it, and corrected it: 1.0 waits on a settled API surface, on functionality
  still wanted, and on critical-bug confidence. R cannot state exit criteria without that, and must
  not fall back on the shorter, false version.
- **F feeds R.** F is R's "living document" trigger: the feature page is what tells the roadmap
  a theme has shipped.

F is otherwise orthogonal in content. It is also the only one of the four with a code component (front
matter schema, checklist gate), so it is the only one whose review lens is code rather than prose.

What makes these parallelisable despite the chain is that the coupling sits at **four named
interfaces**, not throughout the content. Pin those interfaces in the seed and the agents stop needing
to talk to each other. The F to R feed is the one to watch: it is the only coupling whose two ends are
both open questions being answered in the same wave, so it is the one most likely to be designed twice
and discovered at convergence.

## The seed: what every agent is given before it starts

One shared brief, identical for every agent, covering the standard to write to and the interfaces not
to renegotiate. This is the section to lift verbatim into each agent's prompt. It is genuinely
identical: where an agent needs an extra input, that input is named in the wave plan below rather than
folded in here, so no agent has to wonder whether it is missing context a sibling was given.

### The four interface contracts

1. **What 1.0 is waiting on.** Reliability and API stability are separate axes, and the release claims
   reliability now while reserving the API surface. But **1.0 is waiting on three things, not one**: a
   settled API surface, functionality still wanted before 1.0, and confidence about critical bugs
   rather than only about the known set. No agent writes "pre-1.0 reserves the API surface, not
   reliability" - that sentence is neat and false, and it survived being restated through a note, this
   seed and four rounds of review before wave 0 caught it. Settled wording is in
   `next-module-maturity-table.md`; R's 1.0 exit criteria anchor on that, not on the shorter version.
2. **Engineering-system detail lives in T.** The CI and analysis apparatus, the lanes, mutation
   testing, automated review coverage. M and R reference it; only T describes it.
3. **The release gate is not met yet.** The `confluentinc#857` deadlock is still open, with the fix in
   astubbs#29 needing rebase and retarget. No agent writes an unqualified "all known critical defects
   are fixed". M's note explains the design: write as though the gate holds, name the check, amend the
   wording at release if it has not. **The check runs at merge, not only at the release cut.** The
   generated README is the front door and updates on every merge, so wording that assumes the gate
   holds would be publicly readable, and falsifiable, for the whole gap between merge and release. If
   astubbs#29 has not landed by wave 2, what merges is the qualified wording that names the open
   defect.
4. **The shipped-theme trigger.** A theme counts as shipped when a `docs/features/` file carrying a
   `since-version` front matter field lands. F owns the directory name and the schema; R consumes only
   that field. Neither renegotiates it, and neither waits on the other to know what it is.

### The standing constraints

- `README.adoc` is generated. Edit `src/docs/README_TEMPLATE.adoc`, never the README. Regenerate with
  `./mvnw -N asciidoc-template:build` and commit the regenerated `README.adoc` in the same commit. No
  CI check catches template-to-README drift, so an unregenerated edit is invisible to every reader
  while every check stays green.
- Do not build anything new that depends on `README_TEMPLATE.adoc` embedding other documents. Source
  is `parked-docs-site.md`; the docs site is astubbs#208, parked.
- **The venue for this run is the repository as read on GitHub.** The docs site is a later import, not
  a precondition, so no artefact may depend on it existing. Each stands alone as markdown under
  `docs/`, linked from the README template by URL rather than embedded in it: T's section at
  `docs/testing.md`, R's roadmap at `docs/roadmap.md`. Only M's table lands inside
  `README_TEMPLATE.adoc` itself. These addresses are settled here rather than in ideation, because M
  has to write a reference to T's section before T's own cycle has run.
- Maturity is per module. One alpha experiment must not downgrade how the core describes itself, and
  the core's stability must not be lent to the alpha.
- These are ideas, not release commitments. Nothing here is agreed as release-blocking.
- Issue references below `#1000` name the repo: `astubbs#NNN` or `confluentinc#NNN`. Enforced on added
  lines by `.github/scripts/issue-ref-gate.js`.
- No em-dashes or double-dashes. No mechanical counts in prose.
- **Never cite a line number.** Cite the file, the anchor, the heading or the quoted text instead. Line
  numbers are mechanical counts wearing a citation's clothes: a single merge from master shifted every
  one this run had written, silently, within minutes of writing them, and nothing catches it.
- **A contract can be wrong, and saying so is your job.** These contracts exist to stop four agents
  renegotiating the brief in four directions, not to make the brief true. If evidence contradicts one,
  stop and say so rather than working around it or quietly satisfying its letter. Wave 0 broke two of
  its own contracts and both breaks were improvements; neither was found by an agent inside the brief.
- `docs/inflight/AGENTS.md` governs the ledger, including retirement by `git rm`.

### The reader standard

T's note already contains the house voice, and it applies to all four: write for the sceptical expert
who skims, whose judgement gets repeated second-hand. Every claim independently checkable in seconds,
naming the test class or script or number. Brevity as a requirement rather than a preference.

That standard governs the *published* view. R also serves internal planning and scheduling, and an
agent handed only the sceptical-skimmer brief will produce a promotional roadmap that nothing internal
depends on, which is precisely how a living document rots in public. R must additionally name what
internal use keeps it current, and who updates it at a release cut.

### What the ideation is for

The notes already settle *whether* and largely *what*. Ideation that regenerates those decisions burns
tokens re-deciding settled questions. Bound each agent to the shape and content of the artefact: the
status vocabulary and row set for M, the document shape for T, the exit-criteria content for R, and for
F the design questions its own note lists open: the front matter schema, what "user-visible" means
precisely, and whether a user-visible-feature PR must add a `docs/features/` file specifically or can
satisfy the existing checklist box with any documentation edit. That third one is a real fork and was
missing from an earlier draft of this plan; the verification in step 10 below depends on the answer, so
it cannot be quietly assumed. An agent that finds its note leaves the *whether* open should say so and
stop, not invent an answer.

## The wave plan

A wave covers steps 1 to 3 of the cycle below and stops. The parallelism is in the ideation, because
that is where the agents are reading the same repository and answering comparable questions. Once a
direction is picked per idea, the rest of that idea's cycle is its own sequence and only needs to run
in parallel if the tokens are there for it.

**Wave 0, one agent, M.** M is the cheapest and most self-contained: it lands next to the existing
"Java Version per Module" table in `README_TEMPLATE.adoc`, and its note already specifies columns and
candidate rows. It runs first because it is the artefact the other three quote from, so settling it
once costs less than reconciling three independent readings of it later.

Be honest about what wave 0 does not do: it is not a test of whether the seed constrains an agent. M
authors the contracts rather than inheriting them, so a clean wave 0 says nothing about whether an
agent handed someone else's vocabulary will honour it. The seed stays unproven until wave 1 returns.

M's table covers the modules actually in the reactor on this branch. The Streams and Connect rows are
blocked on those modules merging and belong to whichever PR lands the module, not to M. Publishing
maturity rows for artifacts a reader cannot resolve on Maven Central is the one thing the table must
not do.

**Alongside wave 0, and not by an agent:** astubbs#29 needs its rebase and retarget. It is the only
named release blocker, and nothing in this run makes it land. The documentation can be written while it
is open; it cannot merge on the unqualified wording while it is open. Give it an owner before wave 0
starts, or accept that the qualified wording is what ships.

Review M's output, fold the settled vocabulary into the seed, then:

**Wave 1, three agents in parallel: T, R, F.** All three now consume a settled vocabulary. T and R
additionally read M's ideation output. F needs neither and could equally have run in wave 0 alongside
M; running it here keeps the review load in one batch.

**Wave 2, reconcile, no subagent.** Check the four interface contracts actually held, fold the
cross-references between the artefacts, and resolve any collision on `README_TEMPLATE.adoc` between M
and T. Run contract 3's release-gate check here, before any wording reaches master. Record in
`release-0.6.0.0.md` which artefacts the release announcement links, and which sentences need
re-tensing when the release is actually cut: merging prose that describes 0.6.0.0 in the present tense
before 0.6.0.0 exists recreates exactly the problem that file already has to clean up across the
mirrors. Cheap, and it is the step that fan-out makes necessary.

## The full cycle each idea runs

Every idea runs the same pipeline end to end. It is written out in full because the tail is the part
that gets dropped once the artefact exists and looks finished.

| Step | Skill | Notes |
|---|---|---|
| 1 | `ce-worktree` | Only once this idea runs concurrently with another. A solo wave works in `v6-ideas` directly. See Mechanics |
| 2 | `ce-ideate` | Seeded by the note plus the shared brief. Bounded to the shape of the artefact |
| 3 | *user picks* | A check-in, not a step. The agent stops here |
| 4 | `ce-plan` | Only where the chosen direction is more than one artefact. M and R will not need it; F almost certainly will |
| 5 | `ce-work` | Writes the artefact. For M this is the template edit; for F the directory, schema and gate |
| 6 | `ce-simplify-code` | F only, and only its gate and schema |
| 7 | `ce-doc-review` or `ce-code-review` | Prose for T, M and R. Code for F's gate. Both for F overall |
| 8 | *apply the findings* | Distinct from step 7 and routinely skipped. A review that is read but not applied has cost tokens and changed nothing |
| 9 | `ce-commit` | Freely, on the idea's own branch |
| 10 | *verify* | `./mvnw -N asciidoc-template:build` for anything touching the template, and commit the regenerated README; for F, that the gate matches whatever its scope question was answered with, exercising the bot-authored and N/A paths rather than only the red one |
| 11 | `ce-compound` | Per idea, while its branch still exists, not deferred to the end of the run |

Steps 3 and 8 are the two the run will try to skip. Step 3 because the ideation output reads as a
decision already made, and step 8 because the review reads as the finish line.

`ce-pov` is worth one run against M and T together, once both exist. T is the evidence, but M is where
the falsifiable production-readiness claim is actually made, on the README a visitor sees first and in
the wording hardest to walk back. A holistic verdict on whether that claim survives a reader looking
for a reason to discount it is a different question from whether the prose is good.

The shipping tail runs once, at convergence, not per idea: `ce-commit-push-pr`, then `ce-babysit-pr`
and `ce-resolve-pr-feedback` until merge. Pushing is not the end of it. Watch the checks and the
review reports through to completion, and read the duplication and similarity reports rather than the
tick.

## Mechanics

**Worktrees: branch when the concurrency arrives, not before.** The isolation exists for one reason,
which is that agents writing files at the same time in one checkout share an index and see each
other's half-finished work. A wave running a single agent has no concurrency to isolate from, so it
works in the `v6-ideas` worktree directly: the note it edits and the plan it is seeded from are
already there, its commits land on `docs/v6-release-ideas` with nothing to merge back, and no
integration cost is incurred. Wave 0 is exactly this case.

The moment a wave runs more than one agent, each gets its own worktree off `docs/v6-release-ideas`,
held for that idea's whole cycle rather than recreated per CE step, and each writes a
`.worktree-owner` marker at creation recording owner, branch and idea letter, per AGENTS.md "Worktree
ownership". Without the marker `bin/worktree-status.sh` cannot tell concurrent worktrees apart, and
the pre-deletion safety check has nothing to read when they are torn down. Nothing runs in the main
checkout in either case.

**File ownership**, so no two agents ever hold the same file:

| Agent | Owns |
|---|---|
| M | `docs/inflight/next-module-maturity-table.md` |
| T | `docs/inflight/next-testing-suite-as-product-docs.md` |
| R | `docs/inflight/next-living-roadmap.md` |
| F | `docs/inflight/next-feature-index.md` |
| nobody during fan-out | `docs/inflight/release-0.6.0.0.md`, reconciled in wave 2 |

Each agent folds its chosen direction back into its own note, in place. That is what the ledger wants
anyway: the note stops being an idea and becomes a spec carrying its rejected alternatives, and it is
`git rm`'d when its "Delete when" condition is met.

M and T may both want to edit `README_TEMPLATE.adoc` in the implementation phase. Let them. Put each
piece where it belongs and resolve the conflict at convergence rather than relocating content to avoid
it.

**Integration, for whichever ideas ran on their own branch.** Merge them into `docs/v6-release-ideas`
in the order M, T, R, F. M first because the other three quote it; T second because that is where the
`README_TEMPLATE.adoc` conflict lands, and resolving it against one prior change is cheaper than
against three. Regenerate `README.adoc` once, after the last merge, then run the shipping tail on the
integrated branch. An idea that ran solo in `v6-ideas` is already integrated and skips this entirely.
Wave 2 is not this step and cannot be: a wave stops at ideation, so when wave 2 runs no artefact has
been written and there is no template collision to resolve yet.

**Shared scratchpad.** One scratchpad for the run, not one per agent, with every temp file namespaced
by agent letter. Every agent gets the same preamble naming who else is running concurrently and which
files they own. Raw ce-ideate option sets go there; only the chosen direction is committed.

## Settled: the work stays on this branch

Everything proves out here. The notes reach `master` the ordinary way, through the merge, so there is
no case for splitting them off early to make them visible sooner. Every idea's worktree branches from
`docs/v6-release-ideas`.

Whether that merge arrives as one PR or several is decided once there is something to look at, not
now. F is separable on content and is the only one touching CI, so it is the obvious candidate if a
split is wanted, but that is an observation rather than a plan.

## Where it is safe to stop

Wave 0 alone is a complete outcome, with one condition attached. The maturity table published and the
pre-1.0 wording corrected is the cheapest of the four and settles the vocabulary the rest depend on.
But M's readiness claim is written to point at T, so if T is deferred indefinitely the claim points at
nothing, and an unverifiable assertion is exactly what the reader standard says discredits everything
around it. If wave 0 stands alone, M's claim must point directly at `docs/solutions/` and the named
`bin/` test lanes instead. Wave 1 can then wait indefinitely without leaving anything half-built.
Within wave 1, F can be dropped without affecting T or R, at the cost of R's living-document trigger
falling back to release cuts as its only feed.

## Delete when

The four artefacts are written and merged, and each note has been reduced to whatever docs-site
follow-up it has left. Two of the notes cannot fully retire until astubbs#208 ships, which is not this
run's to deliver, so retirement of this file is deliberately not gated on it.

## Deferred / Open Questions

### From the 2026-08-08 review

Two findings were deliberately not applied, because answering them is not the agent's call.

- **Where does F belong?** The reviewers split, and both arguments are good. One says move it
  *earlier*, into wave 0 alongside M, on the grounds that F is the only agent that inherits the seed's
  contracts rather than authoring them, and is therefore the only real test of whether the seed
  constrains anyone. The other says move it *out* of this run entirely until after 0.6.0.0 ships, on
  the grounds that F delivers nothing a release reader can look at (the directory is empty at release),
  its value arrives across PRs not yet written, and it is the only item that raises the cost of
  contributing to a fork actively trying to attract contributors away from an unmaintained upstream.
  The plan as written leaves F in wave 1, which is the option neither reviewer argued for.
- **What does fan-out buy over one agent holding all four notes in context?** Every mechanism in this
  plan exists only because the work is split: the shared seed, the four contracts, the wave 2 reconcile,
  the ownership table, the shared scratchpad. The stated reason to parallelise is that the agents read
  the same repository and answer comparable questions, which is also precisely the condition a single
  agent amortises best. The coordination apparatus is currently unpriced against that alternative.

  First evidence, from wave 0: a solo wave needs none of it. No worktree, no ownership marker, no merge
  order, no integration step, no reconcile. That is not an answer, but it is a data point, and the
  answer should account for it.
