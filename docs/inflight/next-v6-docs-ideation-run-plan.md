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
| **F** | `next-per-pr-docs-and-feature-index.md` | A `docs/features/` convention, front matter schema, and PR gate |

## They are not four independent tracks

The instinct to fan out is right, but only one of them is genuinely independent. The notes
cross-reference each other, and three of those references are load-bearing:

- **T feeds M.** M's readiness claim rests on a pointer to how defects were found and proven, which is
  T. M's own note says the engineering-system detail belongs in T and that the table should reference
  it rather than duplicate it. Two agents writing that content independently produces duplication that
  only surfaces at convergence.
- **M feeds R.** R anchors on a stated definition of 1.0, and M is where the definition comes from:
  pre-1.0 reserves the API surface, not reliability. R cannot state exit criteria without M's two-axis
  split.
- **F feeds R.** F is R's "living document" trigger: the per-PR feature file is what tells the roadmap
  a theme has shipped.

F is otherwise orthogonal. It is also the only one of the four with a code component (front matter
schema, checklist gate), so it is the only one whose review lens is code rather than prose.

What makes T, M and R parallelisable despite the chain is that the coupling sits at **three named
interfaces**, not throughout the content. Pin those interfaces in the seed and the agents stop needing
to talk to each other.

## The seed: what every agent is given before it starts

One shared brief, identical for every agent, covering the standard to write to and the interfaces not
to renegotiate. This is the section to lift verbatim into each agent's prompt.

### The three interface contracts

1. **The two-axis vocabulary.** Reliability and API stability are separate axes. Pre-1.0 reserves the
   API surface, not reliability. Every agent uses those words and does not invent synonyms.
2. **Engineering-system detail lives in T.** The CI and analysis apparatus, the lanes, mutation
   testing, automated review coverage. M and R reference it; only T describes it.
3. **The release gate is not met yet.** The `confluentinc#857` deadlock is still open, with the fix in
   astubbs#29 needing rebase and retarget. No agent writes an unqualified "all known critical defects
   are fixed". M's note explains the design: write as though the gate holds, name the check, amend the
   wording at release if it has not.

### The standing constraints

- `README.adoc` is generated. Edit `src/docs/README_TEMPLATE.adoc`, never the README. Regenerate with
  `./mvnw -N asciidoc-template:build` and commit the regenerated `README.adoc` in the same commit. No
  CI check catches template-to-README drift, so an unregenerated edit is invisible to every reader
  while every check stays green.
- Do not build anything new that depends on `README_TEMPLATE.adoc` embedding other documents. Source
  is `parked-docs-site.md`; the docs site is astubbs#208, parked.
- Maturity is per module. One alpha experiment must not downgrade how the core describes itself, and
  the core's stability must not be lent to the alpha.
- These are ideas, not release commitments. Nothing here is agreed as release-blocking.
- Issue references below `#1000` name the repo: `astubbs#NNN` or `confluentinc#NNN`. Enforced on added
  lines by `.github/scripts/issue-ref-gate.js`.
- No em-dashes or double-dashes. No mechanical counts in prose.
- `docs/inflight/AGENTS.md` governs the ledger, including retirement by `git rm`.

### The reader standard

T's note already contains the house voice, and it applies to all four: write for the sceptical expert
who skims, whose judgement gets repeated second-hand. Every claim independently checkable in seconds,
naming the test class or script or number. Brevity as a requirement rather than a preference.

### What the ideation is for

The notes already settle *whether* and largely *what*. Ideation that regenerates those decisions burns
tokens re-deciding settled questions. Bound each agent to the shape and content of the artefact: the
status vocabulary and row set for M, the document shape and publication venue for T, the file location
and exit-criteria content for R, and for F the design questions its own note already lists open (front
matter schema, and what "user-visible" means precisely). An agent that finds its note leaves the
*whether* open should say so and stop, not invent an answer.

## The wave plan

A wave covers steps 1 to 3 of the cycle below and stops. The parallelism is in the ideation, because
that is where the agents are reading the same repository and answering comparable questions. Once a
direction is picked per idea, the rest of that idea's cycle is its own sequence and only needs to run
in parallel if the tokens are there for it.

**Wave 0, one agent, M.** M is the cheapest and most self-contained: it lands next to the existing
"Java Version per Module" table in `README_TEMPLATE.adoc`, which needs the new modules added anyway,
and its note already specifies columns and candidate rows. It runs first for two reasons beyond cost.
It is where the two-axis vocabulary is actually decided, so its output gets promoted into the seed
rather than guessed at by this plan. And it is a cheap test of whether the seed works at all, before
the same seed is handed to three agents at once.

Review M's output, fold the settled vocabulary into the seed, then:

**Wave 1, three agents in parallel: T, R, F.** All three now consume a settled vocabulary. T and R
additionally read M's ideation output. F needs neither and could equally have run in wave 0 alongside
M; running it here keeps the review load in one batch.

**Wave 2, reconcile, no subagent.** Check the three interface contracts actually held, fold the
cross-references between the artefacts, and resolve any collision on `README_TEMPLATE.adoc` between M
and T. Cheap, and it is the step that fan-out makes necessary.

## The full cycle each idea runs

Every idea runs the same pipeline end to end. It is written out in full because the tail is the part
that gets dropped once the artefact exists and looks finished.

| Step | Skill | Notes |
|---|---|---|
| 1 | `ce-worktree` | One worktree per idea, off the branch settled below, held for the whole cycle |
| 2 | `ce-ideate` | Seeded by the note plus the shared brief. Bounded to the shape of the artefact |
| 3 | *user picks* | A check-in, not a step. The agent stops here |
| 4 | `ce-plan` | Only where the chosen direction is more than one artefact. M and R will not need it; F almost certainly will |
| 5 | `ce-work` | Writes the artefact. For M this is the template edit; for F the directory, schema and gate |
| 6 | `ce-simplify-code` | F only, and only its gate and schema |
| 7 | `ce-doc-review` or `ce-code-review` | Prose for T, M and R. Code for F's gate. Both for F overall |
| 8 | *apply the findings* | Distinct from step 7 and routinely skipped. A review that is read but not applied has cost tokens and changed nothing |
| 9 | `ce-commit` | Freely, on the idea's own branch |
| 10 | *verify* | `./mvnw -N asciidoc-template:build` for anything touching the template, and commit the regenerated README; for F, that the gate actually fails a PR missing its file |
| 11 | `ce-compound` | Per idea, while its branch still exists, not deferred to the end of the run |

Steps 3 and 8 are the two the run will try to skip. Step 3 because the ideation output reads as a
decision already made, and step 8 because the review reads as the finish line.

`ce-pov` is worth one run against T specifically, once written. T is the only artefact addressed to a
hostile reader, and a holistic verdict on whether it survives that reader is a different question from
whether the prose is good.

The shipping tail runs once, at convergence, not per idea: `ce-commit-push-pr`, then `ce-babysit-pr`
and `ce-resolve-pr-feedback` until merge. Pushing is not the end of it. Watch the checks and the
review reports through to completion, and read the duplication and similarity reports rather than the
tick.

## Mechanics

**Worktrees.** One per idea, off `docs/v6-release-ideas`, living for that idea's whole cycle rather
than being recreated per CE step. Nothing runs in the main checkout.

**File ownership**, so no two agents ever hold the same file:

| Agent | Owns |
|---|---|
| M | `docs/inflight/next-module-maturity-table.md` |
| T | `docs/inflight/next-testing-suite-as-product-docs.md` |
| R | `docs/inflight/next-living-roadmap.md` |
| F | `docs/inflight/next-per-pr-docs-and-feature-index.md` |
| nobody during fan-out | `docs/inflight/release-0.6.0.0.md`, reconciled in wave 2 |

Each agent folds its chosen direction back into its own note, in place. That is what the ledger wants
anyway: the note stops being an idea and becomes a spec carrying its rejected alternatives, and it is
`git rm`'d when its "Delete when" condition is met.

M and T may both want to edit `README_TEMPLATE.adoc` in the implementation phase. Let them. Put each
piece where it belongs and resolve the conflict at convergence rather than relocating content to avoid
it.

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

Wave 0 alone is a complete outcome: the maturity table published and the pre-1.0 wording corrected is
the cheapest of the four and settles the vocabulary the rest depend on. Wave 1 can wait indefinitely
after that without leaving anything half-built. Within wave 1, F can be dropped without affecting T or
R, at the cost of R's living-document trigger having no feed.

## Delete when

All four notes have been retired by their own "Delete when" conditions, or the run is abandoned.
