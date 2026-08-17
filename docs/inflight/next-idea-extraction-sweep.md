# Idea-extraction sweep - pull stranded planning docs out of branches onto master

**Handoff: self-contained; any session can execute this without prior context.**

Decision (2026-08-18): committed planning documents are project content even when their
implementation doesn't exist - the idea is part of the project; the implementation isn't yet.
Knowledge stranded on branches is invisible to agents (the 2021 `features/rate-limiting` sketch
sat unfindable for five years; astubbs#305 had to audit 196 branches to recover attribution).
This sweep extracts ideation/planning/notes documents from unmerged branches onto master in
**one PR**, so agents never have to go searching branches for thinking.

## Method

- **One extraction PR**, not one per branch. Copy files out of source branches, commit,
  and let later work resolve any conflicts - conflicts are cheap, stranding is not.
- **Sequencing:** build on astubbs#305's branch audit (its orphans note
  `docs/inflight/next-branch-audit-orphans.md` and the map back-fills are the starting
  inventory). If astubbs#305 is merged, branch from master; if still open, either wait or
  stack on its branch with a `depends on #305` line in the PR body.
- **Provenance header on every extracted doc:** source branch and original path (primary),
  plus the branch tip's short SHA at extraction time (secondary - survives branch re-cuts and
  deletions, per the `docs/refactoring.md` idea-bank convention). Example:
  `> Extracted from origin/docs/ideate-dapr-adapter @d5c8cf1b0, src/docs/dapr-adapter.md`.
- **Triage each doc into one of three shapes:**
  1. Ideation / planning docs -> `docs/ideation/` or `docs/plans/` (a plan for unbuilt work is
     the same species as an ideation doc - committed thought).
  2. Working-state / open-item notes -> `docs/inflight/` with the correct prefix.
  3. Docs entangled with implementation code -> leave on the branch; record the entanglement
     as a `branch-` inflight note instead.
- **Do not** extract generated artifacts, code, or anything a command can answer. Docs only.
- Cross-reference: add extracted ideation/plan docs to `docs/refactoring.md`'s idea bank or
  the relevant inflight note where one exists; keep `src/docs/development/upstream-map.yaml`
  as the mapping source of truth (record mappings there, not in the notes).
- Run `bin/check-issue-refs.sh` before push - extracted older docs likely carry unqualified
  `#NN` references that the gate now rejects; qualify them during extraction.

## Starting inventory (not exhaustive - verify against `git ls-remote --heads origin`)

- `origin/docs/ideate-dapr-adapter` @d5c8cf1b0 and `origin/docs/ideate-perf-comparison-matrix`
  @3dd359260 - ideation-shaped branches, likely extract-whole.
- The three performance-visualisation/comparison branches (identify by name/content) - LIVE
  work, not archaeology: agents there are editing each other's documents because no shared
  base exists on master. Extract the foundational doc first so the others rebase onto it;
  coordinate with any active sessions before moving their files.
- `docs/inflight/next-branch-audit-orphans.md` (lands with astubbs#305) - the unattributed
  remainder; check each orphan for extractable docs.
- Skip branches already catalogued as design-reference-only in `docs/refactoring.md`'s idea
  bank unless they contain actual doc files worth lifting.

## Definition of done

Every unmerged branch either (a) has its planning docs extracted to master with provenance,
(b) is recorded as implementation-entangled in an inflight `branch-` note, or (c) is
catalogued as having no extractable docs. The sweep PR body lists the per-branch verdict -
including "nothing found", which is a real result.
