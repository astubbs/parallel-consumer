# Grooming the in-flight notes - the sweep, start to finish

How a vetting sweep of `docs/inflight/` is run: when, how it is split, the prompt each agent gets,
what comes back, and how the results are consolidated. [`docs/inflight/AGENTS.md`](inflight/AGENTS.md)
-> "Vetting a note" **owns the per-note contract** - the marker, the five outcomes, the owner gate -
and this document owns the sweep around it. Follow this document each time rather than re-deriving
the sweep; the first one, on 2026-09-07, was derived in a session and this is its record.

## When

- **Before every release**, before the issue-response sweep in [`docs/releasing.md`](releasing.md):
  the release is gated on the bugs that are already open, and the notes are where those are.
- **When `bin/inflight.mjs vet` reports more unvetted than vetted notes** in an area you are about to
  work in. That is the signal the add-time duty ("when you add a note, look at the others") has
  stopped holding.

The first sweep is the expensive one - every note read against the tree once. After it the marker
makes each sweep a diff: `vet` lists the unvetted first, and a stamped note is re-read only when a
signal fires against it or its stamp is older than the last release.

## Before you start

1. `git fetch --all --prune`. The sweep reads the baseline; a stale one vets a world that has moved.
2. Read the branch context for the notes you will touch:
   `node bin/inflight.mjs docs header docs/inflight/AGENTS.md` and the same for any register you
   expect to edit. A live re-tagging or re-ranking branch elsewhere is a collision, and the header
   is where it shows.
3. `node bin/inflight.mjs vet` for the shape: counts per area, and which areas carry signals.
4. Cut a tooling branch only if the tooling needs changing; otherwise the area branches cut from
   `origin/master` directly. Every task gets a worktree - the sweep is several.

## Splitting the work

**One agent per filename-prefix area, one worktree and branch each**, cut from the same base. One
file per note is what makes this safe: two areas never touch the same note. Group the small areas
so no agent has fewer than about ten notes or more than about forty; on 2026-09-07 that was `ci`,
`test`, `core`, `bug`, `static+branch+upstream+pr`, and `issue+release+process+perf+deps` with the
singletons. Name the worktrees `.claude/worktrees/inflight-vet-<area>` and the branches
`process/inflight-vet-<area>`, and write each `.worktree-owner` before dispatch.

Give each agent a **status file of its own** in the session scratchpad, namespaced by area, and tell
it to append a line per note as it goes - that file is the only live channel; the agent's report
arrives when it stops. Dispatch with the worktree path spelled literally in the prompt.

## The dispatch prompt

The prompt below is what each agent received on 2026-09-07, with the area and paths as
placeholders. Keep the structure; change the area-specific checks paragraph to match the area.

```text
You are vetting the in-flight notes in `docs/inflight/` for the `<AREA>-` area, as part of a
grooming sweep. Today is <DATE>.

Where to work: your worktree is `<ABSOLUTE WORKTREE PATH>` on branch `<BRANCH>`. Work ONLY there.
Never touch the main checkout or any other worktree. Never `git add -A`, never `git stash`, never
push, never open a PR, never run Maven or any build or test. Do not edit notes outside your area,
and do not edit `docs/inflight/AGENTS.md`, `bin/`, or any code.

Read `docs/inflight/AGENTS.md` -> "Vetting a note" first (and skim "Rules"). It is the contract.

The job:
1. Run `node bin/inflight.mjs vet --area <AREA>`. A signal is a reason to look, never a verdict.
2. For EVERY note listed, read it and check its central claim against the tree at your HEAD.
   <AREA-SPECIFIC CHECKS: which classes to open, which registries to read, `gh ... -R
   astubbs/parallel-consumer` for cited numbers - ALWAYS with -R; a bare gh resolves to the
   wrong repository. Read the "Delete when" condition and decide whether it has been met.>
3. Apply exactly one of the five outcomes from "Vetting a note" (still true -> stamp; not now ->
   deferred; partly true -> shrink then stamp; owned elsewhere -> migrate then rm, only when small
   and clearly right; no longer true -> rm or closed). The owner gate: a `bug` at misdirection,
   blind-spot, crash, data-loss or stall <AND ANY AREA-SPECIFIC GATE, e.g. every release- note>
   gets `PROPOSED <outcome>: <evidence>` in the marker and no other change.
4. Registers are consulted, never completed: fix stale lines in place, then stamp.
5. Do not improve prose, reformat, rename, or retag unless plainly mis-tagged (say so in the
   report). Never leave a "delete this when X merges" line on a note you stamp as still true.

Recording progress: append `<filename> | <outcome> | <one-line evidence>` per note to
`<SCRATCHPAD>/vet-status/<AREA>.txt` as you go. Use `<SCRATCHPAD>/vet-status/<AREA>-*` for any
other temp file.

Finishing: run `bin/check-inflight-tags.sh` and `bin/check-file-refs.sh`; both must pass. Stage
explicit paths, write the commit message to a file, commit with
`git -C <ABSOLUTE WORKTREE PATH> commit -F <file>` (the literal path). Subject:
`docs(inflight): vet the <AREA>- notes for <RELEASE>`. Body: counts per outcome and every
deletion, closure or PROPOSED item with its reason, then the session's trailer lines. Do not push.

Report back with: a table of every note -> outcome -> evidence; the PROPOSED list; migrations and
where to; suspected mis-tags; and which notes you believe gate the release and why. If you could
not verify a claim, say so rather than guessing.
```

## While it runs

Do not read the agents' transcripts; read the status files. Work that does not touch
`docs/inflight/` is safe to do in the tooling worktree meanwhile. Expect an agent to take ten to
fifteen minutes per twenty notes.

## What comes back, and where each result goes

- **Stamped and re-stated notes** stay in place. `vet` is the progress view.
- **PROPOSED markers** are the decisions the sweep could not take. `grep -l 'inflight-vetted:.*PROPOSED'
  docs/inflight/*.md` lists them. Consolidate them into
  [`docs/inflight/process-candidate-ranking.md`](inflight/process-candidate-ranking.md)'s "Decisions
  waiting on the maintainer" section, ranked by how little input each needs - that section's own
  rule - so the owner clears them in one sitting. When a proposal is accepted, apply the outcome and
  replace the marker with a plain stamp; when declined, replace it with a stamp saying so.
- **The release gating list** - the still-open bugs the release waits on, as each agent reported
  them - goes into the ranking register too, not into a new note.
- **Migrations into `docs/refactoring.md` or `docs/solutions/`** are the one place two area
  branches can conflict. Merge the area branches one at a time and resolve by hand.

Merge the area branches with merge commits, not rebases, in any order; each stands alone. The
tooling branch, if there was one, merges first.

## Traps this sweep has already met

- **`bin/check-inflight-tags.sh | tail -1 && git commit`** takes `tail`'s exit status. A gate failure
  landed in a commit that way on the first sweep. Run the gate on its own line.
- **A false anchor signal on a spelt-in-full name.** `vet` reports `IllegalStateException` missing
  when the source names it only in a comment as `java.lang.IllegalStateException`, and reports a
  prose word (`Lazy`, about Dagger) as a symbol. The agent reads the row as a reason to look, which
  is all it is; do not "fix" the note to silence the signal.
- **Dispatching without the branch context.** The area branches are stacked on the tooling branch;
  an agent that does not know that reverses the tooling's own decisions. The dispatch hook injects
  the branch context, and the prompt above names the base anyway.
- **A "Delete when" section is not a violation on its own.** Agents on the first sweep removed some
  and kept others; the rule is that the *condition* is judged, and an unmet one may stay with the
  stamp saying it is unmet.
