# `bin/inflight.mjs` - worked examples

The front door for querying this repository across **every ref**, rather than the working tree. Run
`node bin/inflight.mjs help` for the command list and `help <command>` for one command's usage; this
document is the part help text cannot carry - what the answers look like on real questions, and why
the working-tree version of each answer is wrong.

**The measurement the whole tool rests on:** 580 of the 901 documents under `docs/` exist ONLY on
branches that have not merged, and 570 in-flight note paths exist across the refs against 165 on
`origin/master`. A working-tree `grep` therefore answers a narrower question than the one asked, and
returns a false negative carrying the authority of a completed check.

## Finding out whether a failing test is already known

The case that produced this document. `PartitionStateCommittedOffsetIT.committedOffsetRemoved[1]`
failed in CI on a PR that changed no Java at all.

```
node bin/inflight.mjs prior-art --headings committedOffsetRemoved
```

Three hits, and the third is the one that matters:

| Where | What it says | On master? |
|---|---|---|
| `docs/plans/2026-08-05-001-investigate-committedoffset-latest-reflake.md` | it re-flaked after astubbs#80 un-quarantined it | yes |
| `docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md` | a *previous* instance, SOLVED - an offset-reset race in the test harness | yes |
| `docs/inflight/test-load-tightness-flakes.md` | live entries dated 2026-09-01 tracking **this exact test and parameter**, still re-flaking | **no - 16 refs** |

A working-tree search finds the first two and concludes the flake is fixed. The ledger saying it is
*actively re-flaking* is on sixteen branches and none of them is master. That is the failure mode the
tool exists for, caught on its own pull request.

**`--headings` is why this is readable.** The same term unscoped returns thousands of body-text hits;
scoped to headings it returns what documents are *about*. Reach for it first on a broad term.

## Before editing a note several branches share

```
node bin/inflight.mjs note drift docs/inflight/bug-857-family.md
```

Reports only what is **divergent** - versions carrying content the baseline has never held - and
names each branch by its PR title, else the title of a note it carries that the baseline does not,
else its own name. Sizes are against each branch's merge-base, so the number says what that branch
*added*, not how far master has moved since.

The filtering is most of the value: for that note, 198 of the 274 carrying refs are merely behind.
Reporting them would bury the two dozen that actually differ.

## Finding work that will be lost if nobody acts

```
node bin/inflight.mjs stranded
```

Notes that exist on a branch and have **never** reached the baseline, clustered by the set of refs
carrying them - one workstream's notes share their refs, so listing them per path buries the finding
under its own volume.

Three filters run first, and the middle one was expected to do most of the work and did almost none:
a note present on the baseline now; a note whose blob lives there under another name (a rename,
proven exactly); and a note the baseline's history once held, which landed and was `git rm`d when its
work closed. What survives is genuinely unlanded.

## Locating a note you can only half-name

```
node bin/inflight.mjs note find 857
```

Substring match over every note path that exists on any ref - including the ones that never reached
master, which is most of them. Use it to get the path that `note drift` wants.

## What the exit codes mean

**0 means it RAN, whatever it found. 2 means it could not run.** Every command distinguishes these,
and the distinction is load-bearing rather than decorative: two P0 defects found while building this
were both cases where a failure rendered as a confident empty result. "Nothing, across 436 refs" is a
result; a blank line is not, and neither is a search that never happened.
