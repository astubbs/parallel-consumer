# The v6 vetting sweep of these notes - what is in flight and how it lands

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

**One tooling branch, six area branches stacked on it, merged in that order.** `process/inflight-vet`
carries `bin/inflight.mjs vet`, the `inflight-vetted` marker and the "Vetting a note" section of
[`AGENTS.md`](AGENTS.md). Each `process/inflight-vet-<area>` branch is one agent's sweep of one
filename-prefix area (`ci`, `test`, `core`, `bug`, `rest` = static/branch/upstream/pr, `rest2` =
issue/release/process/perf/deps and the singletons), cut from the tooling branch so the marker and
the gate exist when they stamp. One file per note is what lets six sweeps run at once without a
conflict; the area branches touch no shared file except by migration into `docs/refactoring.md` or
`docs/solutions/`, which a reader resolves by hand when two do.

`git branch --list 'process/inflight-vet*'` and `bin/worktree-status.sh` are the live state; this
note does not repeat them.

## What the sweep produces, and where each result goes

- **A stamped or re-stated note** stays where it is. `bin/inflight.mjs vet` is the progress view:
  the unvetted count is what is left.
- **`PROPOSED` markers on owner-gated notes** - a `bug` at `misdirection` through `stall`, every
  `release-` note - are the decisions the sweep could not take.
  `grep -l 'inflight-vetted:.*PROPOSED' docs/inflight/*.md` lists them. When the area branches have
  merged, those proposals are consolidated into
  [`process-candidate-ranking.md`](process-candidate-ranking.md)'s "Decisions waiting on the
  maintainer" section, ranked by how little input each needs - the same rule that section already
  states - and the markers are then resolved one way or the other.
- **The v6 gating list** - which still-open bugs the release waits on - is the last output. It is
  written into the ranking register, not into a new note, and not into
  [`release-when-is-v6-good-enough.md`](release-when-is-v6-good-enough.md), whose job is the
  "is it enough?" question rather than the blocker list.

## Delete when

The six area branches have merged, the `PROPOSED` markers have been consolidated into the register
and resolved, and the v6 gating list is in the register. Then this note has nothing left that a
command or the register does not say.
