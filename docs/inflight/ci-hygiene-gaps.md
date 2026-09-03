# `bin/check-all.sh` cannot reach `todo-index.sh --check`

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

**The pre-push sweep reports "no gate failed" while a tree-health gate it cannot see is red.** That
is the failure mode `check-all.sh` exists to prevent, arriving through a different door than the one
it was built for.

## What happened

<!-- post-merge: checked-begin -->
On astubbs/parallel-consumer#116, an edit to `src/docs/development/upstream-map.yaml` left
`bin/check-all.sh` reporting clean while CI failed `PR Checklist` on `bin/test-todo-index.sh`:

    FAIL: --check on a current index passes (expected '0', got '1')
<!-- post-merge: checked-end -->

The index was stale because the edit added text the marker scan matched. That specific cause is
fixed in `bin/todo-index.sh` (documentation under `src/docs/` is now out of scope, for the reason
its header already gives about `.adoc`/`.md`). **The gap that let it reach CI is not.**

## Why the sweep cannot see it

`check-all.sh` discovers gates by glob - `bin/check-*.sh bin/check-*.mjs` - and runs each with **no
arguments**. `todo-index.sh` fails both halves:

- its name does not match `check-*`, so discovery skips it;
- and it could not simply be added, because **run bare it REGENERATES the index**. A sweep that ran
  it with no arguments would mutate the working tree, which is the one thing a read-only pre-push
  check must not do. `--check` is the read-only mode, and the uniform "run every gate with no args"
  contract has no way to say so.

So this is a design constraint, not an oversight - which is why it is recorded rather than patched
by special-casing one script inside the glob.

## Options, none taken

- **Teach `check-all.sh` a per-gate argument**, e.g. a `# check-all-args: --check` header line it
  reads. Generalises, but adds a second contract to a script whose whole argument is that it has
  none.
- **Split the read-only half out** as `bin/check-todo-index.sh` that shells to `todo-index.sh
  --check`. Fits the existing glob with no new mechanism; costs one file.
- **Leave it to CI.** Honest, but leaves `check-all.sh`'s promise - "run this before you push" -
  overstated, and that promise is load-bearing: AGENTS.md sends every agent to it precisely so the
  set cannot drift from memory.
<!-- file-refs: N/A - bin/check-todo-index.sh is the proposed filename in option two, not a file that exists -->

<!-- post-merge: checked-begin -->
The second is the cheapest and matches the discovery contract. It was not done in
astubbs/parallel-consumer#116, which found this: that PR is a JStream result-stream fix, and changing
the gate every agent runs before every push is not a ride-along for an unrelated change. The PR is
cited as where the gap surfaced, which stays true once it lands.
<!-- post-merge: checked-end -->

Unowned.
