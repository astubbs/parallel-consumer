# `bin/check-quarantine-owners.sh` re-shallows the whole clone, every run

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

**Running the repo's own gate sweep silently truncates the repository's history**, and every
history query in every worktree then returns a confident wrong answer with nothing red to mark it.

```bash
git fetch --unshallow origin
git rev-parse --is-shallow-repository     # false
bash bin/check-quarantine-owners.sh       # exits 0, prints nothing about this
git rev-parse --is-shallow-repository     # true
```

The gate verifies that an owner PR's merge preview removes its quarantine, and reaches the preview
with `git fetch --quiet --depth=1 origin "$base"` and `... origin "pull/$pr/merge"` - see the two
`--depth=1` lines around `no merge preview (conflicts?)`. A depth-limited fetch writes the `shallow`
file, and that file lives in the shared `--git-common-dir`, so **one run re-shallows every worktree
of the clone at once**, not just the one that ran it.

## Why this is expensive out of proportion to its size

- **It fires from the sweep everybody is now told to run.** `bin/check-all.sh` globs `check-*.sh`,
  so the pre-review sweep and the pre-push hook both include it. The instruction to run all the
  gates is also an instruction to corrupt the clone.
- **The damage is to OTHER commands.** `git merge-base` returns empty, ahead/behind counts read in
  the hundreds, and a commit that plainly landed reports "not an ancestor of master". Every one of
  those reads as a repository catastrophe rather than a missing object.
- **The gates that depend on history degrade to `CANNOT RUN`**, which `check-all` correctly refuses
  to count as a pass - so the sweep's second half is weakened by the sweep's first half.
- `.claude/hooks/check-shallow-history.sh` (astubbs#338's base, `3d5166799`) denies the *queries*
  while shallow, which is the right guard and does nothing about the thing doing the shallowing.

## What a fix has to preserve

The preview check is worth keeping - it is what closes the quarantine loop. The narrow change is to
stop shallow-fetching into the working repository: fetch the ref without `--depth` when the clone is
not already shallow, or fetch into a scratch git dir. `--depth=1` is only free when there is nothing
to lose, which is the one case it is currently never restricted to.

Whatever lands should assert the invariant rather than describe it: unshallow, run the gate, and
fail if `git rev-parse --is-shallow-repository` flipped. `bin/test-check-shallow-history.sh` already
builds a shallow clone in a scratch directory, so the fixture shape exists.

Delete this note when the gate no longer changes the clone's depth.
