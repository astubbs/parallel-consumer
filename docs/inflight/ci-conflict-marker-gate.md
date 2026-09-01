# Nothing in `bin/` looks for conflict markers, and a merge once shipped with them

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

<!-- post-merge: checked-begin -->
**A merge was committed and pushed with most of one file still inside an unresolved conflict, and
nothing went red.** Not a hypothetical: it happened on a branch in this repo while the
astubbs#322 stack was being split, in August 2026, and the only reason it was caught was somebody
reading the file. The branch is gone and the PR has landed; what the citation is for is the dated
record of the incident, which does not change when either does.
<!-- post-merge: checked-end -->

`git` writes `<<<<<<<`, `=======` and `>>>>>>>` into the worktree and is perfectly happy to commit
them; the compiler catches it in Java, and nothing catches it in markdown, YAML, or a shell script -
which is where this repo's merges actually conflict.

## What to write

`bin/check-conflict-markers.sh` plus its `bin/test-check-conflict-markers.sh` self-test, swept by
`bin/check-all.sh` (which globs, so no registration step) and run in the Repo Hygiene workflow.
<!-- file-refs: N/A - the two scripts named above are what this note PROPOSES to write; neither
     exists yet, which is the entire point of it -->

Two traps the self-test has to cover, because both make the gate useless while looking healthy:

- **The gate's own fixtures must carry markers as data**, so it has to skip itself and its self-test
  by name - the same exemption shape `bin/lib/source-patterns.mjs` (rule `sigpipe-into-grep-q`) already uses. Anything else it
  skips is a violation in hiding.
- **A marker at the start of a line is the signal; the same characters mid-line are not.** Diffs
  quoted in `docs/solutions/` and in commit bodies contain them legitimately.

Nothing about this needs a decision - it needs somebody to write it. It is here rather than in
`docs/refactoring.md` because it carries the incident that motivates it, and that evidence is the
part that would be lost.
