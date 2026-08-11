# Promote `docs data: audit` to a required check, after astubbs#273 merges

The `docs data: audit` job added by astubbs#273 runs `bin/check-docs-data.sh` and its self-test on
every PR, and **is not in the required-checks list**, so a red one does not block a merge. That is
most of the point of adding it.

Required checks live in repository ruleset **15055005** (`master`), whose contexts are the ones
listed by `gh api repos/astubbs/parallel-consumer/rulesets/15055005`. `docs data: audit` is absent.

## Do it after the merge, not before

The owner's call, and the reason is worth keeping: adding a required context that no open PR has yet
reported would block every in-flight PR until each one re-runs and produces it. Promoting it once the
job exists on `master` means every subsequent PR reports it naturally.

```
gh api -X PUT repos/astubbs/parallel-consumer/rulesets/15055005 ...   # add context "docs data: audit"
```

Re-read the current ruleset first and add to its existing contexts rather than replacing them.

## Worth checking at the same time

Several other lanes report but are not required, most of them deliberately: the three `Analyze (*)`
jobs roll up into the required `CodeQL` aggregate, and `Chaos Pain Suite`, `Performance (optional)`,
`compat: kafka 4.x (experimental)`, `full build (master)`, `Mutation Tests (PIT, PR-scoped)` and
`static: spotbugs baseline` are non-gating on purpose. `docs data: audit` is the one with neither an
aggregate above it nor "optional" in its name, so it currently reads as gating without being so.

## Delete when

`docs data: audit` appears in ruleset 15055005's required contexts.
