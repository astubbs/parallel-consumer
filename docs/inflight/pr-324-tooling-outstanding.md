# astubbs#324 - the gates and agent harness: what is still open

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Top of the three-PR stack (`split/tooling-gates-and-harness`). Its base astubbs#323 **merged on
2026-08-20**, so this branch now sits on master and takes master by merge, never rebase.
`bin/` gates, workflows, `.claude/` hooks, and the six documents that cite those scripts. Delete this
note when it merges.

## Open

- **`bin/test-check-agent-hooks.sh` is red on this branch, deliberately.** Its "groups open work by
  type and impact" case asserts against the **live** `docs/inflight/` corpus rather than a fixture, so
  it needs astubbs#323's retagged notes to pass. Its two newer siblings build `mktemp` fixtures and
  are hermetic. **Not weakened to go green.** The real fix is to give it a fixture like its siblings -
  a self-test that reads live repo state breaks on any legitimate content change.
- **Five agent-hook commits could not be replayed** onto current master (all collide with
  astubbs#299, `da049f703`). Their content is present; their **bodies** live only on
  `backup/pre-split-322`, at these SHAs - listed here because the claim that a reconciliation commit
  names them is **false**, that commit was lost when the stack was rebuilt:
  `92c9a73c2` (merge guard refuses a merge while background work is in flight),
  `b1b7a4734` (simplify pass, and a self-test suite that could not fail),
  `459710581` (merge guard's full-path bypass, and an unreachable documented override),
  `4d0abec47` (dropped the guard's live-build arm rather than scoping it),
  `aa3a7f267` (repointed citations at the solutions doc a deleted note became).
  Read them before changing `.claude/hooks/check-merge-outstanding-work.sh`.

## Already fixed

The merge-guard bypass - `gh -R owner/repo pr merge` slipped past guards matching `gh pr merge` as
three consecutive tokens - and the same class in `check-squash-subject.sh`, which detected via a
regex that never saw the leading flag and carried the squash sign-off check with it. Twelve
self-test cases, eight proven red against a pristine copy of the unfixed hooks. Every new gate proven
able to fail by neutering it in **both** directions.

Also fixed here: the index listed an impact-carrying feature twice, once beside its consequence and
once under "proposed work" - found by rendering the index rather than reading the script.

## Ordering

Must merge **last**: its gates validate content the two PRs below add. See
[`pr-322-split-plan.md`](pr-322-split-plan.md) for the merge protocol, and
[`pr-323-docs-outstanding.md`](pr-323-docs-outstanding.md) for the window this ordering leaves open.
