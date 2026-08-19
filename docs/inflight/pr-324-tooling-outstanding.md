# astubbs#324 - the gates and agent harness: what is still open

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Top of the three-PR stack (`split/tooling-gates-and-harness`, base `split/docs-ledger-and-plans`).
`bin/` gates, workflows, `.claude/` hooks, and the six documents that cite those scripts. Delete this
note when it merges.

## Open

- **`bin/test-check-agent-hooks.sh` is red on this branch, deliberately.** Its "groups open work by
  type and impact" case asserts against the **live** `docs/inflight/` corpus rather than a fixture, so
  it needs astubbs#323's retagged notes to pass. Its two newer siblings build `mktemp` fixtures and
  are hermetic. **Not weakened to go green.** The real fix is to give it a fixture like its siblings -
  a self-test that reads live repo state breaks on any legitimate content change.
- **`.claude/hooks/pre-commit-gate.sh` over-triggers.** It fires on any Bash command whose *text*
  mentions a gate path, so read-only commands are refused for merely naming one. It made an agent
  fight the harness rather than use it.
- **The session index needs proper markdown headings.** Groups are emitted as `**bold**`, which an
  agent cannot filter structurally; they should be `##`/`###` so the index can be read headings-first
  and grepped.
- **Five agent-hook commits could not be replayed** onto current master (all collide with
  astubbs#299, `da049f703`). Content is present and verified byte-identical; their bodies live on
  `backup/pre-split-322` and are named in this branch's reconciliation commit.

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
