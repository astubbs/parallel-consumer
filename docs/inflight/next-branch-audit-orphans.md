# Branch audit 2026-08-17: orphans to investigate

<!-- inflight-priority: low -->

A full sweep of all 196 origin branches against the tracking corpus (upstream-map.yaml,
docs/refactoring.md, upstream-pr-analysis.adoc, docs/inflight/, docs/upstream.md) found the
actor-IPC and thread-model families had no manifest entries (fixed in the same PR as this note).
The list below is what remains OPEN. Method note: "referenced" meant the full branch name, or its
basename, appearing anywhere in that corpus - existing references were not verified correct (one,
in `sweep-2023-async-produce`, was already known wrong and is fixed alongside this note).

Upstream-tip preservation is DONE, not open: every non-bot `upstream/*` branch tip is now reachable
from an origin branch or pinned under `archive/upstream-branch/*` /
`archive/upstream-pr-*` tags. `preserved_branch_tips` in upstream-map.yaml owns the tag/SHA record;
docs/upstream.md owns the method, including the trap that the 2026-08-14 containment command
(`git branch -r --contains`) cannot see tags, so re-running it verbatim reports already-preserved
heads as lost.

## 1. Interesting orphans - branches referenced nowhere, likely carrying real work

Each needs a look: read the diff vs its merge-base, then either register it in the manifest /
refactoring.md, or record it as dead. Do NOT delete any of these before that look.

Observability cohort (plausibly one work group):

- `feature/progress-monitoring`
- `feature/health-metrics`
- `feature/micrometer` (basename-only reference; confirm what tracks it)

Resolved since the audit: `feature/auto-tuning-pressure` is NOT observability - its commits ("Wip!
Experiments in self tuning", reworking WorkManager/backpressure) make it the hand-rolled twin of
`features/dynamic-concurrency-control`, i.e. the astubbs#227 flow-control family. Catalogued in
`docs/refactoring.md` (Flow control / self-tuning) and tracked by `next-auto-scaling.md`.

Defect / correctness cohort:

- `commit-timeout-supervise` - name suggests confluentinc#857-adjacent supervision work; check
  against the bug-857 family before any new work there
- `bugs/issue-184-reproduce-using-cfacade` and `bugs/issue-184-reproduce-w-cf-reverted` -
  reproduction pair for confluentinc#184
- `bugs/prod-tx-manager-retries` - plausibly belongs to `sweep-2023-tx-failure-taxonomy`
  (confluentinc#144); verify before attaching
- `features/partial-batch-failure`
- `tests/less-keys-than-threads-broker-test`

Feature drafts with likely manifest homes (attribute, then back-fill the entry):

- `features/retry-exception`, `features/retry-exception-w-terminal` - likely confluentinc#291 /
  confluentinc#268 (adoc A10/E1); no dedicated manifest entry exists for that pair yet. NB the
  upstream same-named branch tip diverged from origin's but is contained in another origin branch -
  attribution should look at both lines
- `features/configure-retry` - possibly confluentinc#66 (which was deliberately REMOVED from the
  sweep cohort - see sweep-2023-admin-closure notes); needs its own entry if real
- `features/failure-history`
- `features/broker-connection-status` - likely confluentinc#185/confluentinc#353 (adoc A9);
  related to but distinct from `sweep-2023-broker-disconnect-commit`
- `features/long-encoding` - likely confluentinc#408 runlength-v3 (adoc B4)
- `features/key-partition-combine` - check against `sweep-2023-null-key-ordering` before assuming
- `features/extend-functional` - likely confluentinc#303 (adoc C6); `sweep-2023-api-shape` is the
  nearest entry but its issues are confluentinc#175/confluentinc#372 - verify before attaching
- `feats/jstream-bounded-blocking-buffer` - relates to `refactor/deprecate-jstream`?

Newly preserved upstream tips whose CONTENT was never assessed (read before judging):

- `archive/upstream-branch/PL-176/DontDrainIssue` - name suggests a real drain-behaviour
  investigation; not in docs/upstream.md's earlier ruled-out orphan list
- `archive/upstream-branch/features/batching` - batching shipped upstream separately; the tip may
  carry unmerged event-system work

## 2. Tooling follow-up

`scripts/upstream-sweep.sh --audit` checks for untracked upstream *issues/PRs*; nothing checks for
*branches* unreferenced by the manifest or refactoring.md - which is exactly how the actor family
stayed invisible for three years. Two audit modes wanted:

- branch-audit: origin branches vs manifest `fork.branches` + refactoring.md, with an explicit junk
  allowlist, so untracked work cannot silently regrow
- containment re-check: every `preserved_heads` / `preserved_branch_tips` SHA still reachable on
  origin (branch OR tag - see the docs/upstream.md trap above); docs/upstream.md already records
  this as manual-only, tracked in astubbs#300
