# Branch audit 2026-08-17: orphans to investigate, and upstream tips not preserved on origin

A full sweep of all 196 origin branches against the tracking corpus (upstream-map.yaml,
docs/refactoring.md, upstream-pr-analysis.adoc, docs/inflight/, docs/upstream.md) found the
actor-IPC family and the thread-model family had no manifest entries (fixed in the same PR as this
note), plus the two lists below, which are NOT yet resolved. Method note: "referenced" meant the
full branch name, or its basename, appearing anywhere in that corpus - existing references were not
verified correct (one, in `sweep-2023-async-produce`, was already known wrong and is fixed
alongside this note).

## 1. Interesting orphans - branches referenced nowhere, likely carrying real work

Each needs a look: read the diff vs its merge-base, then either register it in the manifest /
refactoring.md, or record it as dead. Do NOT delete any of these before that look.

Observability cohort (plausibly one work group):

- `feature/auto-tuning-pressure`
- `feature/progress-monitoring`
- `feature/health-metrics`
- `feature/micrometer` (basename-only reference; confirm what tracks it)

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
  confluentinc#268 (adoc A10/E1); no dedicated manifest entry exists for that pair yet
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

## 2. Upstream branches NOT preserved on origin

Answer to "do we have every upstream branch?": **no.** Every tip exists in local clones with the
`upstream` remote, but only origin-pushed refs survive confluentinc deleting or GC-ing branches.
Verified 2026-08-17 by checking every `upstream/*` tip for reachability from any origin ref:

Same-name pairs where the UPSTREAM side has commits origin's copy lacks:

- `0.5.3.x` - upstream ahead
- `improvements/rebalance-messages` - upstream ahead
- `improvements/remove-static` - upstream ahead
- `features/dynamic-concurrency-control` - DIVERGED (upstream tip ba6b71f10 is the SHA the
  manifest cites for draft PR confluentinc#22; origin's copy @6f85eac41 is older)
- `features/retry-exception` - DIVERGED

Upstream-only branches with no origin counterpart (bot branches excluded):

- `PL-176/DontDrainIssue` - unknown content, name suggests a real bug investigation; NOT in
  docs/upstream.md's ruled-out orphan list
- `features/batching` (2022, "Batching feature and Event system improvements")
- `docs/back-pressure` (PR confluentinc#508, adoc E3)
- `improvements/vertx-vertical` @02ab32894 (draft PR confluentinc#204 - the SHA the manifest
  cites; the manifest citation currently resolves only via the upstream remote)
- `fix-charts` (despite the name, tip is "more tests", 2022)
- `pyallel-consumer` + `python-cd-pipeline` (PR confluentinc#443 family)
- `correct-failing-license-check` (2025-10)
- `upstream/master` itself - the 2026 tip (rmoff, "Add link to fork") is ahead of
  `origin/master-confluent`

Proposed preservation action (cheap, one-off, decision pending): push each unpreserved upstream
tip to origin under `upstream-archive/<name>` so the fork holds a durable copy; then
docs/upstream.md's orphan-branch section should be updated to cover the full list above, not just
the five it names.

## 3. Tooling follow-up

`scripts/upstream-sweep.sh --audit` checks for untracked upstream *issues/PRs*; nothing checks for
*branches* unreferenced by the manifest or refactoring.md - which is exactly how the actor family
stayed invisible for three years. Add a branch-audit mode (origin branches vs manifest
`fork.branches` + refactoring.md, with an explicit junk allowlist) so this list cannot silently
regrow.
