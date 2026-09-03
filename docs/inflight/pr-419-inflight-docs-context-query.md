# Inflight docs context query - header at read time, keyword injection, `inflight docs`

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

**What this work is:** one context query in `bin/inflight.mjs` and three agent-facing
deliveries of its answer - a divergence header on any docs file at read time, keyword injection per
prompt, and an `inflight docs` command family. The requirements are in
[`docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md`](../plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md);
this note carries what `gh` cannot show about that work as it lands.

**Ships in this order, and the first is the only one that would have caught the incident that
motivates the work:** the header first and alone; then prompt-keyword injection; then `inflight docs`;
then the session-start index migrates onto the query. The files-touched trigger is deferred, re-keyed
on the text the agent writes; folding the existing write-time solutions hook onto the query goes with it.

**Related notes this work touches, and how:**

- [`ci-inflight-absorbs-the-query-half.md`](ci-inflight-absorbs-the-query-half.md) - its session
  index row is resolved by this work: the hook now renders the three corpus areas from
  `inflight docs index` and keeps only the sections outside the corpus in bash.
- [`ci-inflight-next-commands.md`](ci-inflight-next-commands.md) - its corpus-scope section was stale
  (the enumeration already looks everywhere) and is corrected here; the header reports archival-only
  versions as preserved.
- [`ci-node-query-client.md`](ci-node-query-client.md) - the no-daemon, no-dependency constraint
  binds every delivery here.

**Open as the units land** (the plan's U-IDs are the order):

- Every delivery publishes its measured cold cost in its own header before it ships; the budgets
  are in the plan's cost decision. The full-tier header pays one diff per divergent cluster for the
  preview, which `note drift` now pays too - flag it if that command's wall-clock matters.
- The tag vocabulary has one source, `bin/lib/inflight-tags.mjs`: the index imports it, and the
  bash gate reaches it through `bin/lib/inflight-tags.sh`, a wrapper that evals the Node file's
  `--shell` rendering. The parity self-test that held two copies equal is now a round-trip check on
  that renderer. Migrating the gate itself to Node stays out of scope; it no longer has a drift
  problem to solve.
- Session start breached its budget once the branch-facts block landed, and the lever named in the
  hook header was pulled: `corpusIndex` resolves every ref's `docs/` tree in one
  `cat-file --batch-check` and lists each distinct tree once, since most tips share the baseline's.
  Its header states the shape; `node bin/inflight.mjs --perf docs` prints the figures. The next
  lever, if one is ever needed, is parsing the distinct trees themselves through one batched
  `cat-file` per depth instead of one `ls-tree` per tree - not pulled, no measurement demands it.
