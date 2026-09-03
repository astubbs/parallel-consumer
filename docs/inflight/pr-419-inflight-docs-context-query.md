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
- The tag vocabulary exists twice, in `bin/lib/inflight-tags.sh` for the gates and in
  `bin/lib/inflight-tags.mjs` for the index, held equal by a parity self-test. The bash file is the
  source of truth today; the cheaper end state is the Node file as the source with the shell library
  derived from it.
- Session start is inside its budget but with little headroom, and almost all of it is one
  `ls-tree` per ref inside `corpusIndex`. The lever - dedupe refs by their `docs/` tree object,
  since most tips share one - is named in the hook header and not yet pulled.
