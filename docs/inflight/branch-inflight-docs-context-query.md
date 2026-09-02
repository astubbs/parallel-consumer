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
then the session-start index migrates onto the query. The files-touched trigger waits on lifting the
citation resolver out of `.github/scripts/file-ref-gate.js`.

**Related notes this work touches, and how:**

- [`ci-inflight-absorbs-the-query-half.md`](ci-inflight-absorbs-the-query-half.md) - the session
  index row migrates here (its "start with the session index" paragraph). Update that row when it
  lands, do not duplicate it here.
- [`ci-inflight-next-commands.md`](ci-inflight-next-commands.md) - owns corpus widening to tags and
  backup refs; the header inherits whatever the corpus covers and does not widen it.
- [`ci-node-query-client.md`](ci-node-query-client.md) - the no-daemon, no-dependency constraint
  binds every delivery here.

**Open once the plan has landed:**

- The corpus index rebuilds from git on every run (the `bin/lib/notes.mjs` header says why the disk
  cache was removed). A per-read hook pays that each time until a cache decision is taken; the plan
  carries a latency budget rather than assuming reads are free.
- Which hook moment delivers the header for direct reads is a planning decision: allow-with-context
  before the read, or context after it.
