# astubbs#334 ships windowed aggregation, and this branch's `STRATEGY.md` records it as not offered

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

**The PR and the strategy document disagree, on this branch, right now.** Both statements are
already in this tree:

- `STRATEGY.md` says windowed **"aggregation is therefore recorded as not offered"** - grep that
  phrase; the sentence opens on the previous line, so the fuller quote spans a line break and does
  not grep. It closes the paragraph beginning "Measured 2026-08-25".
- The same tree ships the feature: `windowedBy` / `aggregate` / `toStream` in `TopologyAssembler`,
  `ForeignAggregator`, `WindowedBy` / `Aggregate` / `TimeWindowSpec` and
  `HANDLE_KIND_TIME_WINDOWED_STREAM` in `streams.proto`, and the Python surface's `windowed_by` in
  `_session.py`.

Neither is wrong. The spike that produced the verdict was triggered *by* the implementation, and
the implementation is what made the measurement possible. But a merge resolves the contradiction
one way or another, so it has to be decided rather than discovered by a user.

**This is a decision, not a defect.** Nothing here says the code is broken; the measurements say a
user can hand-roll this faster than the wrapper runs it, at both specifications measured.

## The options, none of them free

1. **Ship it, marked experimental / not recommended for throughput-sensitive use**, with the
   measured verdict cited. Honest, and preserves the reproduction path. Costs: a shipped API that
   the project's own strategy document argues against, which every future reader has to re-derive.
2. **Ship the plumbing without advertising the feature** - protocol, assembler and tests stay, the
   public client surface does not gain `windowed_by`. Keeps the spike reproducible and the seam
   ready for the fast path. Costs: a protocol surface with no user, which the proto's
   `v1alpha1`/unfrozen status makes cheap but not free.
3. **Strip it back out.** Smallest shipped surface, matches the strategy text exactly. Costs: the
   windowing spike's harness and instruments live on this code, and
   [`perf-streams-windowing-multiplier.md`](perf-streams-windowing-multiplier.md) is the
   reproduction path for every F2 figure the program rests on - removing the implementation
   strands it.

## What a reader must not conclude from the verdict alone

The "not offered" verdict was taken **against a stateless, non-durable dictionary** and over the
single-session transport. Two later rounds changed what it may be cited for, and **both live on
`perf/242-crossing-cost-ladder`, not on this branch** - so this branch's `STRATEGY.md` states the
verdict in its strongest, least-qualified form:

- The per-crossing cost that drove it has since been measured at **747ns** (GraalWasm) and
  **19.9ns** (Numba `@cfunc`) against the 135us fitted here, so the paragraph's own stated
  reopening condition is met.
- The floor the verdict was taken against is itself now recorded as mis-specified: a dictionary
  with no store, changelog, restore or recovery is the floor for a product Kafka Streams is not in
  the business of being.

`docs/inflight/perf-streams-engine-floor.md` on that branch owns both, and the corrections appended
there and to the solutions write-up are the current reading. **Whoever takes this decision should
read them first** - deciding from this branch's text alone decides on a superseded reason.
<!-- file-refs: N/A - perf-streams-engine-floor.md is deliberately named as a file on another branch (perf/242-crossing-cost-ladder) and does not exist here; that it is absent from this branch is the paragraph's point -->

## Not a merge blocker on its own

The rest of astubbs#334's merge prep is unaffected and tracked separately:
[`pr-334-code-review-findings.md`](pr-334-code-review-findings.md) holds the review queue, and the
wire error-contract decision was deferred to merge-prep by the owner. This note exists so the
windowing question is decided deliberately at merge rather than noticed afterwards.

Delete this file in the PR that resolves it.
