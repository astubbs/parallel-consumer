# The ordering profiler: what your ordering requirement costs, and where you over-ordered

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - tax measurement is near-free on existing shard state; scope discovery and critical path need the Streams graph -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)). Three stages of one instrument - an
*architectural* profiler: advice about the application's design, not graphs about its load.

1. **Ordering tax.** The scheduler already knows which records are blocked *solely* because their
   ordering domain has work outstanding (the shard in-flight counts astubbs#361 added make this a
   read, not a scan). Report it: backlog 84k, executable now 32k, blocked by key ordering 52k -
   "ordering-limited capacity: 62%" - with the top offending domains named. The simple cousin, the
   hot-key detector, is instrument 2 in [`web-control-plane.md`](web-control-plane.md); this is
   the same data aggregated into a cost.
2. **Ordering-scope discovery.** With many stages (Streams: astubbs#255 / astubbs#271), the
   declared ordering is often broader than the semantics require - customer ordering imposed over
   a whole chain when only `updateCRM` needs it. PC can show where the constraint actually binds
   and suggest narrowing the serial domain to the one operation: *you are carrying ordering
   farther than your semantics require.*
3. **Per-record critical path.** With the graph plus lifecycle timing, identify the contention
   path that dominates end-to-end residence rather than the operator that burns most CPU:
   "optimizing B by 50% changes end-to-end ~2%; removing ordering contention at C changes ~31%."

The measurement never violates ordering in production - it observes what ordering forbade, which
the engine must track anyway to be correct. Research question 1 in
[`docs-research-program.md`](docs-research-program.md) is this feature's publishable form.
