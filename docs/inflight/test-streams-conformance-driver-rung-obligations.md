# Streams conformance: what the driver rung inherits

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

The oracle rung of the Kafka Streams conformance net (astubbs#242) is on master as
`parallel-consumer-streams-conformance`: the case format and its loader, the live oracle under
`TopologyTestDriver`, the differ, the proofs and the guards. It measures **no binding**, because none
is reachable from master. What follows is the work it deliberately leaves to the **driver rung** - the
one that measures our own engine, bases on astubbs#334 and carries astubbs#390's runner contract.

The reasoning for every item below is in
[`docs/plans/2026-09-05-001-test-streams-conformance-net-plan.md`](../plans/2026-09-05-001-test-streams-conformance-net-plan.md);
this note is the checklist, not a second copy of the argument.

## Obligations

- **Declare the wrapper module (`parallel-consumer-proxy-streams`, astubbs#334) as a test dependency
  of this module in the SAME change that lands the engine, and register its row in `BindingRows`.**
  A class reaches this module's test classpath only through a dependency this module declares, so the
  guard cannot fire before the declaration exists. `TheEngineArrivingMustBringTheStreamsRowTest`
  reddens on either half alone - a row with no engine, an engine with no row - and retires itself when
  both are present. The class it probes is one string constant, `WRAPPER_ASSEMBLER` in
  `conformance/BindingRows.java`, currently
  `bz.stub.parallelconsumer.streams.TopologyAssembler`: keep it in step with the wrapper's package, or
  the guard reads a rename as absence.
- **Fill the reserved `call-log` slot (R4)** against astubbs#390's runner-transcript line format, and
  lift the refusal that holds the slot shut - `conformance/CaseLoader.java` refuses any case that
  "declares a call-log, which is reserved and unset on this rung". Until then a case cannot even
  express the slot, which is the point: an unset slot that loaded would be compared against nothing.
- **Reconcile `conformance/BuilderSurface.java` and KTD13's function vocabulary against
  `streams.proto`.** `BuilderSurface` is this module's own copy of the unfrozen `v1alpha1` builder
  surface's ten operations, so the coverage gate's claim is bounded to *what this module knows* - it
  can never say "the corpus covers the wrapper". Carry the translation table's three stated limits
  across with it: `count` and `aggregate` share one node name, so the credit proves an aggregation was
  built and not which one; `group-by-key` is a **structural** credit that no sabotage arm can fire
  independently; and `windowed-by` is witnessed only through a `to-stream` key-select or a pinned-emit
  suppress node, so a windowed case with neither has no witness at all.
- **Add the update-stream observable and its differ path (KTD5).** `final-state+updates` is refused at
  load on this rung, naming the case; the capture and the comparison are the driver rung's cost, and
  the floor this rung evidences is final state only - see `conformance/FinalState.java`.
- **Exercise the pinned-emit case once the wrapper exposes an emit control.** `emit: on-window-close`
  is applied by the oracle alone today; the attribute sits outside the builder grammar, the wrapper
  exposes no suppression or emit strategy, and the coverage gate deliberately does not credit the case
  toward binding coverage.
- **Execute the refusal-class cases (`expects-fault`) against the wire's fault vocabulary.** They load
  flagged never-executed here, because plain Kafka Streams never refuses what this wire invented, so
  the oracle has no row to compute for them. They become real assertions only once a binding can raise
  the named fault. One of the two names a wire rule the loader deliberately does not enforce -
  `retention-ms` below `size-ms + grace-ms` loads here as R15 data - so the driver, not the loader, is
  where that rule first bites.
- **Mirror the input-record `topic:` field in the driver.** Added on this rung: a record may name its
  topic, defaults to the sole source when there is one, and must name one when there are several; a
  topic no source declares is refused. A driver that fans a record out to every source instead will
  disagree with the oracle on every multi-source case - see `conformance/CaseDocument.java`.
- **Reconcile the design note `docs/inflight/test-cross-binding-streams-conformance.md` on
  astubbs#334's branch (`research/kafka-streams-foreign-wrappers`) with what actually shipped here
  (R14).** That note predates the format, the closed function vocabulary and the agreement levels;
  whoever next touches it owns folding this rung's decisions in. It is not a file this rung can edit.
<!-- file-refs: N/A - the design note exists only on research/kafka-streams-foreign-wrappers (astubbs#334); this rung is cut off master and cites it as the document to reconcile, not as a file it touches -->
- **Price the merge lane's runtime budget.** Each executable case costs four driver lifecycles - the
  determinism control arm runs the oracle twice, the positive control runs the original and its
  perturbed twin - and every binding row added multiplies that. No bound has been measured, and none
  is claimed; measure before the corpus or the row count grows.
- **OPEN, and the owner's call: whether the streams engine may reach master without a conformance
  row.** The plan asserted that coupling and the guard cannot enforce it, for the reason in the first
  bullet: a class reaches a module's test classpath only by a dependency that module declares, so the
  obligation sits with the driver rung rather than with the guard. Whether the coupling is wanted at
  all - engine gated behind a row, or not - is undecided, and nothing here decides it.

## The sabotage-arm discipline carries forward

Every proof on this rung has a recorded arm that reddened it, with the untouched tree as the control
arm on either side and the arm restored byte-identically afterwards (KTD9); the record is in the
commit bodies. **A proof no arm can redden is not a proof**, so each new proof the driver rung adds -
the engine row's comparison, the call-log comparison, the update-stream differ - needs its own arm,
run and recorded, in the PR that adds it.
