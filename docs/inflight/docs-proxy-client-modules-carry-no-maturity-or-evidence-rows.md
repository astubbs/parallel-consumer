# The eleven proxy-client modules have no module-maturity or testing-evidence row

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - until a client module has content a reader could depend on -->

`parallel-consumer-proxy-clients` and its eleven language modules are in the reactor and in no
release-documentation record. Nothing goes red about it: the structural checks in
`bin/check-docs-data.sh` validate the records that exist, and neither `docs/data/module-maturity.yaml`
nor `docs/data/testing-evidence.yaml` demands a row per reactor module on `master`.

**Why they were left out rather than filled in.** Both records answer questions about something a
reader might depend on - `module-maturity.yaml`'s reader contract is literally *"can I rely on this
module in production"*, and a maturity row needs an `evidence_id` resolving into the evidence
corpus. These modules publish nothing, export nothing, and contain a program that prints one line.
Every honest value would be a variation on "not applicable yet", eleven times, in a table a release
reader is meant to scan.

**What lifts it.** The rung that gives a client module real content gives it a row at the same time.

**The mechanism that would make this loud already exists, elsewhere.** astubbs/parallel-consumer#293
carries a per-module fragment scheme - `docs/data/module-maturity.d/<artifact>.yaml` merged at check
time, with a `deferred: {reason, lifted_by}` block, plus a cross-check that every module in any
aggregator's `<modules>` has either a row or a recorded deferral, and a companion gate that fails a
module with real source still marked deferred. That is the shape that turns this note into a build
failure instead of a note. It rides the extraction that brings the records it validates, not this
one - the scaffolding rung would otherwise have had to import the fragment machinery to describe
eleven modules that have nothing to say.
