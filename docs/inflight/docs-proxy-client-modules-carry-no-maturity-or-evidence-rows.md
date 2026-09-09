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

**The scaffolding rung was asked to carry the mechanism, and declined for a reason worth
recording.** The hygiene sweep of astubbs/parallel-consumer#293 (astubbs#378) recommended that the
`docs/data/*.d/` fragment machinery ride the polyglot scaffolding, on the grounds that extracting
only its generic half is surgery and that it would collide with that rung's own client fragments.
The second premise does not hold: the rung ships no fragments. And the first half of the machinery
cannot be used here even if it were carried, because the two halves disagree about these modules -
`bin/check-deferred-modules.sh` (the gate that makes a `deferred:` fragment honest) **fails a module
that has source beyond a named skeleton allowlist**, and every one of these modules has exactly
that: a dependency manifest and a program. So the only fragment they could carry is a real maturity
row, which is the thing this note says they have nothing to put in. Carrying the mechanism would
have meant importing a merge path, a schema extension and a gate with nothing on `master` for any
of them to act on.
<!-- file-refs: N/A - bin/check-deferred-modules.sh ships on feats/proxy-requirements and is named here as the reason this rung did not import it; read it with `git show origin/feats/proxy-requirements:bin/check-deferred-modules.sh` -->

**The mechanism that would make this loud already exists, elsewhere.** astubbs/parallel-consumer#293
carries a per-module fragment scheme - `docs/data/module-maturity.d/<artifact>.yaml` merged at check
time, with a `deferred: {reason, lifted_by}` block, plus a cross-check that every module in any
aggregator's `<modules>` has either a row or a recorded deferral, and a companion gate that fails a
module with real source still marked deferred. That is the shape that turns this note into a build
failure instead of a note. It rides the extraction that brings the records it validates, not this
one - the scaffolding rung would otherwise have had to import the fragment machinery to describe
eleven modules that have nothing to say.
