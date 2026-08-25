<!-- post-merge: checked -->
# astubbs#347 - the Lincheck lane: what the review left open

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

<!-- post-merge: checked-begin -->
Findings from the simplify-and-review pass that shipped with astubbs#347, which that PR deliberately
does **not** close. The lane's own false-green guard was fixed before it landed; everything below
outlived it. Delete this note when these items are resolved - not when any PR merges.
<!-- post-merge: checked-end -->

## Nothing runs the lane, so the tripwire it promises cannot fire

`bin/lincheck-test.sh` is excluded from every gating suite by design, and no workflow invokes it. The
ASM instrumentation tripwire - the control that exists because a broken transformer once reported a
clean pass against code that cannot survive two threads - therefore never runs. Three reviewers
converged on this independently.

## The red control has drifted from a standard that landed after it

`LincheckToolchainProbeTest` was calibrated before `18a61321b`, which now requires every red control
to carry a green near-miss arm. It has none. It also omits the `.actorsBefore(0)` / `.actorsAfter(0)`
that all four other harnesses set - Lincheck defaults to 5/5 (verified via `javap`), so the init
prefix can destroy the probe's own fixture.

Neither would make the probe pass, and both change a control the PR calls settled, so they are the
author's call rather than a review fix.

## Smaller, still open

- `containsAtLeastElementsIn` vs `containsExactlyElementsIn` in the exclusion contract test - a
  policy decision about whether a wrapper may over-exclude, raised by two reviewers.
- The fifth exclusion point (the pitest glob) is pinned by nothing.
- The MPL-2.0 test-scope invariant is unenforced, and the ASM pin has no retirement trigger.
- The ASM silent-instrumentation incident deserves a `docs/solutions/` entry: a detector reporting
  success while its transformer failed per-class would have made every calibration verdict read
  "not found".

## Disproven, recorded so it is not re-raised

The claim that core's `<argLine>@{argLine} ${lincheck.jvm.args}</argLine>` feeds a literal
`@{argLine}` to pitest's minion JVMs and silently breaks the mutation lane is **false**. A scoped
`mutationCoverage` run scored 35 mutants across 496 tests with zero minion errors; pitest's own
`SurefireConfigConverter` logs `Replacing properties in argLine` and resolves it. No
`-DparseSurefireArgLine=false` is warranted.

## Cross-branch obligation this note now owns

`test-lincheck-jcstress-evaluation.md` scopes a two-tool evaluation - a Lincheck arm and a jcstress
arm - and it lives on astubbs#344's branch, not on master, so it could not be updated from here. Its
**Lincheck arm is executed**: the calibration ran against a pre-fix tree and refound four real races
unaided, with the verdicts and cost tables in
[`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`](../plans/2026-08-25-001-test-lincheck-poc-plan.md).
Whoever lands astubbs#344 records that against the evaluation note and leaves the jcstress arm open;
`test/jcstress-poc-plain-long-visibility` is the branch carrying it.

This paragraph exists because the handoff note that used to carry the obligation was deleted at merge
prep, as `docs/inflight/AGENTS.md` requires - a "delete this when it merges" marker must never reach
master. Everything else that note held is already stated where it is looked up: the inversion
contract and the red control in [`docs/testing.md`](../testing.md), the five exclusion points in
`bin/lincheck-test.sh`'s own header, and the Jabel and model-checker findings in the plan doc.
