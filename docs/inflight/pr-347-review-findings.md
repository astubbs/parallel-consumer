# astubbs#347 - the Lincheck lane: what the review left open

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

Findings from the simplify-and-review pass on this branch that astubbs#347 does **not** close. The
lane's own false-green guard was fixed on the branch; what is below remains. Delete this note when
these are resolved, not when the PR merges.

## The committed bound does not reliably find the violation

`WorkManagerLincheckTest` asserts a violation and **missed it in 2 of 8 measured single-class runs**;
a run that did hit took 20.1s of a roughly 25s budget. The plan doc records "3/3 at the committed
bound, 2.1-5.0s", so the recorded measurement no longer describes the lane.

The pristine control arm is equally marginal, so this is not an artefact of the review's edits - no
edit touched `options.check()` or the exploration thread. **The lever is raising `iterations` and
re-measuring. Never a retry**, and never loosening the assertion: this harness asserting a violation
IS the calibration.

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

## Obligation attached to `pr-347-handoff.md`

That note must be deleted at merge prep - and its one cross-branch obligation executed or relocated
first: ticking the Lincheck arm of `test-lincheck-jcstress-evaluation.md`, which lives on
astubbs#344's branch.
