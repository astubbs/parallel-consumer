# Branch `docs/833-commit-failure-seam`: commit-failure seam requirements (astubbs#317)

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Requirements-only plan for the commit-failure seam -
`docs/plans/2026-08-24-001-feat-commit-failure-seam-plan.md`. The feature: a commit-failure handler
returning SHUT_DOWN (default) or CONTINUE when a retriable commit failure exhausts its budget
(astubbs#317; the request is embedded in upstream confluentinc#833). No PR yet. The feature's
rationale and research record is `docs/inflight/core-commit-failure-seam.md`; the adjacent open
budget-default question is `docs/inflight/bug-offset-commit-timeout-does-two-jobs.md`.

Delete this file when the plan's work lands; the review record below then lives in that PR's history.

## Requirements review, round 1 (2026-08-24) - findings and decisions

Five-persona document review (coherence, feasibility, product-lens, scope-guardian, adversarial).
Eight actionable findings; **all eight applied** to the plan in one pass. R-numbers below are the
plan's post-edit numbering.

| Finding (reviewer) | Decision |
|---|---|
| Exhausted budget today escapes on the broker-poll thread and arrives at the control thread as a poller death, so "handler fires on exhaustion" and "poller death stays fatal" described one event with opposite outcomes (feasibility) | Applied: new R4 - the seam re-routes exhaustion as a commit-failure outcome that leaves the poller alive; Dependencies bullet corrected |
| Transactional commit path has no budget or exhaustion event, so the EOS pause rule had no trigger (feasibility + adversarial, converged) | Applied: R1 gains the both-paths clause; Dependencies bullet records today's asymmetry; transactional fatal-vs-seam split deferred to planning |
| Rebalance-class commit failures are a third lane - deferred indefinitely today, neither budgeted nor fatal - that could defer forever silently (adversarial + product-lens, converged) | Applied: new R8 - deferrals count toward handler history and repeated deferrals escalate |
| Rebalance during a CONTINUE period unmodeled: revocation-time commits, history scoping, eviction-then-rebalance heal path (adversarial) | Applied: new R13 plus AE9 |
| Uncommitted-state growth during keep-processing vs offset-map payload limits could defeat the healing commit (product-lens) | Applied: new deferred-to-planning question; AE2's bounded-exposure claim qualified |
| A flapping broker (one success in twenty) resets both bound metrics, so the bounded CONTINUE never graduates (adversarial) | Applied: R12 gains a rolling-window trigger |
| AE2 claimed to cover the processing-mode configurability while only exercising the default (coherence) | Applied: AE2 trimmed to R9; new AE8 exercises pause-intake |
| Success criterion was broader than the seam: the confluentinc#833 workaround swallowed every control-loop exception, not just commit-budget exhaustion (product-lens + adversarial, converged) | Applied: success criterion scoped to the retriable commit-failure portion; planning verifies the reporter's actual exception class |

## Carried forward for planning (not plan edits)

FYI observations and residual concerns from the same review, recorded so planning inherits them:

- R14's time bound needs enforcing from a thread other than the one running the handler, or a hung
  handler wedges instead of shutting down (now folded into the plan's thread-placement question).
- The bound is denominated in attempts/time, not in duplicate exposure; R3's context could carry
  uncommitted-work volume so a bound can be exposure-denominated.
- Kafka's producer can enter a fatal/abortable transaction state after repeated commit failures,
  which may cap how many CONTINUE cycles EOS mode survives regardless of forced pause.
- Async commit mode was not fully traced in review - where exhaustion surfaces to a non-waiting
  control thread differs from the sync path.
- AE4 exercises only the consecutive-exhaustions bound; the time and rolling-window bounds have no
  example yet.
- The canned CONTINUE's default bound is where the duplicates-risk mitigation is cashed out; a
  too-generous default re-opens it.
- Handler plus context object become permanent public API, with Kafka-Streams-style source-compat
  expectations.
- PC already has pause/resume intake primitives the pause path should reuse rather than re-implement.
- "Time since last successful commit" needs an epoch definition for an instance that starts into an
  already-failing broker.
- Open question from review: should R16 name consumer-group-lag visibility so operators can tell
  "continuing but failing" from "stalled" on standard dashboards?
- Open question from review: are R3's history fields already tracked by the retry-budget machinery,
  or net-new bookkeeping?
