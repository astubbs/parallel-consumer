# Next candidates, ranked

<!-- inflight-type: register -->
<!-- inflight-vetted: 2026-09-18 - the seven decisions are made and posted; the ranking section is retired to a pointer. Earlier: 2026-09-09 - the astubbs#162 decision was made and shipped, so that line is removed and the ranking is seven; the rest were still open issues awaiting the same reply (astubbs#161, astubbs#181, astubbs#163, astubbs#189, astubbs#241, astubbs#173, astubbs#178) and every note they name is present. 2026-09-07: four settled lines removed - the astubbs#155/astubbs#169/astubbs#170 scheduling sentence and the logging-verbosity pick (merged as astubbs#203 and astubbs#428), the astubbs#40 dedup pick (astubbs#206), and confluentinc#906 out of the contributor-friction pick (astubbs#194 closed) -->


## Decisions waiting on the maintainer - all seven made, 2026-09-18

The ranked list that stood here from the 2026-08-20 mirror triage is decided and posted: each mirror
body corrected, each reporter answered on the upstream original, every decision recorded on the
issue itself. astubbs#161 answered and closed; astubbs#181 closed on the kafka-clients 3.9.2
rationale with astubbs#128 carrying the CI proof; astubbs#163 answered, held open for the fluent API
(astubbs#502) rather than closed as a duplicate; astubbs#189 answered with default-on jitter as the
first rung; astubbs#241 rewritten and relabelled `feature`; astubbs#173 closed as by-design, no
revocation grace period; astubbs#178 ruled a violation and fixed in astubbs#517. The posting list
and what the release still owes is
[`upstream-v6-release-responses.md`](upstream-v6-release-responses.md). The version of this
section that carried the ranking: `git show 2e6f13ef1:docs/inflight/process-candidate-ranking.md`.

## What gated v6, as the sweep read it - retired at the tag

v0.6.0.0 shipped on 2026-09-17, so the sweep's dated reading of what gated it is history. The full
section, kept whole as the record until then: `git show 821c6f36b:docs/inflight/process-candidate-ranking.md`.

## Ready picks

Collisions are in `pr-blockers-and-collisions.md`. The ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; these are the ready picks:

- **Commit-failure seam ([astubbs#317](https://github.com/astubbs/parallel-consumer/issues/317))** -
  **highest-demand item on this list, and the only one with a user shipping a patched build to get
  it.** On confluentinc#833 `ndqvinh2109` reported patching `controlLoop` with a try/catch so the
  exception would not reach `supervisorLoop` and close PC. That is not a feature request in a
  backlog - it is someone maintaining a private fork of the library because the decision PC makes
  for them is the wrong one for their deployment. Kafka's client throws a retriable exception and
  lets the caller choose; PC only terminates. Research, both sides of the upstream argument, and why
  fixing astubbs#177 does not close it: `core-commit-failure-seam.md`.
- **Auto-scaling (astubbs#227)** - runtime-discovered per-instance concurrency; candidate killer
  feature alongside key ordering, priority raised 2026-08-18 (`core-auto-scaling.md`). Spec
  stage; two bitrotted prototypes to mine; async-timing metrics fix is the prerequisite.
- **Contributor-friction build fixes** - `confluentinc#162` (mvn compile without test-jar) and
  `confluentinc#861` (`ManagedTruth` not found). The third, `confluentinc#906` (pom version
  mismatch), is settled - astubbs#194 is closed.
- **Security dependency bumps** - `confluentinc#851` (postgres), `confluentinc#913` (assertj); pom-only.
- **`confluentinc#915` batch construction strategy** - cherry-pick, closes the 4-year-old
  `confluentinc#266`. Medium effort.
- **Point ArchUnit at main code** (`static-archunit-main-code-rules.md`) - the harness is already
  wired into all four modules with a shared rule library, but polices only three test conventions.
  Post-v6: it is what would hold the boundaries the God-class decomposition creates.
- **DLQ** (`confluentinc#310`, or revive `confluentinc#366`) - the most-demanded missing feature. Large, and
  spec-stage only.
