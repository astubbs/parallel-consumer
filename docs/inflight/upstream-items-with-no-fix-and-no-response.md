# Upstream items with no fix PR and no prepared response

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - after the 0.6.0.0 tag; research, not scope -->

Surveyed on 2026-09-08 for the v6 burn-down and moved out of
[`release-v6-scope.md`](release-v6-scope.md) on 2026-09-09, because nothing in it gates the release;
it is what the after-it-ships sweep and the next release's triage start from.


Every upstream open issue carries a fork reply from the 2026-08-05 mirror sweep pointing at its
mirror, so nothing upstream is silent. What follows is what has **no fix PR, open or merged, and no
draft response beyond that pointer**. Reproduce the survey rather than trusting this list: join
`gh issue list -R confluentinc/parallel-consumer --state open`, the mirrors
(`gh issue list -R astubbs/parallel-consumer --state all --label upstream-mirror`), fork PRs citing
`confluentinc#N` (`gh pr list -R astubbs/parallel-consumer --state all --json title,body`), the
manifest `src/docs/development/upstream-map.yaml`, and `scripts/upstream-sweep.sh --audit`.
[`upstream-coverage-completeness.md`](upstream-coverage-completeness.md) owns the standing
obligation; this section is one dated pass at it, kept here because the pre-release sweep is when
these get answered.

**Bugs with no fix PR:**

- confluentinc#843 (astubbs#178) - same key on two threads across a rebalance. A contract question,
  wait-for-info; [`core-178-key-order-across-a-rebalance.md`](core-178-key-order-across-a-rebalance.md).
- confluentinc#546 (astubbs#162) - truncating state; the replay branch is refuted (astubbs#484, merged),
  the false-truncation WARN is decided (INFO, no truncation branch) and being built on its own PR.
- confluentinc#551 (astubbs#164) - batching not as expected; the fork verified the over-request as
  astubbs#311, no PR.
- confluentinc#887 (astubbs#189) - a poison record re-forms the identical batch on every retry;
  manifest says none.
- confluentinc#777 (astubbs#173) - settled as by-design; the documentation reply and the grace-period
  decision are what remain.
- confluentinc#597 (astubbs#166, mirror closed as fixed) - the poller-death residual above.
- confluentinc#803 (astubbs#44) - has astubbs#408, addressed on paper only until it merges.

**Feature requests with nothing behind them** (no branch, PR or note beyond the mirror):

- API surface: confluentinc#78 executor customisation, confluentinc#170 `CompletableFuture`,
  confluentinc#520 safe consumer-API exposure, confluentinc#782 seek to offset, confluentinc#860
  managed-executor params, confluentinc#879 no-commit option.
- Error handling: confluentinc#304, confluentinc#391 and confluentinc#550 - deserialization failures,
  the largest cluster of user asks with no design;
  [`core-163-poll-path-has-no-error-seam.md`](core-163-poll-path-has-no-error-seam.md) confirms there
  is no seam. confluentinc#718 terminate processing.
- Batching and ordering: confluentinc#314 combine queues across partitions, confluentinc#560 min
  batch plus max wait (roadmap: ideated), confluentinc#902 freshest record per key, confluentinc#321
  large-message chunking.
- Performance: confluentinc#322 disk-backed produce queue, confluentinc#394 least-loaded broker,
  confluentinc#540 per-partition backpressure.
- Docs and examples: confluentinc#171 Spring Boot example, confluentinc#178 fan-out with DLQ,
  confluentinc#180 vert.x POST, confluentinc#115 tombstones javadoc.

Two mirror labels overstate coverage: confluentinc#314 and confluentinc#394 carry `pr-available` on
their mirrors, and no fork PR cites either.

**Upstream open PRs with no fork action or comment:**

- confluentinc#915 batch construction strategy - manifest none; the roadmap's batch-composition
  decision is pending. A contributor is waiting on this one.
- confluentinc#867 Vert.x 5 major - nothing in the fork.
- confluentinc#908 virtual threads - the fork went its own way in astubbs#360 and replied on the
  issue, never on the contributor's PR.
- confluentinc#918, confluentinc#919, confluentinc#920, confluentinc#901 - each absorbed by fork work
  (the log-noise fixes, `docs/building.md`, dropping the licence plugin), and none of the four PRs
  has a comment saying so.

**Unanswered conversations:**

- confluentinc#894 got a follow-up on 2026-09-01 asking where interactive replay should live. Partly
  vendor marketing; the only unanswered direct question upstream.
- Upstream discussions with zero replies - `scripts/upstream-sweep.sh --audit` lists them;
  [`upstream-discussions-unanswered.md`](upstream-discussions-unanswered.md) defers them to after v6.

**Drafted and waiting, not gaps:** the `issue-response-*.md` drafts and the astubbs#337 drafts for
confluentinc#894. [`upstream-tell-809-833-the-hang-is-fixed.md`](upstream-tell-809-833-the-hang-is-fixed.md)
is stale in one respect: both reports already carry the 2026-08-05 fork reply, though neither has
been told the fixes merged.

