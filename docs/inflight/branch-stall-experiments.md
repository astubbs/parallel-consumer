# Stall-experiment branches - one result worth keeping, then delete

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-vetted: 2026-09-07 - all three branches (`origin/docs/uber-stall-experiment-results`, `origin/experiment/stall-uber-fix`, `origin/experiment/stall-uber-nofix`) still exist, so the deletion this note asks for has not happened; the solutions write-up it hands the result to is on master -->


`docs/uber-stall-experiment-results`, with the `experiment/stall-uber-fix` / `stall-uber-nofix` arms:
the composition experiment behind astubbs#80, which has merged.

<!-- post-merge: checked -->
One result still matters: **the stall-fix stack composes cleanly with astubbs#29 + astubbs#31** (all guards green,
<!-- post-merge: checked -->
zero conflicts), which is what made astubbs#29's rebase tractable. That result belongs with the deadlock
<!-- post-merge: checked -->
fix's own record. astubbs/parallel-consumer#29 has since landed, so that is now its solutions
write-up - `../solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md`
- and the in-flight note it used to point at was deleted with the merge, as that note's own header
required. After this, all three branches can go.
