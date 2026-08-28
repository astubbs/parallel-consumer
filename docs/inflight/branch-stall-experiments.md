# Stall-experiment branches - one result worth keeping, then delete

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->


`docs/uber-stall-experiment-results`, with the `experiment/stall-uber-fix` / `stall-uber-nofix` arms:
the composition experiment behind astubbs#80, which has merged.

<!-- post-merge: checked -->
One result still matters: **the stall-fix stack composes cleanly with astubbs#29 + astubbs#31** (all guards green,
<!-- post-merge: checked -->
zero conflicts), which is what made astubbs#29's rebase tractable. That result belongs with the deadlock
<!-- post-merge: checked -->
fix's own record - `pr-29-857-deadlock-and-what-the-measuring-taught.md` while astubbs#29 is open, and its
solutions write-up once it lands - after which all three branches can go.
