# Stall-experiment branches - one result worth keeping, then delete

<!-- inflight-priority: medium -->

`docs/uber-stall-experiment-results`, with the `experiment/stall-uber-fix` / `stall-uber-nofix` arms:
the composition experiment behind astubbs#80, which has merged.

One result still matters: **the stall-fix stack composes cleanly with astubbs#29 + astubbs#31** (all guards green,
zero conflicts), which is what makes astubbs#29's rebase tractable. Fold that sentence into astubbs#29 and delete
all three branches.
