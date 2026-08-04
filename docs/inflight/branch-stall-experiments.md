# Stall-experiment branches - one result worth keeping, then delete

`docs/uber-stall-experiment-results`, with the `experiment/stall-uber-fix` / `stall-uber-nofix` arms:
the composition experiment behind #80, which has merged.

One result still matters: **the stall-fix stack composes cleanly with #29 + #31** (all guards green,
zero conflicts), which is what makes #29's rebase tractable. Fold that sentence into #29 and delete
all three branches.
