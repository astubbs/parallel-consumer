# The throughput regression check has never caught anything, and there is a known defect to point it at

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

**A detector that has never fired is not a working detector.** `bin/check-throughput-regression.sh`
was reasoned into existence from a regression that was diagnosed by hand; nothing has yet confirmed it
would have caught that regression, or that it stays quiet on a clean tree. Both halves need showing,
and there is an unusually good subject available: a defect whose presence is a one-line edit.

## The subject

The control loop passed an O(shards) accessor as a plain `log.trace` argument, so it ran on every pass
at every log level. Removing and restoring that one line changes throughput and nothing else - no test
changes, no config changes, no rebase. That makes it a **positive control** in the strict sense: the
only variable is the defect.

Its fix, the write-up of the mechanism, and the sweep that found no second instance are on
`handoff/enable-large-number-of-instances`; the defect itself is still live on
astubbs/parallel-consumer#29, which is where the performance lane that fails is.

## The sequence, which is the whole reason this is a note rather than a line

Four steps, each blocked on the one before, spanning three branches and two owners:

1. **Land the monitoring** - artifact retention, the master-side lane, and the check itself - on
   master, so it exists somewhere both other branches can reach.
2. **astubbs/parallel-consumer#29 merges master**, picking the check up. Note it must NOT also pick up
   the control-loop fix at this point, or there is nothing left to detect.
3. **Red/black on astubbs/parallel-consumer#29**: run its performance lane with the offending line
   present, then with it removed, and compare what the check says.
4. **Read the result against the prediction below**, and either tighten the threshold or record why
   the design does not work.

## What the run should say, and why the prediction matters more than the result

Worked from the numbers already in hand - observed 43,552 against a 71,387 baseline, neighbour classes
~4.5% slow, giving a machine index near 0.957 and an expected near 68,300 - the ratio lands around
**0.64**. Against the thresholds as they ship, that is a **WARNING and not a failure**.

So the expected outcome of the red arm is a warning, and the black arm should sit near 1.0. Three
distinguishable results, and the middle one is the interesting one:

- **Red warns, black near 1.0** - the design works and the only open question is the threshold. Tighten
  the fail bound using the spread the master lane will by then have produced, and say what the spread
  was in that commit.
- **Red and black are indistinguishable** - the normalisation is eating the signal, or the machine
  index is dominated by something other than machine speed. The check is not fit and should be said so
  rather than left running.
- **Black is also low** - the baseline is stale rather than the tree slow. Re-baseline from a master
  run, and say which run.

**Write the prediction down before the run, which this note is doing.** A detector evaluated after
seeing its output is evaluated against whatever it happened to say.

## What this cannot establish

That the check catches regressions in general. It catches one that hits the throughput test harder
than its neighbours, which is the shape this defect has; a regression that slows everything equally is
invisible to within-run normalisation by construction, and no run of this experiment will say
otherwise. That limitation is in the script's header and belongs in whatever conclusion this reaches.
