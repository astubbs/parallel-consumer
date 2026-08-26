# A lost claim is invisible, and it means opposite things on the two engines

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-labels: concurrency -->

Recorded 2026-08-22 on `research/market-analysis-recut`, left open by the change that made the claim
one atomic transition (`WorkContainer.ExecutionState`).

`WorkContainer#onQueueingForExecution()` returning `false` is a `trace` log and nothing else. That is
the right amount of noise for **one** of the two engines and the wrong amount for the other:

- **Direct pull** - every worker selects, so a lost claim is *normal*. It is the mechanism working.
  A rate worth having as a number, because it is the contention signal for the engine whose measured
  behaviour collapses at high worker counts, but not an event.
- **The default engine** - the control loop is the only selector, so a claim can never be contested.
  A lost claim there means the single-selector invariant has been broken, which is a serious and
  currently **silent** condition.

One counter, two meanings. The open question is whether to have one metric read differently per
engine, two, or a metric plus an assertion on the default engine - and whether the default engine's
case should be loud (log at `warn`) rather than merely countable.

## Is it a skip bug? No - the record is not lost, only the signal is

<!-- post-merge: checked -->
Asked on the astubbs#335 review, and worth answering in the note rather than re-deriving. A refused
claim leaves the record exactly where it was: `ProcessingShard#getWorkIfAvailable` takes the `else`
branch, the container stays in `entries`, and
`dcrAvailableWorkContainerCntByDelta(workTaken.size())` counts only what was actually taken - so
nothing is decremented on its behalf and the next scan reaches it again. The refusal excludes it from
*this* batch and from nothing else. Under the default engine the branch is unreachable anyway, since
the control loop is the only selector.

One thing it does skew, and it is a signal problem too rather than a lost record: the refusal path
calls `addToSlowWorkMaybe`, so under direct pull a *contested* record can be reported as **slow**.
The two are unrelated - one is contention, the other is a long-running user function - which is the
same conflation this note is about, one layer up.

**What makes it worth doing rather than shrugging at:** the single-selector assumption is what every
safety property of selection was originally written under, and it is stated nowhere. An engine change
that quietly introduced a second selector would produce exactly this signal and nothing else.

Related: the opt-in engine paths are exercised by no CI lane (that note travels with the direct-pull
branch), so a counter nobody reads in CI buys less than it looks.
