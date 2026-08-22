# A lost claim is invisible, and it means opposite things on the two engines

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

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

**What makes it worth doing rather than shrugging at:** the single-selector assumption is what every
safety property of selection was originally written under, and it is stated nowhere. An engine change
that quietly introduced a second selector would produce exactly this signal and nothing else.

Related: `docs/inflight/test-opt-in-engine-paths-are-unexercised.md` - direct pull is exercised by no
CI lane, so a counter nobody reads in CI buys less than it looks.
