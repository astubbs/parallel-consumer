# Draft addition to the `## Fork status` section of astubbs#119 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - until the pre-release sweep, or an explicit instruction to post -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the sweep posts it. It deliberately
     outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

**Not posted.** Post only on explicit instruction; delete this file when it is posted, not when its
PR merges.

astubbs#119 is the fork mirror of confluentinc/parallel-consumer#857, and it is a **family mirror**:
one symptom, several distinct defects, three of them already landed and one still open. This draft
therefore **adds to** the existing `## Fork status` section rather than replacing it, and it
**does not close the issue** - it records one further mechanism now characterised, and the one
mitigation shipped for it. Everything already written under `## Fork status` stays.

The draft exists because [`AGENTS.md`](AGENTS.md) requires the note mapped to an issue to carry a
draft response before its PR merges - the resolution context is cheapest to write while the work is
fresh. There is no exemption for a family mirror; what a family mirror changes is the *scope* of the
draft, not whether one is written. Raised by the Codex review on
astubbs/parallel-consumer#497 as a P1.

Fully qualified issue and PR references throughout, because this is destined for GitHub, where
`astubbs#NN` renders as plain text.

---

## Draft - to be appended under `## Fork status`

**A fourth mechanism, now characterised: the record-intake load gate latches, and any instance that
retries forever gets there eventually.** This one is not a rebalance defect at all, which is why it
sat inside this thread's symptom for so long without being named. A single instance, no rebalance
and no misconfiguration, holding records that throw on every attempt, stops fetching from the broker
**permanently** - every partition paused - while its workers keep retrying what it already holds.
From outside it looks alive and loaded.

Measured in astubbs/parallel-consumer#487 across four soak arms, each differing from the first by
one term:

- The gate is what stops intake. Raising only the gate's threshold flips the outcome - the gate
  never reads loaded, nothing is paused, and records keep arriving.
- Head-of-line blocking is **not** the mechanism. The same stall happens under `UNORDERED`, where no
  shard head can block anything, and the `KEY` arm held at most one record per key anyway.
- It does not need a high failure rate. At 1% poison the instance ran normally for a minute, then
  latched for good with **eleven of its fourteen workers idle** - it stopped fetching while 78%
  idle. Saturation is not a precondition.

The gate compares records held minus records in retry back-off against the in-flight target. Under
retry-forever the held population only grows, while the back-off term is bounded by worker count
times retry delay over user-function duration - so the comparison crosses eventually, whatever the
poison rate. A **slower** retry service latches it **sooner**, because fewer records are parked and
more therefore read as workable.

**This is the best explanation anyone has produced for the flat
`pc_processed_records_total` in confluentinc/parallel-consumer#809 and
confluentinc/parallel-consumer#833** - a flat processed-records counter across the window a commit
timeout fired in is this state, not a busy one.

**What has shipped for it: the state now says so.** Until
astubbs/parallel-consumer#497 the only export was the `pc.partitions.paused` gauge, and nothing was
logged at all. The consumer now logs one WARN when the intake gate has read loaded across many
consecutive control-loop passes with no record retiring, naming the gate's operands and the paused
partition count, and one INFO when that clears. It changes no behaviour - the gate's decision, the
poller's pausing, the retry service and every counter are untouched.

**What has NOT shipped, and why the issue stays open.** No threshold change fixes this. Raising the
buffer trades a hard stall for an unbounded-memory slow starve - the soak's third arm doubled
throughput and then plateaued while the held population climbed without limit - and counting "only
what is selectable" is worse, because under ordered modes at most one record per shard is ever
selectable, so a healthy instance would fetch without bound. The property that discriminates a
recoverable head from a poisoned one is the liveness of that head, which is not decidable from the
shard's state.

**The fix has to bound the failures rather than the buffer**, which is a dead-letter path -
astubbs/parallel-consumer#149, mirroring confluentinc/parallel-consumer#310. Until that lands, the
mitigation available to you today is a retry limit in your own user function: count attempts on the
record and route it somewhere else rather than throwing forever.

The original deadlock described further up this section is unchanged by any of the above and is
still the reason this issue is open.
