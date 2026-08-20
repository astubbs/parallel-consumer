# A Class 2 chaos RED that was the runner, not a stall - and how to tell which you have

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->


[`test-chaos-phase2.md`](test-chaos-phase2.md) **owns the chaos suite**; this note exists only
because its "widen it if it ever flakes" line still reads as unfired, and the flake happened and was
measured. The measurement is on branch `docs/chaos-class2-contention-finding`, in a single commit
(`git show 6ffd71585`) that nothing on master references.

**What happened.** The eager W4 arm went RED with a signature that looked exactly like the real
Class 2 occurrence the hunt is looking for: 13 of 80 partitions stagnant for 154.5s against the 150s
bound, group STABLE, those partitions holding 94.5% of the run's unconsumed backlog. Replaying the
same seed on an uncontended box peaks at 121.3s, raises no violations and drains past the expected
count. **A real Class 2 stall is unbounded**, so a schedule that finishes on replay does not encode
one. What differed was the clock: `Performance` and `Chaos Pain Suite` share the self-hosted box and
overlapped across the storm phase - the contention
[`pr-highcpu-fast-feedback.yml`](../../.github/workflows/pr-highcpu-fast-feedback.yml) warns about
in its own header. Same seed, 121s to 154s, roughly 27% inflation, over a bound whose modelled worst
legitimate case is 100s.

**The decision is open, and the branch leans one way.** Widen the bound, or stop the two jobs
sharing the box - prefer the second, because widening hides the cause and the bound's sensitivity is
the entire tripwire.

**The sequencing point is the more useful half.** The suite runs against a master missing several
known fixes inside its own blast radius - rebalance, offsets, commit path, executor lifecycle. A RED
seen before those land is odds-on one of them, so hunting it as a new unknown spends effort on an
already-solved bug. Land the backlog, then re-run. That also qualifies the neighbouring "0 hits in a
9-seed sweep" claim, which was measured against the same incomplete tree.

## Reconciled with the family ledger: the same signature has TWO causes

[`bug-857-family.md`](bug-857-family.md) records a `CLASS2_STALL` whose seed
(`6825864417772979246`) replays **RED on plain master, locally, uncontended** - 10 violations,
`lagStagnation` 154387ms. This note records one whose seed replays **GREEN uncontended**, peaking at
121.3s. Both measurements are sound; neither refutes the other, because they are different
occurrences. What they jointly establish is more useful than either alone:

**~154s `CLASS2_STALL/LAG_STAGNATION` is a signature, not a diagnosis.** The 154s figure appears in
both, so it discriminates nothing on its own - it is roughly what a 150s bound produces once crossed,
whatever crossed it. Do not read the number as evidence either way, and do not read "every partition
stagnated at the same instant" as evidence of starvation: violation counts vary run to run on a fixed
seed, and the ledger warns explicitly that the frozen-partition set is not a fingerprint.

**The discriminator is a replay of that seed on an uncontended box**, and nothing cheaper works:

| Replay uncontended | Reading |
|---|---|
| **RED**, unbounded, does not drain | A family occurrence. Record it in `bug-857-family.md`. |
| **GREEN**, drains, peaks under the bound | Contention. Record it here. |

A real Class 2 stall is unbounded, so a schedule that finishes on replay cannot encode one - that is
the whole argument, and it is why the replay is worth the minutes it costs before a hunt begins.

**Both records were written without knowing about the other**, which is how they came to look like
disagreement. Neither cited the other until now.

## Delete when

The contention is fixed or the bound is widened, and the finding is folded into the chaos ledger.
