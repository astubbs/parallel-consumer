# Parked, post-v6: use batching to compete on throughput with a plain consumer

Owner's idea, 2026-08-15:

> "The core consumer is basically doing massive batching. So I don't see any reason why we can't just
> do the same and have per-key ordering."

The claim worth testing, stated so it can be falsified: **a plain consumer's throughput advantage
comes from processing a whole `poll()` batch with no per-record ceremony, and Parallel Consumer with
a batch size set does structurally the same work — so the throughput gap should largely close, while
keeping the per-key ordering a plain consumer can only buy with more partitions.**

If that holds, it is the strongest performance statement this project can make, and it is the direct
answer to the objection that always arrives first.

## Why it is plausible

A plain consumer loops over the records one `poll()` returned. Parallel Consumer with a batch size
hands the user function a batch and does the same. The per-record work a user cares about is
identical in both; what differs is bookkeeping.

For the **proxy clients** the same argument is stronger still, because batching amortises the RPC hop
across N records rather than paying it per record — which is the entire counter to "one record at a
time over a wire protocol will be slow". See
[`next-batching-modes-for-clients.md`](next-batching-modes-for-clients.md), which records that the
API is already batch-shaped in core and that single-record is its degenerate case.

## Why it might not, and what that means for the experiment

**Parallel Consumer's per-record accounting does not disappear when records are batched.** A work
container per record, shard selection, the offset map's encoding of what completed out of order — all
of that is per record whatever the batch size. Batching amortises dispatch and the network hop; it
does not amortise the ledger.

So the honest question is not "does batching close the gap" but **what residual per-record cost
remains, and at what batch size does it stop mattering**. That is a measurement, not an argument, and
it is the one number that decides whether the claim above can be made in public.

Design the run so a bad answer is visible: sweep batch sizes rather than picking a flattering one,
report the plain consumer's number alongside, and publish the point where the curves meet — or do not
meet.

## Reuse rather than reinvent

The Kafka Streams branch already worked out the shape of a credible benchmark: a realistic scenario
alongside a plain unarguable one, **including publishing a case we expect to lose**, and the caveat
that single-run figures are not evidence. Both notes cite it:
[`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md)
and [`parked-perf-against-native-kafka-clients.md`](parked-perf-against-native-kafka-clients.md).
This comparison needs the same discipline, and the ordering-constrained workload is where the claim
actually lives — a comparison with no ordering requirement measures the wrong product.
