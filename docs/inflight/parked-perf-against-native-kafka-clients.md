# Parked, post-v6, **low priority**: reproducible performance against each language's native Kafka client

Owner's idea, 2026-08-15, explicitly "super low priority". Separate from - and deliberately not a
substitute for - the release-time measurement in
[`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md),
which compares each language's client against the standard Java client only. That one is the
shipping number; this one is research.

The comparison here: each language's proxy client against **that language's accepted native Kafka
client** - the library its community actually reaches for (in most cases a librdkafka binding, and
in Go and Java a native-protocol implementation instead).

## Why it is worth doing eventually

It is the question every evaluator will privately ask, and answering it ourselves - with the method
published - is better than leaving it to a benchmark someone else runs badly. It also produces
evidence for a claim `STRATEGY.md` already makes about where this project's advantage lies, and that
document is explicitly a claims document that work must be willing to falsify.

## Why it is low priority, and easy to do badly

**The two things are not doing the same job**, so a naive throughput comparison is close to
meaningless and will flatter whichever side the workload happens to suit. A native client consumes a
partition serially; the point of this project is key-ordered concurrency *beyond* partition count.
Measured on "consume as fast as possible with no ordering requirement", a native client should win -
it has no sidecar hop, no gRPC, no protocol - and that result says nothing about the case the product
exists for. The comparison only means something if the workload is one where a user genuinely needs
ordering guarantees the native client can only get by adding partitions.

**The design work is largely already done elsewhere**: `feats/ks-streams-seam-on-upstream-gate`
worked out the two-shapes approach - a realistic scenario shaped like what this project is for
(including publishing a case we expect to lose) alongside a plain, unarguable model run just to see
what happens - plus the caveat that single-run figures are not yet evidence. Its notes are cited in
full in [`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md);
read them before designing anything here, since this comparison needs the same two shapes and the
same honesty about the unfavourable case.

The rest of the design work is the hard part, not the running:

- Define the workload where the comparison is fair, and state plainly the workloads where it is not.
- Report the sidecar's cost honestly as part of the result rather than engineering it out of the
  benchmark. A number that hides the hop is not reproducible in a user's system.
- **Reproducible means re-runnable by a sceptic**: pinned client versions, pinned broker, fixed
  hardware or a container that fixes it, a fixed dataset, and the method published beside the
  numbers. A benchmark nobody can re-run is quoted forever and corrected never - which is the main
  argument for doing this carefully or not at all.
- Native clients arrive as **test-scope dependencies only**. Several are librdkafka bindings, and the
  client modules exist partly to avoid that dependency; it must never reach their runtime classpath.

## Where it sits

Post-v6, after the shared conformance and performance suites exist - they are the harness this would
reuse, and building it before them would mean building a second one.
