# Publishing patched-Kafka modules: packaging, docs, and licensing

Raised when `parallel-consumer-streams` (astubbs#255) flipped from throwaway to a published
alpha module. A second worktree is doing the same for Kafka **Connect**, so these questions now apply
to a family of modules rather than one, and answering them once is cheaper than twice.

## 1. End-user documentation and promotional material

Neither module has user-facing documentation in the generated README. Both need it, and the Streams
one has a substantiated claim worth using:

> 188 of Apache Kafka's own Streams tests (`StreamTaskTest` 101, `RecordCollectorTest` 59,
> `ProcessorContextImplTest` 28) pass unmodified against the patched classes, zero skipped.

**Do not quote that alone.** It holds with the seam **off**. With the seam **on**, `StreamTaskTest` is
68/101 - the 33 failures are the known semantic gap (offset/commit accounting, buffering, punctuation,
EOS gates, close/suspend, error wrapping, ordering). Quoted without that pairing it reads as
"Kafka-equivalent", which is not true yet. Both facts, or neither.

**The head-of-line blocking measurement is the strongest promotional material this module has.** One
partition, one 1500ms record at the head of the queue, twentyfour 25ms records behind it on other
keys. Same JVM, same patched classes, switching only the seam:

| | Stock Kafka Streams | PC-driven | |
|---|---|---|---|
| Quickest fast record | 1541ms | **27ms** | **57x** |
| Median fast record | 1858ms | 232ms | 8x |

The quickest figure is the one to lead with, because it is the claim itself rather than a summary of
it: under stock dispatch even the luckiest record behind the blocker waited for it, because
`PartitionGroup.nextRecord()` hands the partition over one record at a time. Under PC dispatch the
quickest paid its own 25ms and nothing else.

**Pair it with the control, always** - same rule as the 188 above. With every record on a **single
key** the same benchmark gives **0.69x**: PC is *slower*, because KEY ordering permits at most one
in-flight record per key and the pool handoff still costs something. That is not an embarrassment to
bury; it is what makes the headline credible, and it tells a reader exactly when this helps them.
Quoted alone the headline reads as "PC is 57x faster than Kafka Streams", which is false and which
the first competent reader will falsify.

Two further caveats belong with any published version: the comparison is **within one partition**
(stock Streams parallelises across partitions, and that is not what is being measured), and the
workload is **blocking IO**, which is the case PC is for - CPU-bound work would not behave this way.

**Publish the benchmark's rationale, not only its numbers.** A reader who cannot see *why* the
experiment measures what it measures has no way to judge the result, and the reasoning currently exists
only in the plan: why the latency distribution rather than throughput, why the **minimum** is the
statistic that states the claim while the p99 measures queueing depth, why both arms run in one JVM on
the same patched classes with only the seam switched, and why the per-record cost is a block rather
than a spin. Write that up as the explanation of what the simulation is trying to show.

The same write-up should carry the converging case explicitly - the workload where PC's advantage
disappears should cost **nothing** against stock, which is the reassurance a cautious reader actually
wants before adopting anything. ("No cost for convergence state" is read here as the single-key or
otherwise degenerate case, where key concurrency cannot help; if the owner meant something else, this
is the place to correct it.) That half of the claim is **not true yet**: single-key still measures
0.69x until the poll-wait fix lands, item 3 of `pr-ks-spike-next-work.md`. Write the explanation now,
publish the no-cost claim after.

**Also build a realistic-domain benchmark, as devil's-advocate cover for the synthetic one - BUILT.**
Results in [`docs/plans/2026-08-11-001-realistic-benchmark-result.md`](../plans/2026-08-11-001-realistic-benchmark-result.md);
the demonstration's front door is [`parallel-consumer-streams/DEMO.md`](../../parallel-consumer-streams/DEMO.md)
and `bin/streams-benchmark.sh` runs any of it. The headline is now a relatable one - a 1200-record
backlog cleared in 15s instead of 47s - and the write-up publishes the cells where the seam does
nothing, including a single-key floor at 0.99x and the halving of the advantage when key skew rises
from s=1.0 to s=1.5. The original ask, kept for the reasoning:

The
head-of-line blocking experiment was designed to expose PC's advantage - one blocker, fast records on
other keys, blocking IO. That is legitimate experiment design, because isolating the property is the
whole point, but it is also exactly the shape a sceptical reader dismisses as rigged: attack the design
and you never have to engage with the number. So design a second benchmark around a plausible business
workload - a domain where this data flow is what someone would genuinely build - and publish it
alongside. Its job is not to beat the synthetic figure. Its job is to leave "synthetic, unfair, false
advertising" nowhere to land, and to show what the effect looks like on a workload nobody chose to
flatter it. Pick the domain as though the hostile reviewer picked it.

**Once the implementation settles, write the key points of the user-facing explanation** - leading with
*why this is possible at all* and *why we know it works*, in terms a reader who does not know
`StreamTask` from `PartitionGroup` can follow. Those are the two questions anyone hearing "we patch
Kafka Streams internals so Parallel Consumer drives the processor chain" asks immediately, and both
answers already exist in scattered technical form: the mechanism (a seam at the point where a task
hands records to the processor chain, with key ordering preserved), and the evidence trail (Kafka's own
tests with the seam off, the benchmarks with their controls, the crash-safety test). Deliberately
*after* the work settles, because written earlier it documents a moving target - but before release,
since this is the material the README section gets built from.

Remember `README.adoc` is generated - edit `src/docs/README_TEMPLATE.adoc`. Its "Java Version per
Module" table also does not yet list the new module.

**Whatever is written must also make the blast radius obvious.** An existing PC user reading that
parallel-consumer now patches Kafka internals must not conclude that their own plain PC usage just
became riskier. It did not, and the point is provable rather than merely reassuring:

- The experimental modules are **leaves**. They depend on `parallel-consumer-core`; nothing depends on
  them, core's pom does not reference them, and adding them changed no shipped code in any existing
  module - verified against `origin/master`.
- The patched Apache Kafka classes ship **only inside the experimental module's own jar**.
- The seam is **unreachable** from the core, vert.x and reactor modules.

So depending on the experimental artifact is the entire opt-in, and not depending on it is a complete
opt-out requiring no configuration. Two traps in the wording: maturity is per module, so one alpha
must not downgrade how the release describes itself; and the correction must not overshoot into
burying the alpha, which is worth finding.

## 2. Packaging the forks - groupId AND artifactId

The open question: how do we publish artifacts that contain patched Apache Kafka code?

**A hazard that makes this urgent, found while verifying the alpha publish.** The published
`parallel-consumer-streams` jar contains six compiled classes under
`org/apache/kafka/streams/processor/internals/` - `StreamTask`, `AbstractProcessorContext`,
`ProcessorContextImpl`, `RecordCollectorImpl` and two inner classes. That is the whole mechanism: they
precede the real `kafka-streams` jar on the classpath and win.

Inside our own build that is controlled and tested. As a **published dependency** it is a split
package that a consumer never asked for: anyone with both `kafka-streams` and this module on the
classpath gets behaviour decided by classpath order, which build tools do not guarantee. The alpha
README makes it opt-in and loud, which is defensible for an experiment. It is not defensible as a
general-purpose dependency, and it is the strongest argument for solving packaging before anyone
depends on this.

Options to weigh:

- **Publish a patched `kafka-streams` under our own coordinates** and depend on it normally, instead of
  shadowing. Confluent does the analogous thing with `<version>-ccs`; changing the **groupId** is the
  cleaner fork signal. Costs a real build+publish pipeline for a Kafka fork.
- **Shade/relocate** the patched classes into our own package. Kills the split package, but shadowing
  only works *because* the package matches - relocation would break the mechanism outright.
- **Keep shadowing, alpha-only**, and never let it become a transitive dependency of anything.

Whatever we pick has to work for Streams and Connect the same way.

**Our own names carry "spike", and that should not ship.** The module is
`parallel-consumer-streams` and its code lives in `io.confluent.parallelconsumer.streams`.
"Spike" describes how the work started, not what it is now that it is published. The concrete ask is to
rename the package - at minimum take "spike" out of it - and it wants settling together with the
groupId and artifactId above rather than separately, because all three are one naming decision and a
rename is far cheaper before anyone depends on the coordinates. Two things left open: whether the
module and artifact names change in the same pass as the package, and what replaces "spike" - a
maturity word such as `-alpha` or `-experimental` bakes a status into a name that then has to change
again, while a plain `parallel-consumer-streams` does not but claims more than the module currently
earns. See `next-streams-module-graduation.md` for the rest of the spike-to-module work.

## 3. Licensing legality

Needs an actual answer, not an assumption. Known so far:

- Apache 2.0 permits modification and redistribution, provided §4 conditions hold: retain notices,
  include the licence, and **state prominently that files were changed**.
- `NOTICE` now carries that statement for the four Streams classes (added when the module became
  published; the repo previously shipped no Apache Kafka code at all).
- No Apache Kafka **source** is in this repository - the classes are generated at build time from the
  published sources jar plus a tracked patch. Only compiled output ships.

Still open:

- **Trademark**, which is separate from the licence. Apache's trademark policy is the constraint on
  naming, so publishing anything under `org.apache.kafka` coordinates is the thing to check first -
  this is really a naming question, not a copyright one.
- Whether shipping classes **in** the `org.apache.kafka` package (as opposed to under that groupId)
  raises the same concern.
- Whether the Connect module's patch surface changes any of the above.
- Whether `NOTICE` needs the same treatment for Connect, and whether one combined block or two.

## Why this is here rather than in a plan

Four related open questions spanning two in-flight worktrees, none of them started. When one is picked
up it should become its own plan; this file exists so the second worktree does not rediscover the same
questions independently.

## Delete when

Packaging and licensing are decided and recorded somewhere durable, and both modules have user-facing
documentation.
