# Publishing patched-Kafka modules: packaging, docs, and licensing

Raised when `parallel-consumer-streams-spike` (astubbs#255) flipped from throwaway to a published
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
`parallel-consumer-streams-spike` jar contains six compiled classes under
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
