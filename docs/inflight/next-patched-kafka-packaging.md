# Next: how do we package, publish and license modules that ship patched Kafka classes?

Raised: 2026-08-08. Not started.

Affects every module built on the build-time Kafka patch strategy - today `parallel-consumer-streams-spike`
(`feats/ks-on-pc-spike`), and the Connect work parked in `parked-connect-on-pc.md`. **This blocks
publishing either of them.** It does not block developing them, because both currently generate patched
classes locally and gitignore the output, which distributes nothing.

That distinction is the crux: generating patched classes on a developer's machine is just building.
Publishing them to Maven Central is redistribution of a modified Apache Kafka, and the obligations only
attach to the second.

## Questions to answer

**Coordinates.** Almost certainly a new groupId *and* artifactId. The fork already publishes under
`bz.stub.parallelconsumer` rather than `io.confluent.parallelconsumer` (see `project_fork_licensing`
reasoning and `AGENTS.md` "Fork status"), so the pattern exists - but a jar containing modified
`org.apache.kafka.*` classes is a different thing from a renamed fork of our own code. Decide the
coordinate before anything is published, because coordinates are permanent once released.

**Trademark, which is the sharp edge.** Apache 2.0 grants copyright and patent rights and explicitly does
**not** grant trademark rights. "Apache Kafka" is an Apache Software Foundation trademark. So publishing
under an `org.apache.kafka` groupId is out on both trademark and namespace-ownership grounds, and naming
and README copy must avoid implying ASF endorsement.

**Apache 2.0 redistribution obligations.** If we do ship modified Kafka code, section 4 requires carrying
the license, retaining copyright/patent/attribution notices, reproducing any `NOTICE` file content, and
carrying **prominent notices stating that we changed the files**. Work out what "prominent" means in
practice for a Maven artifact - per-file headers, a `NOTICE` entry, README text, or all three. Note this is
a different obligation from the fork's existing Confluent-header rule in `AGENTS.md`, which is about our
own upstream-derived sources, not about Kafka's.

**Class shadowing.** The patched classes currently keep their original fully-qualified names, so they win
on the classpath by ordering. That is fine for a build-time spike and hazardous for a published artifact:
consumers get split packages and load-order-dependent behaviour, and cannot easily tell which
`WorkerSinkTask` they are running. Options worth pricing:

1. Shade and relocate the patched classes to our own package, so nothing shadows Kafka.
2. Publish the *patch and the tooling* rather than patched bytecode, and apply it in the consumer's build.
   Distributes no Kafka code at all - cleanest legally, worst usability.
3. Publish patched classes under our own coordinate and document the classpath requirement loudly.

Option 1 is the likely answer but needs checking against how the patch actually binds - `PcTaskDispatcher`
and `pcspike.patch` will show whether relocation is even possible or whether the patch depends on being
the same class Kafka's own internals reference by name.

**Version coupling.** A published patched artifact is pinned to one Kafka version. Decide whether we
publish per-Kafka-version artifacts, a version range, or a classifier - and how that interacts with the
existing `kafka.version` matrix and the queued Kafka 4 move (`pr-53-java-baseline-kafka4.md`).

## Caveat

The licensing reading above is a starting point for the investigation, not advice - get a human, and
probably a lawyer, to confirm before anything is published. The trademark point in particular is the one
that most often surprises people who assume Apache 2.0 permits everything.
