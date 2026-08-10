# Next: how do we package, publish and license modules that ship patched Kafka classes?

Raised: 2026-08-08. Not started.

Affects every module built on the build-time Kafka patch strategy - today `parallel-consumer-streams-spike`
(`feats/ks-on-pc-spike`), and the Connect work parked in `parked-connect-on-pc.md`. **This blocks
publishing either of them.** It does not block developing them, because both currently generate patched
classes locally and gitignore the output, which distributes nothing.

That distinction is the crux: generating patched classes on a developer's machine is just building.
Publishing them to Maven Central is redistribution of a modified Apache Kafka, and the obligations only
attach to the second. We intend to publish, so they attach - and only the patch files are version
controlled, so the repository itself never carries Kafka source.

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

**Coexistence, not shadowing.** The distribution model is settled (user, 2026-08-08) and it is the same one
the KS work will use: we publish a **fork of the patched Kafka module under our own coordinates**, as a
drop-in replacement. Classes keep their original fully-qualified names on purpose - the patch depends on
being the same class Kafka's own internals reference by name, so relocating or shading it is not available.
Only the **patch files** are version-controlled; they are applied at build and publish time, and the
generated sources and classes stay gitignored, exactly as `parallel-consumer-streams-spike` already does
with `src/main/patch/pcspike.patch` and `bin/apply-patch.sh` / `bin/regen-patch.sh`.

This is well-precedented - shipping a modified Kafka derivative under your own coordinates is what several
vendors do - and it removes the split-package hazard *provided consumers get exactly one of the two jars*.
So the open work here is dependency hygiene, not classpath archaeology:

- Our published module must depend on our forked artifact and **exclude** the upstream one, and we should
  document the exclusion for consumers who pull `org.apache.kafka` transitively by another route.
- Consider a `bannedDependencies` enforcer rule so a build that ends up with both fails loudly instead of
  resolving by classpath order. The root pom already uses `bannedDependencies` for log4j and javafaker, so
  the mechanism is in place.
- Fork only the modules actually patched. A patched `connect-runtime` still depends on stock `connect-api`
  and `kafka-clients` from upstream, which keeps the republished surface small and the drift low.

**Scope of the fork - decided (user, 2026-08-08): separate artifacts, one per patched Kafka module.** A
`connect-runtime` fork for this work, a `kafka-streams` fork for the KS work, published and versioned
independently. A consumer who wants Connect never pulls a patched Streams, each artifact tracks only the
upstream module it mirrors, and the two spikes can reach release readiness on their own schedules.

**Naming - decided (user, 2026-08-08): a distinct artifactId, not the upstream one.** Reusing
`connect-runtime` under our groupId would have made the drop-in relationship self-evident, but Maven
deduplicates on groupId *and* artifactId together, so it prevents nothing and reads as though it does.
A distinct artifactId makes it plain in a dependency tree that this is our patched build rather than
Apache's, which is the property that matters when someone is debugging why their `WorkerSinkTask` behaves
oddly. Pick names that keep both facts legible - which upstream module it mirrors, and that it is patched.

Naming still enforces nothing. The `bannedDependencies` rule above is the only thing that makes a build
resolving both jars fail rather than silently picking one by classpath order, so it is required, not
optional.

**Version coupling.** A published patched artifact is pinned to one Kafka version. Decide whether we
publish per-Kafka-version artifacts, a version range, or a classifier - and how that interacts with the
existing `kafka.version` matrix and the queued Kafka 4 move (`pr-53-java-baseline-kafka4.md`).

## Caveat

The licensing reading above is a starting point for the investigation, not advice - get a human, and
probably a lawyer, to confirm before anything is published. The trademark point in particular is the one
that most often surprises people who assume Apache 2.0 permits everything.
