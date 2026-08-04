# #53 - 0.7.x: Java baseline + Kafka 4

**The only reason to move off Java 8 is Kafka 4.** kafka-clients 4.x needs **Java 11**, so that is the
target baseline ("don't be stricter than Kafka"). Jabel is what lets `javac` accept Java 17 syntax
while emitting Java 8 bytecode; the branch holds a provisional state (Jabel removed, `release=17`)
plus the Kafka 4 research docs.

Approaches, decided when the work actually starts:

- **Keep Jabel at `--release 11`** - zero source refactor. Currently breaks Lombok `@StandardException`
  generation with 25 errors; unproven whether a Lombok bump fixes it. *Try this first.*
- **Remove Jabel and rewrite** the Java 14+ syntax in ~9 core files, including the offset-encoding hot
  path.
- **Native Java 17** - dispreferred, drops Java 11-16 users.

Remaining units (plan on the branch, `docs/plans/2026-04-23-001-feat-apache-kafka-4-support-plan.md`):
bump `kafka.version` 3.9.1 → 4.2.x plus the TestContainers CP image; migrate removed APIs
(`sendOffsetsToTransaction(Map,String)`, `MockConsumer(OffsetResetStrategy)`,
`new ConsumerGroupMetadata(String)`); downstream module audit; flip `test-kafka-compat` to a blocking
3.9.1 regression check; docs. Deferred further: `parallel-consumer-share` (KIP-932).
