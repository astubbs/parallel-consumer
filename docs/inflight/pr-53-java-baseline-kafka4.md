# astubbs#53 - 0.7.x: Java baseline + Kafka 4

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - 0.7.x by its own scope -->


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
<!-- file-refs: N/A - the sentence says the plan is on the branch, not on master -->

## The ladder above 11, recorded 2026-09-11

**"Native Java 17 - dispreferred" above is no longer the owner's position.** On 2026-09-11 the owner said the project has to plan to move to JDK 17 at some point, and asked whether Kafka 4 forces it. It does not: kafka-clients 4.x forces 11, as the top of this note says, and nothing else in the corpus forces 17 for the core. What has changed is that the rungs above 11 now exist and were being decided in places that do not see each other, so this section owns the ladder and the decision it needs.

| Floor | Forced by | Owned by | State on 2026-09-11 |
|---|---|---|---|
| 11 for the core | Kafka 4 clients | This note and the plan above | Deferred to 0.7.x, as above |
| 17 for the module the web GUI and the MCP server share | The owner's decision KD14 in [`docs/plans/2026-09-11-002-feat-hasten-mcp-interface-plan.md`](../plans/2026-09-11-002-feat-hasten-mcp-interface-plan.md): JVM MCP SDKs are built for 17, and a 17 floor massively simplifies that module's development; an application on 8 or 11 keeps the core and loses the GUI and MCP module, which the owner judged a reasonable compromise | That plan; the Mutiny module's pom is the precedent for a module declaring a floor above the core's | Decided; lands with the MCP work in 6.1 ([`release-v6.1-scope.md`](release-v6.1-scope.md)) |
| 17 to build and test | Already true today (`source.version 17`); JUnit 6 needs 17 too, per [`deps-deferred-majors.md`](deps-deferred-majors.md) | The root pom | In place; building on anything newer than 17 has its own blockers, in the JDK-21 build note on the virtual-threads branches |
| 21 for virtual threads | Virtual threads are a Java 21 feature; the virtual-threads plan (dated 2026-08-22, on the `feats/ideate-distributed-throttling` branch, astubbs#360) needs them at runtime | That plan | Not decided as a baseline; the plan's own shape is the question |

**The decision this leaves open, and whose it is:** whether the core's destination is 17, so that the Kafka 4 move goes straight there instead of stopping at 11, or 11 first and 17 later. Kafka 4 does not force it. What argues for going straight to 17: the build already needs 17, the GUI and MCP module is 17, virtual threads want 21 so 17 is a waypoint, and landing the library's floor once is cheaper than twice. What argues against is the reason "dispreferred" was written: runtime users on 11 to 16, whose number nobody has measured. Owner's call, and the evidence it wants is the runtime-Java distribution of adopters. Decide it before this note's work is picked up, so the plan above lands the library once.
