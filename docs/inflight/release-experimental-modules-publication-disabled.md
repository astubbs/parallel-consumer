# Release gate: the experimental modules publish nothing - do not reverse before merge

Decided: 2026-08-10, by the owner. Applies to `parallel-consumer-streams` and
`parallel-consumer-connect`.

**Neither module publishes anything - not releases, not snapshots - until two conditions hold:**

1. The fork packaging and licensing questions are resolved: `next-patched-kafka-packaging.md`.
2. The module in question is judged sane enough to put in front of consumers.

Both jars contain compiled, modified `org.apache.kafka.*` classes. Publishing one - **including a
snapshot on a routine master push** - is redistribution of a modified Apache Kafka, which is the event
that makes the trademark, NOTICE, and section-4 changed-files obligations attach. That must never happen
as a side effect of a merge; it happens only as its own deliberate, reviewed change that deletes this
file in the same commit.

## How the guard is implemented - both halves are required

Each module's pom carries:

- the three properties `maven.deploy.skip`, `maven.install.skip`, `gpg.skip`, all `true`, and
- a `central-publishing-maven-plugin` block with `<skipPublishing>true</skipPublishing>`.

The plugin block is the half that actually stops Central publication: `skipPublishing` is plugin
**configuration**, not a user property, so a properties-only copy silently protects nothing. Also
version-sensitive: in plugin 0.8.0 `skipPublishing=true` on the reactor-last module skipped the entire
aggregated bundle upload; per-module evaluation arrived in 0.9.0+ (root pom pins 0.10.0). Both halves,
per module, always.

## History, and the argument on the other side

The Streams module shipped briefly as a published alpha, with its NOTICE and README work done properly,
and the case for it was not weak. From that commit: *"depending on the artifact IS the opt-in, so a user
who adds it should not then have to switch it on."* Publishing is what makes a field-testing request real
- a module nobody can depend on gets no field testers, and work that lives only on a branch dies.

That argument is recorded here rather than paraphrased away, because a gate that looks like an oversight
gets "fixed" by the next agent, while a gate that visibly overrode a real argument does not. It was
overridden on a ground the argument does not address: both jars ship compiled, modified
`org.apache.kafka.*` classes, so publishing is redistribution of a modified Apache Kafka, and the
trademark, NOTICE and section-4 obligations attach at that moment - before any of them has an answer.
Opt-in semantics are a usability question; redistribution is a legal one, and it is the owner's call.

So: an agent who believes publication should be re-enabled should surface the argument to the owner, not
edit the poms. The blocking questions are in `next-patched-kafka-packaging.md`, and answering those is
the path to re-enabling - not re-making the opt-in case, which was already heard.
