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

## History, so this does not get "fixed" back

The Streams module briefly shipped as a published alpha, with its NOTICE and README work done properly.
Publishing is an owner call, and the owner has now made it: not yet. This file is the record. An agent
that believes publication should be re-enabled should surface the argument to the owner, not edit the
poms.
