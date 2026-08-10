---
title: Redistribution obligations attach at publish, not at build
date: 2026-08-10
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: high
applies_when:
  - A module generates a patched or modified copy of a third-party dependency at build time
  - You are about to enable deployment or snapshot publication for a module containing modified third-party classes
  - A reviewer asks why an experimental module is excluded from publication and is tempted to re-enable it
  - You are copying a publication guard from one module to another
  - You are recording a decision that overrode a genuine counter-argument
tags:
  - licensing
  - apache-2
  - trademark
  - redistribution
  - maven-central
  - publication-gate
  - notice
  - governance
---

# Redistribution obligations attach at publish, not at build

> **Not legal advice.** This is an engineering summary written to orient the next
> person who touches these modules, and to stop the boundary being crossed by accident.
> Every conclusion below needs a human - and probably a lawyer - to confirm before
> anything is actually published.

## Context

`parallel-consumer-streams` and `parallel-consumer-connect` build patched copies of Apache
Kafka classes at build time: unpack the released sources jar, apply a tracked patch, compile
the result into the module's own output. The technique itself is documented separately in
`docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md`.
No Apache source is committed - only the `.patch` file is tracked, and the generated trees are
gitignored.

That last fact is what makes the licence question easy to get wrong in both directions. Because
nothing third-party is in the repository, it is tempting to conclude either that there is no
obligation at all, or that the obligation was discharged the moment the patch file was committed.
Neither is right, and the reason is a distinction that is easy to state and easy to forget:

**Generating a patched copy of a dependency locally distributes nothing. Publishing it is
redistribution, and that is the moment licence obligations attach.**

For local development and CI, building these modules is just *building*. Nothing leaves the
machine. No obligation is triggered. The moment such a jar is published - **including a routine
`SNAPSHOT` on a master push** - it becomes redistribution of a modified Apache Kafka, and a
specific set of obligations attaches at once.

Context and the open questions live in `docs/inflight/next-patched-kafka-packaging.md`
(raised for astubbs/parallel-consumer#240, the Connect work, and astubbs/parallel-consumer#255,
the Streams work); the resulting gate is recorded in
`docs/inflight/release-experimental-modules-publication-disabled.md`.

## Guidance

### 1. Know which obligations attach, and which one actually bites

**Trademark is the sharp edge, not copyright.** Apache 2.0 grants copyright and patent rights and
explicitly does **not** grant trademark rights. "Apache Kafka" is an Apache Software Foundation
mark. Two consequences: the artifact cannot use `org.apache.kafka` Maven coordinates, and its
naming and documentation must not imply ASF endorsement. This is the clause that surprises people,
because the working assumption is usually that Apache 2.0 permits everything.

**Section 4(b) requires a prominent notice that files were changed.** "Prominent" for a Maven
artifact needs a deliberate answer rather than a default one - per-file headers, a `NOTICE` entry,
README text, or all three. Note this obligation is about *Kafka's* sources, and is distinct from
the fork's existing Confluent-header rule in `AGENTS.md`, which is about this project's own
upstream-derived files (see
`docs/solutions/workflow-issues/copyright-header-rules-for-fork-2026-04-21.md`).

**`NOTICE` content from the upstream project must be reproduced.** `NOTICE:12-29` shows the shape
this repo settled on for Streams: it names the four modified classes, attributes the ASF, states
plainly that the files were changed, and points at the tracked patch as the complete expression of
the change. That paragraph is written for the *published artifact*, not for the repository - which
is exactly the point of this learning.

There are further open questions - coordinates, which upstream modules to fork, version coupling -
and they are enumerated in `docs/inflight/next-patched-kafka-packaging.md`. This doc is not about
answering them. It is about not needing them answered in a hurry because a merge published
something.

### 2. Treat the publish step as a licence boundary, and never cross it as a side effect

This is the practical consequence and the reason the distinction is worth writing down. A module
that publishes by default crosses the boundary on the next merge to master, before anyone has
answered any of the questions above. A legal event should not be reachable from a green CI run.

So the modules publish nothing - not releases, not snapshots - until the packaging and licensing
questions are resolved and the module is judged ready for consumers. Re-enabling is its own
deliberate, reviewed change.

### 3. The guard is two-part per module, and only one part actually stops Central

Each guarded module's pom carries **both**:

- the three properties `maven.deploy.skip`, `maven.install.skip`, `gpg.skip`, all `true`, and
- a `central-publishing-maven-plugin` block setting `<skipPublishing>true</skipPublishing>`.

**The plugin block is the half that actually stops Central publication**, because `skipPublishing`
is plugin *configuration*, not a user property. A properties-only copy silently protects nothing -
and silently is the operative word, since the build stays green either way and the failure is only
observable after the artifact is already on Central.

It is also version-sensitive. In `central-publishing-maven-plugin` 0.8.0, `skipPublishing=true` on
the module that is *last* in reactor order suppressed the entire aggregated bundle upload rather
than just that module; per-module evaluation arrived in 0.9.0+
(`parallel-consumer-examples/pom.xml:28-32` records the incident). 0.9.0 is the floor, not the pin:
the root pom currently pins 0.11.0 at `pom.xml:277-281`, inside the `maven-central` profile.

Both halves, every guarded module, every time.

### 4. Record the argument the gate overrode, beside the gate

This generalises well past licensing. When a gate overrides a genuine counter-argument, write the
overridden argument down next to the gate rather than paraphrasing it away.

Here the counter-argument was real and is recorded verbatim in
`docs/inflight/release-experimental-modules-publication-disabled.md`: *"depending on the artifact
IS the opt-in, so a user who adds it should not then have to switch it on."* Publishing is also
what makes a field-testing request real - a module nobody can depend on gets no field testers.

It was overridden on a ground the argument does not address: opt-in semantics are a usability
question, redistribution is a legal one, and the obligations attach before any of them has an
answer. But the reason to keep the losing argument on the page is not fairness. **A gate that
looks like an oversight gets "fixed" by the next contributor; a gate that visibly beat a
considered argument does not.** An agent or reviewer who believes publication should be re-enabled
then knows to surface the argument to the owner rather than edit the poms, and knows which
questions actually unblock it.

## Why This Matters

**The boundary is invisible in the build output.** Every signal a developer normally reads - tests
pass, the module builds, the jar lands in the module's build output - is identical on both sides of
it. Nothing in a green build distinguishes "we compiled a modified Apache Kafka locally" from "we
shipped one".
Only the publish step differs, and publish steps are the ones most likely to be triggered by
automation rather than by a person.

**A snapshot is a publication.** This is the specific trap. Snapshot publishing feels like an
internal convenience, gets wired into master-push workflows, and is easy to leave out of a
licensing conversation that is framed around "the release". It is redistribution on the same terms
as a release.

**The obligations are cheap to discharge and expensive to discharge late.** Naming the changed
files in `NOTICE` is a paragraph. Picking non-`org.apache.kafka` coordinates costs nothing before
the first publish and is permanent afterwards, because released coordinates cannot be taken back.
The whole point of the gate is to keep these decisions in the cheap window.

**Trademark is the obligation people do not price in.** Copyright compliance is the part everyone
expects and the part the `NOTICE` work already covers. The clause that catches projects out is the
one Apache 2.0 explicitly withholds.

## When to Apply

- Any time a module produces an artifact containing modified third-party code, whether by
  build-time patching, shading with edits, or vendoring - decide the publication posture *before*
  the module is first merged, not before it is first released.
- Any time you are tempted to enable snapshot publication "just so people can try it": that is the
  boundary crossing, not a preview of it.
- Any time you copy a publication guard between modules: copy both halves and verify the plugin
  block landed, not just the properties.
- Any time a reviewer proposes re-enabling publication on usability grounds: the questions in
  `docs/inflight/next-patched-kafka-packaging.md` are the path, not re-making the opt-in case.
- Any time a decision overrides a real argument: record the overridden argument next to the
  mechanism that encodes the decision.

## Examples

**The guard, both halves, as it appears in each module.** Properties
(`parallel-consumer-streams/pom.xml:44-46`, `parallel-consumer-connect/pom.xml:36-38`):

```xml
<maven.deploy.skip>true</maven.deploy.skip>
<maven.install.skip>true</maven.install.skip>
<gpg.skip>true</gpg.skip>
```

Plugin configuration (`parallel-consumer-streams/pom.xml:216-222`,
`parallel-consumer-connect/pom.xml:266-272`) - this is the half that stops Central:

```xml
<plugin>
    <groupId>org.sonatype.central</groupId>
    <artifactId>central-publishing-maven-plugin</artifactId>
    <configuration>
        <skipPublishing>true</skipPublishing>
    </configuration>
</plugin>
```

**The comment that makes the gate survive contact with the next contributor.** From
`parallel-consumer-streams/pom.xml:37-43` - it states the decision, its owner, its expiry
condition, what re-enabling requires, and the footgun in the mechanism itself:

```xml
<!-- PUBLICATION DISABLED - owner decision, 2026-08-10: neither this module nor the Connect one
     publishes anything, releases or snapshots, until the fork packaging and licensing questions
     are resolved (docs/inflight/next-patched-kafka-packaging.md) and the module is judged sane.
     Do NOT remove before merge; re-enabling is its own reviewed change that deletes
     docs/inflight/release-experimental-modules-publication-disabled.md in the same commit.
     Mirrors parallel-consumer-examples/pom.xml - note skipPublishing below is plugin
     CONFIGURATION, not a property; a properties-only copy silently protects nothing. -->
```

**Bad - the properties-only copy.** This is a plausible, well-intentioned partial copy, and it
leaves the module publishing to Central:

```xml
<properties>
    <maven.deploy.skip>true</maven.deploy.skip>
    <maven.install.skip>true</maven.install.skip>
    <gpg.skip>true</gpg.skip>
</properties>
<!-- ...and no central-publishing-maven-plugin block. Central publication proceeds. -->
```

**The obligation the published artifact needs, once the boundary is deliberately crossed**
(`NOTICE:12-29`):

```
The parallel-consumer-streams artifact additionally contains MODIFIED versions of
four classes from Apache Kafka:
  ...
These files have been CHANGED by Antony Stubbs and contributors ... The changes are
expressed as, and limited to, the patch tracked at
parallel-consumer-streams/src/main/patch/pc-streams.patch; no Apache Kafka source is
redistributed in this repository.
```

Written for the artifact, not for the repository - the repository never carried the obligation.

## Related

- `docs/inflight/next-patched-kafka-packaging.md` - the open packaging and licensing questions
  (coordinates, trademark, section 4, coexistence, version coupling) that must be answered before
  publication is re-enabled
- `docs/inflight/release-experimental-modules-publication-disabled.md` - the gate itself, its
  two-part implementation, and the overridden opt-in argument recorded verbatim
- `docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md` -
  the build-time patching technique these modules use, including the split-package hazard that
  makes the distribution question hard in the first place
- `docs/solutions/workflow-issues/copyright-header-rules-for-fork-2026-04-21.md` - the fork's own
  copyright-header rules, a *different* obligation covering this project's upstream-derived sources
  rather than Kafka's
- astubbs/parallel-consumer#240 (Kafka Connect integration) and astubbs/parallel-consumer#255
  (Kafka Streams on PC) - the two workstreams that produce these artifacts
- `NOTICE:12-29` - the changed-files statement for the Streams artifact
- `parallel-consumer-examples/pom.xml:28-32` - the `central-publishing-maven-plugin` 0.8.0
  reactor-order incident that made per-module `skipPublishing` a version requirement
