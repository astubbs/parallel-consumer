---
title: Status words belong in status artefacts, not identifiers
date: 2026-08-10
category: conventions
module: naming
problem_type: convention
component: development_workflow
severity: medium
applies_when:
  - Naming a class, package, module, method, config key, or test fixture while the work is still exploratory
  - Reviewing a PR that introduces "spike", "experiment", "poc", "new", "v2", or "temp" into an identifier
  - Deciding whether an experimental API needs a warning at its import site
  - Renaming a module, package, or property before it is published anywhere
tags:
  - naming
  - conventions
  - code-review
  - interfacestability
  - spike
---

# Status words belong in status artefacts, not identifiers

## Context

An agent working on a sibling Kafka Connect branch (astubbs#240) named a shared
test helper `SpikeTestSupport`. The repo owner corrected it: "Spike is meta
about the PR, it doesn't belong INSIDE the PR code." The helper was renamed to
`TestEnvironment` - the name for what it actually is, a class that sets up a
test environment, not a class about the PR's development stage. The agent
carried the correction forward because it generalised past its own code.

The same defect existed at much larger scale in the Kafka Streams module
(astubbs#255). "Spike" had spread into the module name, the Java package, patch
filenames, a system property, several `pom.xml` properties, test topic and
store names, and roughly thirty comment markers. PR astubbs/parallel-consumer#271
renamed all of it to describe what each thing IS - the module, the seam, the
dispatch mechanism - rather than what stage of development it was in.

## Guidance

A status word ("spike", "experiment", "poc", "new", "v2", "temp", "wip",
"draft") describes where a piece of work stands relative to a timeline. An
identifier describes what a piece of code IS. These are different axes, and
conflating them puts a fact that is true today and false tomorrow into a
place that is expected to stay true indefinitely.

When you catch yourself reaching for a status word in a class name, package,
module, config key, property, or test fixture, ask what the thing actually
does or represents, and name that instead:

- `SpikeTestSupport` -> `TestEnvironment` (it sets up a test environment)
- module `parallel-consumer-streams-spike` -> `parallel-consumer-streams` (it is
  the Kafka Streams integration module)
- package `io.confluent.parallelconsumer.streams.spike` ->
  `io.confluent.parallelconsumer.streams` (it is the streams seam)
- a "spike dispatch" class -> name it for the dispatch mechanism it implements

Status belongs in the artefacts whose job is to track status and that are
*expected* to change: the PR title, the plan document, the inflight/ledger
entry, a module's README description, or a maturity annotation the language
or framework provides for exactly this purpose. In this codebase that
mechanism is `org.apache.kafka.common.annotation.InterfaceStability.Evolving`,
applied to `ParallelConsumerOptions` in
`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java`.
That annotation says "this API may still change" without lying about what the
class is.

**Timing matters.** Renaming before an artefact is published - before anyone
outside the branch has imported it - is free: it is a local rename with no
external callers to break. Renaming after publication breaks every caller
that imported the old name. Do the rename while the publication gate is still
closed, not after.

## Why This Matters

Status is temporary; the code that carries the name is not. The moment an
experiment graduates, every identifier carrying the status word starts
lying, and nobody schedules the mechanical sweep to fix it - it becomes a
silent, spreading inaccuracy instead.

It is a lie that reads as true. A newcomer reading `SpikeTestSupport` has no
reason to doubt it - they will reasonably assume it supports something
formally called a "spike," not that it is an ordinary test helper that
happened to be written during one. The name actively misleads rather than
merely under-informing.

The failure mode compounds with scale. One misnamed helper is a five-minute
fix. Thirty scattered markers across a module name, package, patch files, a
system property, pom properties, and test fixtures (astubbs/parallel-consumer#271)
is a coordinated sweep across build config, source, and tests - all because
the word was allowed to spread past the one place (the PR title) where it was
actually accurate.

## When to Apply

- Naming any class, package, module, method, config key, or test fixture
  while the surrounding work is still exploratory or unmerged
- Reviewing a PR and spotting "spike", "experiment", "poc", "new", "v2",
  "temp", "wip", or "draft" inside an identifier rather than in the PR title,
  plan, or ledger
- Deciding how to flag an unstable API to callers - reach for a stability
  annotation or a contract word ("preview", "experimental", "unstable") that
  stays true until the API is declared stable, not a word that describes the
  team's development history
- Before an artefact (module, package, class) is published anywhere -
  this is the free window to rename; after publication the rename has real
  callers to fix

## Examples

**Bad - status word baked into the identifier:**

```java
// astubbs#240, before correction
public class SpikeTestSupport {
    // sets up Kafka + Connect containers for tests
}
```

**Good - identifier names what the thing is; status stays in the PR/plan:**

```java
// astubbs#240, after correction
public class TestEnvironment {
    // sets up Kafka + Connect containers for tests
}
```

**Bad - status word spread across a whole module (astubbs#255, before PR astubbs/parallel-consumer#271):**

- Module: `parallel-consumer-streams-spike`
- Package: `io.confluent.parallelconsumer.streams.spike`
- System property, pom properties, patch filenames, test topic/store names,
  and roughly thirty comment markers, all carrying "spike"

**Good - identifiers describe the thing, status lives in the PR title (PR astubbs/parallel-consumer#271):**

- Module: `parallel-consumer-streams`
- Package: `io.confluent.parallelconsumer.streams`
- PR title carries the status instead: "feat(streams) astubbs#255: give a
  Kafka Streams topology PC's per-key concurrency"

**Correct usage that the sweep deliberately kept - status vocabulary describing
status, or method vocabulary describing method, is not the defect:**

`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java`
keeps "Experiment A" and "control arm" throughout:

```java
/**
 * Experiment A of this module's benchmarks: does PC-driven dispatch remove head-of-line blocking?
 */
...
log.info("=== Experiment A - head-of-line blocking | blocker={}ms fast={}ms poolSize={}", ...);
...
log.info("=== Experiment A3 - negative control, single key");
// Compared on the same statistics the experiment is asserted on, or the control would not be controlling
// would hand the control arm the very parallelism this experiment says it lacks.
```

These are the scientific terms for a controlled measurement - they describe
the *method* used on the benchmark, not the *maturity* of the code. A
Javadoc that calls an experiment an experiment, in a class whose whole job is
to run one, is accurate and should stay. The test that separates correct use
from the anti-pattern: does the word describe the thing's identity, its
development status, or the method applied to it? Only the middle case - status
describing a thing that is not itself a status artefact - is the problem this
convention targets.

**Counter-argument, and why it does not change the guidance:** does a status
word in a package name usefully warn a caller at the import site, where a
README cannot reach? Partly - but it answers the wrong question. It tells the
reader about the team's development history, not about the reader's risk.
The fix that actually serves the import-site-warning goal is a contract word
that stays true after the status changes ("preview", "experimental",
"unstable"), or the mechanism the language already provides for it:
`@InterfaceStability.Evolving` on `ParallelConsumerOptions` in
`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java`
signals instability at the declaration site without claiming the class is
about a "spike."

## Related

- PR astubbs/parallel-consumer#271 (issue astubbs#255) - the Kafka Streams module rename
- astubbs#240 - the Kafka Connect branch where the correction originated
  (`SpikeTestSupport` -> `TestEnvironment`)
- `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java` -
  `@InterfaceStability.Evolving` precedent for signaling instability without a status word
- `parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java` -
  preserved "Experiment A" / "control arm" as correct method vocabulary, not status vocabulary
