---
title: A gate cannot certify a run whose filters it cannot see
date: 2026-09-03
category: workflow-issues
module: parallel-consumer-core
problem_type: workflow_issue
component: test_infrastructure
severity: high
root_cause: config_error
resolution_type: test_improvement
applies_when:
  - "Building a gate that reports on OTHER tests - a coverage register, a roster guard, a claim index"
  - "A gate reads compiled annotations, source text, or any other static artifact to decide whether something is tested"
  - "Deciding whether to tag a gate the same way as the tests it guards"
  - "Forwarding a Maven property into the test JVM, and choosing what an absent value should mean"
symptoms:
  - "A documented build invocation reports full coverage while the tests behind that coverage were never selected"
  - "A register or index test passes identically whether or not the tests it certifies ran"
  - "A green suite total that drops sharply when one tag is removed from excluded.groups, with no test reporting anything"
tags: [ci, silent-failure, false-negative, guard-design, tags, surefire, junit-platform, coverage-register]
---

# A gate cannot certify a run whose filters it cannot see

## Context

`TransactionalClaimCoverageTest` guards the `TransactionalClaim` register: every claim recorded as
covered must be referenced by a `@ProvesClaim` test method, and every claim's recorded sentence must
still appear in its source file. It is deliberately broker-free and untagged so that it gates every
default build - a register only checked when someone remembers to run a slow lane is not a gate.

Nearly every `@ProvesClaim` method, though, sits behind `@Tag("transactions")`, and `pom.xml`'s own
help text documented `-Dexcluded.groups=transactions,performance,chaos` as a supported override. Run
that and the build executed **no claim proof at all**, while the register reported every claim
covered, every parked claim explained and every sentence intact.

The two arms, differing in one term:

```
# default excluded.groups
Tests run: 9  ParallelConsumerOptionsTest
Tests run: 4  TransactionalBulkCommitTest
Tests run: 11 ProducerManagerTest
Tests run: 4  TransactionalClaimCoverageTest      <- green
BUILD SUCCESS

# -Dexcluded.groups=transactions,performance,chaos,quarantined,lincheck
Tests run: 4  ParallelConsumerOptionsTest          <- five tagged methods gone
Tests run: 4  TransactionalClaimCoverageTest      <- still green, certifying all of it
BUILD SUCCESS                                      <- ProducerManagerTest and
                                                   #  TransactionalBulkCommitTest never appear
```

The gate reads compiled annotations via ArchUnit, so it could not tell a proof that ran and passed
from one that was never selected. **The guard's selection criteria and the guarded tests' selection
criteria were disjoint**, and nothing connected them.

## Guidance

**A gate that reports on other tests must be able to see how this run selected them - or say it
cannot.** Static evidence (annotations, source text, file rosters) answers "is this written down
correctly", never "did it run". Those are different claims, and a gate that only has the first must
not phrase its result as the second.

**A test JVM cannot see its own launcher's filters, so forward them deliberately.** Surefire and
failsafe pass `groups`/`excludedGroups` to the JUnit Platform, not to the tests. Forward the same
properties the filters are configured from, through `systemPropertyVariables`, on **both** plugins:

```xml
<groups>${included.groups}</groups>
<excludedGroups>${excluded.groups}</excludedGroups>
<systemPropertyVariables>
    <pc.run.includedGroups>${included.groups}</pc.run.includedGroups>
    <pc.run.excludedGroups>${excluded.groups}</pc.run.excludedGroups>
</systemPropertyVariables>
```

Forwarding from the *same* property the filter uses is what makes the reading unable to drift from
the filtering. Setting it on both plugins means moving a gate between the lanes does not blind it.

**An absent filter must not read as "nothing was filtered".** That default would turn any later pom
edit that drops the forwarding into a gate that passes without having checked anything - the very
failure the gate exists to catch, now one level up and about itself. `RunTagFilter` raises instead,
and distinguishes the two cases with a marker surefire sets itself (`surefire.real.class.path`)
rather than one this repo sets, since a marker of ours would vanish along with the block it was
meant to detect the loss of. Outside Maven - an IDE, and pitest's minion JVMs, which run
`bz.stub.parallelconsumer.*` with neither the properties nor the marker - absent genuinely does mean
unfiltered.

**Resolve tags with JUnit's own `AnnotationSupport`, not by matching `@Tag` by hand.** `@Quarantined`
is a `@Tag("quarantined")` *carrier*, not a `@Tag`, and tags are inherited from superclasses; hand
matching gets both wrong. `JavaMethod.reflect()` bridges from ArchUnit to the reflective element, and
it resolves the integration-test classes too, because both lanes compile into one output directory.

**Prefer the loud failure to the convenient silence.** The obvious alternative was to tag the gate
the same way as the tests it guards, so excluding the tag excluded the now-meaningless report as
well. It was rejected for three reasons, and the first is the general one:

- It buys silence where the run needs an explanation. Silence is what produced this defect.
- It is a proxy that goes stale. The premise "every `@ProvesClaim` method lives in a class tagged
  `transactions`" was already false by the time it was acted on - `ParallelEoSStreamProcessorTest`'s
  proof, added days earlier, carries no tag at all. A single tag cannot express the condition.
- It would take the two source-drift checks - which depend on no test running at all - down with it.

Nothing in the repo actually passed `-Dexcluded.groups=transactions`; every real invocation uses the
default list, an empty value, or `quarantined`. The example in the pom's help text was illustrative,
so the loud failure breaks no real workflow. Check that before choosing loud over silent - the answer
is a grep, and it decides the question.

## Why This Matters

Same family as "A lane nothing runs cannot catch its own guard drifting" - a `workflow-issues`
write-up that arrives with astubbs/parallel-consumer#392 and is not on master yet - from a third
direction. There the guard was well-reasoned and never invoked; here the guard was invoked on every
build and reasoning about a configuration it could not observe. Both produce a green that reads
as proof and is not, and in both the design reasoning was sound in isolation - what was missing was
any connection between the mechanism and the run it claimed to describe.

The register makes this sharper than a normal test would, because its output is a *statement about
other tests*. A failing ordinary test is its own evidence; a passing register is evidence only if the
things it names actually ran. That is why the fix is a refusal rather than a warning.

## When to Apply

- When writing any gate whose output is a claim about other tests - a coverage register, a roster
  count, an index, a "these files are all tested" assertion. Ask what the gate would report in a run
  that selected none of its subjects, and make that answer a refusal.
- When a gate hardcodes an assumption about the build's configuration ("the default `excluded.groups`
  drops this tag"). That is true until someone passes a different one. Read the real value.
- When choosing what an absent input means: if reading it as a benign default would make the check
  vacuously pass, it must be an error instead.
- When tempted to tag a gate the same way as its subjects to make an exclusion self-consistent: check
  first whether anything actually uses that exclusion, and whether the tag really covers every
  subject.

## Related

- "A lane nothing runs cannot catch its own guard drifting" (`workflow-issues`, arrives with
  astubbs/parallel-consumer#392) - the same silent-green family: a guard nothing ever invoked, rather
  than one blind to its own run. Reachable before it merges with
  `node bin/inflight.mjs docs show <path> --ref feats/hasten-micro-mvp`.
- `docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md` - signal
  destroyed after the fact, rather than never observed.
- `docs/inflight/next-transactional-register-hardening.md` - the review that recorded this as the
  top item, and the findings still open behind it.
- `docs/testing.md`, "The transactional claim register" - the lane's current behaviour and what to do
  when the gate fires.
