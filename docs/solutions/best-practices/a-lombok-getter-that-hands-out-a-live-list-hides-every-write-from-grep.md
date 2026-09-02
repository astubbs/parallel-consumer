---
title: "A Lombok getter that hands out a live list hides every write from grep"
date: 2026-09-03
category: best-practices
module: parallel-consumer-core
problem_type: search_trap
component: shared_state_audit
severity: high
applies_when:
  - Auditing a class for unsynchronised mutable state reachable from a public API
  - A field is iterated somewhere and a search for its name finds the declaration and the iteration but no write, so it reads as dead
  - A Lombok `@Getter` on a collection field is public, or the class is
tags: [lombok, getter, concurrency, audit, grep, CopyOnWriteArrayList]
---

## The trap

`WorkManager.successfulWorkListeners` was a plain `ArrayList`, iterated by the control thread and
mutated from user threads - the same defect as `controlLoopHooks`, fixed in the same PR
(astubbs/parallel-consumer#267). It hid through one review pass because a Lombok
`@Getter(PUBLIC)` handed out the live list, so the mutation was spelled
`getSuccessfulWorkListeners().add(..)`. A search for the field name finds the declaration and the
`forEach`, never a write, and the field looks dead.

## The rule

When auditing a collection field, search for the **getter** as well as the field, and treat a public
getter that returns the live collection as a write site at every caller. Better still, remove it:
the fix replaced the getter with a real `addSuccessfulWorkListener(..)`, so the next such search
finds its callers by name.

The audit this came from cleared four siblings with reasons, which is the shape a second-instance
check should report: `partitionStates` (already concurrent plus a snapshot), the three per-partition
counter maps (point lookups only, no traversal to break - later made concurrent anyway), the
`RetryQueue` accessors (package-private, test-only), and `OffsetSimultaneousEncoder.sortedEncodings`
(getter never called).
