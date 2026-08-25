---
title: "A racing lazy singleton announces itself as a duplicate meter, not as a bug"
date: 2026-08-24
category: logic-errors
problem_type: concurrency
component: dependency_injection
severity: high
applies_when:
  - "A lazily-initialised field in PCModule (or any hand-rolled DI holder) is reached from more than one thread"
  - "A component reports a plausible but frozen value while the log shows something else entirely happening"
  - "Micrometer logs 'This Gauge has been already registered' and the tags look identical"
  - "A feature 'does not work' for one caller while working perfectly for another in the same JVM"
tags:
  - concurrency
  - lazy-initialisation
  - micrometer
  - adaptive-concurrency
  - control-arm
---

# A racing lazy singleton announces itself as a duplicate meter, not as a bug

## The failure

`PCModule#admissionController()` was an unsynchronised lazy initialiser on a non-volatile field. Once
adaptive concurrency started ticking, three threads could reach it - the control thread every loop
pass, the broker-poll thread on every rebalance callback, and user or test code holding the module as
a documented seam - and on the default configuration nothing forced it during construction.

Two threads reading `null` both construct. One wins the field. **The loser's caller keeps a
live-looking object that nothing ever ticks**: it does not throw, does not log, and reports a
perfectly plausible value - its seed - forever. `AdaptiveConcurrencyEnforceIT` watched a target sit
flat at 2 for a ninety-second await while a complete `2 -> 12` ramp printed beside it in the same log.

That is the shape worth remembering: **the failure mode of a split singleton is indistinguishable
from "the feature does not work"**, which sends you to debug the feature instead of the wiring.

## What actually made it findable

Nothing about the frozen value points at construction. The tell was elsewhere, in a warning that
looks like noise:

```
WARN [pc-broker-poll-d3c94895] This Gauge has been already registered
  (MeterId{name='pc.admission.target', tags=[tag(pcinstance=d3c94895-89d6-4b04-a9d5-15e7bf0b9595),
  tag(subsystem=processor)]}), the registration will be ignored.
```

**Read the tags before dismissing it as two instances colliding.** `PCMetrics` mints a fresh UUID per
instance, so an *identical* `pcinstance` on both registrations proves one `PCMetrics` was asked twice -
which means two objects ran the same `initMetrics`, which means the singleton was not one. The thread
name on the warning names the racer for free.

Generalising: a duplicate-registration warning from any metrics library is a **cheap, always-on
duplicate-construction detector** for anything that registers meters in its constructor. It fires
whether or not anyone is looking at the object that was duplicated.

## The control arm that settled it

A fix that works is not evidence of the cause. The experiment was same-magnitude-different-position:
**remove exactly one keyword** - `synchronized` on that one accessor - keep the rest of the change,
and re-run the same integration test.

| Arm | `grep -c 'already registered'` |
|---|---|
| accessor `synchronized` | 0 |
| accessor not `synchronized` (one keyword reverted) | 1 |

One term, one flip, everything else identical - including the two other fixes in the same working
tree, which is what makes the attribution clean rather than "the branch is green now".

## Choosing the repair

Three candidates, and the discriminator is *structurally impossible* versus *unlikely*:

- **`synchronized` on the accessor** - chosen. Mutual exclusion serialises the check-then-create AND
  publishes the write, so a second construction cannot happen at all. Uncontended in practice (one
  acquire per control-loop pass) and reentrant, so the initialiser's call back into `pcMetrics()` is
  safe.
- **Force it from the processor's constructor** - rejected. It fixes only the engine's own path: a
  bare module (the test seam) still races, and the field is still published unsafely, so the repair
  depends on nobody ever adding a caller.
- **`volatile` alone** - rejected. It fixes visibility and still admits two constructions.

**Do not generalise the lock to the whole holder.** The other accessors on `PCModule` share the shape
and are safe because only single-threaded construction touches them; the thing to audit is *which
accessors are reachable from a second thread*, not *which accessors are lazy*. In the same audit,
`parallelEoSStreamProcessor` - written during construction, read from the control and broker-poll
threads - became `volatile` for the same reason: a stale `null` there is a legal answer meaning "bare
module, no processor", so the wrong answer is silent.
