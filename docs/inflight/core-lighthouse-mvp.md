# The lighthouse MVP: a miniature universe built to falsify the thesis

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - the prototype plan and the two sequencing rulings from the handoff supplement; nothing is scheduled by recording it -->

From the handoff supplement
([`docs/ideation/2026-08-30-hasten-handoff-supplement.md`](../ideation/2026-08-30-hasten-handoff-supplement.md),
sections 7, 25, 26 own the detail).

## The spike: thin end-to-end, maximum dimensions, hollow insides

One Kafka cluster; a Java/Kotlin Streams+PC kernel; a Python route optimizer; a Go customs
component; a small GUI; Kafka-backed control/catalog/resource/skipped-state topics. Twelve
critical dimensions to prove at minimal depth - transparent client substitution, key-ordered
scheduling, scheduler failover following Kafka ownership across two instances, one named resource
with a tiny lease implementation, Why Wait?/Proceed states, native unresolved-work plus explicit
Skip with queryable skipped state, cross-language RPC, one actor, distributed IQ routing, a
runtime catalog, one maintenance fence, and a scaling recommendation that distinguishes compute
shortage from a downstream binding constraint. **Sophisticated implementations stay deliberately
hollow** - the purpose is to prove these surfaces are projections of one execution kernel, not
systems glued together. The governing rule: *no subsystem gets completed for its own sake;
Hasten Logistics pulls through the minimum implementation necessary to prove the whole
architecture.*

## The owner's one-sentence acceptance form (2026-08-31)

A two-node Hasten Logistics where a global resource constraint prevents dispatch, Prescience
finds useful work around it, Why Wait explains every withheld execution, sparse completion
preserves causality through failure, and the system survives losing a node - **with the Spice
headers added automatically by the wrapped producer from the producing function's registered
requirements**, so producer-side declaration is proven transparent from day one, not retrofitted.

## Ruling 1: the multilingual Streams wrapper is lighthouse-scoped

**Hasten Logistics defines the MVP surface area of multilingual Kafka Streams** - implement only
what the lighthouse topologies need, then stop until further coverage is the next
highest-value increment. And the sharper half: **PC-inside-Streams (astubbs#255 / astubbs#271) is
a later optimization, not core to the MVP** - technically high-risk (task/state/EOS/punctuation
coupling) and it proves less of the new thesis than Prescience, embedded scheduling, resources
and decision telemetry do. This re-sequences a claim elsewhere in these notes:
[`core-per-function-capacity-arbitration.md`](core-per-function-capacity-arbitration.md) calls a
Streams topology the likelier first multi-function host - still architecturally true, now
explicitly *after* the lighthouse per this ruling.

## Ruling 2: the lighthouse exists to falsify, and the falsifiers are named

The supplement's devil's-advocate pass treats the architecture as a grounded hypothesis (inferred
from a working system, not invented), and the lighthouse should *attempt to break it* on four
claims: that shared scheduling primitives actually simplify the derived mechanisms; that deep
Prescience materially beats a modest buffer; that transparency survives contact (benefits without
application redesign); and that virtualizing physical topology creates less correctness
complexity than the value it removes. Surviving those makes the case; assuming them does not.
This is the repo's own falsification discipline
([`docs-research-program.md`](docs-research-program.md)) applied to the architecture itself.

**Upgraded by the cross-model review (2026-08-31, finding 4): names are not
preregistration.** Each falsifier needs a null arm, a metric, a rejection threshold and a stop
rule - and the sequencing becomes a **falsification staircase** instead of navigator-then-broad-
lighthouse: (1) a local admission A/B against a conventional limiter, (2) the twenty-node lease
conservation test under churn (measuring overshoot bounds, per the corrected safety claim), (3)
one *stateless* frontier cutover with failure injection. The twelve-dimension lighthouse is built
only if all three rungs survive, so a failure isolates to its rung instead of hiding in a
composite.
