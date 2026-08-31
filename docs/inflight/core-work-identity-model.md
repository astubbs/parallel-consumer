# Identity, position, incarnation: the three things a piece of work is, kept separate

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - a foundational model to establish early, like the admission model it accompanies -->

From the 2026-08-31 JMS-bridge conversation (model root:
[`core-admission-scheduling-model.md`](core-admission-scheduling-model.md); donor catalogue:
[`process-csid-repo-archaeology.md`](process-csid-repo-archaeology.md)).

## The three concepts

- **Identity** - "shipment validation 123", the logical work, stable across everything below.
- **Position** - the original (topic, partition, offset) and the ordering-domain position; the
  causal coordinate.
- **Incarnation** - execution attempt 1, retry 2, recovery attempt 3; a particular crossing of
  the admission boundary.

PC already implicitly separates position from attempts; multiple transports make the full
distinction unavoidable, and it gives clean semantics to operations that otherwise blur: retry is
same identity, same position, new incarnation; skip terminates the position without a successful
effect; reprocessing abandoned work is a NEW identity causally derived from the old one (its
original ordering position was surrendered); duplicate delivery is the same identity arriving
through another physical incarnation; replay is a new incarnation with an explicit relationship
to history. **Effect identity stays stable across incarnations** - which is what the recoverable
side-effect contract keys on.

## Terminal dispositions are richer than success/failure/skip

JMS expiry forced the point: work can become *permanently* ineligible because its usefulness
ended - which is a policy completion, not a failure. The disposition vocabulary wants EXPIRED,
SUPERSEDED, CANCELLED, REJECTED alongside succeeded/failed/skipped, and the distinction pairs
with the temporal one in [`core-temporal-horizons.md`](core-temporal-horizons.md): *eligibility
time* (do not run before X) and *validity deadline* (do not run after Y) are different
predicates with different terminal outcomes.

## Transport transitions preserve logical identity

The bridge's `jmsbridge.origin` loop-prevention header is the embryonic form: once one logical
operation can appear as a Kafka record, an RPC, an actor message, and a protocol-facade message,
the runtime must distinguish *new work* from *another representation of existing work* - which is
deduplication, cycle detection, tracing and workflow correctness all at once. A single logical
work ID should mean the same thing to Prescience, the scheduler, lineage, retry, RPC, side-effect
recovery and the UI ([`core-decision-lineage.md`](core-decision-lineage.md) needs exactly this
to hang the causal graph on). Giving these systems the same identities early is cheap; retrofit
is the accidental-architecture generator.
