# A language-neutral function manifest - the defensible half of "pc deploy"

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - furthest from current work of the batch; the platform half is explicitly against positioning -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)).

The sidecar/FFI work (astubbs#293, astubbs#340) standardised the boundary between Kafka execution
and the user function - at which point deployment need not care which language produced the
function. The conversation's sketch runs from a declarative manifest (`function: fraud-check /
input: payments / ordering: key / handler: ./fraud.py`) up to `pc deploy fraud.py`, with every
function receiving the same operational semantics: ordering, retries, backpressure, adaptive
concurrency, metrics, scaling, commits.
<!-- file-refs: N/A - the handler path is part of a hypothetical manifest example, not a repo file -->

**Split it, because the two halves land on opposite sides of the positioning line.** The
*manifest* - a language-neutral description of a Kafka function that any PC host can run - is
cheap, additive, and strengthens the bindings into an application format. The *deploy verb* is a
serverless platform, and `STRATEGY.md`'s embedded-not-cluster positioning (via
[`core-engine-thesis.md`](core-engine-thesis.md)) argues the project's advantage is precisely NOT
becoming one - the application keeps its own deployment. Adopt the manifest, leave `pc deploy` to
the platforms that already exist (a manifest is exactly what a Dapr/K8s operator integration would
consume - [`core-dapr-adapter.md`](core-dapr-adapter.md)).
