# Record-level semantic tracing: "why did this record take 1.8 seconds?"

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs per-gate timestamps; astubbs#359 stamps only the endpoints -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)).

PC owns a record's whole lifecycle, so it can produce what generic OpenTelemetry instrumentation
cannot reconstruct: a per-record timeline of *why it waited* - behind its own key, for admission,
for dispatch, in retry backoff, for the commit frontier - alongside handler and foreign-hop time.
Kafka-semantic tracing, not generic tracing; and because one engine sits under all the language
bindings, a Python record's trace reads the same as a Java one's.

The gap between here and there is measured honestly: astubbs#359's residence timer stamps arrival
and completion - the *total*. The per-phase breakdown needs a timestamp at each gate of the
[`core-execution-opportunity-model.md`](core-execution-opportunity-model.md) ladder, which is the
same instrumentation that model wants for its own reasons - build them once. Payloads never enter
the trace; it is timing and lifecycle metadata only.
