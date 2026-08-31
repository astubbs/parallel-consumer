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

**Addition from the owner's side (2026-08-29/30): transparent propagation.** Because PC sits on
the consume path (and, for produce flows, the produce path), it can carry trace context for users
who never configured a tracer - the industry standard is W3C Trace Context (`traceparent`), which
is what OpenTelemetry's Kafka instrumentation propagates. **The design boundary that keeps this
safe: Kafka record HEADERS, never payload envelopes.** Wrapping payloads would break every non-PC
consumer of the topic and every schema-registry integration; headers are the compatible channel
built for exactly this. And "we own the pipe" holds only where PC touches the record - topics
produced by non-PC producers arrive without context, so the feature degrades per-hop, not
all-or-nothing.

The gap between here and there is measured honestly: astubbs#359's residence timer stamps arrival
and completion - the *total*. The per-phase breakdown needs a timestamp at each gate of the
[`core-execution-opportunity-model.md`](core-execution-opportunity-model.md) ladder, which is the
same instrumentation that model wants for its own reasons - build them once. Payloads never enter
the trace; it is timing and lifecycle metadata only.
