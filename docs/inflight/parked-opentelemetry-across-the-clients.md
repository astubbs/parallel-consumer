# Parked, post-v6: OpenTelemetry across the client libraries

Owner's idea, 2026-08-15, for after v6. The framework is **OpenTelemetry** — the one with an SDK in
every language this project targets, which is exactly why it is the right choice here rather than
one instrumentation approach per ecosystem.

## Why it fits this architecture unusually well

A sidecar deployment is *already* a distributed system, and the hop between the application's worker
and the JVM engine is a real boundary that a trace must cross. Today that boundary is opaque: an
application tracing its own work sees a record appear from nowhere and a result vanish into nowhere.
Closing it would make the proxy's cost visible — which is the honest thing to do about the very
overhead people will ask about, and the same argument as exposing batching.

There is a second, larger prize: the engine is the natural place to **inject and extract** trace
context, because it is the side that touches Kafka. Do it there and nine languages get consistent
propagation without each writing it — the same leverage argument as putting schema resolution in the
sidecar.

## Blocked on a gap already recorded

**W3C trace context travels in Kafka record headers, and the frozen wire has no headers field** — see
[`next-serialization-and-record-metadata.md`](next-serialization-and-record-metadata.md), which
records the same gap for its own reasons. Nothing useful can be done here until a record's headers
reach the client and an outbound record can set them. That is additive and cheap now; it is the
prerequisite either way, so tracing is a second reason to do it early rather than a separate task.

The produce direction matters as much as the consume one: a client that receives a traced record and
emits an untraced one breaks the trace precisely at the hop this feature exists to illuminate.

## What to decide when it is picked up

- **Where spans are created** — engine-side only, client-side only, or both with the engine as parent.
  Both-with-parent is the useful answer and the most work; engine-only is nearly free and still
  closes the "what is the sidecar doing" question.
- **Whether the client library instruments automatically or exposes a seam.** Automatic is friendlier
  and drags an OpenTelemetry dependency into a client whose selling point is thinness; a seam keeps
  the client thin and puts the work on the user. This is the same tension as logging, and it should
  probably be resolved the same way each ecosystem resolves it.
- **What is worth a span at all** — the record's processing certainly; the produce ack probably; the
  handshake and lease traffic almost certainly not, or a busy session will drown its own trace.
