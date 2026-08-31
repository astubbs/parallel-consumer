<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The proxy protocol - the frozen v1 wire contract, and the gate that keeps it frozen

> **⚠️ EXPERIMENTAL - not for production use.** Nothing here is published to any package registry,
> and the v1 protocol - though frozen - has never carried production traffic. Tracking: astubbs#242,
> upstream confluentinc#154.

This module holds **the wire contract and the tests that defend it, and nothing that decides
behaviour**. Everything that will speak the protocol - the sidecar and eleven client languages -
generates from the single source here; none of them is on `master` yet, so today this module is the
contract on its own. The language module shells are under
[`parallel-consumer-proxy-clients`](../parallel-consumer-proxy-clients); the sidecar arrives with the
proxy module (astubbs#242).

## What is in it

- `src/main/proto/parallelconsumer/proxy/v1/proxy.proto` - the entire wire contract.
- `docs/protocol-specification.md` - the normative reference. It and the schema are the two artifacts
  a client author works from, and `SpecificationCoverageTest` holds them to each other.
- `docs/client-authoring-guide.md` - how to build a client against them.
- Per-language option lines (`go_package`, `csharp_namespace`, `ruby_package`, and the rest). These
  are **wire-invisible**, but they are not optional extras: adding one after the freeze gate arms is
  a breaking change by the gate's own reckoning, which is why they all landed before it did.
- `src/main/java/.../WireDurations.java` and `WireTimestamps.java` - the **one** implementation each
  of the `google.protobuf.Duration` ↔ `java.time.Duration` and `google.protobuf.Timestamp` ↔
  `java.time.Instant` bridges, for both JVM speakers of the protocol. These are the module's only
  hand-written production Java, and they are here rather than in either caller because a conversion
  between a wire field and a language type *is* wire semantics: hand-written copies (which is what
  the sidecar and the Java client each had - for the timestamp, the encoder and the decoder lived in
  different modules and could not see each other) can drift on a nanos or negative edge case, and the
  result is a protocol bug the conformance suite catches only if a scenario happens to exercise the
  value that drifted. The class javadoc carries the measurements behind not using
  protobuf-java-util's `Durations`/`Timestamps` instead - neither offers a `java.time` bridge at the
  pinned version. Anything that *interprets* a message rather than decoding a well-known type belongs
  in a caller, not here.

## Why v1 is frozen, and what that costs you

A protocol with eleven independent implementations cannot be revised casually - a change is only
landed once every client has been regenerated and re-proved, so the cost of a change is paid eleven
times. Freezing it makes that cost explicit rather than discovering it later.

Four tests enforce the freeze, and they fail for different reasons on purpose:

| Test | Catches |
|---|---|
| `GoldenSessionBytesTest` | A change in the **encoded bytes** of a representative session. Field renumbering, a changed type, a dropped field. |
| `ProxyProtocolRoundTripTest` | A message that no longer survives encode/decode - the semantic half of the same guarantee. |
| `SpecificationCoverageTest` | A schema element the specification does not document, read from the generated descriptors so there is no list to rot. |
| `GeneratedCodePlacementTest` | Generated sources escaping where the build expects them, which would otherwise surface as a confusing compile failure in a downstream module. |

`bin/check-proto-breaking.sh` runs `buf breaking` (FILE category) against the committed baseline and
is what actually blocks a breaking change in CI. **It exits 100 on a breaking change**, including for
edits that alter no bytes at all.

## Changing the protocol

Do not, without deciding first that v1 is being superseded. If you are here because a client needs a
field:

1. Check whether the capability negotiation already covers it - the handshake carries a capability
   list (`"dispatch"` today) precisely so behaviour can be extended without changing the schema.
2. If it genuinely needs a schema change, that is a **v2 conversation**, not a patch. The
   [specification](docs/protocol-specification.md) owns what the messages mean and the
   [client-authoring guide](docs/client-authoring-guide.md) owns what an implementation must do with
   them; the `.proto` owns only their encoding.

## Related

- [`parallel-consumer-proxy-clients`](../parallel-consumer-proxy-clients) - the eleven language
  module shells that will carry the generated clients. Build scaffolding today: they neither
  generate from this schema nor depend on this module yet.
