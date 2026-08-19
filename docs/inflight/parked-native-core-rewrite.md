# Parked: native-core rewrite of the engine

<!-- inflight-priority: low -->

Rewriting the core engine as a native shared core (Rust or C, librdkafka-style) with per-language
wrappers, deleting the Java engine and keeping only its wrapper.

**Parked behind a user-base-and-confidence gate** - no footprint, portability, or performance
signal opens it early. The triggers evaluated after the gate, the
engine-swap-behind-the-sidecar-contract framing that makes deferral safe, and the analysis behind
both live in the decision record:
[`docs/plans/2026-08-17-001-feat-native-core-rewrite-deferral-plan.md`](../plans/2026-08-17-001-feat-native-core-rewrite-deferral-plan.md).

Depends on the language-proxy sidecar architecture (astubbs#242, confluentinc#154; PR astubbs#293),
whose conformance suite carries the one live requirement: pin engine behaviour at the protocol
contract, never at Java internals, so a second engine implementation can pass it.
