---
title: Native Core Rewrite Deferral - Plan
type: feat
date: 2026-08-17
topic: native-core-rewrite-deferral
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
---

# Native Core Rewrite Deferral - Plan

## Goal Capsule

- **Objective:** Record the decision to defer rewriting the core engine in a native language (Rust or C - a librdkafka-style shared core with per-language wrappers, deleting the Java engine and keeping only its wrapper) behind a user-base-and-confidence gate, name the triggers evaluated only after that gate, and pin the one live requirement that keeps the option cheap.
- **Product authority:** `STRATEGY.md` owns the client-side bet this plan defers to. The language-proxy plan (`docs/plans/2026-08-14-001-feat-language-proxy-plan.md` on `feats/proxy-requirements`, PR astubbs#293) owns the sidecar and FFI architecture this plan leans on. No rewrite work is active scope anywhere.
- **Open blockers:** none - the deferral is the decision.

---

## Product Contract

### Summary

No native-core rewrite starts now, and no technical signal alone starts one later. The rewrite is parked behind a user-base-and-confidence gate; if it ever happens it is an engine swap behind the sidecar's protocol contract rather than a big-bang replacement; and the proxy work carries the one requirement - a contract-level conformance suite - that keeps that swap cheap.

### Problem Frame

The proxy work (astubbs#242, confluentinc#154; PR astubbs#293) gives every language the same client wrapper over a vendored, invisibly-spawned sidecar running the Java engine. That raises the librdkafka-shaped question: if one engine now serves eleven languages, should it eventually be a native shared core - smaller, faster-starting, embeddable - with the Java implementation deleted?

Two facts set the cost shape. The fork is rebuilding a user base and confidence after taking over from unmaintained upstream, so there is no evidence base yet to justify a rewrite and no audience to absorb one. And the engine's ~11.5K LOC embody years of concurrency and offset-encoding correctness that the test-hardening effort exists to pin down - a rewrite restarts the confidence clock the fork is currently winding.

### Key Decisions

- KD1. **Defer the rewrite behind the user-base-and-confidence gate; technical triggers count only after the gate passes.** Governs R1, R2. (session-settled: user-directed - chosen over starting when a technical trigger fires: with no users a trigger has no evidence behind it, and the gate is overriding.)
- KD2. **A future rewrite is a second engine implementation behind the sidecar's gRPC contract, not a rewrite-and-delete.** Governs R3. Wrappers, protocol, and users survive the swap; validation is black-box against the conformance suite, canaried per language. (session-settled: user-approved - chosen over big-bang replacement: the contract makes the engine swappable, converting the rewrite from a bet into a deferred option.)
- KD3. **Language choice stays open.** Prior art (Temporal `sdk-core`, Bytewax) leans Rust over C, but nothing turns on it until the gate opens.

### Requirements

- R1. No native-core work - prototyping, bindings, benchmarking, language selection - starts until the fork has re-established a user base and confidence in the Java engine. No footprint, portability, or performance signal opens the gate early.
- R2. After the gate, a rewrite starts only on a named trigger: edge/FFI demand the JVM cannot serve (the FFI track's qualification probe proving out beyond its current Rust/C++/edge scope), measured sidecar footprint blocking adoption even as a native-image executable, or a performance ceiling attributed to the engine itself rather than the loopback hop or user code.
- R3. The proxy conformance suite pins engine behavior at the protocol contract, black-box, so a second engine implementation can pass it with no reference to Java internals. Owned by the proxy work (astubbs#293); this plan names the property, not the tests.

### Scope Boundaries

- No rewrite prototyping, benchmarking, or language evaluation now.
- In-process embedding design - a native core calling back into user-language code - stays owned by the proxy plan's FFI track, which already carries a qualification probe and kill criterion.
- The GraalVM `native-image --shared` embedding path is not reopened: the ideation record (`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`, `feats/proxy-requirements`) ruled it a dead end - untested thread-attach and isolate semantics, GC coexistence with a foreign runtime, callbacks re-entering from foreign threads.

### Dependencies / Assumptions

- The vendored invisible sidecar (the proxy plan's KTD41) lands as designed: every language package vendors a platform-matched binary, spawned on first use, dying with the app. That keeps `STRATEGY.md`'s "library you add to a pom", nobody's-permission bet intact without a rewrite - which is what makes the rewrite deferrable at all.
- A native-image *executable* sidecar stays viable (that gate cleared in the proxy work; only `--shared` was rejected), so footprint and startup pressure have a cheaper answer than a rewrite.
- The engine-attributed-performance trigger in R2 is only measurable once the end-to-end record latency metric named in `STRATEGY.md`'s Observability track exists.
- PC's semantics stay unreplicated per language - no competitor reimplements key-ordered concurrency in each ecosystem, so the engine's correctness remains the moat a shared core protects.

### Outstanding Questions

- **Deferred to planning (post-gate):** Rust vs C vs other. Precedent leans Rust; decide when the gate opens, on that day's ecosystem.
- **Deferred to planning (post-gate):** Java's path after a swap. Today Java "sits directly on the engine with no protobuf underneath" (ideation record); after a swap Java becomes a binding like every other language, losing that degenerate-case privilege. Whether Java keeps an in-process JVM engine through a transition, or moves to the native binding immediately, is a real decision the swap forces.

### Sources / Research

Analysis behind the decisions, kept so the next reader does not re-derive it:

- **Nothing in the Kafka client ecosystem attempts what PC attempts.** franz-go, KafkaJS, and kafka-python are protocol clients: partition-level consumption, per-partition ordering, no key-level parallelism, no sub-partition offset tracking. The ecosystem drifted away from wrapping librdkafka because a protocol client is cheap enough to reimplement per language; PC's engine is not - which argues *for* one shared core (sidecar today, native later if ever), not against one.
- **The shared-native-core shape has precedent**: Temporal consolidated its per-language SDKs onto a shared Rust `sdk-core` with thin bindings after starting with independent implementations; Bytewax is a Rust core with in-process Python FFI. Both are cited in the ideation record above.
- **The hard part of embedding is PC-specific.** librdkafka mostly moves data across the FFI boundary; PC's product is running user functions at high concurrency, so an embedded native core must drive user-language threads and async runtimes via callbacks on every record. The sidecar sidesteps exactly this, which is why "embed like librdkafka" is harder for PC than it was for librdkafka - and why the FFI track's kill criterion matters.
- **Prior-art checks** (per AGENTS.md "Before you investigate anything"): greps of `docs/plans/`, `docs/solutions/`, and `docs/inflight/` for rust/librdkafka/native/FFI returned only the language-proxy branch material cited above; the issue search returned astubbs#242 (confluentinc#154) as the multi-language demand record; the open-PR collision check returned astubbs#293 (no collision - this plan touches no code); merged PRs and `docs/refactoring.md` hold no prior native-rewrite work.
