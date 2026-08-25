# Branch: perf/242-crossing-cost-ladder - the crossing-cost ladder spike

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Cut 2026-08-25 from astubbs#334's head (post-windowing-verdict), on the owner's direction, to run
the spike the bet-off named as its reopening condition.
[`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md) **owns the
candidate** - the compile-the-function design, its prior-art survey, the owner's
WASM-subset-is-fine direction, and the one-C-ABI-seam refinement; what is here is only what the
BRANCH is for.

**The question:** what does one host-function crossing cost under each in-process mechanism, and
does any clear the pre-registered bar of **~1.35us marginal** (two orders under the measured 135us
gRPC crossing)? Arms: (a) the gRPC figure as context (measured, astubbs#334), (b) an in-process
queue handoff (the embedded pull seam's shape), (c) a raw C-ABI call (ctypes/FFM into a no-op and
a realistic fold), (d) a Numba `@cfunc` pointer called from the engine side, (e) a GraalPy
polyglot call, (f) a **GraalWasm** UDF - (f) primary per the owner's direction, and a GraalWasm
result alone green-lights a product slice. Method inherits the windowing spike's discipline:
predictions in the tree before runs, an instrument check that can move (a busy-wait injection must
show up), a realistic-fold arm beside every no-op (whether the fast path covers real workloads,
not benchmarks), and the end-to-end ceiling restated against U6's engine floor (free crossings
reach arm D's rate, not arm H's).

**Named companion gap, owner-corrected 2026-08-25 - Kafka Streams has never run under GraalVM
here.** The embedded `--shared` library covers PC core only; the Streams fast path's end state
needs one of two unproven routes: (1) native-image including Kafka Streams (RocksDB JNI,
reflection over serde config, unknown metadata surface), or (2) **libjvm embedding** - a full JVM
hosted in the client process (JIT retained, no native-image build, heavier footprint). The ladder
measures call mechanics with a minimal harness, so neither route gates the measurement - but a
green ladder without settling this gap is not a green light for Streams-in-process, and the
write-up must say which route it assumes. **Probed 2026-08-25, gap crossed for the in-memory
surface**: route (1) is proven and cheaper - the engine builds into a 78MB native binary that
passes the demo, one traced capture was the whole wall, and libjvm demotes to fallback;
[`perf-streams-under-native-image.md`](perf-streams-under-native-image.md) owns the result and its
durability boundary.

**A second workstream now lives on this branch: the feature-crossover ladder.** The transport
ladder above asks how cheaply a call can cross the boundary. That question is answered, and the
answer did not settle the strategy - so the branch also carries a ladder in the other dimension,
adding the features a user actually came for back to the *reimplementation* one at a time to find
where hand-rolling becomes the worse choice. Durability is the first rung, measured; exactly-once
is the candidate for the second. [`perf-streams-engine-floor.md`](perf-streams-engine-floor.md)
**owns both sets of numbers** - what is here is only that the branch has two purposes, so a reader
arriving at the transport ladder does not take it for the whole.

Delete this note when the branch lands or is superseded; the spike's results note (created on this
branch) will carry the numbers.
