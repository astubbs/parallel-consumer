# The compiled-function seam: one C-ABI contract, and the accumulator stays on the callee's side

<!-- inflight-type: feature -->

Design for the fast path the crossing-cost ladder green-lit; nothing below is built. The ladder
([`perf-crossing-cost-ladder.md`](perf-crossing-cost-ladder.md)) owns the measurements and
[`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md) owns the candidate
and the owner's two directions. What is here is the seam those two imply and neither specifies.

Three measurements bind it, and the second is the one nobody predicted:

- **A second thread disqualifies a mechanism.** Every arm that cleared the 1.35us bar made the call
  on the caller's own thread; the one that did not - `SynchronousQueue round trip`, 10,028 ns fold -
  failed by ~7x. The seam is a *call*, never a handoff, and the embedded pull-queue shape cannot be
  the fast path however cheap the rest of it gets.
- **Data placement dominates mechanism.** The same GraalPy call costs `1,024` ns with guest-staged
  bytes and `121,775` ns crossing a host `byte[]` per call; the same GraalWasm call costs `747` ns
  staged and `9,160` ns with a per-call `writeBufferByte` copy. A 100x swing with the mechanism held
  constant - so this is an invariant of the design, not a tuning note.
- **The ceiling relocates rather than vanishes.** With crossings free the wrapper reaches arm D's
  `20,062 rec/s` engine floor ([`perf-streams-windowing-multiplier.md`](perf-streams-windowing-multiplier.md)),
  leaving 4.5x (hopping-12) to 36x (tumbling) to the reimplementation floor - engine cost, which
  this design does not touch.

## The contract

One entry point per function token, over a shared arena. Every field is an **offset into the
arena**, never a host pointer, so the identical layout is legal in a wasm module's linear memory and
in a native heap block - which is what lets one contract serve both producers instead of two calling
conventions wearing one name.

```c
#define PC_ABI_V1 1u

/* Written by the engine into the arena, once per call. ~72 bytes, fixed size. */
typedef struct {
    uint32_t abi_version;                  /* PC_ABI_V1 */
    uint32_t kind;                         /* mirrors InvocationKind: MAP/REDUCE/JOIN/AGGREGATE */
    uint32_t present;                      /* bit per span: PRESENT vs ABSENT (see below) */
    uint32_t key_off,   key_len;
    uint32_t value_off, value_len;
    uint32_t right_off, right_len;         /* the table side, for a joiner */
    uint32_t acc_off,   acc_len, acc_cap;  /* IN/OUT - the callee rewrites acc_len */
    uint32_t out_off,   out_cap, out_len;  /* OUT    - the callee writes out_len */
    uint32_t err_off,   err_cap, err_len;  /* OUT    - UTF-8 detail, becomes InvocationResult.error */
} pc_call_v1;

/* The registered symbol / wasm export. Native form takes the arena base; the wasm form drops it,
   because the module's exported linear memory IS the arena and offset 0 is its base. */
int32_t pc_invoke_v1(void *arena, uint32_t call_off);   /* wasm: (param i32) (result i32) */
```

- **Return codes**: `0` ok; `1` user error (`err_*` populated, the record fails - the wire's rule
  that "a wrong value entering an aggregation is worse than a failed record" is unchanged); `2`
  `out_cap` too small, the engine's one legal retry with a larger `out` region; anything negative is
  a contract violation and the *registration* is killed, not the record.
- **`present` is a bitmask because absence is meaningful and `len == 0` is not absence.** The wire
  already turns on this - `Invocation.aggregate` is "absent rather than empty on the first value for
  a key", and `GetResult.found` exists for the same reason. A null offset would collide with a
  legitimate arena offset of 0.
- **Flat scalars only, no structs by value, no callee allocation** - producer constraints
  discovered rather than chosen: wasm has no aggregate parameter types, and Numba's `nopython` fold
  compiled first try precisely because its arguments expressed as `types.CPointer(types.uint8)`
  plus lengths.

### Carried as an additive capability on the existing token registration

`RegisterFunction` in
[`streams.proto`](../../parallel-consumer-proxy-streams/src/main/proto/parallelconsumer/streams/v1alpha1/streams.proto)
carries only `token` and `description` today. The fast path is one additive `CompiledFunction` field
beside them - artifact kind (`WASM_MODULE` bytes, a `NATIVE_LIBRARY` path the engine `dlopen`s, or a
raw `NATIVE_POINTER`), symbol (default `pc_invoke_v1`), execution mode, requested arena size. Absent,
the token behaves exactly as it does now.

**The wire path remains the universal fallback, per function, and it is never entered silently.** A
token registering no artifact takes the wire; a token registering one the engine cannot load, or
whose `abi_version` it does not know, has its **registration refused by name** rather than
downgraded behind the host's back - the Go demo's rule, on the grounds that a silent fallback lets
"a run that was meant to exercise the embedded engine prove nothing". `NATIVE_POINTER` is
additionally refused over gRPC by construction: an address from another process names nothing.

## The data-placement rule, as an invariant

**The accumulator lives on the function's side of the fence. The engine passes offsets and copies it
zero times per call.** The ladder's headline restated as a rule; a producer that cannot satisfy it
does not qualify. The arena is the accumulator's *home*, not a staging buffer:

- **WASM module (sandboxed)**: a region of the module's own exported linear memory, grown by
  `memory.grow` at registration. A `(key, window)` accumulator holds a slot for the life of that
  window. `747` ns is what this costs; `9,160` ns is what copying instead costs.
- **Numba `@cfunc` / native library / raw pointer (trusted)**: an engine-allocated native block
  handed to the artifact at registration. Same slots, same offsets, real addresses.
- **What the engine gives up**: it no longer holds the canonical accumulator bytes between calls -
  the genuine tension with Kafka Streams, whose window store owns them for the changelog and for
  restore. The reconciliation is that the store's value backing *becomes* the arena slot: the engine
  reads a slot out only when a changelog write or store flush needs it, once per commit interval
  rather than once per record, and rebuilds slots from the changelog on restore. **Restore and
  rebalance are where this design is thinnest** (open question 3).

## Producers of the contract

| Producer | Mode | Measured, as the ladder ran it | What it costs the host |
|---|---|---|---|
| **GraalWasm module** (primary) | sandboxed, in-engine | `747` ns staged | a to-WASM toolchain |
| **Numba `@cfunc`** | trusted | `19.9` ns, called from a C driver | `nopython` subset; nothing to install beyond numba |
| **Raw native pointer / `dlopen`** (Rust, Mojo, C) | trusted | `1.3` ns no-op, `392` ns fold via a C function pointer | nothing - the function already *is* the contract |
| **wasm2c / AOT lowering** | trusted, from a sandboxed artifact | **not measured** - no C-to-wasm toolchain on the box | one build step; portable authoring at native speed |

**Sandboxing is a per-registration policy, not an architecture fork.** The same wasm artifact runs
in-engine when isolation matters and lowered-to-native when the last microsecond does; the host
picks per token, and can `dlopen` and unit-test the exact native artifact the engine will call
before registering it. Rust, not C, is the zero-friction wasm producer here - the ladder's artifact
is `fold_wasm.rs`, because no C-to-wasm toolchain was reachable.

## What this deliberately does not solve

- **The engine's own floor.** Free crossings reach `20,062 rec/s`, not the reimplementation's
  89k-723k. The residual 4.5x-36x is engine cost, closed against a different floor -
  parity-plus-durability, per `STRATEGY.md`'s reopening condition.
- **Kafka Streams under GraalVM.** The ladder's JVM-hosted arms model route (2), libjvm embedding;
  route (1), native-image including Streams, is unmeasured and a sibling probe is running -
  [`branch-crossing-cost-ladder.md`](branch-crossing-cost-ladder.md) owns that gap. **This design is
  route-agnostic and can afford to be**: the C-ABI arms are route-independent. The *sandboxed* mode
  is not - GraalWasm's `747` ns is a libjvm-embedding number, so a route-(1) answer moves the
  primary producer's cost and nothing else here.
- **Crash isolation in trusted mode.** A segfaulting compiled function takes the topology process
  with it; the sidecar dies loudly instead. [`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md)
  names it as an inherited hazard, and the sandboxed mode is the only answer this design has.
- **Versioning and ops for shipped artifacts.** Who builds the `.wasm`, where it lives, how a
  running engine learns a token's artifact changed, what an `abi_version` bump does, and how any of
  it meets the release-matrix objection - all unaddressed.

## Open questions

1. **Does the lowered-wasm path reach native speed?** No `wasm2c` or Wasmtime-AOT artifact has been
   measured through this contract, and the whole "same artifact, two execution modes" claim rests on
   it. Owner: the ladder's missing arm, which needs a C-to-wasm toolchain the box did not have.
2. **Does the fast path re-impose the constraint the binding design boasts of avoiding?**
   [`docs/language-bindings.md`](../language-bindings.md)'s function-delivery axis records ours as
   "Token and local lookup", with "**no serializability constraint whatsoever**". A compiled
   artifact is not serializability but is its sibling - user code must be *compilable to the
   contract*. That axis needs a third row; the doc owns it.
3. **Arena lifetime across rebalance, restore and window expiry.** Who zeroes a slot, who grows the
   arena when occupancy does, and what a restore costs when every slot must be rebuilt from the
   changelog before the first record is processed.
4. **Polyglot/JDK version lockstep as a shipping hazard.** The ladder's wasm arm silently fell to
   the Truffle interpreter, 145x slower, on a version mismatch that warns only on stderr. Shipping
   the sandboxed mode means shipping a startup assertion that runtime compilation is on.
5. **Which slice ships first.** The one-C-ABI-seam refinement re-admits Python at full speed through
   Numba, while the coverage direction scopes the first slice to to-WASM-mature bindings. They do
   not disagree about the architecture, only about who gets it first. **Owner's call, 2026-08-25:
   spike BOTH in parallel to spike depth - one function shape end to end per path, measured - then
   reassess together before anything is fleshed out.** The two spikes also test the fork's hidden
   asymmetry: a WASM artifact is bytes that ship over the wire to today's sidecar unchanged, while
   a Numba pointer is meaningful only in-process and therefore forces the embedded (`--shared`)
   engine shape. Branches `spike/242-fastpath-wasm` and `spike/242-fastpath-numba`, each with its
   own branch note.

## Reassessment, 2026-08-25: both spikes ran, and the fork partially inverted

Owner's gate, exercised. Both paths proved end to end with sabotage checks (each spike's note
lives on its branch: `spike/242-fastpath-wasm` and `spike/242-fastpath-numba`,
`docs/inflight/perf-spike-fastpath-wasm.md` / `perf-spike-fastpath-numba.md`).
<!-- file-refs: N/A - the two spike notes live on their own branches, not this one; branch names above locate them -->

- **Path A held its shape**: a 966-byte wasm artifact over today's wire into today's sidecar,
  1.9x end to end, 94.9 percent of the wire-to-control gap closed, artifact identity checked at
  registration, Temurin refusal instead of silent interpretation. Deployment verdict: topology
  unchanged, sidecar artifact ~50MB fatter with a GraalVM pin.
- **Path B inverted the premise**: embedding deleted gRPC's ~165us/record reliably, and the
  compiled pointer added ~27us-at-the-median on top - not separable from noise. Its raw-address
  registration is a hole in a protocol, not a capability, and does not ship in that shape. Its
  accidental discovery outranks its thesis: **the embedded streams engine itself** - the whole
  engine as a --shared native library inside the Python process, built first try on the traced
  metadata - is a product capability independent of compiled functions.
- **Convergent finding, both spikes independently: the engine floor is the next question.**
  ~250us/record (embedded, B) and ~132us control (A's box) with NOTHING crossing. The crossing is
  solved; what stands between the wrapper and the reimplementation floor is Kafka Streams' own
  per-record cost.
- **Convergent test hole, both spikes independently: the streams demo's assertions are
  value-blind** - a wrong-valued transform passes the count checks on every path. Spike A's
  --verify-mapped sink is the fix and should be adopted regardless of the fork.
- **Disposition**: Path A is the ship-shape candidate; Path B rescopes to the embedded engine with
  the pointer mechanism parked until artifacts carry identity; nothing is fleshed out until the
  engine floor is understood (spike dispatched, results in
  [`perf-streams-engine-floor.md`](perf-streams-engine-floor.md) once it lands, created by that
  spike).
<!-- file-refs: N/A - perf-streams-engine-floor.md is created by the engine-floor spike this entry dispatches -->
