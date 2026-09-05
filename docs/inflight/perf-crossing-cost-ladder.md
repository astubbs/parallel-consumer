# The crossing-cost ladder: what one host-function crossing costs, mechanism by mechanism

<!-- inflight-type: register -->

The pre-registration and results record for the crossing-cost ladder spike
([`branch-crossing-cost-ladder.md`](branch-crossing-cost-ladder.md) owns the branch's purpose;
[`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md) owns the
candidate this serves). **Everything above the Results line was written and committed before any
arm ran** - the windowing spike's discipline
([`perf-streams-windowing-multiplier.md`](perf-streams-windowing-multiplier.md)) inherited whole:
predictions in the tree before runs, an instrument check that can move, a realistic fold beside
every no-op, and the ceiling restated against U6's engine floor.

## The question and the bar

What does ONE host-function crossing cost, marginally, under each in-process mechanism - and does
any clear the pre-registered bar of **~1.35us marginal** (two orders under the measured 135us gRPC
per-crossing cost from U6's fitted line, `t(m) = 33us + m x 135us`)?

**The fold** (the realistic arm beside every no-op): a windowed-aggregation-shaped operation -
take (key bytes ~16B, value bytes ~1KB, accumulator bytes ~1KB), fold the value into the
accumulator element-wise, return a result byte. This is the shape the windowing verdict was lost
on: compute-light, called once per (record x window), the accumulator serially dependent on
itself. A bounded element-wise fold rather than an unbounded append, so the per-call work is
constant across a batch.

## The ceiling, restated before any number lands (binding)

**Free crossings reach U6's arm D (~20,000 rec/s on that box), NOT arm H's rate.** Arm D is the
same engine with zero crossings - the engine's own floor. Arm H (the in-process single-threaded
reimplementation) measured 89k rec/s (hopping-12) to 723k rec/s (tumbling). So even a mechanism
that makes the crossing *free* leaves the wrapper at ~20k rec/s against H's 89k-723k: the ladder
can close the crossing term from ~100x to single digits, and no further. A green ladder relocates
the verdict question to parity-plus-durability against a published rate bound; it does not win
the F2 comparison by itself.

## The route assumption (the named companion gap)

Kafka Streams has never run under GraalVM here; the embedded `--shared` library covers PC core
only. This ladder measures **call mechanics with a minimal harness** - the JVM-hosted arms (b, e,
f) run on an ordinary JIT JVM, so the end state they most directly model is **route (2), libjvm
embedding**: a full JVM hosted in the client process, JIT retained, no native-image build,
heavier footprint. Route (1), native-image including Kafka Streams (RocksDB JNI, reflective serde
config, unknown metadata surface), is NOT what these numbers were taken on - Truffle-under-
native-image warmup and peak behaviour can differ, and that gap is not settled by this spike.
Arms (c) and (d) measure the raw C-ABI seam itself and are route-independent.

## Environment facts recorded before running

32-core Linux box (`6.14.11-4-pve`), ambient load ~4-6 from other agent sessions (1-minute load
recorded beside every arm's run). Toolchains found: gcc 14-era system cc (no clang, no wat2wasm,
no wasm-ld); Python 3.13.5 with venv (pypi reachable - numba installable); Temurin 17.0.20+8 and
GraalVM CE 25.0.2 via mise; maven central reachable (GraalWasm/GraalPy polyglot deps
downloadable); rustc 1.97.1 with only the native target installed (`rustup target add
wasm32-unknown-unknown` is the reachable to-WASM toolchain; C-to-wasm is NOT reachable on this
box - no clang/wasm-ld/emcc). The fold-to-wasm artifact will therefore be **Rust-compiled, not
C-compiled**, and a hand-assembled binary no-op module covers the no-op arm if the target
install fails.

## Method (binding)

- **Marginal cost per call**: warmup discarded (counts reported per arm; Truffle/JIT arms get
  large warmups), then many batches of N calls; ns/call = batch elapsed / N; report **median and
  p99 over >= 30 batches**, never a single number.
- **Instrument check per arm**: a variant of the fold with a calibrated ~1us busy-wait inside;
  every arm's fold figure must move by roughly +1us. An arm whose number does not move is not
  measuring the call and its row is void.
- **Machine load** (`uptime` 1-minute figure) recorded beside each arm's run.
- **Sabotage/controls**: the busy-wait injection IS the sabotage arm (a number that cannot move
  is dead); arm (c) additionally runs the C-driver-side loop as its own control (the same fold
  called with no Python in the loop), separating Python-side marshalling from the callee.
- Arm (a), gRPC, is **not re-measured**: 135us per crossing is the fitted per-crossing cost from
  U6 (astubbs#334's branch, `perf-streams-windowing-multiplier.md`), cited as context.

## Pre-registered predictions (written before any run; honest guesses with reasoning)

| Arm | Mechanism | Predicted no-op ns/call (median) | Predicted fold ns/call | Predicted verdict vs 1.35us bar |
|---|---|---|---|---|
| (a) | gRPC crossing (context, measured) | 135,000 marginal | - | fails, by two orders (that is the point) |
| (b) | Java SynchronousQueue round trip, 2 threads | ~4,000 (p99 ~40,000) | ~4,300 | **fails** - two scheduler wakeups per round trip, ~1-3us each on a loaded box; this is the embedded pull seam's floor |
| (c) | Python ctypes -> C | ~800 | ~950 | **passes, narrowly** - ctypes per-call marshalling is ~0.5-1us with argtypes set; three pointer args push toward 1us |
| (c') | C driver -> function pointer (engine-side proxy) | ~2 | ~100 | **passes trivially** - an indirect call is nanoseconds; the 1KB fold is ~1 cycle/byte at -O2 |
| (d) | Numba @cfunc pointer called from C driver | ~3 | ~150 | **passes trivially** - LLVM-compiled native code behind a raw pointer; slightly worse codegen than gcc on the loop. Subset risk: byte-pointer args must be expressible as `types.CPointer(types.uint8)`; predicted to compile |
| (e) | GraalPy polyglot call (JIT JVM host) | ~300 | ~3,000 | **no-op passes, fold fails** - Truffle inlines the call after warmup, but per-element interop access to host byte arrays is the killer unless bytes are staged guest-side |
| (f) | GraalWasm call (JIT JVM host), bytes staged in wasm memory | ~300 | ~700 | **passes** - post-warmup `Value.execute` of an export is sub-microsecond; fold reads/writes wasm linear memory directly so no per-element interop |
| (f2) | GraalWasm, 1KB copied into wasm memory per call via the polyglot buffer API | - | ~5,000 | **fails** - per-byte `writeBufferByte` is ~1k API calls; recorded to bound what "realistic data handoff" costs if staging is impossible |

Reasoning summary: the ladder should split cleanly into *pointer-call* mechanisms (c', d - single
nanoseconds, the "crossing disappears" class), *managed-boundary* mechanisms (c, e, f - hundreds
of nanoseconds, pass the bar with care about data placement), and *thread-handoff* mechanisms (b
- microseconds, fails the bar because the cost is the scheduler, not the call). If (b) fails as
predicted, the embedded pull seam alone does NOT clear the bar and the compile-the-function
candidate is the only route that does - which is exactly the claim the candidate note makes and
this spike exists to test.

**Warmup plan**: (b) 50k round trips discarded; (c/c'/d) 100k calls; (e/f) 200k calls (Truffle
compilation thresholds are in the tens of thousands).

---

## Results

Appended as the arms run, each beside its prediction, confirmed or refuted. Nothing above this
line changes after the first run; corrections land as dated entries here.

### The ladder, measured 2026-08-25

Harness: `ffi/crossing-ladder/` (`fold.c` + `libfold.so` for c/c'/d, `QueueHandoffBench.java`
for b, `bench_ctypes.py` for c/c', `bench_numba.py` for d, `fold_wasm.rs` -> `fold.wasm` +
`GraalWasmBench.java` for f, `GraalPyBench.java` for e, `graal/pom.xml` for the polyglot deps).
Exact versions: gcc 14.2.0 (`-O2`), Python 3.13.5, numba 0.67.0 / llvmlite 0.49.0, rustc 1.97.1
(`wasm32-unknown-unknown`, `-C opt-level=3`), Temurin 17.0.20+8 (arm b), GraalVM CE 25.0.2 (Java
25) with **polyglot 25.0.2** for arms e/f. The box's ambient `JAVA_TOOL_OPTIONS`
(`ActiveProcessorCount=8`, `MaxRAM 48g` at 20%) applied to every JVM arm. 1-minute load at each
arm's run is in its row. All figures are medians over the stated batches; p99s were within ~1.2x
of the median on every passing arm (worst: arm b's p99 1.27x median).

| Arm | Mechanism | no-op ns/call (median / p99) | fold ns/call (median / p99) | warmup | load | verdict vs 1.35us |
|---|---|---|---|---|---|---|
| (a) | gRPC crossing (cited, U6 fitted) | 135,000 marginal | - | - | - | **fails** (context) |
| (b) | SynchronousQueue round trip, Temurin 17 | 9,351 / 11,871 | 10,028 / 12,334 | 50k round trips | 5.8 | **fails, ~7x over** |
| (c) | Python ctypes -> C | 696 / 830 | 1,115 / 1,499 | 100k | 4.6 | **passes** (fold under the bar; p99 straddles it) |
| (c') | C driver -> function pointer | 1.3 / 1.3 | 392 / 445 | 100k | 4.6 | **passes, ~1000x under** |
| (d) | Numba @cfunc pointer from C driver | 2.0 / 2.3 | 19.9 / 20.1 | 100k | 4.0 | **passes, ~68x under** |
| (e) | GraalPy polyglot (JIT JVM host) | 201 / 257 | guest-staged bytearray 1,024 / 1,074; host byte[] **121,775** / 123,665 | 100k (no-op), 20k (fold) | 2.1 | **splits**: no-op and guest-staged pass; per-element host interop fails ~90x |
| (f) | GraalWasm polyglot (JIT JVM host) | 105 / 113 | staged in wasm memory 747 / 792 | 200k | 2.0 | **passes** |
| (f2) | GraalWasm, 1KB copied per call via `writeBufferByte` | - | 9,160 / 9,619 | 50k | 2.0 | **fails** - the per-byte buffer API copy alone costs 8,414 ns |

30-50 batches x 5,000-20,000 calls per arm (printed by each harness).

**Instrument-check deltas (a ~1us clock- or count-calibrated busy-wait injected into the fold;
an arm that does not move is not measuring the call):**

| Arm | Injected | Measured delta | Reads as |
|---|---|---|---|
| (b) | 1,000 ns (nanoTime spin) | +1,668 ns | moved; excess is scheduler jitter on a loaded box |
| (c) | 1,000 ns (clock spin in C) | +1,086 ns | moved |
| (c') | 1,000 ns (same) | +1,074 ns | moved |
| (d) | count-calibrated 1,278 ns | +1,287 ns | moved, matches calibration within 1% |
| (e) | count-calibrated 10,463 ns | +10,189 ns | moved, matches within 3% - deliberately injected ~10us, because 1us is under this arm's noise floor (fold base 122us, batch spread ~2us) |
| (f) | count-calibrated 1,160 ns | +1,182 ns | moved |

Two instrument incidents worth keeping: numba's first spin (a side-effect-free counting loop
guarded by an impossible branch) was **dead-code-eliminated by LLVM - the arm's number did not
move at all**, exactly the failure the check exists to catch; replaced with a serial
data-dependent chain written to observable memory, which all count-calibrated arms now use. And
the first GraalWasm run silently fell back to **Truffle interpreter mode** (polyglot 25.2.4 on
the 25.0.2 JVMCI fails its version check and drops runtime compilation): staged fold read 108,431
ns - 145x the compiled figure. The warning is printed but easy to grep away; a harness that
did not read stderr would have recorded the wasm arm as failing the bar when it passes by 2x.
Pinning polyglot to the JDK's own version fixed it.

### Predictions against measurements

| Arm | Predicted (no-op / fold) | Measured | Reading |
|---|---|---|---|
| (b) | 4,000 / 4,300, fails | 9,351 / 10,028 | **verdict confirmed, magnitude refuted** - 2.3x worse than guessed; two scheduler wakeups per round trip cost more under ambient load |
| (c) | 800 / 950, passes narrowly | 696 / 1,115 | **confirmed** - fold 17% over guess, still under the bar |
| (c') | 2 / 100, passes | 1.3 / 392 | **verdict confirmed; fold 4x the guess** - gcc does not vectorise the aliasing `char*` loop |
| (d) | 3 / 150, passes | 2.0 / 19.9 | **confirmed, better than guessed** - LLVM vectorises the fold (7x under the guess); the numba subset cliff did NOT bite: byte-pointer args expressed cleanly as `types.CPointer(types.uint8)`, nopython compiled first try |
| (e) | 300 / 3,000, no-op passes fold fails | 201 / 121,775 (host bytes) | **direction confirmed, magnitude refuted by 40x** - per-element interop on host arrays is catastrophic, not merely slow. Unpredicted third variant: guest-staged bytearray passes at 1,024 |
| (f) | 300 / 700, passes | 105 / 747 | **confirmed** - the closest prediction on the board |
| (f2) | ~5,000, fails | 9,160 | **direction confirmed, ~2x worse** |

The pre-registered three-class split (pointer-call / managed-boundary / thread-handoff) survived
contact: c', d in single-to-tens of ns; c, e, f in hundreds; b in thousands. What the
predictions missed, both times in the same direction: **data placement dominates the mechanism**
- the same GraalPy call is 1,024 ns with guest-staged bytes and 121,775 ns with host bytes; the
same GraalWasm call is 747 ns staged and 9,160 ns with a per-call buffer-API copy. The call is
cheap; moving 1KB across a polyglot boundary per call is not, unless the bytes already live on
the callee's side.

### The ceiling, restated with the measured numbers (binding restatement from above)

Free-ish crossings reach **U6's arm D: ~20,000 rec/s** on that box (50us/rec engine floor), not
arm H's 89k-723k. Substituting the ladder's winners into U6's fitted line `t(m) = 33us + m x
135us`: a GraalWasm crossing at ~0.75us gives hopping-12 ~16,500 rec/s (vs the measured 603) and
tumbling ~19,600 rec/s - a ~27x improvement that lands almost exactly ON the engine floor,
i.e. the crossing term becomes noise. The remaining gap to the reimplementation floor is then
the engine itself: ~4.5x (hopping-12) to ~36x (tumbling) - and that gap is what
parity-plus-durability has to argue against, not the crossing.

### Blockers and subset cliffs, named

- **Numba subset cliff: not hit.** The windowed-fold shape (byte pointers + lengths -> byte)
  compiled under `nopython` first try. The cliff presumably waits for richer folds (allocation,
  objects); this shape - the one the windowing verdict was lost on - is inside the subset.
- **C-to-wasm toolchain: absent on this box** (no clang/wasm-ld/emcc/wat2wasm). The fold arm of
  (f) is **Rust-compiled** (`rustup target add wasm32-unknown-unknown`, one 20s download) rather
  than C-compiled; no hand-written .wat fallback was needed. Consequence for the candidate: the
  "one C-ABI seam, WASM as producer" refinement should not assume C is the reference producer -
  Rust was the zero-friction one here.
- **Polyglot/JDK version lockstep** (the interpreter-fallback incident above): the maven
  polyglot version must match the GraalVM JDK's compiler version or Truffle silently drops to
  interpreter (~145x on the wasm fold). It warns on stderr and keeps running.
- **GraalPy host-array interop**: not a blocker, a measured hazard - any design that hands the
  engine's `byte[]` to a GraalPy UDF per element pays ~119us/KB. Bytes must be staged
  guest-side (or handed as a buffer the guest reads natively) before GraalPy's fold is usable.
- **GraalPy cost not measured under native-image** - the route assumption above stands: these
  are libjvm-embedding numbers.

### Summary line

**Arms (c), (c'), (d) and (f) clear the 1.35us bar - (f) GraalWasm, the owner's primary, clears
it at 747ns staged - while (b), the embedded pull-queue seam alone, fails it by ~7x; with a
sub-microsecond crossing the end-to-end ceiling relocates to U6's ~20k rec/s engine floor
(crossing cost becomes noise), leaving a ~4.5x (hopping) to ~36x (tumbling) gap to the
reimplementation floor that the crossing can no longer explain.**
