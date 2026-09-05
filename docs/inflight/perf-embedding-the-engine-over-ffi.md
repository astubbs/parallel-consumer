# Three languages now run Parallel Consumer in-process, and the callback problem never arose

<!-- inflight-type: feature -->
<!-- inflight-impact: throughput -->
<!-- inflight-labels: release-note, needs-measurement -->

**Branch: `feats/go-vendored-pc`**, stacked on `feats/native-image-sidecar`. (The branch name
predates the Python half and is now narrower than its contents.)

Measured 2026-08-22. The Go demo's own standard output, from a build with `-tags pcffi`. The
40,000-record replay is the one worth reading:

```
Big replay - 40000 records, parallel arms only (AK core is serial and would take 80s+)
  arm                          records     keys    elapsed        msg/s  vs AK core*
  pc-go-grpc (this client)      40,000    1,000       5.5s        7,293        29.3x
  pc-go-ffi (embedded)          40,000    1,000       5.0s        7,955        31.9x
```

and the small replay, where all three arms are comparable:

```
  AK core (franz-go)             2,000    1,000       8.0s          249         1.0x
  pc-go-grpc (this client)       2,000    1,000       3.6s          555         2.2x
  pc-go-ffi (embedded)           2,000    1,000       3.3s          604         2.4x
```

The last row is Parallel Consumer running **inside the Go process**: no sidecar, no gRPC, no JVM.
`40,000 records / 1,000 keys` is the deterministic pair the seeding predicts, and getting it right
is the point of the row.

**The ~9% margin over the sidecar is NOT a transport result, and must not be quoted as one.** The
embedded arm is an ahead-of-time native image and the sidecar arm is a JIT-compiled JVM, which is
exactly the confound
[`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md) names as
invalidating a naive comparison - it says to run the sidecar as a native image too, so the engine is
identical on both arms. This is also a single run on a busy box with no warm/cold separation. What
the figures do support is the weaker and still useful claim that **embedding is not slower**, which
is not obvious in advance: the C ABI could have cost more than it saved.

## Python, second, and chosen because it was the one we might be wrong about

```
  200 records over 200 keys in 3.6s
  PARALLEL CONSUMER RAN INSIDE THIS PYTHON PROCESS - no sidecar, no gRPC, no JVM
```

**Rust would have been the wrong experiment.** It is the better first product - trivial callbacks,
true parallelism, a population that wants an in-process engine - and that is exactly why binding it
would have taught us nothing. Python is where the prediction was weakest.

**The GIL objection is measured dead.** The claim was that `ctypes` releases the GIL while Python
blocks in `pc_next`, so a pull does not stall the interpreter. That was reasoning, so
`ffi/probe_gil.py` measured it, against a control that differs in exactly one flag - `CDLL`
releases the GIL, `PyDLL` does not, same library, same function, same 2.02s block:

| | Python thread advanced |
|---|---|
| `CDLL` (releases the GIL) | 57,237,944 |
| `PyDLL` (holds it) | 50,130 |

1142x. A fast counter under `CDLL` alone would only have shown that something was fast.

**This does not mean Python should embed.** The other half of the argument is untouched and is the
stronger half: Python's typical work is I/O-bound and slow, so the hop is noise. What changed is
that "Python is hard" is now false, while "Python is not worth it" still stands. Those are
different claims and the parked note conflated them.

**The generalisation held at the intended bar.** A transport implementation and a switch; the
session state machine did not move. Python's gRPC bidi stream is a request *iterator* rather than a
send method, so its transport owns an outbound queue and a generator where Go's owned a `Send`
call - and both still reduce to push, pull, half-close, cancel.

**Forking turned out to be safe by accident, which is the finding most likely to have bitten.** The
Python client forks its worker pool at step 1 and `pool.start()` only messages the already-forked
launcher, so the GraalVM isolate - created at step 3 - is never inherited across a fork. The
fork-safety design built because gRPC Core cannot survive a fork protects the embedded engine for
free.

## Node, third, and it found the design's edge

Node is the only remaining runtime *shape*. Go and Python have real threads, so something else runs
while one blocks; Node is a single event loop, and the question neither could ask is whether the
pull model works when there is nothing else to run.

```
  baseline (no FFI call)   loop turned 152,860 times
  blocking on MAIN thread  loop turned       0 times (rc -3, 2006ms)
  blocking on WORKER       loop turned 149,106 times (rc -3, 2022ms)
```

**Yes, off the main thread - and the main-thread arm is a stall, not a slowdown.** Zero turns. Same
call, same duration, same session; the only difference is which thread makes it.

### koffi cannot call this library at all, and the symptom lies

This is the transferable part. The first attempt died inside `graal_create_isolate` with a fatal
`StackOverflowError` whose own first suggested cause is *"the wrong IsolateThread"* - and the thread
was correct.

koffi runs foreign calls on **a stack it allocates itself**; its configurable `sync_stack_size` is
the tell. GraalVM derives its stack guard zones from the calling OS thread's *real* stack, so the
two cannot agree.

- **Size ruled out by controlled variation**: 1 MiB and koffi's 16 MiB maximum fail identically.
- **Mechanism confirmed the same way**: an N-API addon - which calls down the thread's own stack,
  exactly as `ctypes` and cgo do - worked first time, same process, same library.

So a GraalVM shared library is incompatible with any FFI layer that swaps stacks. That is now
`CTD7` of the plan, because it constrains every future binding and nothing about the failure points
at it.

The addon is about 150 lines of C against Node's own headers, built with clang and no node-gyp.
The probe runs with `node_modules` deleted, so the Node path costs no third-party dependency.

## The finding that matters most: the callback problem is avoidable

[`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md) contains a
per-language callback table introduced with the words *"that breakdown turns out to decide the whole
proposal"*. It ranks Go **medium** - "cgo `//export`, real per-call overhead, cgo's pointer rules" -
and KTD41 then scopes Go to stay on the sidecar.

**None of that was exercised, because the design has no callbacks in it.** The table assumes the
engine calls *out* of native-image Java into the host runtime on many threads. This surface inverts
that: the host **pulls** work frames and **pushes** verdict frames, on its own threads, at its own
pace. Nothing ever re-enters Java from a foreign thread.

That was not cleverness, it was forced. Core's entry point is
`void poll(Consumer<PollContext<K,V>>)`, which takes a Java lambda, and a lambda cannot cross a C
ABI. Wrapping it would also run the caller's function while holding a core thread. The protocol was
already the right shape - `rpc Session(stream ClientMessage) returns (stream ProxyMessage)` is two
queues - so the C ABI mirrors the protocol rather than the API.

**So the callback table ranks the difficulty of a design nobody has to build.** Whether the pull
model changes the Python/Ruby/Node conclusions is now an open question rather than a settled one:
their hard cases were the GIL and the event loop, both of which are callback-entry problems. It does
**not** follow that embedding is right for them - the "their work is slow, so the hop is noise"
argument is untouched, and it was always the stronger half.

## What the seam turned out to be, and why it was small

`ConfigureHandler` is a gRPC service, but its session state machine is expressed purely as
`StreamObserver<ClientMessage>` in and `StreamObserver<ProxyMessage>` out - a three-method
interface. So the FFI implements the outbound side itself and calls `handler.session(...)`
directly. No server, no port, no Netty.

The Go client needed the mirror image, and it was already almost there: in 551 lines the transport
appeared in five places (`grpc.NewClient`, `.Session()`, two `.Recv()`, one `.Send()`). Narrowing
the field to a `Send`/`Recv`/`CloseSend` interface changed no behaviour - the generated gRPC stream
type already satisfied it.

**Both sides exchange the same protobuf frames the wire carries**, so eleven clients' encoding logic
is reused rather than reinvented.

## The `--shared` caveats, scored against what actually happened

The plan rejected `native-image --shared` partly on four untested differences from the executable
build. Three now have data:

| Caveat | Outcome |
|---|---|
| entry-point surface | fine - `@CEntryPoint` exports generated a usable C header |
| isolate and thread-attach semantics | works, with one real trap (below) |
| two garbage collectors sharing a process | **still untested** - nothing here put the host under allocation pressure |
| callbacks re-entering from foreign threads | **avoided by design**, so still untested |

**The trap, and it is the one to write down.** A GraalVM isolate thread belongs to the OS thread it
was attached on, and Go migrates goroutines between OS threads at will. A cached
`graal_isolatethread_t*` is therefore silently wrong the first time the scheduler moves the caller,
and the failure mode is memory corruption rather than an error. The fix is to call
`graal_get_current_thread` on **every** entry and attach if it returns null.

**The "worst shape for agentic development" objection did not materialise.** The prediction was
segfaults and memory corruption with no useful stack trace. Every failure in this work was legible:
a Maven reactor error, a javac crash naming its own missing field, an unresolved protobuf parse. The
objection may still be right under GC pressure and foreign-thread callbacks - the two things not
tested - but it is not right about the surface as built.

## Reflection transferred, which was the other live risk

The Probe 0 library returned `-1` from a reflective enum read where a static read returned `3`: the
`--shared` build did **not** inherit the executable build's reflection auto-registration. Since the
Kafka client resolves serialisers reflectively from configuration strings, that predicted a runtime
failure inside `Configure` - exactly how the native sidecar first failed.

It did not happen. The metadata captured from the traced **sidecar** run, which ships at
`META-INF/native-image/reachability-metadata.json`, is discovered from the classpath and **covers
the `--shared` build unchanged**. One capture, both artifacts.

Its limits are inherited too, and they are the next thing to bite: that trace walked one unordered
run with no failures, no retries, no rebalance and no transactional commit. See
[`perf-native-image-sidecar-works.md`](perf-native-image-sidecar-works.md).

## What is deliberately invisible

The embedded arm appears only in a `-tags pcffi` build. `bin/ci-demo-conformance.sh` compares every
language's stdout skeleton after normalising arm names to the roles `AK-CORE` and `SIDECAR`; a third
arm in the default build would put Go in permanent violation of a cross-language gate. Verified by
running the untagged demo - two arms, exit 0.

The library therefore stays pure Go. cgo and the native library are opt-in, and the default build
reports what is missing rather than falling back to the sidecar, because a silent fallback would let
a run that was meant to exercise the embedded engine prove nothing.

## What this does NOT show

- **No transport throughput claim.** The ~9% above is uncontrolled for AOT-versus-JIT, as noted
  at the top. The crossover point the parked note asks for - the per-record duration at
  which the hop stops mattering - needs a sweep of processing durations against a **native-image**
  sidecar, so that ahead-of-time versus just-in-time is not charged to the transport by mistake.
- **Nothing beyond the happy path.** No failure, retry, rebalance or commit-mode coverage over this
  transport.
- **macOS/arm64 only**, and one language.
- **The release-matrix objection is untouched**, and it is the one the parked note calls decisive:
  an embedded engine rebuilds on every Kafka bump, because the Kafka client is inside the binary.
  Nothing here makes that cheaper. This proves feasibility, not that it should ship.

## Where this goes next

[`docs/plans/2026-08-22-001-feat-shared-c-transport-plan.md`](../plans/2026-08-22-001-feat-shared-c-transport-plan.md)
generalises this into one shared library serving every FFI-capable language. It is **gated on the
release-matrix question** and carries a written kill criterion, because a feasibility result is the
easiest thing to mistake for a decision to ship.

## Prior art this builds on

- [`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md) - the
  decision, the callback table this partly invalidates, and KTD41's re-scoping.
- [`perf-native-image-sidecar-works.md`](perf-native-image-sidecar-works.md) - the executable build,
  the five-attempt log, and the reachability capture reused here.
- `docs/plans/2026-08-14-001-feat-language-proxy-plan.md` - KTD13, KTD41, and the Dead ends section.

## Candidate raised 2026-08-25: compile the FUNCTION, not just embed the engine

Raised by the owner after the windowing spike's bet-off
([`perf-streams-windowing-multiplier.md`](perf-streams-windowing-multiplier.md)): intercept user
functions at registration and compile them to C-ABI artifacts the engine calls directly - in the
embedded shape, the engine invokes a function POINTER the host registered, and the crossing
disappears entirely rather than shrinking to a queue handoff.

- **Mechanism exists for Python: Numba `@cfunc`** - restricted-Python JIT to an LLVM-compiled C
  function pointer, no interpreter, no GIL. The registration wire already carries function tokens,
  so "this token is also callable natively" is one additive capability field. The nopython coverage
  cliff routes per function: compute-light folds (exactly the class the windowing verdict was lost
  on) compile; I/O-bound functions stay on the wire, where the hop is noise against the work.
  GraalPy is the competing shape (host Python on the engine runtime) and changes what users install.
- **Sizing before any design work**: a direct native call (~0.1-1us) is the only shape that reaches
  the two-orders-of-magnitude reopening condition `STRATEGY.md` names; the embedded queue seam alone
  still pays two thread handoffs. But the ceiling relocates rather than vanishes - U6's arm D
  (zero crossings) ran ~20k rec/s, the engine's own floor on that box - so the perfect version
  closes the reimplementation gap from ~100x to single digits, and the verdict question becomes
  parity-plus-durability against a published rate bound rather than a rout.
- **The spike to run first is a crossing-cost ladder, not the interception machinery**: (a) gRPC
  baseline (135us, measured), (b) embedded pull-queue handoff, (c) Numba cfunc pointer called from
  the native-image engine, (d) GraalPy polyglot comparison - pre-registered bar for (c): at or
  under ~1.35us marginal, with the end-to-end ceiling re-derived against the engine floor.
  Hazards inherited from this note: crash isolation (a segfaulting compiled function kills the
  topology process where the sidecar dies loudly), CTD7's stack-swap FFI incompatibility, the
  per-entry isolate thread-attach rule, and the release-matrix gate on shipping anything embedded.

### Prior art for the candidate, surveyed 2026-08-25 - the repo had not looked

The earlier sweeps covered streaming architectures (Beam/PyFlink/PySpark/Faust/Quix/Bytewax, in
the windowing plan's Problem Frame) and per-language FFI mechanics (the parked C-client note);
the UDF-compilation family was unsurveyed until this entry.

- **The pattern ships today.** cuDF/RAPIDS intercepts user Python UDFs at runtime, compiles them
  with Numba, and runs them inside the engine; `scipy.LowLevelCallable` + Numba `@cfunc` is the
  established CPU idiom for native libraries taking compiled-Python C callbacks. Bodo and Codon
  are the whole-program end of the same family. **Redpanda Data Transforms is the nearest
  neighbour in this domain**: user functions compiled to WASM, run inside the broker, sandboxed.
- **The Graal-family strategy** is hosting rather than compiling: GraalPy/Truffle puts Python on
  the runtime this repo already builds with (near-free polyglot calls after warmup), composing
  with the native-image tooling - against the Jython cautionary tale (died on the C-extension
  cliff, which GraalPy still fights) and a real AOT-versus-Truffle-warmup tension.
- **WASM may fit this product best and stays in the family (GraalWasm)**: Numba serves Python
  only, but a WASM UDF fast path gives all ten bindings one target, one sandboxed engine-side
  runtime, and restores the crash isolation a raw C pointer gives up - at the cost of Python's
  to-WASM toolchain being the least mature of the bindings'.
- **The third ecosystem answer is already half-built here**: DuckDB/Polars answer slow UDFs by
  growing the engine's expression algebra so users declare rather than ship code - the declared
  combine (U5) generalised. Cheapest lane; parity-limited by construction (KTD16).

The crossing-cost ladder therefore gains arms: (e) GraalPy polyglot call, (f) GraalWasm UDF -
with (f) the only candidate serving every binding, and the same pre-registered bar.

### Owner direction, 2026-08-25: the full binding set does not gate the fast path

A WASM fast path that serves only the WASM-capable bindings is an acceptable product shape for a
while - do not let ten-language coverage block a simple win. Consequences for the candidate above:

- **The GraalWasm arm is promoted to primary candidate**; Numba `@cfunc` demotes to a
  Python-specific fallback rather than the lead mechanism.
- The first fast-path slice may target the to-WASM-mature bindings (Rust, Go, TypeScript/JS,
  C/C++, .NET) and skip Python/Ruby until their toolchains catch up - which aligns the coverage
  boundary with the workload boundary: the compiled-language audiences bring the compute-tight
  functions the fast path exists for, while I/O-bound work stays on the wire path where the hop
  is noise.
- The crossing-cost ladder keeps every arm (the bar is unchanged), but a GraalWasm result alone
  is now sufficient to green-light a product slice.

### Refinement, 2026-08-25: the seam is one C-ABI contract; WASM is a producer, not the seam

Owner's observation, and it collapses the candidate's shape: define the fast path as a C-signature
registration contract (a native callable over byte spans), and every route becomes a producer of
that one seam - Numba `@cfunc` emits it directly (so Python needs no to-WASM toolchain to join),
wasm2c / Wasmtime-AOT lower a WASM module to it (so WASM is the portable authoring and interchange
format rather than the calling convention), and a Mojo or Rust function is it already. Sandboxing
becomes a per-registration POLICY rather than an architecture fork: run the WASM form inside the
engine's embedded runtime when isolation matters, lower to native and call raw when the last
microsecond does - same artifact, two execution modes. Side benefit: a host can dlopen and
unit-test the exact native artifact the engine will call before registering it. The trade to keep
explicit: a raw pointer carries no sandbox - safety exists only in the in-engine mode. The ladder
spike's arms are unchanged; what this settles is that they all land on one engine-side surface.
