# Two languages now run Parallel Consumer in-process, and the callback problem never arose

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
