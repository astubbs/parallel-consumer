# Parked: a C client, and the FFI question underneath it

Asked 2026-08-15: librdkafka is written in C rather than C++, so should there be a C client too?
Considered and parked, because the reasoning generalises to any future "why not language X" and is
worth not re-deriving.

## Why librdkafka is C, and why that reasoning does not transfer

C is the FFI lingua franca. One C core with a stable ABI is callable from Python, Ruby, Node, Go,
PHP, Perl and R, so a single implementation reaches every ecosystem through thin bindings. That is
librdkafka's whole architecture, and C is the only language that makes it work.

**This project deliberately does not need that model.** The shared core is the *sidecar*, on the JVM,
where the mature Kafka client already lives; each language gets a native client generated from the
frozen protocol. Protobuf codegen is our "one core, many bindings" — with the boundary at the
**process** edge rather than the **FFI** edge. It is the same distinction that makes this fork's
Kafka currency a version bump where librdkafka's is a reimplementation.

## Three practical objections

- **There is no pure-C gRPC.** gRPC Core is C++ behind a thin C surface almost nobody writes clients
  against. A C client would either bind the C++ stack — at which point it is the C++ client with
  worse ergonomics — or hand-roll HTTP/2 framing. Protobuf is the same: no official C implementation,
  only third-party (`protobuf-c`, `nanopb`).
- **Every ecosystem a C shim would reach, we already reach natively**, with better ergonomics than an
  FFI wrapper and no ABI to keep stable.
- **The population is thin**: someone writing pure C who wants key-ordered concurrency, and who would
  probably reach for librdkafka directly.

## Where it would genuinely make sense

If the target were **languages whose native gRPC support is poor or absent** — embedded targets, Lua,
Perl, R, older runtimes — then a small C shim over the protocol becomes the reach mechanism and the
FFI argument is the right one. That is a decision about serving a long tail, and it should be taken
on evidence that the tail exists rather than on symmetry with librdkafka.

## The other C proposal: embed the engine, do not wrap the protocol

Raised 2026-08-15, and it is **not the client this note parked** - it inverts it. Rather than a C
client speaking gRPC to a sidecar, compile Parallel Consumer itself with GraalVM `native-image
--shared` into `libparallelconsumer.so`, expose `@CEntryPoint` functions, and let every FFI-capable
language link it directly.

**Same programming model as the existing clients** (owner's framing, and it is the load-bearing
part): register a worker, receive dispatched work, report verdicts. Nothing about the semantics
changes. So this is **a third binding rather than a new product** - and in the transport-seam terms of
[`parked-http-dialect-and-generated-clients.md`](parked-http-dialect-and-generated-clients.md) it is
not a third transport either. It *collapses* the transport: the wire becomes a function call. It is
what `java-direct` already is for the JVM, offered to everyone else.

**It contradicts this document's central claim, deliberately.** Above, the boundary is argued to
belong at the **process** edge rather than the **FFI** edge. This proposal moves it to the FFI edge.
That is the disagreement to settle, not a detail.

**Where it genuinely wins:**

- **Fast records.** `STRATEGY.md` already makes this argument in the other direction - for a base
  client the per-record hop is proportionally large. Slow work makes the hop noise; fast work makes it
  the cost. In-process is the right answer for the fast end, and no amount of protocol tuning gets a
  sidecar there.
- **No process to operate**, which is the single biggest adoption objection to the sidecar. It
  restores the property `STRATEGY.md` names as the reason this project works at all: invisible to the
  cluster, needing nobody's permission to deploy.
- Edge and embedded targets, where a second process is not merely inconvenient but unavailable.
- Fast startup and low memory against a JVM sidecar.

**Four costs, and the first two are the ones that decide it:**

- **It couples the platform release matrix to Kafka versions.** Stated too absolutely in the first
  draft, and corrected by the owner: *any* C-based channel needs per-platform binaries, so a matrix
  exists either way. **The difference is what re-triggers it.** A C client speaking the protocol
  rebuilds when the client changes; an embedded engine rebuilds on **every Kafka bump**, because the
  Kafka client is inside the binary. So a version bump goes from a dependency change to a rebuild and
  re-release of every platform binary and every language package that vendors one - and it is
  precisely this fork's currency argument that pays the price.
- **The callback concurrency model collides, differently in every language** - broken down below,
  because that breakdown turns out to decide the whole proposal.
- **Native-image reachability metadata for `kafka-clients`**, which uses reflection for serialisers and
  config. Solved territory (Quarkus does it) but real work.
- **Crash isolation is lost** - a segfault or OOM in the native image takes the host process with it,
  where the sidecar's process boundary contains it.

### The callback breakdown, and the finding that falls out of it

The question per language is the same: the engine must call *out* of native-image Java, on many
threads at once, into the host runtime. Ordered easiest to hardest. **All of it is from reasoning
about each runtime's published threading model, none of it measured** - treat it as the hypothesis to
test, not a result.

| Language | Mechanism | Real concurrency? | Difficulty |
|---|---|---|---|
| **Rust** | `extern "C"` function pointer | **yes, true parallelism** - no runtime, no GC pause, no interpreter lock | trivial |
| **C++** | plain C callback | **yes** | trivial |
| **Swift** | `@convention(c)` closure | **yes** | easy |
| **C#/.NET** | reverse P/Invoke, `[UnmanagedCallersOnly]`; the runtime attaches foreign threads itself | **yes** | easy |
| **Go** | cgo `//export`; the thread is attached to a goroutine | **yes**, but with real per-call overhead and cgo's pointer rules to respect | medium |
| **Python** | `PyGILState_Ensure` per call | **only while the lock is released** - I/O-bound handlers do release it, CPU-bound ones do not | hard mechanically |
| **Ruby** | `rb_thread_call_with_gvl`, thread must be registered | same shape as Python, via the global VM lock | hard |
| **Node/TypeScript** | N-API thread-safe function, marshalled onto the event loop | **no** - callbacks are serialised onto one thread; concurrency only from async I/O inside the handler | hardest |

**The finding: the difficulty ranking and the demand ranking align, and that is not a coincidence.**

- The **easy** languages - Rust, C++, Swift, C# - are also **where embedding is worth having**: the
  edge and embedded targets where a second process is unavailable, and the latency-sensitive
  fast-record workloads where the per-record hop is the cost rather than noise.
- The **hard** ones - Python, Ruby, Node - are where **the sidecar is already the right answer**.
  Their typical work is I/O-bound and slow, which makes the hop noise; and for I/O-bound handlers
  their in-process concurrency profile would be *the same as the threading they already have*, so
  embedding buys them very little at the highest cost.

So the owner's instinct that the easiest one is a good win is right, and for a stronger reason than
being easiest: **easiest and most valuable are the same set.** If this is ever attempted, **Rust
first** - trivial callbacks, true parallelism, and a population that actually wants an in-process
engine.

That inverts the earlier advice in this section to prove it where the callback question is hardest.
That advice suits a proposal that must eventually serve everyone; this one does not, because the
languages where it is hardest are the ones it need not serve at all.

**What makes it cheap to evaluate**: the conformance suite keys bindings on *(language, dialect)*, so
a native binding is more rows and no new scenarios. The Rust binding already exists over gRPC, so the
experiment is a second dialect for a language already covered - and the suite decides whether it
behaves identically rather than merely appearing to.

## What to do instead, for now

**Let demand decide.** The clients' root README should say that if someone wants a language that is
not here, they should open an issue and we will give it a go. That costs nothing, and one person
asking is better evidence than any amount of reasoning about who might want it — the same standard
applied to every other claim in this work.
