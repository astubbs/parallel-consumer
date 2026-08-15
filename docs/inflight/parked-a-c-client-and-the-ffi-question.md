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

- **It recreates librdkafka's distribution problem** - the exact thing this fork's currency argument
  rests on avoiding. One binary per platform and architecture, vendored or downloaded by every
  language package. Today a Kafka version bump is a dependency bump; under this it is a release matrix.
- **The callback concurrency model collides, differently in every language.** PC's value is running
  many callbacks at once, and calling *out* of native-image Java into a host runtime is where that
  gets hard: CPython's global interpreter lock, cgo's scheduler interaction and callback overhead,
  Node requiring thread-safe functions marshalled onto the event loop. I/O-bound work releases the
  lock in some of these, which is the case PC is for - but it must be established per language rather
  than assumed, and a language where it fails gets no concurrency at all.
- **Native-image reachability metadata for `kafka-clients`**, which uses reflection for serialisers and
  config. Solved territory (Quarkus does it) but real work.
- **Crash isolation is lost** - a segfault or OOM in the native image takes the host process with it,
  where the sidecar's process boundary contains it.

**What makes it cheap to evaluate**: the conformance suite keys bindings on *(language, dialect)*, so
a native binding is more rows and no new scenarios. **Prove it in one language before believing any of
the above** - and pick one where the callback question is hardest, not easiest, since that is what the
proposal actually turns on.

## What to do instead, for now

**Let demand decide.** The clients' root README should say that if someone wants a language that is
not here, they should open an issue and we will give it a go. That costs nothing, and one person
asking is better evidence than any amount of reasoning about who might want it — the same standard
applied to every other claim in this work.
