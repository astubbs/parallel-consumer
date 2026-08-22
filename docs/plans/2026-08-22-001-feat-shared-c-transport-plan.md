---
title: Shared C Transport - Plan
type: feat
date: 2026-08-22
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: legacy-requirements
execution: code
origin: docs/inflight/perf-go-embeds-the-engine-over-ffi.md
---

# Shared C Transport - Plan

Tracking issue: astubbs#242 (the language-proxy fan-out). Parent plan:
[`2026-08-14-001-feat-language-proxy-plan.md`](2026-08-14-001-feat-language-proxy-plan.md).
Proven on branch `feats/go-vendored-pc`.

**Identifiers in this document are prefixed `CT` (requirement) and `CTD` (key technical decision),
never `R` or `KTD`.** The parent plan states that no ID may mean two things across documents, and it
is still allocating in the `R`/`KTD` space. Where this plan cites a parent decision it uses the
parent's ID unchanged.

## Goal Capsule

**One shared native library exposing the proxy protocol as a C ABI, so any FFI-capable language can
run Parallel Consumer in-process instead of beside it - without any language reimplementing
anything.**

This is not a C client and it is not librdkafka's architecture. Protobuf codegen still produces
every language's real client. The library replaces **only the transport**: where a client today
writes a `ClientMessage` onto a gRPC stream and reads a `ProxyMessage` back, it instead pushes and
pulls the same serialised bytes across a function call.

The Go proof exists and is measured -
[`perf-go-embeds-the-engine-over-ffi.md`](../inflight/perf-go-embeds-the-engine-over-ffi.md).
This plan is about whether and how that generalises, and it is **gated**: CTD1 is a decision, not a
task.

## Why this is now possible, when the parent plan rejected it

Two objections carried the rejection, and the Go work moved both.

**The callback problem was the decisive one and it does not arise.**
[`parked-a-c-client-and-the-ffi-question.md`](../inflight/parked-a-c-client-and-the-ffi-question.md)
carries a per-language callback table introduced as the breakdown that "decides the whole proposal".
It assumes the engine calls *out* of native-image Java into the host on many threads - hence the
GIL, the global VM lock, and Node's single-threaded event loop as the hard cases. **The surface
built instead never calls out.** The host pulls work frames and pushes verdict frames on its own
threads. Go's "medium" ranking was never exercised because none of the mechanism it describes is
used.

**`native-image --shared` was rejected on four untested differences from the executable build.**
Two are now tested and fine (entry-point surface; isolate and thread attachment). Two remain
untested (GC coexistence under host allocation pressure; foreign-thread callback re-entry - avoided
by design, so it is not on the critical path). The associated "worst shape for agentic development,
failures are segfaults with no stack trace" objection did not materialise across the Go work.

**What did NOT move**, and it governs this whole plan: the release-matrix objection. See CTD1.

## Product Contract

### CT1 - The library is a transport, not a client

The library MUST expose only session lifecycle and opaque frame exchange. It MUST NOT expose typed
records, ordering modes, verdicts, or any other protocol semantics as C types. Any language binding
that needs to understand a frame MUST do so with its existing generated protobuf code.

*Why it is a requirement and not a style note: the moment one protocol concept becomes a C struct,
every protocol change becomes an ABI change, and the eleven clients stop being the source of truth
for their own encoding.*

### CT2 - The frames are the same frames

The bytes crossing the ABI MUST be the identical serialised `ClientMessage` and `ProxyMessage` that
the gRPC transport carries. No embedded-only message, field or framing.

### CT3 - Pull, never push

The library MUST NOT invoke host code. All movement is initiated by the host calling in.

### CT4 - A binding is a transport swap, not a fork

Adding a language MUST NOT require changes to that language's client beyond substituting its
transport. The Go proof sets the bar: a `Send`/`Recv`/`CloseSend` implementation, and a
four-line change to the client.

### CT5 - Conformance decides equivalence

An embedded binding MUST pass the existing conformance suite as an additional dialect of a language
already covered. It MUST NOT introduce new scenarios. Behaving identically is the claim; the suite
is what tests it.

### CT6 - The default build stays native-free

For every language, the ordinary package MUST NOT require the shared library, an FFI toolchain, or
a platform-matched binary. Embedded support is opt-in, and an opt-in that cannot be satisfied MUST
fail loudly rather than fall back to the sidecar.

*A silent fallback makes a run that was meant to exercise the embedded engine prove nothing. The Go
implementation does this with a build tag.*

### CT7 - One library, all languages

There MUST be exactly one shared library per platform, serving every binding. A per-language library
would multiply the release matrix by eleven, and CTD1 is already the hardest constraint here.

## Key technical decisions

### CTD1 - GATE: this ships only if the release-matrix cost is answered first

**An embedded engine rebuilds and re-releases on every Kafka version bump, because the Kafka client
is inside the binary.** The parked note calls this decisive and nothing in the Go proof makes it
cheaper. It is also the exact opposite of this fork's currency argument, where a Kafka bump is a
dependency change.

**No language binding beyond the existing Go proof is built until this has a written answer.** The
answer may be "automated per-platform release, and here is the pipeline", or "embedded is pinned to
an LTS Kafka line and says so", or "we accept the cost for N languages and no more". It may not be
silence.

### CTD2 - The kill criterion, written before building

The parked note requires this and it is honoured here. **The embedded track is deleted, not fixed,
if any of these holds:**

- The controlled measurement (CTD3) shows the embedded transport is **not meaningfully faster** than
  a native-image sidecar for fast-record workloads. Without a throughput win, everything CTD1 costs
  buys only "no second process", which the parent plan's invisible-sidecar work (KTD41) already
  delivers.
- GC coexistence under host allocation pressure produces failures that cannot be diagnosed from the
  host's own tooling. That was the "worst shape for agentic development" objection and it would be
  confirmed rather than refuted.
- A second language cannot be bound within the CT4 bar. If it takes a fork rather than a transport
  swap, the generalisation claim is false and the Go result was a special case.

### CTD3 - The measurement must compare like with like

Any throughput comparison MUST run the sidecar arm as a **native image**, not a JVM. The Go
measurement recorded so far did not, and its ~9% margin is therefore uncontrolled for AOT-versus-JIT
and is not quotable as a transport result.

The output should be a **crossover point** - the per-record processing duration at which the hop
stops mattering - measured across a sweep (nothing, 1ms, 10ms, 100ms, 1s), reporting warm and cold
separately. A single duration cannot show a crossover, and the crossover is the number that is
useful in documentation whichever side wins.

### CTD4 - The isolate thread is looked up per call, never cached

A GraalVM isolate thread belongs to the OS thread it was attached on. Runtimes that migrate work
between OS threads - Go's scheduler, .NET's thread pool, any M:N runtime - make a cached
`graal_isolatethread_t*` silently wrong, and the failure is memory corruption rather than an error.
Every entry point calls `graal_get_current_thread` and attaches if it returns null.

**This is the single most transferable hazard in the whole design** and every binding MUST document
that it does this.

### CTD5 - Host-thread serialisation is the binding's responsibility

`ConfigureHandler.SessionObserver` documents that its state needs no locking **because gRPC
serialises a stream's inbound callbacks**. Nothing serialises a foreign host's threads. The C
surface re-establishes that guarantee with an explicit lock, and any future entry point MUST too.
Getting this wrong corrupts the handshake state machine under concurrency rather than throwing.

### CTD6 - Languages are ranked by value, not by ease

The parked note's finding that difficulty and demand rank together survives, but the pull model
changes the difficulty column, so the ranking is now driven by demand alone:

| Tier | Languages | Reasoning |
|---|---|---|
| **Build** | Rust, C++ | Edge and embedded targets where a second process is unavailable, plus the latency-sensitive fast-record workloads where the hop is the cost |
| **Cheap to add** | Swift, C#, Go | Straightforward FFI, real parallelism; Go is already done |
| **Do not build** | Python, Ruby, Node, PHP, Java | Their work is I/O-bound and slow, so the hop is noise. The pull model removes their *mechanical* difficulty but not the reason it is not worth doing. Java has `java-direct` already |

**Python, Ruby and Node move from "hard" to "possible but pointless".** That is a real change in the
reasoning and should not be mistaken for a change in the recommendation.

## Planning Contract

**Settled by the Go proof, do not re-derive:** the C surface shape; that `ConfigureHandler.session`
is the seam; that the sidecar's captured reachability metadata covers the `--shared` build unchanged;
that a client's transport coupling is small enough to narrow to an interface without behaviour
change.

**Open, and each is a unit below:** CTD1's answer; the controlled measurement; GC coexistence; the
second language.

## Implementation Units

### U1. Answer CTD1, or stop

Not code. Produce a written answer to the release-matrix question and record it in this plan. **No
other unit starts until this exists.** If the answer is that the cost is unacceptable, this plan is
closed and the Go work stays as a measured feasibility result.

### U2. Give the Go demo `PC_DEMO_SIDECAR`, on the demos branch

The Go demo can only launch a JVM sidecar, which is why CTD3's control arm was unavailable. Python,
Ruby, C++ and .NET already honour `PC_DEMO_SIDECAR`. This is a parity gap, unrelated to Graal, so it
belongs on `feats/polyglot-demos` and merges down. Tracked in
[`branch-polyglot-demos.md`](../inflight/branch-polyglot-demos.md) item 4, which already records that
the sidecar-location variable has three names.

Files: `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/demo/sidecar.go`, and its
demo README. Test: the existing demo option tests, plus a case asserting an absolute
`PC_DEMO_SIDECAR` wins over the classpath route.

### U3. The controlled measurement (CTD3)

Depends on U2. Build the sidecar as a native image, run both arms AOT, sweep the processing
duration, report warm and cold separately, publish the crossover point. **Feeds CTD2's first kill
condition**, so the result is allowed to end this plan.

### U4. Extract the C surface into a first-class module

Today the entry points live in
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/ffi/java/`, which is the wrong
home for something CT7 says all languages share. Move to its own module producing one library per
platform, with the Go binding as its first consumer and no Go-specific content remaining.

Test: the Go demo's embedded arm keeps reporting its deterministic records/keys pair after the move.

### U5. Bind a second language (see the experiment note below)

Depends on U4. **Feeds CTD2's third kill condition**: if it is not a transport swap within CT4's
bar, the generalisation claim is false.

### U6. GC coexistence under pressure

Depends on U4. Drive the embedded engine while the host allocates hard, and record whether failures
are diagnosable from the host's own tooling. **Feeds CTD2's second kill condition.**

### U7. Reachability beyond the happy path

The captured metadata covers one unordered run with no failures, retries, rebalance or transactional
commit. Those fail at *runtime*, invisibly at build time. Prefer a test that **asserts** reachability
over extending the trace, because a one-off capture goes stale silently. An exactly-once scenario is
the sharpest single addition.

## Verification Contract

- The embedded dialect passes the existing conformance suite for every bound language (CT5).
- Every demo with an embedded arm reports the same deterministic records/keys pair as its sidecar
  arm - the pair, never the rate, is the oracle.
- The default (non-embedded) build of every client continues to pass with no native library present,
  and `bin/ci-demo-conformance.sh` sees no additional arms (CT6).
- CTD3's measurement runs both arms as native images, or is not reported.

## Definition of Done

CTD1 answered in writing; one shared library serving at least two languages; both passing
conformance as an extra dialect; the crossover point published; and CTD2's kill criterion either not
triggered or acted on by deleting the track.

## Deferred / Open Questions

- **Crash isolation is lost and there is no mitigation proposed.** A segfault or OOM in the embedded
  engine takes the host process down, where the sidecar's process boundary contained it. This is not
  solvable within the design; it is a property users must be told about.
- **Whether the pull model reopens Python, Ruby and Node** is now a genuinely open question rather
  than a settled no. CTD6 still says do not build them, on value rather than difficulty.
- **Unix domain sockets versus the C ABI.** UDS already removes the network hop while keeping process
  isolation. If CTD3's crossover shows the remaining gap is small, UDS may dominate this entire plan
  on cost - it needs no per-platform binary and no Kafka-bump rebuild.
