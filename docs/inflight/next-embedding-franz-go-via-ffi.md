# VERDICT 2026-08-22: not worth doing. The control was run and the prize is about 7%.

<!-- inflight-type: next -->
<!-- inflight-impact: architecture -->
<!-- inflight-state: closed - measured, the premise did not survive -->

**Antony's call after seeing the control: "I agree it's not worth even considering rewriting
anything."** This note is kept for the reasoning, not as open work.

## What the control said

The proposal rested on a measured gap: the bare Java client reaching **31-67%** of franz-go, no engine
on either side. That figure was real, and it was taken **before virtual threads**.

The control - the same Java floor with one term changed, workers as virtual threads rather than
platform threads, 200,000 records over ten partitions, no HTTP stub in the path:

| Arm | 0ms | as % of the Go floor |
|---|---:|---:|
| `franz` - the Go floor | 62,384 | - |
| **`pool-vt`** - Java floor, virtual threads | **58,064** | **93%** |
| `pool` - Java floor, platform threads | 42,803 | 69% |

**The Java client reaches 93% of franz-go.** At 2ms the thread model alone is worth **2.15x** on the
Java side - 55,890 against 25,957.

**So most of the gap was never `kafka-clients`. It was platform threads, and Java has fixed that.**
The remaining client difference is about 7%, against which the costs below were always going to be
the deciding factor:

- two runtimes installing signal handlers in one process
- rebalance callbacks travelling Go to C to Java, on the path PC exists to get right
- records crossing as bytes
- a second toolchain on the critical path of the **core**

**Seven percent does not buy that.** Closed.

## What survives, and it is not performance

The capability arguments in
[`next-franz-go-as-a-client-option.md`](next-franz-go-as-a-client-option.md) are untouched by this
measurement and should be judged on their own: **KIP-932 share groups**, which the Java client does not
have and which no other Go client has either, and **`kfake`**, an in-process protocol-level fake broker
with fault injection. Neither depends on a throughput gap.

**If franz-go is ever embedded, it should be for share groups**, and the FFI mechanics recorded below
are still the right mechanism for it - a C-shared library called from Native Image, no gRPC.

## The mechanism, kept because it was researched and is correct

## Yes, and it is a plain C ABI call

- Go builds a C-shared library: `go build -buildmode=c-shared` produces a `.so`/`.dylib` plus a header.
- GraalVM Native Image calls C directly through its own C interface - `@CFunction`, `@CEntryPoint`,
  `CTypeConversion` - with **no JNI and no socket**. It is a direct call in one address space.
- Java 22's FFM API (Panama) is the other route and also works under Native Image.

So the transport question is settled: **no gRPC, no serialisation, no loopback.**

## The difficulties are not the calling convention

1. **Two runtimes in one process.** Go's runtime installs its own signal handlers - `SIGURG` for
   goroutine preemption, `SIGSEGV` for stack growth - and so does Native Image. This is the classic
   embedded-cgo conflict. Solvable, fiddly, and it fails in ways that look like nothing else.
2. **Callbacks are the hard direction.** Consumer-group semantics need rebalance notifications
   travelling *from* the client *into* PC, so Go -> C -> Java with Go's scheduler on the far side.
   PC's whole value is precise offset management across rebalances, so this is not an edge case, it is
   the main path.
3. **Records cross as bytes.** Copy per record, or share memory and manage lifetime by hand. At tens
   of thousands per second a copy is fine; the design should not assume that holds at a million.
4. **Build and distribution.** A native image per platform, each linking a Go archive. The project
   already carries a native-core deferral plan and eleven language clients; this adds a second
   toolchain to the critical path of the *core*, which is a different proposition from adding one to a
   client.

## The premise IS measured - and it may have expired

**This is not a hunch.** `next-franz-go-as-a-client-option.md` records a controlled comparison of bare
clients, no engine on either side, 500,000 records over ten partitions: the Java client reaches
**31-67%** of franz-go depending on the operating point.

**But every one of those numbers predates virtual threads**, and the same note now records why that
matters: the Java floor plateaued at ~2,848 records in flight where the Go floor reached exactly
5,000, and **~2,800 is the platform-thread ceiling, not a client property** - the identical figure
turned up again on 2026-08-22 with a different partition count and a different arm. Goroutines are not
platform threads; virtual threads are Java's answer to that, and `core-vt` now holds 5,000, and
40,000 when asked.

**So an unknown but possibly large fraction of the measured gap is a thread model that Java has since
fixed.** Re-running the Java floor *with virtual threads*, at the same 500,000 records and ten
partitions, is the one input that sizes this proposal - and it costs one sweep.

## Do not start here

**Run that control first.** If the gap largely closes, this note is closed with it and the effort goes
somewhere else. If a large gap survives, it is genuine client efficiency and this becomes one of the
most valuable things on the list - because it would put PC's engine on the fastest Kafka client that
exists, without a rewrite, and the GraalVM work is already wanted for
[`next-virtual-threads-under-graalvm-native.md`](next-virtual-threads-under-graalvm-native.md).

**A cheaper intermediate exists and should be considered in the same breath**: franz-go also has
KIP-932 share-group support that the Java client lacks, and `kfake`, an in-process protocol-level fake
broker. Those are capability arguments that do not depend on the performance gap at all, and they are
recorded in the client-option note.

