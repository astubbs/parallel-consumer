# Parallel Consumer, from C

**A reach probe, not a supported client.** It exists to answer one question the other eleven
clients cannot: can a language with **no gRPC stack at all** use Parallel Consumer?

```
ok   isolate created from a C process
ok   Configured: max_concurrency=16 executor_count=16

  200 records over 200 keys

PARALLEL CONSUMER RAN INSIDE THIS C PROCESS - no sidecar, no gRPC, no JVM
```

`200 records / 200 keys` is the deterministic pair the seeding predicts. That pair, never a rate, is
what says the work really happened.

## Why C, when there is already a C++ client

Every existing client already has gRPC, so none of them can test the argument the whole shared-C-
transport plan rests on: **reaching runtimes that do not**. Embedded targets, Lua, R, older
runtimes. C is what all of them bind *through*, so if C works the surface is genuinely
language-neutral rather than three lucky fits.

[`docs/inflight/parked-a-c-client-and-the-ffi-question.md`](../../docs/inflight/parked-a-c-client-and-the-ffi-question.md)
raised two objections to a C client. This answers both, one of them by dissolving it:

- **"There is no pure-C gRPC."** True, and irrelevant here - there is no gRPC anywhere on this path.
  The engine is linked in and the frames cross a function call. The objection was aimed at a C
  client that spoke the protocol over a socket; this one does not.
- **"No official C protobuf, only third-party."** This is the real one, and it turned out to have
  teeth.

## protobuf-c cannot compile this protocol at all

The obvious choice fails immediately:

```
proxy.proto: is a proto3 file that contains optional fields, but code generator
protoc-gen-c hasn't been updated to support optional fields in proto3.
```

Not a corner case. This protocol uses proto3 `optional` in **42 places**, because throughout the
design absence has to be distinguishable from zero - `max_concurrency` absent means "take the
proxy's default" while zero would be a nonsense ceiling, and a null Kafka key is not an empty one.
So protobuf-c is out, and would be out for any client of this protocol.

**nanopb accepts it**, and is the better fit anyway for the targets this argument is about.

## Bounded fields, and why that is the right default here

Without size hints nanopb emits a `pb_callback_t` for every string, bytes, map and repeated field,
and the caller writes an encode and a decode callback for each. [`proxy.options`](proxy.options)
gives them bounds instead, which turns every one into a plain C struct member:

```c
char topics[4][128];        /* not pb_callback_t topics */
```

There is **no malloc in this client at all**, and every message has a size known at compile time.
For an embedded target that is not a convenience, it is the requirement.

The cost is real and worth stating: in nanopb a field's maximum **is part of your API**. A real
client would publish those numbers as documented limits rather than leave them in a build file.

## Building and running

```bash
# once: the engine, as a GraalVM shared library (about 90 seconds, needs native-image)
../parallel-consumer-proxy-client-go/ffi/build-shared-library.sh session

brew install nanopb protobuf     # or your distro's libprotobuf-nanopb-dev
./build.sh

PC_BROKER=localhost:19092 PC_TOPIC=pc-ffi-demo PC_EXPECT=200 ./build/pc-c-probe
```

`build.sh` names anything it cannot find rather than working around it, and runs on macOS and Linux.

**The well-known types have to be generated too** - nanopb ships no pre-generated `Duration` or
`Timestamp`, unlike the C++ and Java runtimes where they arrive with the library. They are generated
in a *separate* protoc invocation on purpose: doing it in one pass makes the generator report every
pattern in `proxy.options` as "did not match any fields", which is exactly what a genuinely broken
options file says. The bounds apply either way; the warning is the problem.

## What this does not do

Handshake and the dispatch loop, reporting every record a success. No retries, no failures, no
rebalance, no produce-on-success, no reconnect - the capability set declared is `dispatch` alone.
It is a probe of the *reach* claim, and it should not be mistaken for a client anyone can ship.

## Prior art

- [`docs/plans/2026-08-22-001-feat-shared-c-transport-plan.md`](../../docs/plans/2026-08-22-001-feat-shared-c-transport-plan.md) -
  the plan this is `U5` of, its kill criterion, and the release-matrix gate that blocks shipping any
  of it.
- [`docs/inflight/perf-embedding-the-engine-over-ffi.md`](../../docs/inflight/perf-embedding-the-engine-over-ffi.md) -
  Go, Python and Node, and the hazards each surfaced.
