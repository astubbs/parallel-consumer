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

## What to do instead, for now

**Let demand decide.** The clients' root README should say that if someone wants a language that is
not here, they should open an issue and we will give it a go. That costs nothing, and one person
asking is better evidence than any amount of reasoning about who might want it — the same standard
applied to every other claim in this work.
