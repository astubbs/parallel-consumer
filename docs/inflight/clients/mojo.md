# Client: Mojo (candidate, astubbs#242)

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - candidate only; revisit when the crossing-cost ladder settles the compiled-function direction, or when Mojo's ecosystem matures -->

Per-language working note, candidate stage - no wave has run and no module exists. Raised by the
owner on 2026-08-25, during the compile-the-function discussion that followed the windowing
spike's bet-off
([`../perf-embedding-the-engine-over-ffi.md`](../perf-embedding-the-engine-over-ffi.md) carries
that candidate and its prior-art survey).

**Why Mojo is interesting twice over:**

- **As a binding audience**: Mojo is Modular's Python-superset compiled to native via MLIR - its
  users are precisely the performance-sensitive Python crowd this product courts, arriving with
  Python ergonomics and no Kafka client of their own.
- **As a compiled-function strategy**: a Mojo user function is *already* a native artifact. The
  interception-and-compilation machinery the Python candidate needs (Numba `@cfunc`) simply does
  not arise - registration could hand the engine a function pointer directly, which makes Mojo a
  natural first passenger for whichever fast path the crossing-cost ladder validates.

**Why deferred rather than queued:** the language is young - toolchain still stabilising, standard
library partially open-sourced, interop story moving - and the ten committed bindings plus the
unresolved crossing-cost question outrank a speculative eleventh. The cheap first probe, when
picked up: whether Mojo's Python interop can drive the existing Python client as-is (which would
make a Mojo *binding* nearly free and leave only the fast-path question).
