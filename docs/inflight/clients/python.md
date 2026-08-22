# Python client (astubbs#242)

Wave one landed: connect, configure, one dispatched record through a worker process, report, clean
drain. Leases, heartbeats, reconnect, worker death, terminal outcomes, the demo and PyPI packaging
are all deferred and named in the module's testing-evidence `limitation`.

## Two spec divergences, both confirmed independently by the Go wave

These are **specification defects, not Python's local choices** — the Go client hit both without
seeing the Python work, which is why they are recorded here rather than in a module README.

- **A client cannot answer a protocol violation with a status code.** The authoring guide says to
  fail the stream with `FAILED_PRECONDITION` naming the count when the dispatch queue overflows.
  Only the server side of a gRPC call sets a status; a client can cancel, nothing more. Python
  cancels the call and raises a `ProtocolViolation` naming the count; Go treats it the same way.
  The guide's rule needs rewording for whoever owns the next doc pass — it is unimplementable as
  written, in every language.
- **`Released` on shutdown contradicts capability negotiation.** The guide's shutdown section makes
  the drain unconditional ("`Released` for the queue"), while its negotiation section forbids
  sending any message outside the negotiated set — and the test-mode harness negotiates only
  `["dispatch"]`, so there is no legal action for queued records. Python sends `Released` only when
  `shutdown` is negotiated and otherwise discards the queue for the proxy to reclaim, reasoning that
  sending outside the set would be the client's own violation. That choice is defensible but it is
  the client picking a winner between two rules; the specification should pick.

## The fork ordering, which the plan states as two requirements that cannot both hold

The plan asks for the worker pool to exist before any channel does, *and* to be sized by the count
`Configured` supplies — but that count only arrives after a handshake on an open channel. Resolved
with a **launcher process** forked from a channel-free, thread-free image, which forks the workers
once the count arrives; the application process never forks again after a channel exists. Ordering
inside `poll()` is pool → sidecar → channel, and the sidecar is deliberately second: its
stdout/stderr drain threads would otherwise be inherited by the fork, which Python 3.13 flags.

Worth reading before the same question is answered differently in another language with real
processes (Ruby, C++).

## Not done, and owed to whoever picks the module up

`src/docs/development/upstream-map.yaml` has no entry for this work — outside the wave's file scope.
