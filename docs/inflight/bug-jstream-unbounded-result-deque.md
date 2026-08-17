# JStream result deque is unbounded, and stays that way

`astubbs#122` (mirrors `confluentinc#912`) - **open, and deliberately not fixed**. astubbs#116
deprecates the `JStream*` API for removal instead, because the deque cannot be bounded without
choosing between blocking the producer (a hang instead of a leak), dropping results (silent loss),
or throwing at a workload-dependent limit. The reasoning lives on the issue; this file exists so the
open defect is visible to anyone scanning the directory rather than only to whoever reads the
tracker.

What ships is the deprecation, a clear-on-close, and a warning when close finds results nobody read.
The growth itself is unchanged: consume the stream as results arrive, or use the callback API.

## Follow-ups this leaves open

- **No in-band signal while the backlog grows.** astubbs#116 removed the sampled backlog warning -
  its size check was Θ(N²/10,000) against an unbounded deque, worst exactly as it approached the
  OOM. What remains is a `WARN` at close, which a process that dies first never reaches. The real
  signal is the gauges in **astubbs#216**, and that issue now carries the whole answer rather than
  half of it.
- **`close()` and `closeDrainFirst()` never reach the Vert.x cleanup.**
  `VertxParallelEoSStreamProcessor` overrides only `close(Duration, DrainingMode)`, where
  `webClient.close()` and `vertx.close()` live, so the no-argument shutdowns leak the web client and
  the event-loop group. Pre-existing, out of scope for astubbs#116, and worked around in that PR's
  own vertx tests with an `@AfterEach` calling the `Duration` form. Fixing the processor is
  unclaimed.
- **Clear-on-close is best-effort on a failed close.** If shutdown times out, workers can still be
  live and enqueue after the clear runs. A close that completes normally has no such window.
  Preventing it needs a closed flag on each processor; judged not worth the machinery on an API
  being deleted.
- **The clear does not run at all when the control thread closes itself.** On an unhandled error the
  control task calls the internal shutdown directly, not through any caller-facing `close`, so the
  override never fires and the backlog survives. Covering it needs a close hook on
  `AbstractParallelEoSStreamProcessor` that both JStream processors override - a change to shared core
  for an API being removed, which is why astubbs#116 documented the boundary instead. Found by review,
  not in the field.
- **The `finally` that releases the backlog on a failed close is untested**, because forcing
  `super.close` to throw needs a seam the class does not expose. The success path is covered by
  `JStreamMemoryLeak912Test` and `JStreamVertxMemoryLeak912Test`; the failure path is reasoning only.

## Why this file is not a `branch-` note any more

It replaces `branch-912-vertx-leak.md`, which tracked getting the branch onto a PR - work astubbs#116
finished. The defect it was about did not finish with it.
