# Gauges for the unbounded buffers - the issue's premises moved when astubbs#116 merged

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-vetted: 2026-09-07 - `JStreamParallelEoSStreamProcessor#userProcessResultsStream` is still an uncapped `LinkedBlockingQueue` with no `@Deprecated`, `PCMetricsDef` still gauges neither buffer, `gh issue view 216 -R astubbs/parallel-consumer` is OPEN and 122 is CLOSED/COMPLETED - every premise correction in this note still holds -->

[astubbs#216](https://github.com/astubbs/parallel-consumer/issues/216) asks for gauges over the
buffers that grow when the *user* stops draining them, starting with the JStream result backlog.
The issue and its two comments carry the argument and the audit; do not restate them here.

**What this note carries is the part the issue cannot know: three of the four premises it reasons
from stopped being true when astubbs#116 merged, and every one of them moved in the issue's
favour.** A reader who takes the issue at face value will design around constraints that are gone
and will believe a signal exists that does not.

## What changed under it

Read `JStreamParallelEoSStreamProcessor`'s javadoc on `userProcessResultsStream` and the body of the
squash commit that merged astubbs#116 (`git log -1 c30aaee15`); both say this from their own side.

- **The structure is no longer a `ConcurrentLinkedDeque`.** It is a `LinkedBlockingQueue`, and the
  field's javadoc says it was chosen *because* its `size()` is a maintained counter rather than a
  walk, naming the gauge this issue asks for as the reason. So the issue's central design caveat -
  that a gauge must keep a counter alongside the collection or give up exact depth - is dissolved.
  The gauge is now a supplier over `size()`.
- **The close-time WARN the issue describes as "what is left in its place" never shipped.** The
  merge removed the clear-on-close, the close-time warning and the deprecation together, as
  mitigations for a symptom of the stream defect it fixed properly. There is now no signal of any
  kind between the backlog growing and the heap ending, which makes the gauge the whole answer
  rather than the better half of two.
- **The JStream API is not deprecated and its queued removal is withdrawn.** The comment's argument
  that a repair could not "earn its place on an API being deleted" no longer has its premise.

## What is still true

The queue has no capacity, deliberately: a consumer that keeps up holds nothing, one that is merely
slower than the producer still grows it, and what astubbs#116 fixed is the consumer that stops
taking at all. So the unboundedness the issue exists for is untouched, and observability remains the
only available mitigation. `astubbs#122` closed as *completed* rather than won't-fix, which is a
different closure reason from the one the issue's "Related" list gives, but not a different
conclusion.

The worker-pool queue in the issue's audit comment is a `LinkedBlockingQueue` too, so both candidates
now share the same O(1) sampling story that comment drew a contrast on.

## Doing it

Cheaper than the issue budgets for, and the first move is to re-read the issue against the two
sources above rather than against its own text. Naming follows `PCMetricsDef`; nothing there gauges
either buffer today. Depth alone still under-reports the failure mode - the shape that matters is
depth that never decreases - which is the one design note on the issue that survives intact.
