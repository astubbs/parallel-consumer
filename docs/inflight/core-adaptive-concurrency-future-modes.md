# Where adaptive concurrency goes next: catch-up, rate-limit feedback, and the ordering nuance

<!-- inflight-type: feature -->
<!-- inflight-impact: throughput -->

Owner's thinking, 2026-08-24, after watching the first closed-loop run. These are directions for the
controller, not defects in what shipped - the defects live in
[`pr-333-adaptive-concurrency-outstanding.md`](pr-333-adaptive-concurrency-outstanding.md). Read that
note's item 0 first: none of this can be designed until *what the controller optimises* has an
answer.

## Catch-up: lag is a third axis, and it suspends the latency goal

Consumer lag is a dimension the current design does not model at all. There are cases where an
operator will knowingly accept much worse per-record latency for a period, because being far behind
the head of the topic is the more expensive problem.

Two shapes, and they are the same mechanism:

1. **Cold start on a pre-populated topic.** Millions of offsets behind. Per-record latency three
   times the eventual goal is fine, because nothing is waiting on any individual record - the whole
   backlog is late already.
2. **A traffic spike.** An event dumps an enormous volume onto the input topic. Same trade: burn
   latency to get back to the head.

In both, once lag falls under some proportion of the topic (or some absolute threshold), the
controller returns to its normal objective.

**Note what it does and does not interact with.** Catch-up suspends a *latency* goal; it is
meaningless against a *throughput* goal, because catching up IS maximising throughput - the two
coincide. So this is not a fourth peer mode alongside the objectives; it is a temporary override
that only exists when the standing objective is latency-flavoured.

Open: what measures "caught up" (absolute lag, lag as a fraction of the topic, or the derivative -
is lag shrinking?), and the hysteresis so a workload sitting near the threshold does not flap
between modes.

## Rate-limit feedback: jump to the answer instead of searching for it

When a downstream tells us its limit outright, searching for it by gradient is absurd. If a service
rejects with *request limit of 100/s exceeded* and the group has four instances running, this
instance's share is 25/s - and PC already knows its own group membership, so it can compute that
share itself. Set the admission target from the limit directly, just under it, then keep adapting
from there if the rejections continue.

Sharper than the exception path: a user function can succeed AND report a limit at the same time -
many APIs return their remaining quota in a header on a perfectly good response. So the signal
should not be exception-only. Let the user function hand back the limits that apply.

On combining multiple limits: the instinct is that PC should not try to reconcile a chain of
different limits from different services. But taking a *list* and using the lowest is trivial, so
there is no reason to refuse it. Limits may be supplied at build time or discovered at runtime, and
they change - so whatever is taken must be continuously reassessed rather than latched.

**This is the competitive line against Share Groups.** With share groups, adaptive rate limiting is
still the user's problem to implement. It is also what a PC wrapper over share groups could offer:
a simpler queueing engine underneath (or, initially, just the existing one - the overhead may not
matter enough to justify a second), with work dispatched through this concurrency system so the
user never writes that logic. See the share-groups notes for the surrounding argument.

## Ordering is not a fixed constraint - it is a runtime distribution

Earlier framing here was too absolute: *where ordering starves a workload, admission cannot help*.
That is right at an instant and wrong over time. **In the best case key ordering degenerates into
unordered** - and which case you are in depends entirely on the key distribution of the records
currently buffered, which changes minute to minute on a real topic.

So adaptive concurrency can genuinely help key-ordered workloads: it discovers the parallelism the
*current* buffer actually permits, which no static number can track. What it cannot do is manufacture
parallelism that the key distribution does not contain at that moment. The starvation report exists
to distinguish those two, and the distinction is temporal, not a property of the workload.

## Prior art to read before publishing anything

**Flink and Google Dataflow both have serious autoscaling machinery, and Dataflow's is key-aware
streaming autoscaling specifically.** Anyone knowledgeable will raise them immediately, so the
comparison should be made deliberately rather than met defensively. Read them for what they solved,
what signals they use, and where the honest differences are - PC's vantage is per-record and
client-side, theirs is pipeline-level with a scheduler that can add machines, and the objective
question in item 0 is one they have both had to answer.
