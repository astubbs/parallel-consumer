# SLO objectives: configure outcomes, not concurrency

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - subordinates the astubbs#333 controller to a declared objective; API-philosophy direction, not scheduled work -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (first review's breakdown:
[`core-engine-thesis.md`](core-engine-thesis.md)). The feature-side twin of
[`perf-benchmark-cost-to-slo.md`](perf-benchmark-cost-to-slo.md): that note measures what it costs
to meet an SLO; this one has the engine *drive to* one.

## The claim

Maximum throughput is not what most systems want; "p99 under 500ms, process as much as possible"
is. With residence time measured (astubbs#359) and the controller probing experimentally
(astubbs#333), concurrency becomes subordinate to a declared objective: walk the discovered
concurrency->(throughput, p99) curve and hold the highest concurrency whose p99 satisfies the
constraint. The API endpoint is the philosophical shift - no executor size, no concurrency, a
builder like `objective().p99Under(ofMillis(500)).maximizeThroughput()` - the developer describes
what good service looks like and the engine discovers the machinery. This is the natural
completion of astubbs#227's "stop making users pick maxConcurrency": stop making them pick a
*proxy* at all.

What stacks on it, each a separate stage:

- **SLO violation as the scale signal.** "No concurrency value on this instance satisfies the
  declared objective" is a stronger +1 than any CPU or lag threshold - and its converse is the
  strongest do-NOT-scale signal: if probing upward worsened latency without raising throughput,
  the dependency is saturated and five more instances would hammer it harder. Two applications
  with identical "p99 > 2s" symptoms get opposite recommendations, from experiments
  ([`core-bottleneck-attribution.md`](core-bottleneck-attribution.md)).
- **Latency budgets through a topology.** Per-stage residence shows which operator is consuming
  the end-to-end budget, not merely which has lag. Needs the Streams work (astubbs#255 /
  astubbs#271) for the graph.
- **Importance-aware allocation.** The per-function allocator
  ([`core-per-function-capacity-arbitration.md`](core-per-function-capacity-arbitration.md))
  gains an objective: not fair shares, but *where does another unit of compute buy the most SLO
  improvement?* `emailReceipt` 30 seconds behind a 5-minute SLO gets nothing;
  `authorizePayment` nearing its 300ms budget gets the capacity. Prior art: astubbs#236 (topic
  priorities) is the static ancestor of this idea.

## Why the machinery is closer than it looks

Gradient2 (the law astubbs#333 ports) is already a latency-gradient law, and that PR's
min-composed-ceilings design is exactly how a constraint composes: an SLO-derived ceiling min'd
with the throughput-derived target, no second controller. The conversation's closing line earns
its place because PC *controls* execution rather than observing it - it has a route to holding an
SLO instead of drawing a red line on a chart after the violation.

## The caveat that decides whether v1 of this is honest

**Residence time under backlog measures the backlog, not the engine**
([`perf-benchmark-cost-to-slo.md`](perf-benchmark-cost-to-slo.md) names this gap already; it is
Little's law). A consumer recovering from lag has a p99 no concurrency can satisfy, and a naive
SLO controller would thrash or permanently vote scale-out during every catch-up. The controller
must separate service-time-dominated residence (concurrency can help) from queue-dominated
residence (only capacity or time helps) - which is the attribution taxonomy again, applied to the
SLO term. Ship the objective API without that distinction and the first Monday-morning backlog
discredits it.
