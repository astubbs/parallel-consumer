# Web GUI / demo: retry-queue visibility and making offset encoding's advantage obvious

Two user-raised ideas for the demo/GUI surface, parked 2026-08-18. The web GUI lives on the
unmerged `feats/web-gui` branch; the demonstration-app idea is likewise on an unmerged
experimental branch.

## 1. Expose retry-queue information in the web GUI

Question asked: is the retry queue already exposed in metrics? **Partially, not distinctly.**
`PCMetricsDef` today has:

- `inflight.records` - "processing OR waiting for retry" **mixed into one gauge**; retries are not
  separable
- `failed.records` - a counter of failures (per topic/partition), not a queue depth
- `waiting.records`, `slow.records`, `incomplete.offsets.total`, `partition.incomplete.offsets` -
  adjacent, but none says "N records are currently backed off awaiting retry, next due at T"

So a GUI retry view needs either a new dedicated gauge (retry-queue depth, ideally with
next-retry-due timing) or direct WorkManager state reads. Adding the metric first would benefit
all operators, not just the GUI.

## 2. Demonstrate what offset encoding actually buys (range beyond the committed offset)

The project makes little attempt to explain or demonstrate its headline trick: PC can keep
processing and acking records far beyond the committed base offset, because the incomplete map is
encoded into commit metadata. With run-length encoding and a single unacked offset the
continuable range is enormous - and with astubbs#306's density work (delta-list + Z85, plus the
committed benchmark report's engagement-point table) we now have real numbers for how far each
encoding stretches before back-pressure.

Idea: make this a centerpiece of the live demo app - visualize the incomplete map, the chosen
encoder, the encoded payload size vs the 4096 cap, and how far ahead of the committed offset
processing has run. `docs/offset-encoding-density-benchmark.md` already carries the numbers to
script such a demo. Related: `docs/inflight/perf-192-followups.md`.
