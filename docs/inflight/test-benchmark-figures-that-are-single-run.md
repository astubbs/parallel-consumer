# Which benchmark figures are reproduced, and which are one run

**Capture-now note so the numbers are not quoted with more confidence than they earned.** Written
2026-08-11 from the realistic-domain benchmark on `test/ks-streams-realistic-domain-benchmark`.

Full results: `docs/plans/2026-08-11-001-realistic-benchmark-result.md`. Front door: `parallel-consumer-streams/DEMO.md`.

## Reproduced, safe to quote

- **Headline backlog catch-up: 3.72x** - stock 25.8/s to PC 96.0/s, **47s to 15s** on a 1200-record
  backlog with Zipf-skewed keys and a 20/200ms cost split. Reproduced hours apart at the same figure.
- **Single-key whole-batch drain: 1.01x.** The no-penalty claim, on the statistic a sceptic computes
  first. Drain is now printed in every cell so a regression cannot hide.

## One run per arm - anecdotes, not results

Everything else. Each is plausible and none is confirmed:

- Depth sweep 4.11x / 3.78x / 3.45x at 200 / 1200 / 3000
- Key distribution: single 0.99x, Zipf 1.5 2.00x, Zipf 1.0 4.03x, uniform 4.05x, one-key-per-record 4.09x
- Profile: blocking 3.74x, mixed 3.87x, CPU idle 3.85x, **CPU at equal threads 1.19x**
- Partition composition 15.65x, and stock 4-partition 3.90x vs seam 1-partition 3.78x
- Payload size: no effect at 16x

`--repeat` exists for exactly this. **Re-run before any of these is published**, starting with the depth
sweep and the equal-threads CPU figure, since those two carry claims that shape how the module is
described rather than merely how fast it is.

## The claim to publish

**A range, 3.4x to 4.1x**, not a single figure - because the advantage varies with depth and the depth
sweep is itself a single run. And the bound that keeps it honest: **below saturation it is 0.99x**. The
advantage is a saturation phenomenon, which is a feature to state plainly rather than a caveat to bury.

## Delete when

The single-run cells have been repeated and either confirmed or corrected in the results document.
