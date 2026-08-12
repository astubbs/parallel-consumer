# The upstream suite, seam ON: first full measurement, and the gap nobody had looked at

For `parallel-consumer-streams` (astubbs#255). The seam-**off** execution has always been the
behaviour-preservation gate. This is its opposite: what Kafka's own suite says about the path this module
actually ships. It is the evidence U11's reinstatement rule needs, because a refused API only comes back
when Kafka's suite exercises it seam-**on** and passes.

Run it deliberately - it is skipped by default, because the failures are the measured divergence rather
than a regression, and a red gate on every build trains people to ignore it:

```
./mvnw -pl .,parallel-consumer-streams test -Dseam.on.upstream.skip=false
```

Counts come from `target/surefire-reports-kafka-upstream-seam-on/`. **Do not narrow it with `-Dtest=`** -
that overrides the execution's `<includes>`, the suite never runs, and the build goes green having
computed nothing.

## Measured, 2026-08-12, on this branch

Re-derive rather than copy; these drift with the branch.

| Class | Seam off | Seam on | Failing seam-on |
|---|---|---|---|
| `ProcessorContextImplTest` | 28 / 0 fail | 28 / **0 fail** | - |
| `RecordCollectorTest` | 59 / 0 fail | 59 / **0 fail** | - |
| `StreamTaskTest` | 101 / 0 fail | 101 / 24 fail + 6 err | **30** |
| `StreamThreadTest` | 231 / 0 fail (21 skipped) | 231 / 12 fail + 25 err (21 skipped) | **37** |
| **Total** | **419 / 0 fail** | **419 / 36 fail + 31 err** | **67** |

Two things in that table are new.

**Two of the four patched classes pass completely with the seam on.** 87 tests - the whole of
`ProcessorContextImplTest` and `RecordCollectorTest` - exercise patched code on the PC path and pass.
Before this run the module's entire seam-on evidence was one class out of four, and this is the first
positive evidence the U11 gate has ever had to work with.

**`StreamThreadTest` has 37 seam-on failures and nobody has ever looked at them.** That is a *larger*
gap than `StreamTaskTest`'s 30, which is the one every document discusses. `StreamThread` was added to
the patched set with wake-on-work; its seam-off arm was added at the same time, but its seam-on arm never
was. At least some of the 37 are the EOS refusal firing correctly - `UnsupportedOperationException` from
`PcUnsupportedConstruct` is the mechanism working, not a defect - but the split has not been done.

## What this owes

1. **Triage `StreamThreadTest`'s 37.** Same three piles as the `StreamTaskTest` triage: plumbing, PC's
   home turf, semantics. Separate the EOS-refusal errors first - those are the envelope working and
   should not be counted as divergence.
2. **Re-derive `StreamTaskTest`'s number wherever it is quoted.** This run says 30 failing. Documents in
   this repo variously say 33, 36 and "65/101". The counts move with the branch; the habit of copying
   them is what makes them wrong.
3. **Widen beyond the four patched classes.** The harness scans the `kafka-streams` test jar via
   `dependenciesToScan`, so widening is adding `<include>` lines rather than new plumbing - the
   patched classes are already ahead of the jar on the classpath. Expect ~2500 tests and treat the first
   pass as triage, not a pass/fail number.

## Delete when

`StreamThreadTest`'s 37 are triaged and recorded, the quoted `StreamTaskTest` count is consistent across
the documents that cite it, and the widening decision in item 3 has been made either way.
