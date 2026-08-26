# `forbidden-apis` is on, but not yet banning `parallelStream()`

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

`de.thetaphi:forbiddenapis` runs over main code with the `jdk-unsafe` bundle, so the default charset,
locale and timezone are banned and the build is green. Register item 6 asked for one more thing that
is **not** enabled: a ban on `parallelStream()`.

**`profile: new`.** Nothing is wrong with banning `parallelStream()`; only three legacy call sites
block it. Under the two-profile scheme in
[`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md) this is banned on new code
from day one with those three untouched, and this entry stops being blocked work and becomes a wiring
task. It is the clearest example in the repo of what single-profile enforcement costs.

## Why not yet

Three main-code call sites exist today:

- `ProcessingShard`, in `slowWork.parallelStream()`
- `PartitionState`, twice, in the accessors returning incomplete offsets

The reason to remove them is real and already recorded elsewhere: the Lincheck PoC found
`parallelStream()` breaks model-checking replay determinism, because it runs on ForkJoinPool threads
the scheduler cannot own. But swapping to `stream()` is a **behaviour change in a hot path**, not a
lint fix, and it belongs in a change that can measure the throughput effect. A rule that fails on
arrival needs the code fixed or the rule narrowed, never a suppression - so the rule stays narrow
until the sites are worked off.

Note the register said "two `PartitionState` accessors". There are three sites; `ProcessingShard` was
missed.

## Top rules to turn back on whole-tree

**There is exactly one, and this note is it.** `jdk-unsafe` is fully on; `parallelStream()` is the
only signature held back, on three sites. A five-item list here would be padding - the other engines
have backlogs because they arrived with hundreds of findings, and this one arrived with eight and
they were all fixed.

| # | Signature | Sites | Why | Effort |
|---|---|--:|---|---|
| 1 | `java.util.Collection#parallelStream()` | 3 | Breaks Lincheck replay determinism, runs on ForkJoinPool threads a controlled scheduler cannot own. | Behaviour change in a hot path - needs a throughput check, not a sweep |

The next signatures worth *considering* once that lands are the ones this repo has been bitten by
rather than a catalogue: `new BigDecimal(double)` and the `jdk-deprecated` bundle. Neither has a
recorded incident here, so neither is proposed yet.

## The signature, ready to paste

Add to the `forbiddenapis` plugin's `<configuration>` in the parent pom, alongside
`<bundledSignatures>`:

```xml
<signatures>
    java.util.Collection#parallelStream() @ Runs on ForkJoinPool threads a controlled scheduler cannot own - breaks Lincheck replay determinism
</signatures>
```

**Turning it on with the three sites still present is the definition of not-done**, and the build
will tell you immediately.

## What is already enforced, and what it cost to get there

Enabling `jdk-unsafe` found eight violations, all fixed in the same change rather than suppressed:

- Six in `PCMetricsDef`'s markdown generator - `toUpperCase()`, `toLowerCase()` and four
  `String.format` calls, all default-locale. This is generated *documentation*, so the output would
  differ on a machine with a Turkish locale. Now `Locale.ROOT`.
- Two in the metrics example's Prometheus endpoint, and that pair was a genuine defect rather than a
  lint hit: `response.getBytes()` was called **twice**, once for the `Content-Length` header and once
  for the body, in the platform default charset. Now encoded once, explicitly UTF-8.

Bound to the `check` goal only, not `testCheck`. Test code is its own decision with its own haul, the
same way [`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md) treats it, and nobody
has measured it yet.
