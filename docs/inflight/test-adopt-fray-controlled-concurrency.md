# Fray: a controlled-scheduler concurrency tester, to be tried against the Lincheck harness classes

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->
<!-- inflight-labels: concurrency -->

**The proposal:** run a proof of concept of [Fray](https://github.com/cmu-pasta/fray), CMU's
controlled-concurrency testing platform for the JVM, against the classes this repo's Lincheck lane
already covers, and ask the one question that lane set as the standard for a concurrency tool: does
it rediscover the known defects unaided? Raised by Antony on 2026-09-09 after KPipe's migration to
it; nothing here has run it yet.

## What it is

A JVMTI agent takes control of the JVM's thread scheduler at the bytecode level, without replacing
or mocking any concurrency primitive, and explores interleavings of an ordinary JUnit 5 test under
probabilistic concurrency testing or partial-order sampling. When a schedule fails it writes a replay
file, so the interleaving is reproduced deterministically rather than hoped for. It controls
scheduling only: hardware reordering and visibility are outside what it can find, which is the
half [`../plans/2026-08-25-002-test-jcstress-poc-plain-long-visibility.md`](../plans/2026-08-25-002-test-jcstress-poc-plain-long-visibility.md)
went to jcstress for. Paper: OOPSLA 2025, <https://dl.acm.org/doi/10.1145/3764119>.

## Why it is worth a day

- **The Lincheck proof of concept left its strongest strategy on the table.**
  [`../plans/2026-08-25-001-test-lincheck-poc-plan.md`](../plans/2026-08-25-001-test-lincheck-poc-plan.md)
  adopted only the stress strategy: the model checker, the one strategy that placed a switch between
  two named reads, was blocked by a Lombok `super.hashCode()` rewrite and by replay non-determinism
  from micrometer and `parallelStream()` on the commit path. Fray does not rewrite the class under
  test that way, so it may reach that capability by a different door. It may also trip on the same
  non-determinism; that is a result either way.
- **KPipe's numbers.** Its 21-class jcstress suite reached one eviction-tombstone window 78 times in
  23,427 runs, 0.33 percent, found by luck on every run. Under Fray the ported suite covered 7,701
  schedules across 16 classes in 5m45s on a GitHub runner, against 30 to 35 minutes for jcstress.
  Its ADR is worth reading whole before ours is written:
  <https://github.com/eschizoid/kpipe/blob/main/docs/adr/0001-concurrency-testing-tooling.md>.
- **The stress probes here are instruments, not gates.** A tool that explores schedules rather than
  sampling them is the difference between a calibration and a regression detector, which is what
  the Lincheck plan's recommendation 1 is waiting on.

## What has to be checked first, and neither is a formality

- **JDK 17 running Java 8 class files.** Fray's own CI covers JDK 11, 21 and 25. Nobody has run it
  against this build's shape - Java 17 source, Java 8 bytecode via Jabel, tests on Temurin 17. The
  Lincheck PoC found Jabel a non-issue and Lombok the real blocker; assume nothing until the racy
  counter probe below has failed under Fray on this toolchain.
- **The silent no-op.** On a platform without a published JVMTI agent (KPipe's ADR: `linux-x8664`,
  `windows-x8664`, `macos-aarch64` only), the Gradle plugin prints a line and the tests pass green
  with no scheduler in control. That is the failure
  [`../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md)
  names, and the Lincheck PoC's first run was the same false PASS. A red control - a deliberately
  racy `counter++` that Fray must reject - is the first test written, and the lane refuses to
  report success unless it ran. There is no Maven plugin; the agent is attached by JVM argument,
  which is how this build's Lincheck lane already passes its arguments.

## The experiment

1. The red control, on Temurin 17 against a Java 8 class file. If it passes green, stop and record why.
2. The four Lincheck harness classes and the torn-read family, unchanged, under Fray with the
   default scheduler, one thousand iterations each. Verdict table in the same shape as the Lincheck
   plan's section 0: which of the named defects it finds, at what cost, and one it did not know.
3. Cost accounting on the box and on CI, beside the Lincheck lane's numbers.

The outcome is a dated plan under `docs/plans/`, and this note closes into it. Not a replacement
for Lincheck or jcstress until it has earned one on this evidence; KPipe dropped jcstress because
its only memory-model gate could not fail on x86, which is a finding about that suite, not about
the tool.

## Related

- [`core-hasten-adjacent-systems-register.md`](core-hasten-adjacent-systems-register.md) - the
  KPipe entry that surfaced this.
- [`ci-adopt-just-command-runner.md`](ci-adopt-just-command-runner.md) - the other adoption
  candidate from the same survey.
