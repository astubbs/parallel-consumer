# Disabled tests, and the v6 release gate on them

**0.6.0.0 does not ship while any test is disabled.** Handled by astubbs#263, which audits every test
that does not run, assert, or exist. Do not start separate work on these; track the PR.

Tests carrying `@Disabled` when this was written: `VertxTest`, `ParallelEoSStreamProcessorTest` (two),
and `MultiInstanceRebalanceTest`. All predate the fork, added in 2021 and 2022, before this repo had a
rule against muting or the `@Quarantined` mechanism that replaced it. So this is inherited debt rather
than a rule being broken today. One is not a muted test at all: `VertxTest.handleHttpResponseCodes`'s
entire body is `assertThat(true).isFalse()`, a stub that was never written.

Quarantining does not clear the gate on its own, because a release is separately blocked while the
quarantine registry is non-empty, so it defers the same gate by another route. AGENTS.md gives the
reasoning for why muting is the wrong answer: it "loses the signal - a 'known flake' can be a real
product bug".

Why it matters to the release rather than only to the codebase: `docs/data/testing-evidence.yaml`
asserts flake discipline as evidence for the release claim, and a reader who greps for `@Disabled` a
minute after reading it is exactly the reader that data is written for.

## Delete when

astubbs#263 has landed and no test carries `@Disabled`.
