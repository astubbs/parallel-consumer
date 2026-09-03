---
title: "commitInterval's '== DEFAULT_COMMIT_INTERVAL' was it set? check - the recorded defect had the direction backwards, and equals() would have been a regression"
date: 2026-09-02
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: ParallelConsumerOptions
symptoms:
  - "A recorded defect's illustrative reproduction does not fail when actually run"
  - "A '== CONSTANT' idiom used as a proxy for 'did the caller set this field', on a value object with no interning guarantee"
tags:
  - reference-identity
  - options-validation
  - transactional-commit
  - prior-art-correction
---

`git show d3d9e7bea:docs/inflight/bug-commit-interval-identity-check.md` (astubbs, 2026-08-07; the
note itself is deleted now that this is fixed) recorded that
`ParallelConsumerOptions#transactionsValidation` decided "did the user set a commit interval?" by
reference identity (`getCommitInterval() == DEFAULT_COMMIT_INTERVAL`), and claimed a user who wrote
`commitInterval(Duration.ofSeconds(5))` under a transactional commit mode would have it silently
replaced by the 100ms transactional default, since the two `Duration` objects are `equals` but not
`==`. This work picked that note up to fix it, wrote the described scenario as a JUnit test first
(per this repo's red-before-fix rule), and it passed unmodified.

## The direction was backwards

`boolean commitIntervalHasNotBeenSet = getCommitInterval() == DEFAULT_COMMIT_INTERVAL;` is `true`
(and triggers the reduction) only when the field is *reference-equal* to the constant. `Duration`
never interns non-zero values - confirmed directly on JDK 17:

```
jshell> Duration.ofMillis(5000) == Duration.ofSeconds(5)
$1 ==> false
jshell> Duration.ofMillis(5000).equals(Duration.ofSeconds(5))
$2 ==> true
```

So *any* explicitly-constructed `Duration` - including one numerically equal to the default - is a
different object from the constant, and the `==` check correctly treats it as "the user set this":
the reduction does not fire, and the user's value is kept. The identity check does what
`docs/features/commit-interval.yaml` documents ("an explicitly set value is kept"), for exactly the
scenario the note said it broke.

**The narrow, real failure mode is the mirror image**: a caller who explicitly hands back the public
`DEFAULT_COMMIT_INTERVAL` constant *object itself* - `.commitInterval(ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL)`
- is reference-indistinguishable from never having called the setter, and gets silently reduced
anyway. This is narrower than the recorded claim (it needs the literal constant, not merely an equal
value) but is a genuine, provable defect - and it shares the note's root cause: an identity or
equality comparison on the resolved value can never robustly answer "did the caller call the
setter?", because the caller is free to pass back a value indistinguishable from the default by
either measure.

## Why `equals()` is not the fix - it is the regression

The obvious fix - swap `==` for `.equals()` - was rejected. It "fixes" the narrow case at the cost of
resurrecting the exact scenario the original note worried about, for real this time: any explicit
`commitInterval(Duration.ofSeconds(5))` would then equal the constant and get silently reduced to
100ms, contradicting the documented boundary that an explicit value is kept.

## The actual fix

Stop resolving "was it set" from the value at all. Track it as its own signal: the field is left
`null` unless a caller explicitly provides a value (via the builder or the deprecated setter), and
`getCommitInterval()` resolves `null` to `DEFAULT_COMMIT_INTERVAL`. `transactionsValidation()` reads
the raw (pre-getter) field for nullness - `commitInterval == null` - which is `true` if and only if
nothing was ever explicitly assigned, regardless of what value equals or reference-matches what.
This closes both the recorded case and the narrow real one, and needs no case-by-case reasoning about
what a future JDK might or might not intern.

Applied in `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/ParallelConsumerOptions.java`
(`commitInterval` field, `getCommitInterval()`, `transactionsValidation()`).

## The general lesson

**A `== CONSTANT` (or `.equals(CONSTANT)`) test for "was this ever set" is a defect waiting for the
right value**, on any type without an interning guarantee - and even where interning holds today,
nothing pins it against a future JDK. Prefer a dedicated nullable field (or explicit boolean)
resolved once, at read time, over inferring intent from what the value happens to equal. The repo-wide
sweep for this idiom (`grep -rnE '(==|!=) *DEFAULT_' parallel-consumer-core/src/main/java
parallel-consumer-vertx/src/main/java parallel-consumer-reactor/src/main/java
parallel-consumer-mutiny/src/main/java`) found exactly the one instance fixed here; the two other
`DEFAULT_*`-prefixed `Duration` constants in the tree (`DEFAULT_STATIC_RETRY_DELAY`,
`DEFAULT_TIMEOUT`) are used only as plain values, never compared against to infer "set-ness".

**Prior art that turns out to have the wrong direction is still worth writing down properly** - a
future session grepping "commitInterval" would otherwise re-derive the same wrong conclusion the
inflight note reached, or worse, "fix" it with the regression this record rules out.
