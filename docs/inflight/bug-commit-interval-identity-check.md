# An explicitly configured 5s commit interval is silently overwritten with 100ms

Found while proving claim C5 (`COMMIT_INTERVAL_AUTO_REDUCED`) for the transactional battle test. Not
fixed here - that plan's scope boundary keeps main-code fixes out - so it is recorded rather than lost.

## The defect

`ParallelConsumerOptions#transactionsValidation` decides whether the user set a commit interval by
**reference identity**:

```java
boolean commitInternalHasNotBeenSet = getCommitInterval() == DEFAULT_COMMIT_INTERVAL;
```

`DEFAULT_COMMIT_INTERVAL` is `ofMillis(5000)`. A user who explicitly writes
`commitInterval(Duration.ofSeconds(5))` produces a `Duration` that is `equals` to that constant but not
`==` to it, so the check concludes they never set one and replaces their value with the 100ms
transactional default.

## Why it matters

The javadoc sells the auto-reduction as a convenience applied "only if the user never set it". For the
one value where `equals` and `==` disagree, that is not what happens: the user is silently given a
commit interval **50x more frequent** than the one they asked for, which the same javadoc warns
"places higher load on the broker". There is no warning log - the value simply changes.

It is narrow (exactly 5 seconds, exactly transactional mode) which is precisely why nobody has hit it
loudly, and why it will keep not being noticed.

## Status

- **Not fixed.** The obvious fix is `equals` rather than `==`, but the surrounding
  "did the user set this?" idiom may be used elsewhere and deserves one look before changing.
- **Not asserted by a test.** It is not part of C5's documented sentence, so encoding it as an
  assertion would have meant the battle test asserting behaviour the docs do not promise. C5 is proved
  as documented; this sits beside it.
- **No issue filed yet** - needs one, and the fix wants its own PR with its own diagnosis.
