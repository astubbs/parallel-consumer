# Register hardening: what the review found and this branch deliberately did not fix

Six reviewers went over the transactional battle test. The mechanical findings were applied on the
branch; these are the ones that need a design decision rather than an edit, so they are recorded
instead of rushed. Ranked by how much false assurance each one buys.

## 1. A documented, supported build invocation runs zero claim proofs and still reports green

Every `@ProvesClaim` method lives in a class tagged `@Tag("transactions")`.
`TransactionalClaimCoverageTest` is deliberately **untagged**, so it gates every default build.

`pom.xml`'s own help text documents `-Dexcluded.groups=transactions,performance,chaos` as supported.
Run that and the build executes **no claim proofs at all**, while the register still reports every
claim covered, every parked claim explained, and every sentence intact. A fully green report on a run
that verified nothing.

The gate reads compiled annotations via ArchUnit, so it cannot tell a proof that ran and passed from
one that was never selected. The guard's selection criteria and the proofs' selection criteria are
disjoint, which is the whole bug.

**Options.** Simplest: give the coverage test the same `@Tag("transactions")` as everything it
guards, so excluding the tag excludes the now-meaningless report too. Stronger, if an untagged gate
is wanted: fail when `excluded.groups` names a tag carried by any class holding a `@ProvesClaim`
method, so the register refuses to report coverage in a run that could not have exercised it.

## 2. The drift check can be satisfied while the guarantee is weakened

`everyRecordedSentenceStillAppearsInItsSource` does `contains(expected)` on whitespace-normalised
source. So editing a javadoc claim from

> All records produced from a given source offset will either all be visible, or none will be

to

> Except where a send fails terminally, all records produced from a given source offset will either
> all be visible, or none will be - though this is best effort.

leaves the recorded substring intact, the gate green, and the claim still `PROVED` - now certifying a
materially weaker promise. **Qualification is the form a walked-back guarantee actually takes**, and
it is exactly the form this check cannot see.

Separately, `Source.README_TEMPLATE` passes `null, null` for its markers and is checked whole-file, so
a claim sentence moved out of the guarantees section into, say, a known-limitations block still
matches.

**Options.** Compare a bounded window rather than a bare substring - require the match to start at a
sentence boundary, so a prepended qualifier fails. And give the README template the same tag
treatment the javadoc has, so a claim moved off the published region fails even though the file still
contains the words.

## 3. Two ITs prove the same defect, and only one is gated

`TransactionalPartialResultSetIT` and
`TransactionalBatchVisibilityIT#aTerminallyFailedSendLeavesTheWholeTransactionInvisible` drive the
same defect the same way - a five-record result set whose middle send is oversized. They arrived by
different routes: the former is astubbs#261's self-contained regression test, written so that PR could
stand alone off master; the latter is the register-wired version that already existed here.

Consequences worth naming:

- `TransactionalPartialResultSetIT` carries **no `@ProvesClaim`**, so the coverage gate does not watch
  it. The README points users at "the test that found it", and that test gates nothing.
- It also has **no trigger guard**: nothing asserts the oversized send actually failed, and its final
  assertion accepts `isAnyOf(0, 5)`. If `OVERSIZED_VALUE_BYTES` ever stops exceeding the effective
  `max.request.size`, it silently stops exercising the defect while staying green. Its sibling does
  guard this, by awaiting a `RecordTooLargeException` on the worker path.
- Two broker ITs for one defect double the cost on a shared broker and give the pair somewhere to
  drift apart.

**Options.** Keep one. The `BatchVisibilityIT` arm is stronger (trigger guard, `read_uncommitted`
control arm, sentinel proving the verifier read past the region), so fold in
`PartialResultSetIT`'s one genuinely distinct idea - the commit-nudge loop, a cleaner way to force the
commit attempt than `requestCommitAsap` - and delete the duplicate. If both are kept deliberately,
annotate the survivor with `@ProvesClaim` and add its trigger guard.

**Note on where:** the trigger-guard gap is in astubbs#261's own file. Fixing it there, while that PR
is still open, keeps the standalone PR sound rather than having it inherit a fix from downstream.

## 4. The new shared teardown makes "PC failed to close" unobservable

`BrokerIntegrationTest.closeRegisteredTestClients` catches `Exception` per closeable and logs a
warning. `register()` takes PC instances, not just Kafka clients.

The wedge recorded in `bug-wedged-after-poisoned-transaction.md` has exactly one visible symptom: the
instance "dies only at close". That signal is now routed to `log.warn` for every subclass. The
tolerance is genuinely needed for the two arms that deliberately break a producer - it should not be
the default for the ~29 classes that will start using `register()`.

**Option.** Split it: keep `register(T)` strict, and add `registerExpectedToFailOnClose(T)` for the
arms that intend a broken client.

## 5. The gate that guards every other gate is itself ungated

`TransactionalClaimCoverageTest` has no self-test. Its four scenarios were verified by hand:

- an enforced constant with no `@ProvesClaim` fails
- editing a javadoc claim sentence fails
- a `NOT_YET_COVERED` constant with no reason fails
- a `@ProvesClaim` in an uncollectable class fails

`Source.readPublishedText()` in particular has no unit test at all - missing markers, empty region,
and the javadoc-prefix/whitespace normalisation are untested, yet the entire drift half rests on that
parser. The suite contains two exemplary demonstrations that its *test assertions* are load-bearing
(`theAbsenceAssertionIsVacuousWithoutTheNonVacuityGuard`,
`thePartialResultSetAssertionRejectsASetSplitAcrossTwoTransactions`); the register's own checks have
no equivalent.

## 6. Smaller items

- **Fixture composition is duplicated** across three ITs - two topics, a warm-up transaction, verifiers
  caught up, failure capture - about 10-15 near-identical lines each. The shared *utilities* were
  extracted; their *composition* was not.
  **Do not expect fixing this to move the `dups: similarity` gate** - it will barely register there,
  and the reason is worth knowing before anyone spends a refactor on it. That check is a TF-IDF cosine
  over `word_tokenize`d file text with comments *included*, and on the first CI run it reported these
  three ITs at 97.2% / 88.3% / 88.3%. Decomposing the dot product showed **96.6% of it came from one
  token, `--`**: the 110-character rules in the `// ------` section headers tokenise into ~55 `--`
  each, and no other file in the 233-file corpus used that style, so the term carried a high idf and a
  huge raw term frequency at once. Deleting the 24 rule lines - no code touched - dropped the same
  pairs to 55.6% / 44.6% / 45.3%. The fixture duplication below contributes on the order of 0.1%.
  So this stays a code-quality item, judged on its own merits; the gate is not an argument for it.
- **`TransactionalTopicVerifier`'s non-vacuity guard is a convention, not a mechanism.** Nothing in
  `assertNoneSeen` checks that `requireLiveAndCaughtUp` ran, though the class javadoc says it must.
  Every current caller is correctly guarded; this is a future-caller risk. A flag would close it, but
  needs an opt-out for the test that deliberately calls the unguarded form.
- **Per-claim notes assert specific negative controls** ("turned it red with 'Wanted 1 time ... but was
  2 times'") and cite proving methods by name. Nothing checks either. All cited methods exist today;
  nothing stops a rename or a stale control, and those notes are the only place `PROVED` is grounded.
- **`NOT_YET_COVERED` has no current user**, so `parkedClaimsMustSayWhoOwnsThem` has never executed
  against a real value.
- **`theReplayRecombinesTheSameResultsIntoDifferentTransactions` asserts `retry.size() >= 2`** on
  roughly 400ms of work against a 200ms commit interval - about a 2x margin, and the one assertion in
  the set whose truth is a scheduling outcome. If it flakes, the fix is more work-delay or a shorter
  interval, not a weaker assertion.
