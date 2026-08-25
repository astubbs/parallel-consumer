# Two live documents assert more than their evidence carries

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

Both were found in astubbs#323's review, flagged rather than fixed because changing a rule's stated
rationale is a judgement its author should make. Both were **re-verified before this note was
written** - a carried-forward finding nobody re-checks is how a stale claim gets a second life.

- **`docs/quarantined-tests.md` overstates the case for its own rule change.** The rewrite says the
  owner-granted exception *"had become the routine path"*; the record shows exactly **one** exception.
  The rule stands on its own without that sentence, which is why this is a wording fix and not a
  reversal. Still present - grep the file for `had become the routine path`.
- **`core-stale-arrival-guard-needs-a-null-safety-decision.md` never names its third fixture.** It
  reports the guard NPEs three existing tests and names two: `compactedTopic`,
  `committedOffsetLower`, *"and one more"*. Whoever picks that decision up has to re-run to find the
  third, which is the work the note exists to save them. Still present.

**Checked and dropped, so nobody re-adds it**: astubbs#323 also flagged the CI failure values in
`bug-pcmetrics-committed-offset-vs-completion-count.md` as "internally consistent with the test's
constants, not re-derivable". That note now cites the run it came from (32244188439), so the numbers
are traceable and the finding no longer holds. A fourth item - `STRATEGY.md` repeating two incidents
`docs/compound-engineering.md` owns - was a decision recorded as deliberate, not an open item.

## Delete when

Both sentences are corrected, or an owner rules that each says what it means to say.
