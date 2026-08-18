# `ProgressBarTest.width` is a human eyeball test - give it an assertion without losing the demo

Separate stream of work; not urgent, and not blocking anything.

## What exists

`ProgressBarTest.width` carries `@Disabled("For reference sanity only")` and has been disabled since
it was written (`af1fa5de41`, 2020-06-17). It is the one disabled test on master that documents its
own reason: it renders a progress bar so a human can look at it. There is nothing for CI to gain by
enabling it as-is, which is why
`docs/test-hardening/inactive-tests-audit-2026-08-08.md` §1.5 lists it as a deletion candidate.

## The better outcome

Deleting it loses a useful thing - being able to *see* the rendering. Splitting it keeps both:

- **A machine assertion.** Render to a string or an in-memory sink at a known terminal width and
  assert the output: total width respected, bar plus label fits, no wrap, truncation where expected,
  behaviour at narrow widths and at a width smaller than the label. That is a real test and can be
  enabled in the normal suite.
- **The demo, kept deliberately runnable.** The visual check stays, but as something a human invokes
  on purpose rather than a disabled test pretending to be part of the suite - tagged out of the
  gating lanes, the same way the chaos and performance lanes already work here.

The distinction worth preserving: the assertion answers *is the rendering correct*, the demo answers
*does it look right*. Only the first belongs in CI, and today neither runs.

## Related

- `docs/test-hardening/inactive-tests-audit-2026-08-08.md` §1.5
