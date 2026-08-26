# `check-shell-sigpipe.sh` is a hazard category wearing a gate's clothes

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

`bin/check-shell-sigpipe.sh` and `bin/check-shell-hazards.sh` police the same class: a shell
construct that **answers wrongly rather than erroring**, invisible to ShellCheck, found the hard way.
The hazards file is named for that class rather than for today's members precisely so it can absorb
sigpipe. Sigpipe is separate only because it predates it.

Both files say so from their own side, so either entrance finds this note.

## What the move needs

- **A per-hazard PRECONDITION field in the hazards table.** Piping into `grep -q` is a defect only
  when the file sets `pipefail`; every current entry applies unconditionally. This is the only real
  design work.
- **Sixteen self-test arms** move from `bin/test-check-shell-sigpipe.sh` into
  `bin/test-check-shell-hazards.sh`. They carry hard-won detail worth preserving verbatim - notably
  that `||` must not read as a pipe, which an earlier regex got wrong and which made the gate fire on
  the very herestring it prescribes as the fix.
- **Self-exclusion becomes a marker.** Sigpipe excludes itself and its self-test by basename; the
  hazards file already has `hazard-ok-file:`, which is the better mechanism and states a reason.
- **`.githooks/pre-commit` names it in a hardcoded list.**
- **Roughly a dozen comments across `bin/` and `docs/` cite it as the authority** for the herestring
  rule. They are one-line repoints, and `bin/check-file-refs.sh` catches any missed.

## Already done, so this is smaller than it looks

The corpus resolution both gates share lives in `bin/lib/shell-corpus.sh`. Extracting it was not
tidiness: the two copies **had already drifted** - sigpipe changed directory only when given no
argument while hazards did it unconditionally, so an explicit *relative* scan directory resolved
against different roots. Nothing would have caught it, because both self-tests pass absolute paths.

## Delete when

Sigpipe is a category in the hazards table and both scripts are one script - or the decision is taken
that they stay separate, in which case say why here rather than leaving this to be rediscovered.
