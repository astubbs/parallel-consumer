# The 2026-09-07 vetting sweep - what is left after it landed

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

The first grooming sweep of these notes ran on 2026-09-07: one tooling branch, six area branches
stacked on it (one agent per filename-prefix area), merged back one at a time. The procedure it
followed and the traps it met are in [`docs/grooming.md`](../grooming.md), which owns them; this
note holds only what the sweep left open. `node bin/inflight.mjs vet` is the progress view and this
note does not repeat its counts.

## What is left

- **The owner's pass over the proposals.** Every owner-gated note the sweep read as anything but
  "still true" carries a `PROPOSED` marker; they are consolidated and ranked in
  [`process-candidate-ranking.md`](process-candidate-ranking.md) under "Proposals from the
  2026-09-07 vetting sweep". Accepting one applies the outcome and replaces the marker with a
  plain stamp; declining replaces it with a stamp saying so. Until then the markers stand, and
  `grep -l 'inflight-vetted:.*PROPOSED' docs/inflight/*.md` is the list.
- **The v6 gating reading** is in the same register, under "What gates v6, as the sweep read it".
  It is the agents' reading, stated with their confidence, not a decision.
- **Three shape questions the sweep raised and did not settle**, each the owner's call because it
  changes what the gate or the index does with the note:
  - `next-transactional-register-hardening.md` keeps a `next-` prefix the directory's rules retired;
    renaming breaks citations, which is why the sweep left it.
  - `bug-857-family.md` calls itself a register and is typed `bug` (proposal 18 in the register).
  - `static-guardedby-is-inert-on-readwritelock-guarded-state.md` is a `register` carrying
    `misdirection`, an impact the vocabulary table lists for `bug` only; the gate accepts it.
- **`issue-index.md` cannot be regenerated safely**: `bin/issue-index.sh`'s header template lacks
  the hand-added "the inverse failure is real too" paragraph, so a run would delete it. The index
  was stamped with its drift recorded instead. Fold the paragraph into the script's template before
  the next regeneration.

## Delete when

The proposals above have all been accepted or declined, the three shape questions are answered,
and the issue-index template carries its paragraph. Then everything here is either in the register
or answered by a command.
