# The 2026-09-07 vetting sweep - what is left after it landed

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

The first grooming sweep of these notes ran on 2026-09-07: one tooling branch, six area branches
stacked on it (one agent per filename-prefix area), merged back one at a time. The procedure it
followed and the traps it met are in [`docs/grooming.md`](../grooming.md), which owns them; this
note holds only what the sweep left open. `node bin/inflight.mjs vet` is the progress view and this
note does not repeat its counts.

## What is left

- **The owner's pass over the proposals is done** (2026-09-08): every proposal was accepted as
  stated and applied - closes, shrinks, one merge, one retype - and astubbs#139 was ruled out of v6
  scope, so `core-139-public-api-thread-safety-contract.md` is deferred rather than re-premised.
  `grep -l 'inflight-vetted:.*PROPOSED' docs/inflight/*.md` should now list only the file that
  documents the marker.
- **The v6 gating reading** is in [`process-candidate-ranking.md`](process-candidate-ranking.md)
  under "What gates v6, as the sweep read it". It is the agents' reading, stated with their
  confidence, not a decision.
- **Two shape questions the sweep raised and did not settle**, each the owner's call because it
  changes what the gate or the index does with the note:
  - `next-transactional-register-hardening.md` keeps a `next-` prefix the directory's rules retired;
    renaming breaks citations, which is why the sweep left it.
  - `static-guardedby-is-inert-on-readwritelock-guarded-state.md` is a `register` carrying
    `misdirection`, an impact the vocabulary table lists for `bug` only; the gate accepts it.
- **`issue-index.md` cannot be regenerated safely**: `bin/issue-index.sh`'s header template lacks
  the hand-added "the inverse failure is real too" paragraph, so a run would delete it. The index
  was stamped with its drift recorded instead. Fold the paragraph into the script's template before
  the next regeneration.
- **`bug-857-family.md` is far past what the session index can carry** - now a register, it sorts
  to the top of the index at its full length. Splitting its sightings ledger out was flagged by the
  sweep and not part of any accepted outcome.

## Delete when

The two shape questions are answered, the issue-index template carries its paragraph, and the
857 family ledger has been split or accepted at its size. Then everything here is either in the
register or answered by a command.
