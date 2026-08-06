# The changelog gate does not enforce the changelog policy

**Open.** The wording defect that hid this is fixed (#113's "from the next release on"); the gap in
the gate itself is not, and is deliberately left alone.

## What the policy says vs what the gate checks

`AGENTS.md` -> Changelog: **a PR never adds a changelog entry.** The section for the release being cut
is regenerated at release time from the commit log. The only edit a PR may make is correcting a
factual error in text that is already there (as #198 did for the Kafka client version).

`.github/scripts/changelog-ref-gate.js` checks something else entirely: that an *added* bullet under
`Breaking` / `Improvements` / `Fixes` / `Examples` carries an explicit `/issues/NN` link. The two
overlap only by accident:

- **False pass.** An added entry that cites an issue is a policy violation the gate waves through.
  #57's entries all cite issues.
- **False fail.** The gate cannot distinguish an edit from an addition, so the one edit the policy
  *permits* - a correction - reads as a new entry. `changelog-ref: N/A - <reason>` in the PR body is
  the intended escape, and the workflow comment names this case.

## Why it was not tightened to "reject any addition"

Considered while fixing the wording, and rejected:

1. **The distinction the policy turns on is not mechanically detectable.** "Adds an entry" vs
   "corrects an existing one" both appear as `+*` lines in the patch. The gate's own header records
   that fuzzy-matching removed bullets against added ones was implemented, became the largest and
   subtlest part of the file, and still mispaired entries built from the same template. Re-deriving
   it would repeat known-failed work.
2. **A blanket rule would be enforced by the honour system anyway.** Its only escape is
   `changelog-ref: N/A - <reason>`, a self-declared line in the PR body. The same line that
   legitimises a correction legitimises a policy-violating addition, so the strict gate buys no
   enforcement over the written rule - it buys an opt-out line on every legitimate correction.
3. **It would block four PRs opened in good faith.** #51, #57, #105 and #106 all modify
   `CHANGELOG.adoc` and all predate the policy. Tightening now fails them for following the rule that
   was live when they were written.

Weakening the existing citation check was never on the table: it still does its own job correctly.

## If this is revisited

The mechanically honest version is a **warning, not a gate**: any `CHANGELOG.adoc` change in a PR
posts a comment saying entries are generated at release time and asking the author to confirm this is
a correction. No false failures, and it puts the policy in front of the person who would otherwise
learn it from a merge conflict at release time. Worth doing only if entries actually start
reappearing in PRs after 0.6.0.0 ships - until then the four pre-policy PRs are the entire population,
and they are known.
