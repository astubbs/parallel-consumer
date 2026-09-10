# The file-refs gate cannot see a root-level file, so a root rename leaves nothing to go red

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

**A rename of `CHANGELOG.adoc`, `README.adoc` or `AGENTS.md` breaks no gate**, because
`.github/scripts/file-ref-gate.js` reads a token as a path only when it has at least two segments -
its own comment says why: "so a bare `README.md` in prose is not read as a path". A root file has
one segment, so no prose mention of it is ever a citation, and `bin/check-file-refs.sh` and the
`repo: hygiene` gate stay green whatever the file is now called.

## What happened, 2026-09-10

astubbs#501 renamed `CHANGELOG.adoc` to `CHANGELOG.md` and swept every live reference on master.
<!-- post-merge: checked-begin -->
The v6 scope note on astubbs#475 was branch-only, so the sweep never reached it, and after master was
merged into that branch the note still named `CHANGELOG.adoc` in several places and described a
`release.yml` heading-match bug that astubbs#501 had fixed. Every gate ran green on that head. An
automated review caught it, not a check.
<!-- post-merge: checked-end -->

The gate's rule is deliberate and right for what it guards against - prose containing a dot
(`Set.removeAll`, `check-all.sh: message`) would otherwise read as paths - so this is a gap, not a
defect in the rule. The dated records that still say `CHANGELOG.adoc` (under `docs/plans/` and
`docs/solutions/`) are correct as history and may not be rewritten; the gap is only about live
documents.

## Options, none taken

- **An allow-list of root filenames treated as citations** - `CHANGELOG.md`, `README.adoc`,
  `AGENTS.md`, `CLAUDE.md`, `CONCEPTS.md`, `STRATEGY.md` - matched only when written as a bare
  token with its extension, and resolved against the tree like any other citation. Small, and it
  closes exactly this hole; the risk is a prose mention of the old name that is deliberately
  historical, which the existing `<!-- file-refs: N/A - reason -->` marker already covers.
- **A rename sweep at the PR that renames** - a check that, when a root file is renamed in a diff,
  greps the whole tree for the old name and fails on live hits. Catches the case at its source but
  cannot see branch-only documents, which is the case that bit here.
- **Leave it to readers and reviews**, which is what caught this one.

## Delete when

The allow-list lands in `.github/scripts/file-ref-gate.js` with a test in `file-ref-gate.test.js`
that a bare `CHANGELOG.adoc` in a live document fails while the same token in a dated record under
`docs/plans/` or `docs/solutions/` does not - or the owner decides the gap stays open and this note
moves to `docs/refactoring.md` as a one-liner.
