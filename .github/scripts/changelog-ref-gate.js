// Copyright (C) 2026 Antony Stubbs and contributors

// Pure logic behind the "Verify new changelog entries cite an issue" step in
// .github/workflows/pr-checklist.yml. It lives here rather than inline in the workflow YAML so it
// can be unit tested - the same job runs changelog-ref-gate.test.js before the gate, so a
// regression here fails the PR Checklist rather than silently misjudging changelogs.
//
// This is a reminder for ourselves, not a defence against someone gaming it. Where being exactly
// right would need real cleverness, it takes the simple option and says so: a check nobody can
// follow is worse than one that occasionally asks for a `changelog-ref: N/A` line.

"use strict";

// The opt-out must sit on its own line AND carry a reason. Anchoring is the point: an unanchored
// match meant any PR body that merely QUOTED the syntax mid-prose - documentation, a review
// reply - silently disabled the gate for that PR.
const OPT_OUT = /^\s*changelog-ref:\s*N\/?A\b\s*-\s*\S[^\n]*/im;

/** The opt-out declaration in a PR body, or null if there isn't a valid one. */
function findOptOut(body) {
  const match = (body || "").match(OPT_OUT);
  return match ? match[0].trim() : null;
}

// What counts as citing an issue. Deliberately NOT a bare `#NN`: GitHub numbers issues and pull
// requests from one sequence, so `#104` alone cannot be told apart from a PR reference without an
// API call - and "cite the issue" is the entire point. An explicit /issues/ URL can be, and the
// changelog already links that way. Fork and upstream issues both count.
const ISSUE_LINK = /\/issues\/\d+\b/;

/** Whether an entry cites an issue (fork or upstream) by explicit link. */
function citesIssue(entry) {
  return ISSUE_LINK.test(entry);
}

// Sections whose entries describe a user- or operator-visible change, and so should say which
// reported problem or request they address.
//
// `Build & CI` is deliberately absent. This project's tooling work is self-directed and has no
// issue behind it: of the 12 Build & CI entries predating this rule, 7 cite nothing at all and the
// rest cite a PR. Requiring one there would mean inventing issues, or writing `changelog-ref: N/A`
// on every CI PR.
const SECTIONS_REQUIRING_AN_ISSUE = ["Breaking", "Improvements", "Fixes", "Examples"];

const isBullet = line => /^\s*\*\s+\S/.test(line);

function headingOf(line) {
  const match = line.match(/^===\s+(.+?)\s*$/);
  return match ? match[1] : null;
}

/** Asciidoc bullets added by this diff. */
function addedBullets(patch) {
  return (patch || "")
    .split(/\r?\n/)
    .filter(line => line.startsWith("+") && isBullet(line.slice(1)))
    .map(line => line.slice(1));
}

/**
 * The `=== Section` an entry sits under, found in the CHANGELOG ITSELF rather than in the diff.
 *
 * Reading the file is the whole trick, and it is worth saying why, because inferring the section
 * from the patch looks obviously fine and is not: git has no funcname pattern for asciidoc, so a
 * hunk header for this file reads `@@ ... @@ endif::[]` rather than the heading. Entries are also
 * one long line each, so three lines of context rarely reach back to a heading either. An earlier
 * version inferred from the patch, got null for essentially every real entry, and - because an
 * unknown section counted as exempt - passed everything silently. The job already checks the repo
 * out, so the real file is right there.
 */
function sectionOf(changelog, entry) {
  const lines = (changelog || "").split(/\r?\n/);
  const index = lines.indexOf(entry);
  if (index === -1) return null;
  for (let i = index; i >= 0; i--) {
    const heading = headingOf(lines[i]);
    if (heading) return heading;
  }
  return null;
}

/**
 * Added entries that ought to cite an issue and don't - what the gate fails on.
 *
 * Deliberate simplification: an EDIT to an existing entry is an added line too, so reworking an old
 * uncited entry can ask for a citation it never had. Telling edits from additions means fuzzy
 * matching removed bullets against added ones - that was tried, became the largest and subtlest
 * part of this file, and still mispaired entries built from the same template. The
 * `changelog-ref: N/A` opt-out covers the rare case, which is a better trade than machinery nobody
 * can follow.
 */
function entriesMissingIssue(patch, changelog) {
  return addedBullets(patch)
    .filter(entry => !citesIssue(entry))
    .filter(entry => SECTIONS_REQUIRING_AN_ISSUE.includes(sectionOf(changelog, entry)));
}

module.exports = { findOptOut, citesIssue, sectionOf, entriesMissingIssue, SECTIONS_REQUIRING_AN_ISSUE };
