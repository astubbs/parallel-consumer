// Pure logic behind the "Verify new changelog entries cite an issue" step in
// .github/workflows/pr-checklist.yml. It lives here rather than inline in the workflow YAML so
// that it can be unit tested - the same job runs changelog-ref-gate.test.js before the gate, so
// a regression in this file fails the PR Checklist rather than silently misjudging changelogs.

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

const isBullet = line => /^\s*\*\s+\S/.test(line);
const citationsOf = text => new Set(text.match(/#\d+\b/g) || []);
const wordsOf = text => new Set(text.toLowerCase().match(/[a-z0-9]+/g) || []);

/** Dice coefficient over word sets: 1 for identical prose, ~0 for unrelated. */
function similarity(a, b) {
  const wordsA = wordsOf(a);
  const wordsB = wordsOf(b);
  if (wordsA.size === 0 || wordsB.size === 0) return 0;
  let shared = 0;
  for (const word of wordsA) if (wordsB.has(word)) shared++;
  return (2 * shared) / (wordsA.size + wordsB.size);
}

// How alike a removed and an added bullet must be before the pair reads as an edit of one entry
// rather than a deletion plus an unrelated new entry.
//
// KNOWN LIMITATION (see the "mispairs same-template uncited bullets" test). When neither bullet
// carries a citation, pairing falls back to plain word overlap - and this changelog is full of
// same-template entries ("build(deps): Bump <lib> to <version>"). Two uncited bullets in one diff
// block can then pair on boilerplate alone: a genuinely new entry gets consumed as an "edit" and
// escapes the citation check, which is the false negative this gate exists to prevent.
//
// Left as-is rather than tuned blind. If it bites, the safer direction is to FAIL CLOSED - raise
// this threshold so an uncertain pair is treated as a new entry needing a citation. That trades a
// silent miss for a visible false alarm, which has an escape hatch (`changelog-ref: N/A - ...`)
// whereas the miss has nothing.
const EDIT_THRESHOLD = 0.5;

// An entry keeps its (#NN) link even when the prose around it is rewritten wholesale, so a shared
// citation is the strongest available signal that two bullets are the same entry.
function editScore(added, removed) {
  const addedCitations = citationsOf(added);
  for (const citation of citationsOf(removed)) {
    if (addedCitations.has(citation)) return 1 + similarity(added, removed);
  }
  return similarity(added, removed);
}

// Pair added bullets with the removed bullets they replace by CONTENT, strongest match first.
// Position is not a usable signal: pairing by count alone turns a block holding two edits with a
// new entry inserted between them (-A, -B, +A', +NEW, +B') into exactly the wrong answer - +NEW
// consumed as an edit and so never checked, +B' falsely flagged as a new entry.
function newEntriesInBlock(added, removed) {
  const candidates = [];
  added.forEach((addedLine, addedIndex) =>
    removed.forEach((removedLine, removedIndex) =>
      candidates.push({ addedIndex, removedIndex, score: editScore(addedLine, removedLine) })));
  candidates.sort((a, b) => b.score - a.score);

  const pairedAdded = new Set();
  const pairedRemoved = new Set();
  for (const candidate of candidates) {
    if (candidate.score < EDIT_THRESHOLD) break;
    if (pairedAdded.has(candidate.addedIndex) || pairedRemoved.has(candidate.removedIndex)) continue;
    pairedAdded.add(candidate.addedIndex);
    pairedRemoved.add(candidate.removedIndex);
  }
  return added.filter((_, addedIndex) => !pairedAdded.has(addedIndex));
}

/**
 * The genuinely new entry lines in a unified diff of CHANGELOG.adoc - added asciidoc bullets that
 * are not simply an edited form of a bullet removed in the same change block.
 *
 * Entries are single-line bullets by convention, so a citation added only on a wrapped
 * continuation line would not be seen. Callers also get nothing useful when GitHub omits `patch`
 * for a very large diff; that is treated as "nothing to check" rather than a failure.
 */
function findNewEntries(patch) {
  return findNewEntriesWithSection(patch).map(entry => entry.line);
}

/**
 * As {@link findNewEntries}, but each entry carries the asciidoc section (`=== Fixes`) it sits
 * under, so the caller can require an issue only where one is meaningful. The section comes from
 * the nearest preceding heading in the patch - a context line, an added line, or the trailing
 * context GitHub puts on the `@@` hunk header. Null when the patch does not show one.
 */
function findNewEntriesWithSection(patch) {
  const newEntries = [];
  let added = [];
  let removed = [];
  let section = null;

  const endBlock = () => {
    if (added.length) {
      for (const line of newEntriesInBlock(added, removed)) newEntries.push({ line, section });
    }
    added = [];
    removed = [];
  };

  for (const line of (patch || "").split(/\r?\n/)) {
    if (line.startsWith("-")) {
      if (isBullet(line.slice(1))) removed.push(line.slice(1));
    } else if (line.startsWith("+")) {
      if (isBullet(line.slice(1))) added.push(line.slice(1));
      else {
        const heading = line.slice(1).match(/^===\s+(.+?)\s*$/);
        if (heading) { endBlock(); section = heading[1]; }
      }
    } else {
      endBlock(); // a context line or hunk header closes the change block
      const hunk = line.match(/^@@[^@]*@@\s*===\s+(.+?)\s*$/);
      const heading = line.replace(/^ /, "").match(/^===\s+(.+?)\s*$/);
      if (hunk) section = hunk[1];
      else if (heading) section = heading[1];
    }
  }
  endBlock();
  return newEntries;
}

// What counts as citing an issue. Deliberately NOT a bare `#NN`: GitHub numbers issues and pull
// requests from one sequence, so `#104` alone cannot be distinguished from a PR reference without
// an API call - and "cite the issue" is the entire point. An explicit /issues/ URL can be, and the
// changelog already links that way (`https://github.com/.../issues/857[#857]`).
//
// Fork and upstream issues both count; most fixes here trace to an upstream issue, and AGENTS.md's
// reference convention already spells those `upstream #NN`.
const ISSUE_LINK = /\/issues\/\d+\b/;

/** Whether an entry cites an issue (fork or upstream) by explicit link. */
function citesIssue(entry) {
  return ISSUE_LINK.test(entry);
}

// Sections whose entries describe a user- or operator-visible change, and so should say which
// reported problem or request they address.
//
// Build & CI is deliberately EXEMPT. This project's tooling work is self-directed and has no issue
// behind it: of the 12 Build & CI entries predating this rule, 7 cite nothing at all and the rest
// cite a PR. Requiring an issue there would mean inventing issues, or writing `changelog-ref: N/A`
// on every CI PR - the same paperwork this change set out to remove, pointing the other way.
const SECTIONS_REQUIRING_AN_ISSUE = ["Breaking", "Improvements", "Fixes", "Examples"];

/**
 * New entries that ought to cite an issue and don't - what the gate fails on.
 *
 * An entry in an unrecognised or unseen section is not required to cite one: the patch may simply
 * not show the heading, and failing on that would punish a diff's shape rather than its content.
 */
function entriesMissingIssue(patch) {
  return findNewEntriesWithSection(patch)
    .filter(entry => SECTIONS_REQUIRING_AN_ISSUE.includes(entry.section))
    .filter(entry => !citesIssue(entry.line))
    .map(entry => entry.line);
}

module.exports = {
  findOptOut,
  findNewEntries,
  findNewEntriesWithSection,
  citesIssue,
  entriesMissingIssue,
  SECTIONS_REQUIRING_AN_ISSUE,
};
