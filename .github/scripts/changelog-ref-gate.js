// Pure logic behind the "Verify new changelog entries reference this PR" step in
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
  const newEntries = [];
  let added = [];
  let removed = [];

  const endBlock = () => {
    if (added.length) newEntries.push(...newEntriesInBlock(added, removed));
    added = [];
    removed = [];
  };

  for (const line of (patch || "").split(/\r?\n/)) {
    if (line.startsWith("-")) {
      if (isBullet(line.slice(1))) removed.push(line.slice(1));
    } else if (line.startsWith("+")) {
      if (isBullet(line.slice(1))) added.push(line.slice(1));
    } else {
      endBlock(); // a context line or hunk header closes the change block
    }
  }
  endBlock();
  return newEntries;
}

/** Whether an entry links this PR. \b stops #100 matching inside #1000. */
function citesPr(entry, prNumber) {
  return new RegExp(`(pull/${prNumber}\\b|#${prNumber}\\b)`).test(entry);
}

module.exports = { findOptOut, findNewEntries, citesPr };
