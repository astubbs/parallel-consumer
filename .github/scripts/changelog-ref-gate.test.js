// Unit tests for changelog-ref-gate.js, run by the PR Checklist job before the gate itself.
// Plain node, no dependencies, no runner: `node .github/scripts/changelog-ref-gate.test.js`.
// Exits non-zero on the first sign of trouble, so a broken gate fails CI loudly.

"use strict";

const assert = require("assert");
const { findOptOut, citesIssue, sectionOf, entriesMissingIssue } = require("./changelog-ref-gate.js");

let failures = 0;
function test(name, fn) {
  try {
    fn();
    console.log(`  ok  ${name}`);
  } catch (error) {
    console.log(`FAIL  ${name}\n      ${error.message.replace(/\n/g, "\n      ")}`);
    failures++;
  }
}

function lines(...parts) {
  return parts.join("\n");
}

// A changelog shaped like the real one: several long entries per section, so a realistic diff's
// three lines of context cannot reach back to the heading.
const CHANGELOG = lines(
  "== Unreleased",
  "",
  "=== Build & CI",
  "",
  "* some tooling change with no issue",
  "* another tooling change",
  "",
  "=== Fixes",
  "",
  "* fix: an old entry that cites nothing",
  "* fix: one that cites (https://github.com/confluentinc/parallel-consumer/issues/857[#857])",
  "* fix: a newly added entry with no issue",
  "* fix: a newly added entry (https://github.com/astubbs/parallel-consumer/issues/42[#42])",
  "",
  "=== Improvements",
  "",
  "* an improvement with no issue",
);

console.log("findOptOut - the escape hatch only fires when deliberately declared");

test("a declaration on its own line opts out", () =>
  assert.strictEqual(findOptOut("Intro\nchangelog-ref: N/A - CI-only change\nmore"),
    "changelog-ref: N/A - CI-only change"));

test("a body merely quoting the syntax mid-prose does not opt out", () =>
  assert.strictEqual(findOptOut("You can write `changelog-ref: N/A - reason` to opt out."), null));

test("a declaration without a reason does not opt out", () =>
  assert.strictEqual(findOptOut("changelog-ref: N/A"), null));

test("no declaration at all", () =>
  assert.strictEqual(findOptOut("Just a normal description."), null));

console.log("\ncitesIssue - an explicit issue link, fork or upstream");

test("a fork issue link counts", () =>
  assert.strictEqual(citesIssue("* Entry (https://github.com/astubbs/parallel-consumer/issues/42[#42])"), true));

test("an upstream issue link counts", () =>
  assert.strictEqual(citesIssue("* Entry (upstream https://github.com/confluentinc/parallel-consumer/issues/857[#857])"), true));

test("a PULL link does not count - a PR is not an issue", () =>
  assert.strictEqual(citesIssue("* Entry (https://github.com/astubbs/parallel-consumer/pull/104[#104])"), false));

test("a bare #NN does not count - issues and PRs share one number sequence", () =>
  assert.strictEqual(citesIssue("* Entry (#104)"), false));

console.log("\nsectionOf - read from the changelog, not the diff");

test("finds the enclosing section for an entry deep in a section", () =>
  assert.strictEqual(sectionOf(CHANGELOG, "* fix: a newly added entry with no issue"), "Fixes"));

test("finds Build & CI for an entry in that section", () =>
  assert.strictEqual(sectionOf(CHANGELOG, "* some tooling change with no issue"), "Build & CI"));

test("an entry not present in the file has no section", () =>
  assert.strictEqual(sectionOf(CHANGELOG, "* fix: never written down"), null));

console.log("\nentriesMissingIssue - the gate itself");

// THE regression case. A real GitHub patch for this file gets `endif::[]` as its hunk-header
// context, because git has no funcname pattern for asciidoc - and the entries are long enough that
// three lines of context never reach the heading. An earlier version inferred the section from the
// patch, saw null here, treated unknown as exempt, and passed uncited entries silently.
test("flags an uncited Fixes entry even when the patch never shows the heading", () =>
  assert.deepStrictEqual(
    entriesMissingIssue(lines(
      "@@ -30,6 +30,7 @@ endif::[]",
      " * fix: an old entry that cites nothing",
      " * fix: one that cites (https://github.com/confluentinc/parallel-consumer/issues/857[#857])",
      "+* fix: a newly added entry with no issue"), CHANGELOG),
    ["* fix: a newly added entry with no issue"]));

test("passes a Fixes entry that cites an issue", () =>
  assert.deepStrictEqual(
    entriesMissingIssue(lines(
      "@@ -30,6 +30,7 @@ endif::[]",
      "+* fix: a newly added entry (https://github.com/astubbs/parallel-consumer/issues/42[#42])"), CHANGELOG),
    []));

test("Build & CI is exempt - tooling work here has no issue behind it", () =>
  assert.deepStrictEqual(
    entriesMissingIssue(lines(
      "@@ -3,4 +3,5 @@ endif::[]",
      "+* some tooling change with no issue"), CHANGELOG),
    []));

test("an entry the file does not contain is not judged", () =>
  assert.deepStrictEqual(
    entriesMissingIssue(lines(
      "@@ -1,1 +1,2 @@",
      "+* fix: never written down"), CHANGELOG),
    []));

test("no changelog change at all is nothing to check", () =>
  assert.deepStrictEqual(entriesMissingIssue("", CHANGELOG), []));

test("non-bullet additions are ignored", () =>
  assert.deepStrictEqual(
    entriesMissingIssue(lines(
      "@@ -1,1 +1,2 @@",
      "+=== Fixes",
      "+",
      "+NOTE:: some prose, not an entry"), CHANGELOG),
    []));

// Accepted consequence of dropping edit-detection: reworking an old uncited entry asks for a
// citation it never had. Pinned so the trade-off is visible rather than surprising, and so that a
// future change reinstating edit-detection shows up here.
test("editing an old uncited entry does ask for a citation (accepted trade-off)", () =>
  assert.deepStrictEqual(
    entriesMissingIssue(lines(
      "@@ -30,2 +30,2 @@ endif::[]",
      "-* fix: an old entry that cited nothing, original wording",
      "+* fix: an old entry that cites nothing"), CHANGELOG),
    ["* fix: an old entry that cites nothing"]));

console.log(failures ? `\n${failures} test(s) failed` : "\nAll tests passed");
process.exit(failures ? 1 : 0);
