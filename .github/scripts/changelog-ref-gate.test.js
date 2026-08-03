// Unit tests for changelog-ref-gate.js, run by the PR Checklist job before the gate itself.
// Plain node, no dependencies, no runner: `node .github/scripts/changelog-ref-gate.test.js`.
// Exits non-zero on the first sign of trouble, so a broken gate fails CI loudly.

"use strict";

const assert = require("assert");
const { findOptOut, findNewEntries, citesPr } = require("./changelog-ref-gate.js");

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

function patch(...lines) {
  return lines.join("\n");
}

console.log("findOptOut - the escape hatch only fires when deliberately declared");

test("a declaration on its own line opts out", () =>
  assert.strictEqual(findOptOut("Intro\nchangelog-ref: N/A - CI-only change\nmore"),
    "changelog-ref: N/A - CI-only change"));

test("leading whitespace and the NA spelling are both accepted", () =>
  assert.strictEqual(findOptOut("  changelog-ref: NA - docs only"), "changelog-ref: NA - docs only"));

test("prose that merely documents the syntax does not opt out", () =>
  assert.strictEqual(
    findOptOut("Opt out with `changelog-ref: N/A - <reason>` in the body, mirroring the convention."),
    null));

test("a quoted line in a review reply does not opt out", () =>
  assert.strictEqual(findOptOut("> changelog-ref: N/A - something"), null));

test("a declaration with no reason does not opt out", () =>
  assert.strictEqual(findOptOut("changelog-ref: N/A"), null));

test("a declaration with an empty reason does not opt out", () =>
  assert.strictEqual(findOptOut("changelog-ref: N/A - "), null));

test("an empty body does not opt out", () => assert.strictEqual(findOptOut(""), null));

test("a missing body does not throw", () => assert.strictEqual(findOptOut(null), null));

console.log("\nfindNewEntries - added bullets, minus those that are edits of removed bullets");

test("a pure addition is a new entry", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -26,6 +26,7 @@",
      " == Unreleased",
      "+* New feature entry (#104)",
      " * Older entry (#73)")),
    ["* New feature entry (#104)"]));

test("editing one existing entry is not a new entry", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,7 +28,7 @@",
      " context",
      "-* Dependencies and build plugins refreshed, JUnit 5.10.2 -> 5.14.4 (#73)",
      "+* Dependencies and build plugins refreshed, JUnit 5.10.2 -> 5.14.3 (#73)",
      " context")),
    []));

test("an edit and an addition in one block yields only the addition", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,7 +28,8 @@",
      "-* Old entry version one (#73)",
      "+* Old entry version two (#73)",
      "+* Brand new unrelated entry about metrics (#104)",
      " context")),
    ["* Brand new unrelated entry about metrics (#104)"]));

test("an addition listed before the edit it accompanies still pairs by content", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,6 +28,7 @@",
      "+* Brand new unrelated entry about metrics (#104)",
      "-* Old entry version one (#73)",
      "+* Old entry version two (#73)",
      " context")),
    ["* Brand new unrelated entry about metrics (#104)"]));

// The regression this file exists for: positional pairing consumed the new entry as an edit and
// flagged the trailing real edit instead, so an uncited new entry sailed through the gate.
test("a new entry inserted between two edits is the only one reported", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,8 +28,9 @@",
      " context",
      "-* Entry A original wording here (#10)",
      "-* Entry B original wording here (#20)",
      "+* Entry A revised wording here (#10)",
      "+* Totally new entry about chaos testing, no citation yet",
      "+* Entry B revised wording here (#20)",
      " context")),
    ["* Totally new entry about chaos testing, no citation yet"]));

test("three edits with two insertions report only the insertions", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,9 +28,11 @@",
      "-* Entry A original wording here (#10)",
      "-* Entry B original wording here (#20)",
      "-* Entry C original wording here (#30)",
      "+* Entry A revised wording here (#10)",
      "+* First insertion about broker polling",
      "+* Entry B revised wording here (#20)",
      "+* Second insertion about offset encoding",
      "+* Entry C revised wording here (#30)",
      " context")),
    ["* First insertion about broker polling", "* Second insertion about offset encoding"]));

test("a heavily rewritten entry is still an edit when it keeps its citation", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,7 +28,7 @@",
      "-* Terse original note (#73)",
      "+* A completely rewritten description with entirely different vocabulary throughout (#73)",
      " context")),
    []));

test("an uncited old entry edited in place is still an edit", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,7 +28,7 @@",
      "-* Legacy entry from before the citation convention",
      "+* Legacy entry from before the citation convention, typo fixed",
      " context")),
    []));

test("a deletion plus an unrelated addition reports the addition", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -28,7 +28,7 @@",
      "-* Dropped note about vertx module wiring (#50)",
      "+* Unrelated new entry covering commit-mode defaults (#104)",
      " context")),
    ["* Unrelated new entry covering commit-mode defaults (#104)"]));

test("a context line closes the block so distant bullets do not pair", () =>
  assert.deepStrictEqual(
    findNewEntries(patch(
      "@@ -10,3 +10,2 @@",
      "-* Entry A original wording here (#10)",
      " context resets the block",
      "+* Entry A original wording here (#10)")),
    ["* Entry A original wording here (#10)"]));

test("a pure deletion reports nothing", () =>
  assert.deepStrictEqual(
    findNewEntries(patch("@@ -10,2 +10,1 @@", "-* Removed entry (#50)", " context")),
    []));

test("non-bullet edits such as a heading tweak report nothing", () =>
  assert.deepStrictEqual(
    findNewEntries(patch("@@ -5,2 +5,2 @@", "-== Unreleased", "+== Unreleased (0.6.x)")),
    []));

test("an absent patch reports nothing", () => assert.deepStrictEqual(findNewEntries(undefined), []));

console.log("\ncitesPr - the citation itself");

test("a bare #NN counts", () => assert.strictEqual(citesPr("* Entry (#104)", 104), true));

test("a full pull URL counts", () =>
  assert.strictEqual(citesPr("* Entry (https://github.com/o/r/pull/104[#104])", 104), true));

test("a longer number containing it does not count", () =>
  assert.strictEqual(citesPr("* Entry (#1040)", 104), false));

test("an unrelated citation does not count", () =>
  assert.strictEqual(citesPr("* Entry (#73)", 104), false));

console.log(failures ? `\n${failures} test(s) failed` : "\nAll tests passed");
process.exit(failures ? 1 : 0);
