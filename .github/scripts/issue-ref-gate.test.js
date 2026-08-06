// Unit tests for issue-ref-gate.js. Run by the PR Checklist job before the gate itself, so a
// broken rule fails loudly rather than silently passing - or failing - every PR.
const assert = require("assert");
const {
  suspectRefs, findOptOut, isExempt, stripQualified, QUALIFY_BELOW,
} = require("./issue-ref-gate.js");

const file = (filename, ...added) => [{ filename, patch: added.map((l) => "+" + l).join("\n") }];

let run = 0;
function check(name, fn) {
  fn();
  run++;
  console.log("  ok  " + name);
}

check("the threshold is 1000", () => {
  assert.strictEqual(QUALIFY_BELOW, 1000);
});

check("flags any unqualified low number, whichever repo it means", () => {
  const hits = suspectRefs(file("docs/x.md", "See #857 for the stall family."));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#857"]);
});

check("flags a low number even though it resolves in this repo - the old blind spot", () => {
  // #200 resolves here (a docs issue about ManagedTruth) while authors mean upstream #200, the
  // shared-nothing architecture. Resolving is not evidence the reference is right.
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "part of the #200 rework")).map((h) => h.ref),
                         ["#200"]);
});

check("allows a bare number at or above the threshold - only this fork can have one", () => {
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "Tracking issue: #1042.")), []);
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "and #999 is still ambiguous"))
                           .map((h) => h.ref), ["#999"]);
});

check("allows the fork-qualified prose form", () => {
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "Fixed by astubbs#119.")), []);
});

check("allows upstream prose, including 'PR #N' and 'issue #N'", () => {
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "See upstream #857.")), []);
  assert.deepStrictEqual(
    suspectRefs(file("docs/x.md", "orphaned implementation in upstream PR #270")), []);
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "see upstream issue #857")), []);
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "confluentinc#857 is the original")), []);
});

check("still flags trailing items in a list - only the first is qualified", () => {
  const hits = suspectRefs(file("docs/x.md", "the upstream #233 / #326 / #857 cases"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#326", "#857"]);
});

check("allows the owner-qualified PR and issue variants", () => {
  for (const s of ["carried in confluentinc PR #548", "see confluentinc issue #857",
                   "fixed by astubbs PR #100"]) {
    assert.deepStrictEqual(suspectRefs(file("docs/x.md", s)), [], s);
  }
});

check("allows a fully qualified cross-repo ref", () => {
  assert.deepStrictEqual(
    suspectRefs(file("docs/x.md", "See confluentinc/parallel-consumer#857.")), []);
  assert.deepStrictEqual(
    suspectRefs(file("docs/x.md", "See astubbs/parallel-consumer#119.")), []);
});

check("allows a markdown link whose target is a URL", () => {
  const line = "| [#117](https://github.com/astubbs/parallel-consumer/issues/117) | " +
               "[#233](https://github.com/confluentinc/parallel-consumer/issues/233) |";
  assert.deepStrictEqual(suspectRefs(file("docs/refactoring.md", line)), []);
});

check("allows an html anchor - the href qualifies the number in its link text", () => {
  const line = ' * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/329">' +
               "Github issue #329</a> for the original report";
  assert.deepStrictEqual(suspectRefs(file("T.java", line)), []);
});

check("still flags a number OUTSIDE the anchor on the same line", () => {
  const line = '<a href="https://github.com/confluentinc/parallel-consumer/issues/329">#329</a> ' +
               "and also #857 which is bare";
  assert.deepStrictEqual(suspectRefs(file("T.java", line)).map((h) => h.ref), ["#857"]);
});

check("ignores javadoc member links", () => {
  assert.deepStrictEqual(suspectRefs(file("A.java", " * {@link #close()} releases it.")), []);
});

check("ignores refs inside a URL", () => {
  assert.deepStrictEqual(
    suspectRefs(file("docs/x.md", "https://github.com/confluentinc/parallel-consumer/issues/857")),
    []);
});

check("ignores refs inside a code span", () => {
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "the literal `#857` marker")), []);
});

check("ignores removed and context lines", () => {
  const files = [{ filename: "docs/x.md", patch: "-old #857 line\n unchanged #858 line" }];
  assert.deepStrictEqual(suspectRefs(files), []);
});

check("exempts the files where a bare number means upstream", () => {
  for (const p of ["CHANGELOG.adoc",
                   "src/docs/development/upstream-map.yaml",
                   "src/docs/development/upstream-pr-analysis.adoc",
                   ".github/scripts/issue-ref-gate.test.js"]) {
    assert.ok(isExempt(p), p + " should be exempt");
    assert.deepStrictEqual(suspectRefs(file(p, "fix: something (#857)")), []);
  }
});

check("the threshold is overridable, so it can be tightened without editing tests", () => {
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "#1042"), { qualifyBelow: 2000 })
                           .map((h) => h.ref), ["#1042"]);
});

check("stripQualified leaves an unqualified ref alone", () => {
  assert.ok(stripQualified("plain #857 here").includes("#857"));
});

check("opt-out is recognised, and needs a reason", () => {
  assert.ok(findOptOut("blah\nissue-refs: N/A - all refs here are upstream by construction\nblah"));
  assert.ok(!findOptOut("no marker here"));
  assert.ok(!findOptOut("issue-refs: N/A"), "bare N/A without a reason must not count");
});

console.log("\n" + run + " assertions passed");
