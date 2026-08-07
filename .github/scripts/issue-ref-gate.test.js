// Unit tests for issue-ref-gate.js. Run by the PR Checklist job before the gate itself, so a
// broken rule fails loudly rather than silently passing - or failing - every PR.
const assert = require("assert");
const {
  suspectRefs, findOptOut, isExempt, stripQualified, formatFailure, QUALIFY_BELOW,
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
  // #200 resolves here (a docs issue about ManagedTruth) while authors mean confluentinc#200, the
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

// The tolerance is gone, and this assertion is what stops it drifting back. `upstream #N` was
// accepted while the tree still used it; the sweep removed the last use, so it is now flagged like
// any other unqualified reference. If someone re-adds the alternation to stripQualified, this fails.
check("flags upstream prose - the role word is not a repo name", () => {
  for (const s of ["See upstream #857.",
                   "orphaned implementation in upstream PR #270",
                   "see upstream issue #857"]) {
    assert.ok(suspectRefs(file("docs/x.md", s)).length > 0, "must be flagged: " + s);
  }
  assert.deepStrictEqual(suspectRefs(file("docs/x.md", "confluentinc#857 is the original")), []);
});

check("still flags trailing items in a list", () => {
  // Every item is bare now that the leading `upstream` no longer qualifies the first one.
  const hits = suspectRefs(file("docs/x.md", "the upstream #233 / #326 / #857 cases"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#233", "#326", "#857"]);
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

// The asciidoc link macro, which the README is written in. The target names the repo, so the number
// in the link text is already unambiguous - exactly the reasoning behind the markdown and html rules.
check("an asciidoc link macro qualifies the number in its link text", () => {
  const macros = [
    "* https://github.com/confluentinc/parallel-consumer/issues/65[Enhanced retry epic #65]",
    "See {base_confluent_url}/issues/12[issue #12], and the JavaDoc:",
    "(see https://github.com/confluentinc/parallel-consumer/pull/291[PR #291 Explicit exceptions] for more)",
  ];
  for (const line of macros) {
    assert.deepStrictEqual(suspectRefs(file("README.adoc", line)), [], line);
  }
});

// The direction that matters: stripping MORE is how a real ambiguous ref goes silently unflagged.
check("a link macro does not swallow a bare ref elsewhere on the line", () => {
  const cases = [
    "https://example.com/x[some text] and a bare #857",
    "* https://github.com/confluentinc/parallel-consumer/issues/65[Enhanced retry epic #65] - see also #29",
    "{base_confluent_url}/issues/12[issue #12] but #100 is ours",
  ];
  for (const line of cases) {
    assert.ok(suspectRefs(file("docs/x.md", line)).length > 0, "must still flag: " + line);
  }
});

// An anchor wrapped across two lines: suspectRefs sees each line separately, so neither half carries
// a complete element. Both halves must strip, without breaking the single-line or the outside case.
check("an html anchor split across two lines still qualifies its link text", () => {
  const opening = ' * See <a href="https://github.com/confluentinc/parallel-consumer/issues/433">Different results';
  const closing = " * obtained with different max concurrency for the same consumer #433</a>";
  assert.deepStrictEqual(suspectRefs(file("X.java", opening)), []);
  assert.deepStrictEqual(suspectRefs(file("X.java", closing)), []);
});

check("a ref outside a split anchor is still flagged", () => {
  // `#29` sits after the element closes, so it is prose, not link text.
  assert.ok(suspectRefs(file("X.java", " * text</a> and then #29")).length > 0);
  // A complete single-line anchor must still leave a trailing bare ref visible.
  assert.ok(suspectRefs(file("X.java", ' <a href="https://x/1">t #433</a> plus #29')).length > 0);
});

check("opt-out is recognised, and needs a reason", () => {
  assert.ok(findOptOut("blah\nissue-refs: N/A - all refs here are upstream by construction\nblah"));
  assert.ok(!findOptOut("no marker here"));
  assert.ok(!findOptOut("issue-refs: N/A"), "bare N/A without a reason must not count");
});

// The failure message is shared by the CI gate and bin/check-issue-refs.sh. Two hand-written copies
// drifted apart within hours, so these pin the parts that made them disagree.
const HITS = [{ file: "docs/x.md", ref: "#857", text: "See #857" }];

check("the failure message recommends the owner forms, never the role form", () => {
  const msg = formatFailure(HITS);
  assert.ok(msg.includes("`astubbs#NN` for this repo or `confluentinc#NN`"), "must lead with owners");
  assert.ok(!/Write .*`upstream #NN` for/.test(msg), "must not tell the author to write the role form");
  // Was "it is being swept out" while the tolerance existed. The sweep removed the last use and
  // stripQualified no longer accepts the form, so the message has to say so - an author told it
  // "still passes" would go looking for why their line was flagged anyway.
  assert.ok(msg.includes("no longer accepted"), "must say the role form is now rejected");
  assert.ok(!msg.includes("still passes"), "must not claim the form is still tolerated");
});

check("the failure message names the owner in its prose too, not the role", () => {
  const msg = formatFailure(HITS);
  assert.ok(!msg.includes("upstream's range"), "prose must say confluentinc's range");
  assert.ok(!msg.includes("Every upstream issue"), "prose must say Every confluentinc issue");
});

check("the mirror-lookup hint searches the owner form, matching the mirror titles", () => {
  // Mirror titles are `confluentinc#NNN: ...`, so the hint must search that. They used to read
  // `upstream #NNN:` - an import that deviated from its own plan - and the hint followed it.
  assert.ok(formatFailure(HITS).includes('--search "confluentinc#NN"'));
  assert.ok(!formatFailure(HITS).includes('--search "upstream'), "no stale role-form lookup");
});

check("formatFailure takes the repo from its caller, and defaults to the fork", () => {
  assert.ok(formatFailure(HITS, { repo: "acme/thing" }).includes("gh issue list -R acme/thing"));
  assert.ok(formatFailure(HITS).includes("gh issue list -R astubbs/parallel-consumer"));
});

check("the opt-out tail matches whether the caller can read the PR body", () => {
  assert.ok(!formatFailure(HITS).includes("does not read the PR body"));
  assert.ok(formatFailure(HITS, { readsPrBody: false }).includes("does not read the PR body"));
});

check("every hit is listed, with its count in the first line", () => {
  const two = [...HITS, { file: "a.md", ref: "#29", text: "and #29" }];
  const msg = formatFailure(two);
  assert.ok(msg.startsWith("2 reference(s) below #" + QUALIFY_BELOW));
  assert.ok(msg.includes("  docs/x.md: #857  See #857"));
  assert.ok(msg.includes("  a.md: #29  and #29"));
});

console.log("\n" + run + " assertions passed");
