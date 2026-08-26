// Copyright (C) 2026 Antony Stubbs and contributors

// Unit tests for issue-ref-gate.js. Run by the PR Checklist job before the gate itself, so a
// broken rule fails loudly rather than silently passing - or failing - every PR.
const assert = require("assert");
const {
  suspectRefs, findOptOut, hasFileOptOut, isExempt, stripQualified, formatFailure, prBodyEntry,
  QUALIFY_BELOW, PR_BODY_LABEL,
} = require("./issue-ref-gate.js");

const file = (filename, ...added) => [{ filename, patch: added.map((l) => "+" + l).join("\n") }];

// A PR body, as the workflow feeds it in: one synthetic entry alongside the real files.
const body = (...lines) => [prBodyEntry(lines.join("\n"))];

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
                   ".github/scripts/issue-ref-gate.test.js",
                   "bin/test-check-issue-refs.sh"]) {
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

// ---------------------------------------------------------------------------------------------
// The PR body, which reaches the rule as a synthetic file rather than through a path of its own.
// These pin the ADAPTER - which text reaches suspectRefs - rather than re-testing the rule; the
// point of feeding the body through suspectRefs is that everything above already covers the rule.
// ---------------------------------------------------------------------------------------------

check("a bare ref in the PR body is flagged, and attributed to the body rather than a file", () => {
  const hits = suspectRefs(body("This carries the fix for #123."));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#123"]);
  assert.strictEqual(hits[0].file, PR_BODY_LABEL);
  assert.strictEqual(hits[0].text, "This carries the fix for #123.",
                     "the prefix used to present the body as a diff must not leak into the report");
});

check("the body label cannot be an exempt path, and cannot be mistaken for one", () => {
  assert.ok(!isExempt(PR_BODY_LABEL), PR_BODY_LABEL + " must not match EXEMPT_PATHS");
  assert.ok(/[<>]/.test(PR_BODY_LABEL), "must contain a character no file path can");
});

check("a qualified ref in the PR body is not flagged", () => {
  for (const s of ["Fixes astubbs/parallel-consumer#123.",
                   "astubbs#123 mirrors confluentinc#857",
                   "carried in confluentinc PR #548",
                   "see [#233](https://github.com/confluentinc/parallel-consumer/issues/233)",
                   "the literal `#857` marker"]) {
    assert.deepStrictEqual(suspectRefs(body(s)), [], s);
  }
});

check("a NOT_A_REF construct in the PR body is not flagged", () => {
  assert.deepStrictEqual(suspectRefs(body("This changes {@link #close()} handling.")), []);
  assert.deepStrictEqual(suspectRefs(body("It reworks #poll( and its callers.")), []);
});

check("the threshold applies in the PR body exactly as it does in a file", () => {
  assert.deepStrictEqual(suspectRefs(body("Tracking issue: #1042.")), []);
  assert.deepStrictEqual(suspectRefs(body("and #999 is still ambiguous")).map((h) => h.ref),
                         ["#999"]);
});

// The workflow checks the opt-out BEFORE it calls suspectRefs, so a body that opts out skips the
// whole gate - itself included. Both halves are pinned: the body would otherwise fail, and the
// opt-out is what stops it mattering.
check("the opt-out still skips the whole gate, body included", () => {
  const b = "Depends on #205.\nissue-refs: N/A - that is a PR number in this repo, not an issue";
  assert.ok(suspectRefs(body(b)).length > 0, "the body alone would fail");
  assert.ok(findOptOut(b), "and the opt-out must short-circuit the gate before it does");
});

// GitHub does not autolink inside a fence, so a number there cannot become the wrong link - the
// same reasoning by which stripQualified already drops code spans. Bodies quote logs, `gh` output
// and this gate's own failure message, every one of them full of bare numbers.
check("a fenced code block in the body is out of scope", () => {
  assert.deepStrictEqual(
    suspectRefs(body("Gate output:", "```", "  docs/x.md: #857  See #857", "```")), []);
  assert.deepStrictEqual(suspectRefs(body("~~~text", "#857", "~~~")), []);
  assert.deepStrictEqual(suspectRefs(body("```console", "$ gh issue view 857  # was #857", "```")),
                         []);
  assert.deepStrictEqual(suspectRefs(body("   ```", "#857", "   ```")), [],
                         "a fence indented up to three spaces is still a fence");
});

check("prose after a fence closes is scanned again", () => {
  assert.deepStrictEqual(
    suspectRefs(body("```", "#857 inside", "```", "and #29 outside")).map((h) => h.ref), ["#29"]);
});

check("fence closing follows the opening fence's character and length", () => {
  assert.deepStrictEqual(
    suspectRefs(body("```", "#857", "````", "and #29")).map((h) => h.ref), ["#29"],
    "a longer run of the same character closes the block");
  assert.deepStrictEqual(suspectRefs(body("````", "#857", "```", "and #29")), [],
                         "a shorter run does not close it");
  assert.deepStrictEqual(suspectRefs(body("```", "#857", "~~~", "and #29")), [],
                         "nor does the other fence character");
});

// CommonMark lets a closing fence be followed "only by spaces or tabs". Accepting a closer with
// trailing content closed the block earlier than GitHub does, so lines GitHub still renders as code
// were scanned and flagged - a reference a reader never sees as a link. Raised in review on
// astubbs#221 and fixed there.
check("a closing fence with trailing content does not close the block", () => {
  assert.deepStrictEqual(suspectRefs(body("```", "#857", "``` see the note", "#29 still fenced")),
                         [], "trailing content means it is not a closer, so the block stays open");
  assert.deepStrictEqual(suspectRefs(body("```", "#857", "```  \t", "and #29")).map((h) => h.ref),
                         ["#29"], "spaces and tabs after the run are allowed, so this one closes");
});

// The opening side, which fails in the direction that actually costs something: treating a non-fence
// as a fence drops lines GitHub DOES auto-link, and drops them silently.
check("a backtick fence whose info string contains a backtick opens nothing", () => {
  assert.deepStrictEqual(suspectRefs(body("```a`b", "#857 is prose here")).map((h) => h.ref),
                         ["#857"]);
  assert.deepStrictEqual(suspectRefs(body("~~~a`b", "#857")), [],
                         "a tilde fence carries no such restriction");
});

check("an unclosed fence swallows the rest of the body, exactly as GitHub renders it", () => {
  assert.deepStrictEqual(
    suspectRefs(body("before #29", "```", "#857 and everything after")).map((h) => h.ref), ["#29"]);
});

// A body is not a diff, but suspectRefs reads it as one. `++` is the trap: prefixing with a bare
// "+" would produce "+++", which suspectRefs skips as a diff file header - silently exempting the
// line, and handing anyone who noticed a way to hide a reference. Checklist items make `-` routine.
check("body lines that look like diff markers are still scanned", () => {
  for (const line of ["++ an asciidoc passthrough mentioning #857",
                      "+++ #857",
                      "+ #857",
                      "- [ ] Docs updated for #857",
                      "--- #857"]) {
    assert.deepStrictEqual(suspectRefs(body(line)).map((h) => h.ref), ["#857"], line);
  }
});

check("a CRLF body - what the API actually returns - splits into lines correctly", () => {
  assert.deepStrictEqual(
    suspectRefs([prBodyEntry("intro\r\n```\r\n#857\r\n```\r\nand #29")]).map((h) => h.ref), ["#29"]);
});

check("an absent or empty body contributes nothing", () => {
  for (const b of [null, undefined, ""]) {
    assert.strictEqual(prBodyEntry(b).patch, "");
    assert.deepStrictEqual(suspectRefs([prBodyEntry(b)]), []);
  }
});

check("the body is judged alongside the files, in the one call the workflow makes", () => {
  const input = [...file("docs/x.md", "See #857 here."), prBodyEntry("and #29 in the body")];
  assert.deepStrictEqual(suspectRefs(input).map((h) => `${h.file}: ${h.ref}`),
                         ["docs/x.md: #857", `${PR_BODY_LABEL}: #29`]);
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

check("the opt-out tail matches whether the caller could read the PR body", () => {
  assert.ok(!formatFailure(HITS).includes("could not read the PR body"));
  const local = formatFailure(HITS, { readsPrBody: false });
  assert.ok(local.includes("could not read the PR body"));
  // CI now scans the body as well as honouring the opt-out from it, so a caller with no body is
  // missing both. A tail naming only the opt-out would understate what the local run cannot see.
  assert.ok(local.includes("scans the body itself"), "must say CI reads the body for references too");
});

check("the fix/opt-out reminder comes AFTER the last hit, where truncated reads still see it", () => {
  // Readers of a failing tool consume its tail - often literally, via `| tail`. Guidance printed
  // only above the hits list is invisible to them; this trailer is the copy that survives.
  const msg = formatFailure(HITS);
  const lastHit = msg.lastIndexOf("docs/x.md: #857");
  const trailer = msg.lastIndexOf("issue-refs: N/A - <reason>");
  assert.ok(lastHit !== -1 && trailer !== -1, "both the hit and the trailer must be present");
  assert.ok(trailer > lastHit, "the opt-out reminder must follow the hits list");
  assert.ok(msg.slice(lastHit).includes("Fix: qualify each ref"), "the trailer must lead with the fix");
  assert.ok(msg.slice(lastHit).includes("issue-refs: exempt"), "the trailer must name the line opt-out too");
});

// The LINE opt-out: `issue-refs: exempt` on the flagged line itself. It exists for the ref that
// must stay bare - quoted source material above all, where qualifying the number edits the quote -
// and unlike the PR-body opt-out it travels with the file, so it holds for future PRs and for
// local runs on branches that have no PR body to opt out in.
check("a line carrying issue-refs: exempt is not flagged", () => {
  const hits = suspectRefs(file("docs/x.md", 'He wrote "see #857 for that" <!-- issue-refs: exempt -->'));
  assert.deepStrictEqual(hits, []);
});

check("the line opt-out exempts ONLY its own line", () => {
  const hits = suspectRefs(file("docs/x.md",
    "See #857 for the stall family.",
    "And #858 too. <!-- issue-refs: exempt -->"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#857"]);
});

check("the line opt-out is case-insensitive and comment-syntax-agnostic", () => {
  const hits = suspectRefs(file("src/X.java", "int x = 857; // was #857 upstream - Issue-Refs: EXEMPT"));
  assert.deepStrictEqual(hits, []);
});

check("the line opt-out works in the PR body too - same rule, same adapter", () => {
  const hits = suspectRefs(body("Quoting the old thread: fixes #858 <!-- issue-refs: exempt -->"));
  assert.deepStrictEqual(hits, []);
});

check("issue-refs: exempt without a ref on the line exempts nothing else", () => {
  const hits = suspectRefs(file("docs/x.md",
    "issue-refs: exempt",
    "See #857 for the stall family."));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#857"]);
});

// The BLOCK opt-out: exempt-begin/exempt-end around a pasted run of lines - the tool for quoted
// material too dense for per-line markers, where marking every line would be a mess in itself.
check("exempt-begin/exempt-end exempts the lines between, and only those", () => {
  const hits = suspectRefs(file("docs/x.md",
    "Before the quote: #100 is flagged.",
    "<!-- issue-refs: exempt-begin -->",
    "> the old thread said #857 and #858 fix it",
    "> and #859 was a duplicate",
    "<!-- issue-refs: exempt-end -->",
    "After the quote: #200 is flagged again."));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#100", "#200"]);
});

check("an unclosed exempt-begin swallows the rest of the file's added lines, like an unclosed fence", () => {
  const hits = suspectRefs(file("docs/x.md",
    "<!-- issue-refs: exempt-begin -->",
    "quoted #857 forever"));
  assert.deepStrictEqual(hits, []);
});

check("block state resets between files - one file's begin cannot exempt another file", () => {
  const hits = suspectRefs([
    { filename: "docs/a.md", patch: "+<!-- issue-refs: exempt-begin -->\n+quoted #857" },
    { filename: "docs/b.md", patch: "+plain #858" },
  ]);
  assert.deepStrictEqual(hits.map((h) => [h.file, h.ref]), [["docs/b.md", "#858"]]);
});

check("blocks work in the PR body too - same rule, same adapter", () => {
  const hits = suspectRefs(body(
    "Real text, so #100 is flagged.",
    "<!-- issue-refs: exempt-begin -->",
    "unfenced paste mentioning #857",
    "<!-- issue-refs: exempt-end -->"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#100"]);
});

// The FILE opt-out is content-based, applied by the callers via hasFileOptOut - so it holds
// however small the patch, unlike line/block markers which must be IN the patch to be seen.
check("hasFileOptOut finds the file marker in any comment syntax, and not its absence", () => {
  assert.strictEqual(hasFileOptOut("<!-- issue-refs: exempt-file - generated doc -->\ntext"), true);
  assert.strictEqual(hasFileOptOut("// Issue-Refs: EXEMPT-FILE"), true);
  assert.strictEqual(hasFileOptOut("issue-refs: exempt on a line is not the file marker"), false);
  assert.strictEqual(hasFileOptOut(""), false);
  assert.strictEqual(hasFileOptOut(null), false);
});

// MENTION IS NOT USE. Before span-stripping, this review round's own docs made five files -
// including docs/issue-references.md, the convention's defining doc - permanently self-exempt,
// because documenting a marker in backticks activated it. These pin the fix.
check("a file marker quoted in a backtick span is a mention, not a use", () => {
  assert.strictEqual(hasFileOptOut("put `issue-refs: exempt-file` anywhere in the file"), false);
  assert.strictEqual(hasFileOptOut("real marker below\nissue-refs: exempt-file"), true,
    "an unquoted marker still counts - only backtick spans are inert");
});

check("a line marker quoted in a backtick span neither exempts its line nor blocks anything", () => {
  const hits = suspectRefs(file("docs/x.md",
    'append `issue-refs: exempt` to the flagged line, e.g. for #857'));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#857"],
    "the quoting line's own bare ref must still be flagged");
});

check("a line QUOTING the block markers does not open a block - the doc-section shape", () => {
  const hits = suspectRefs(file("docs/x.md",
    "wrap it in `issue-refs: exempt-begin` / `issue-refs: exempt-end` markers",
    "and a bare #57 after it must still be flagged"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#57"]);
});

check("an unquoted line carrying BOTH block markers changes no state", () => {
  const hits = suspectRefs(file("docs/x.md",
    "use issue-refs: exempt-begin and issue-refs: exempt-end around it",
    "bare #58 after the discussing line is still flagged"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#58"]);
});

check("a fence in the PR body closes any open exempt block - fail closed, not open", () => {
  // The end marker sits INSIDE the fence, where prBodyEntry blanks it; before the synthetic
  // fence-close rule, the block never ended and #200 silently escaped.
  const hits = suspectRefs(body(
    "before #100",
    "<!-- issue-refs: exempt-begin -->",
    "```",
    "quoted #857",
    "<!-- issue-refs: exempt-end -->",
    "```",
    "after #200"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#100", "#200"],
    "#100 before the block and #200 after the fence must both flag; only the fenced quote is spared");
});

check("a fence with no open block stays a plain fence - synthetic close is a no-op", () => {
  const hits = suspectRefs(body("real text #100", "```", "fenced #857", "```", "tail #200"));
  assert.deepStrictEqual(hits.map((h) => h.ref), ["#100", "#200"]);
});

// dropFileOptedOutHits is the shared control flow both callers run (local script and CI step),
// each supplying only its reader - so the drift-prone part is pinned here, once. The block is
// captured and awaited before the summary line prints, so the count and ordering stay honest.
const asyncChecks = (async () => {
  const HITS3 = [
    { file: "docs/a.md", ref: "#857", text: "a" },
    { file: "docs/a.md", ref: "#858", text: "a2" },
    { file: "docs/b.md", ref: "#859", text: "b" },
    { file: PR_BODY_LABEL, ref: "#860", text: "body" },
  ];

  await (async () => {
    const reads = [];
    const dropped = [];
    const out = await require("./issue-ref-gate.js").dropFileOptedOutHits(HITS3, async (p) => {
      reads.push(p);
      return p === "docs/a.md" ? "<!-- issue-refs: exempt-file -->" : "plain";
    }, { onDrop: (p) => dropped.push(p) });
    assert.deepStrictEqual(out.map((h) => h.ref), ["#859", "#860"],
      "marked file's hits drop; other file and PR body survive");
    assert.deepStrictEqual(reads, ["docs/a.md", "docs/b.md"],
      "each file read once (deduped), PR body never read");
    assert.deepStrictEqual(dropped, ["docs/a.md"], "onDrop fires per dropped file");
    console.log("  ok  dropFileOptedOutHits drops the marked file's hits, deduped, skipping the body");
    run++;
  })();

  await (async () => {
    const errors = [];
    const out = await require("./issue-ref-gate.js").dropFileOptedOutHits(HITS3,
      async () => { throw new Error("unreadable"); },
      { onError: (p) => errors.push(p) });
    assert.deepStrictEqual(out, HITS3, "a read failure keeps the hits - the gate must not fail open");
    assert.deepStrictEqual(errors, ["docs/a.md", "docs/b.md"], "onError fires per unreadable file");
    console.log("  ok  dropFileOptedOutHits keeps hits when the reader throws");
    run++;
  })();
})();

check("a body hit reads as the body in the failure listing", () => {
  const msg = formatFailure([{ file: PR_BODY_LABEL, ref: "#857", text: "Fixes #857" }]);
  assert.ok(msg.includes(`  ${PR_BODY_LABEL}: #857  Fixes #857`), msg);
});

// The headline advice ("write astubbs#NN") is right for a file and a trap in the body: that form is
// not cross-reference syntax, so GitHub renders it as plain text and the body loses its link. An
// author who follows the message literally would trade a wrong link for no link.
check("a body hit adds the fully-qualified advice; a file-only failure does not", () => {
  const bodyMsg = formatFailure([{ file: PR_BODY_LABEL, ref: "#857", text: "Fixes #857" }]);
  assert.ok(bodyMsg.includes("`astubbs/parallel-consumer#NN`"), "must name the auto-linking form");
  assert.ok(bodyMsg.includes("closes nothing"), "must warn that Fixes astubbs#NN closes nothing");
  assert.ok(!formatFailure(HITS).includes("In the PR BODY"),
            "a failure with no body hit must not carry body-only advice");
});

check("every hit is listed, with its count in the first line", () => {
  const two = [...HITS, { file: "a.md", ref: "#29", text: "and #29" }];
  const msg = formatFailure(two);
  assert.ok(msg.startsWith("2 reference(s) below #" + QUALIFY_BELOW));
  assert.ok(msg.includes("  docs/x.md: #857  See #857"));
  assert.ok(msg.includes("  a.md: #29  and #29"));
});

asyncChecks.then(() => console.log("\n" + run + " assertions passed"));
