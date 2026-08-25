// Copyright (C) 2026 Antony Stubbs and contributors
// Unit tests for file-ref-gate.js. Run by the PR Checklist job before the gate itself, so a broken
// rule fails loudly rather than silently passing - or failing - every PR.
//
// The cases that matter most here are the NEGATIVE ones. This gate's failure mode is not missing a
// dangling path; it is flagging prose, and a gate that cries wolf gets opted out of. Every
// narrowing rule below was written against a real line already in this repo, cited in its case.
const assert = require("assert");
const {
  danglingRefs, newFindings, readTreeDocs, historyPointersIn, citationsIn, resolves, treeFrom, findOptOut,
  isExempt, formatFailure, normalise, CITING_FILE,
} = require("./file-ref-gate.js");

const file = (filename, ...docLines) => [{ filename, lines: docLines }];

// A filesystem oracle over a fixed list of paths, matching the shape the real callers build.
const treeOf = (...paths) => ({
  has: (p) => paths.includes(p),
  endsWith: (suffix) => paths.some((p) => p.endsWith(suffix)),
});

const TREE = treeOf(
  "bin/build.sh",
  "docs/ci.md",
  "docs/inflight/ci-review-agent.md",
  "parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ConsumerManager.java",
);

let run = 0;
function check(name, fn) {
  fn();
  run++;
  console.log("  ok  " + name);
}

// ---------------------------------------------------------------- rule 1: added dangling paths

check("flags the citation this gate was written for", () => {
  // docs/ci.md told readers to run this when editing the canonical review-gate paragraph. The
  // script has never existed - the citation was born dangling and survived every review since.
  const hits = danglingRefs(
    file("docs/ci.md", "If you change this paragraph, run bin/check-review-gate-contract.sh."),
    TREE);
  assert.deepStrictEqual(hits.map((h) => h.ref), ["bin/check-review-gate-contract.sh"]);
});

check("an unbackticked path in an HTML comment is still a citation", () => {
  // The real one was inside `<!-- ... -->` and unbackticked. A rule that only read markdown links,
  // or only backticked spans, would have passed it.
  const hits = danglingRefs(file("docs/ci.md", "<!-- see bin/nope.sh for the check -->"), TREE);
  assert.deepStrictEqual(hits.map((h) => h.ref), ["bin/nope.sh"]);
});

check("a markdown link to a missing neighbour is flagged", () => {
  const hits = danglingRefs(file("docs/inflight/x.md", "see [the gate](../gate-that-left.md)"), TREE);
  assert.deepStrictEqual(hits.map((h) => h.ref), ["../gate-that-left.md"]);
});

check("every line counts, not only the ones a change touched", () => {
  // The whole point of scanning the tree: a citation nobody has edited for months is still a
  // citation, and deleting what it points at is how it breaks. An added-lines gate cannot see that.
  const hits = danglingRefs(file("docs/x.md", "written long ago: bin/gone.sh", "", "and bin/also-gone.sh"),
                            TREE);
  assert.deepStrictEqual(hits.map((h) => h.ref), ["bin/gone.sh", "bin/also-gone.sh"]);
});

check("a hit names the line it is on", () => {
  const hits = danglingRefs(file("docs/x.md", "clean line", "", "see bin/gone.sh"), TREE);
  assert.deepStrictEqual(hits.map((h) => h.file), ["docs/x.md:3"]);
});

// ---------------------------------------------------------------- rule 1: what must NOT fire

check("resolves from the repo root", () => {
  assert.deepStrictEqual(danglingRefs(file("docs/x.md", "run bin/build.sh first"), TREE), []);
});

check("resolves relative to the citing file - the house style for neighbouring docs", () => {
  // docs/inflight/*.md cite their neighbours as ../ci.md, the only form that links on GitHub.
  assert.deepStrictEqual(danglingRefs(file("docs/inflight/x.md", "see [CI](../ci.md)"), TREE), []);
});

check("resolves as a path tail - the package-suffix shorthand AGENTS.md asks for", () => {
  assert.deepStrictEqual(
    danglingRefs(file("docs/x.md", "the lock is taken in internal/ConsumerManager.java"), TREE), []);
});

check("a token with no file extension is prose about a directory, not a citation", () => {
  // "unit tests live under src/test/java" - there is no such path from the root, and saying so
  // would be noise, not a finding.
  assert.deepStrictEqual(citationsIn("tests live under src/test/java and src/main"), []);
});

check("a bare filename is not a path", () => {
  assert.deepStrictEqual(citationsIn("the README.md says otherwise"), []);
});

check("globs, placeholders and elisions are patterns, not paths", () => {
  assert.deepStrictEqual(citationsIn("the bin/check-*.sh scripts"), []);
  assert.deepStrictEqual(citationsIn("name it bin/check-foo.sh and it is granted"), []);
  assert.deepStrictEqual(citationsIn("run bin/<name>.sh"), []);
  assert.deepStrictEqual(citationsIn("parallel-consumer-core/.../chaostests/ChaosConductor.java"), []);
});

check("a path this repo does not own is not a citation, even when its tail resolves", () => {
  // Each of these ends in something real - bin/build.sh, .m2/settings.xml - so a rule that read the
  // tail alone would approve a path it had not checked. The runner docs are full of them.
  assert.deepStrictEqual(citationsIn("${GITHUB_WORKSPACE}/bin/build.sh"), []);
  assert.deepStrictEqual(citationsIn("sudo /usr/local/bin/reboot-into-windows.sh"), []);
  assert.deepStrictEqual(citationsIn("credentials go in ~/.m2/settings.xml"), []);
});

check("a link writes its target twice and is one finding, not two", () => {
  // The house link style is [`docs/ci.md`](docs/ci.md). Reported per occurrence, every broken link
  // in the tree doubles - and the count in the failure headline stops matching what a reader sees.
  assert.deepStrictEqual(citationsIn("see [`docs/gone.md`](docs/gone.md) for this"), ["docs/gone.md"]);
  const hits = danglingRefs(file("docs/x.md", "see [`docs/gone.md`](docs/gone.md)"), TREE);
  assert.strictEqual(hits.length, 1);
});

check("a real directory containing 'example' is not treated as a placeholder", () => {
  // The placeholder list must not swallow parallel-consumer-examples/.
  assert.deepStrictEqual(
    citationsIn("parallel-consumer-examples/pom.xml"), ["parallel-consumer-examples/pom.xml"]);
});

check("a URL is somebody else's file, and cannot leak a token from its path", () => {
  assert.deepStrictEqual(citationsIn("see https://github.com/astubbs/parallel-consumer/blob/master/bin/build.sh"), []);
  assert.deepStrictEqual(citationsIn("www.yourkit.com/java/profiler/index.js is the profiler"), []);
});

check("only citing file types are scanned", () => {
  // A .java import is the compiler's business; a .yml path fragment is usually a key, not a file.
  assert.deepStrictEqual(danglingRefs(file("src/A.java", "import bz.stub.nope/Gone.java;"), TREE),
                         []);
});

check("an exempt document is skipped entirely", () => {
  // docs/self-hosted-runner.md documents the runner package on the HOST - ./svc.sh, ~/.m2 - none of
  // which are in this repo, and none of which should be.
  assert.ok(isExempt("docs/self-hosted-runner.md"));
  assert.deepStrictEqual(danglingRefs(file("docs/self-hosted-runner.md", "run ./svc.sh status"),
                                      TREE), []);
});

check("a line may name a dead path when it says why - the documented repair does exactly that", () => {
  // docs/citations.md tells authors that when a target is gone, the repair is to point at the
  // history holding it - which writes the dead path out. Without this escape the gate would flag
  // the repair it asks for.
  const marked = "removed in a1b2c3d; `git show a1b2c3d^:docs/gone.md` <!-- file-refs: N/A - the "
                 + "file is deliberately named after its deletion -->";
  assert.deepStrictEqual(danglingRefs(file("docs/x.md", marked), TREE), []);
});

check("the line escape still needs a reason", () => {
  const bare = "see docs/gone.md <!-- file-refs: N/A -->";
  assert.deepStrictEqual(danglingRefs(file("docs/x.md", bare), TREE).map((h) => h.ref),
                         ["docs/gone.md"]);
});

check("the marker covers its paragraph, not just the line - which is where prose puts it", () => {
  // The shape this was written against, from docs/citations.md itself: the sentence carrying the
  // citation wraps, so the marker ends up two lines below the path it is about.
  const hits = danglingRefs(
    file("docs/x.md", "docs/ci.md told readers to run bin/check-review-gate-contract.sh, a script",
                      "that has never existed, and the citation survived every review since.",
                      "<!-- file-refs: N/A - the point of the sentence is that it never resolved -->"),
    TREE);
  assert.deepStrictEqual(hits, []);
});

check("the marker's paragraph stops at a blank line", () => {
  const hits = danglingRefs(
    file("docs/x.md", "see bin/gone.sh for this",
                      "",
                      "<!-- file-refs: N/A - about the paragraph below, not the one above -->"),
    TREE);
  assert.deepStrictEqual(hits.map((h) => h.ref), ["bin/gone.sh"]);
});

check("the marker never reaches DOWNWARD, so it cannot silence what follows it", () => {
  // The direction matters: covering the line below would let one marker excuse a breakage the
  // author never looked at, and an escape that wide is no narrower than having no gate.
  const hits = danglingRefs(
    file("docs/x.md", "old: docs/gone.md <!-- file-refs: N/A - a record of the deletion -->",
                      "new: bin/still-wrong.sh"),
    TREE);
  assert.deepStrictEqual(hits.map((h) => h.ref), ["bin/still-wrong.sh"]);
});

check("a path behind a git revision is a history pointer, not a citation", () => {
  // docs/citations.md PRESCRIBES this form when the target is gone, so the path is correct
  // precisely because it does not resolve. Reading it as live makes the gate fail its own repair.
  assert.deepStrictEqual(citationsIn("read it with `git show bd7172418^:docs/gone.md`"), []);
  assert.deepStrictEqual(citationsIn("git show 0de96fc^:docs/inflight.md"), []);
  assert.deepStrictEqual(citationsIn("git show HEAD~2:bin/gone.sh"), []);
});

check("an ordinary colon in prose is still checked", () => {
  // The revision rule must not become "anything after a colon", which would silence half the
  // citations in the tree - they routinely follow "see:" and "Fix site:".
  assert.deepStrictEqual(citationsIn("Fix site: docs/gone.md"), ["docs/gone.md"]);
  assert.deepStrictEqual(citationsIn("see:bin/gone.sh"), ["bin/gone.sh"]);
});

check("a document that carries the prescribed repair pointer is not reporting a defect", () => {
  // The shape of 30 of the 31 findings left on master: eight documents citing docs/inflight.md
  // throughout, each carrying one `git show 0de96fc^:docs/inflight.md` pointer. The repair is
  // done; a gate that fires on it teaches people to stop applying it.
  assert.deepStrictEqual(danglingRefs(file("docs/x.md",
    "The ledger was `docs/inflight.md` until it became a directory.",
    "Read it as this report did: `git show 0de96fc^:docs/inflight.md`."), TREE), []);
});

check("a pointer excuses only the path it names", () => {
  assert.deepStrictEqual(danglingRefs(file("docs/x.md",
    "cites docs/gone-a.md and docs/gone-b.md",
    "`git show 0de96fc^:docs/gone-a.md`"), TREE).map((h) => h.ref), ["docs/gone-b.md"]);
});

check("both revision readers share one grammar - they drifted the moment they did not", () => {
  // Review finding on astubbs/parallel-consumer#320: the inline suppressor accepted `origin/master:`
  // and the pointer reader did not, so this form was ignored as a citation yet failed to excuse the
  // same path elsewhere in the document. One `REVISION` grammar now feeds both.
  assert.deepStrictEqual(citationsIn("git show origin/master:docs/gone.md"), []);
  assert.deepStrictEqual([...historyPointersIn(["git show origin/master:docs/gone.md"])],
                         ["docs/gone.md"]);
  assert.deepStrictEqual(danglingRefs(file("docs/x.md",
    "the note lived at docs/gone.md before the split",
    "read it with `git show origin/master:docs/gone.md`"), TREE), []);
});

check("historyPointersIn reads the forms the repo actually writes", () => {
  assert.deepStrictEqual([...historyPointersIn(["git show 0de96fc^:docs/inflight.md"])],
                         ["docs/inflight.md"]);
  assert.deepStrictEqual([...historyPointersIn(["git show 15f1ebe23^:a/b/c.properties"])],
                         ["a/b/c.properties"]);
  assert.deepStrictEqual([...historyPointersIn(["nothing to see here"])], []);
});

// ---------------------------------------------------------------- the base-relative ratchet

check("a finding the base already had is not this branch's to answer for", () => {
  // Master gained 90 dangling references from ordinary work in a single day - about fifty of them
  // documents describing modules and plans that live on feature branches. A gate failing every PR
  // for those would be switched off within a week.
  const current = [{ file: "docs/a.md:3", ref: "x/inherited.md" },
                   { file: "docs/a.md:9", ref: "x/mine.md" }];
  const base = [{ file: "docs/a.md:1", ref: "x/inherited.md" }];
  assert.deepStrictEqual(newFindings(current, base).map((h) => h.ref), ["x/mine.md"]);
});

check("identity ignores the line number, so inserting a paragraph is not a new finding", () => {
  // Same reasoning that makes AGENTS.md forbid citing a `file:line`: an unrelated insertion above a
  // citation must not turn an inherited finding into one this branch introduced.
  assert.deepStrictEqual(newFindings([{ file: "docs/a.md:400", ref: "x/y.md" }],
                                     [{ file: "docs/a.md:12", ref: "x/y.md" }]), []);
});

check("the same path dangling in a DIFFERENT document is new", () => {
  // Otherwise copying a broken citation into another doc would inherit its way past the gate.
  assert.deepStrictEqual(newFindings([{ file: "docs/b.md:3", ref: "x/y.md" }],
                                     [{ file: "docs/a.md:3", ref: "x/y.md" }]).map((h) => h.file),
                         ["docs/b.md:3"]);
});

check("an empty base reports everything - a missing base must not silence the gate", () => {
  assert.strictEqual(newFindings([{ file: "a.md:1", ref: "b/c.md" }], []).length, 1);
});

check("readTreeDocs slices by BYTE length, not character count", () => {
  // The reason this reader exists once instead of twice. `cat-file --batch` reports bytes, and this
  // repo's docs are full of arrows and dashes, so a character-count slice truncates a document and
  // silently drops every citation after the truncation. Two documents here: the first is non-ASCII,
  // so if its length is mishandled the second is read from the wrong offset and lost.
  const bodies = {
    // MULTI-BYTE ON PURPOSE. An em dash is 3 bytes and 1 JS character, so a character-count slice
    // reads the SECOND document from 2 bytes short and loses it. With an ASCII fixture this test
    // passes either way, which is how it shipped the first time - caught in review on
    // astubbs/parallel-consumer#320.
    "a.md": "master \u2014 fork \u2192 done\nsee bin/gone.sh",
    "b.md": "and docs/gone.md",
  };
  // The fixture has to earn its name: if someone "tidies" it back to ASCII, fail here rather than
  // silently going back to testing nothing.
  assert.ok(Buffer.byteLength(bodies["a.md"], "utf8") > bodies["a.md"].length,
            "fixture must be multi-byte or this test proves nothing");
  // THE FRAMING BELOW WAS CHECKED AGAINST REAL GIT, not inferred - this test has been vacuous
  // twice, so the model it depends on is measured rather than assumed. To redo it:
  //
  //   printf 'HEAD:AGENTS.md\nHEAD:nope.md\n' | git cat-file --batch | head -c 200 | xxd
  //
  // What that shows, and what this stub reproduces: `<sha> SP blob SP <size> LF`, `<size>` being
  // the CONTENT byte count; then the body; then ONE LF - present after the final object too, and
  // present even when the body already ends in a newline, which is the part worth measuring
  // because it is the one a reasonable person would guess wrong. A missing object is
  // `<name> SP missing LF` with no content block, which is what the `!Number.isFinite(size)`
  // branch in readTreeDocs advances past.
  //
  // The stub HONOURS opts.encoding, which is the whole point: the bug this case exists for lives at
  // the boundary between how the runner decodes the stream and how readTreeDocs slices it. A stub
  // that always returned latin1 would pass whatever encoding the caller asked for - which is how
  // the first version of this test managed to prove nothing twice over.
  const git = (args, opts = {}) => {
    if (args[0] === "ls-tree") return "a.md\nb.md\nsrc/Thing.java\n";
    const stream = Buffer.concat(Object.values(bodies).map((b) => {
      const body = Buffer.from(b, "utf8");
      return Buffer.concat([Buffer.from(`deadbeef blob ${body.length}\n`, "utf8"),
                            body, Buffer.from("\n", "utf8")]);
    }));
    return stream.toString(opts.encoding || "utf8");
  };
  const out = readTreeDocs("somerev", git);
  assert.deepStrictEqual(out.names, ["a.md", "b.md", "src/Thing.java"]);
  assert.deepStrictEqual(out.docs.map((d) => d.filename), ["a.md", "b.md"], "the .java is not a doc");
  assert.strictEqual(out.docs[0].lines[0], "master \u2014 fork \u2192 done",
                     "non-ASCII survives the latin1 round-trip");
  assert.deepStrictEqual(out.docs[1].lines, ["and docs/gone.md"], "the SECOND doc is not lost");
});

check("readTreeDocs returns null for a revision that is not present", () => {
  // A shallow clone without the base fetched. Callers must be able to tell "no base" from "a base
  // with no findings" - the first cannot judge new-vs-inherited at all.
  const git = () => { throw new Error("fatal: Not a valid object name"); };
  assert.strictEqual(readTreeDocs("nope", git), null);
});

check("readTreeDocs skips an object missing from the tree", () => {
  const git = (args) => args[0] === "ls-tree" ? "a.md\n" : "a.md missing\n";
  assert.deepStrictEqual(readTreeDocs("rev", git).docs, []);
});

// ---------------------------------------------------------------- resolution and normalisation

check("normalise collapses . and .. without walking off the root", () => {
  assert.strictEqual(normalise("docs/inflight/../ci.md"), "docs/ci.md");
  assert.strictEqual(normalise("./bin/build.sh"), "bin/build.sh");
  assert.strictEqual(normalise("../../x.md"), "../../x.md");
});

check("treeFrom builds the oracle BOTH callers use, directories included", () => {
  // Hand-copied into the workflow and the local script it would eventually differ between them, and
  // the two would then disagree about what exists - the one thing they cannot differ on. The
  // fixture oracle above stays hand-rolled on purpose: it is what these rules are tested against.
  const t = treeFrom(["bin/build.sh", "docs/inflight/ci-review-agent.md"]);
  assert.ok(t.has("bin/build.sh"), "a tracked file");
  assert.ok(t.has("docs/inflight"), "and every directory above one, so a cited directory resolves");
  assert.ok(!t.has("bin/nope.sh"), "and nothing else");
  assert.ok(t.endsWith("/ci-review-agent.md"), "path tails, for the package-suffix shorthand");
  assert.ok(!t.endsWith("/agent.md"), "at a segment boundary, since resolves() prefixes the slash");
});

check("resolves() reports each of its three routes", () => {
  assert.ok(resolves("bin/build.sh", "docs/x.md", TREE), "root");
  assert.ok(resolves("../ci.md", "docs/inflight/x.md", TREE), "relative to the citing file");
  assert.ok(resolves("internal/ConsumerManager.java", "docs/x.md", TREE), "path tail");
  assert.ok(!resolves("bin/nope.sh", "docs/x.md", TREE), "and says no otherwise");
});

// ---------------------------------------------------------------- opt-out and message

check("the opt-out needs a reason, like its sibling gates", () => {
  assert.ok(findOptOut("file-refs: N/A - the paths are on the runner host"));
  assert.ok(!findOptOut("file-refs: N/A"), "a bare N/A is not an opt-out");
  assert.ok(!findOptOut("nothing here"));
});

check("the failure message names every hit, and both escapes", () => {
  const msg = formatFailure([{ file: "docs/ci.md:12", ref: "bin/gone.sh", text: "run bin/gone.sh" },
                             { file: "a.md:3", ref: "b/c.md", text: "see b/c.md" }]);
  assert.ok(msg.startsWith("2 cited file path(s) do not resolve"));
  assert.ok(msg.includes("  docs/ci.md:12: bin/gone.sh"));
  assert.ok(msg.includes("  a.md:3: b/c.md"));
  assert.ok(msg.includes("git show <sha>^:<path>"), "must name the history-pointer repair");
  assert.ok(msg.includes("file-refs: N/A"), "must name the paragraph marker");
});

// AN .html FILE IS A CITING FILE. It was excluded, so a path inside one was never checked at all -
// and a rename left an ideation document pointing at a note that no longer existed, silently,
// because the gate could not see the file. Found in review of astubbs#323, where the sweep over
// renamed notes covered the .md tree and the one .html citation survived it.
check("html is scanned as a citing file", () => {
  assert.ok(CITING_FILE.test("docs/ideation/a.html"), "an .html document must be scanned");
  assert.ok(CITING_FILE.test("x.md") && CITING_FILE.test("x.adoc") && CITING_FILE.test("x.txt"),
    "the formats that already worked must keep working");
  assert.ok(!CITING_FILE.test("x.java") && !CITING_FILE.test("x.png"),
    "code and binaries are still not citing documents");

  const docs = file("docs/ideation/d.html", "<code>docs/inflight/gone.md</code>");
  const found = danglingRefs(docs, treeOf("docs/inflight/here.md"));
  assert.ok(found.length === 1, "a dangling path inside html must be reported");
  assert.ok(found[0].ref === "docs/inflight/gone.md");
});

console.log("\n" + run + " assertions passed");
