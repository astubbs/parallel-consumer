// Copyright (C) 2026 Antony Stubbs and contributors

// Flags a repo file path cited in the docs that does not exist. Whole tree, every PR.
//
// House convention (AGENTS.md -> "Cite by anchor, never by line number") tells authors to cite a
// path plus a greppable string, and to "run the grep before you commit the citation". Nothing ran
// it. docs/citations.md said so outright - "nothing in CI checks any of this, so the only thing
// standing between a reader and a confidently wrong pointer is the author having run it" - and the
// cost showed up on schedule: docs/ci.md told readers to run `bin/check-review-gate-contract.sh`,
// and that script has never existed. The citation was born dangling in
// astubbs/parallel-consumer#287 and survived every review since, because a path that looks right
// reads as right.
//
// WHY THE WHOLE TREE RATHER THAN THE DIFF. Deleting a file does not change one line in the
// documents that cite it, so a diff-scoped gate is blind to the commonest way a citation breaks -
// and that is not a corner case: two moves, `docs/inflight.md` becoming a directory and the
// `io.confluent` -> `bz.stub` rename, produced 59 of the 87 dangling references this repo had.
// An earlier draft covered that with a second rule sweeping the tree for citations of paths a PR
// removed. Scanning the tree outright subsumes it: those citations simply stop resolving. The
// second rule is gone, and with it three defects that were only ever about reconciling it with the
// first.
//
// It is affordable because it RATCHETS against the base rather than demanding a clean tree - see
// newFindings() for what master did to that assumption within a day of the gate landing. A red
// result on a branch means that branch broke something, not that it inherited something.
//
// WHY NOT EXTEND issue-ref-gate.js. It answers one question, "does this `#NN` say which repo it
// means", and every line of it is about that question. This asks a different one against a
// different oracle - the filesystem rather than a threshold - so it is a sibling module wired into
// the same job, not a second rule bolted into the first. What IS shared is the shape: unit-tested
// module, local mirror script, one copy of the rule.
//
// WHY NOT JUST RESOLVE EVERY PATH-SHAPED TOKEN. Because most of them are not citations. Prose says
// "under `src/test`" and "the `bin/check-*.sh` scripts"; a plan says `internal/ConsumerManager.java`
// meaning the class, not a path from the repo root; a repaired record names a path precisely
// BECAUSE it is gone. Flagging those trains authors to ignore the gate - so a token must carry a
// real file extension to be read as a citation at all, it resolves three ways before being called
// dangling, and a document's own history pointers excuse the paths they name.
//
// Pulled out of the workflow so it can be unit tested; file-ref-gate.test.js runs first in CI.

// A citation is a path with a file extension. Without one, a token is prose about a directory or a
// package ("under src/test", "the io.confluent tree") and this gate has nothing to say about it.
const EXTENSIONS = [
  "sh", "java", "md", "adoc", "yml", "yaml", "xml", "json", "properties", "js", "ts", "py", "cmd",
  "tsv", "txt", "toml", "sql", "gradle", "kt",
];

// At least two segments, so a bare `README.md` in prose is not read as a path. `..` and `.` are
// ordinary segments here - relative links like `../ci.md` are how docs/ cites its neighbours.
const TOKEN = new RegExp(
  String.raw`[A-Za-z0-9_.@-]+(?:/[A-Za-z0-9_.@-]+)+\.(?:${EXTENSIONS.join("|")})\b`,
  "g",
);

// Anything with a scheme or a hostname is somebody else's file. Stripped from the line before
// tokens are read, rather than filtered afterwards, so a URL's path segments cannot survive as a
// token in their own right - `github.com/astubbs/parallel-consumer/blob/master/bin/build.sh` would
// otherwise contribute a very plausible-looking `master/bin/build.sh`.
const URLS = /(?:https?:\/\/|ftp:\/\/|www\.|mailto:)\S+/g;

// Files whose paths are NOT repo paths, so resolving them against this tree is meaningless.
const EXEMPT_PATHS = [
  // Documents the GitHub Actions runner package installed on a host - `./svc.sh`, `./config.sh`,
  // `/usr/local/bin/...`, `~/.m2/settings.xml`. None of them are in this repo and none should be.
  /(^|\/)docs\/self-hosted-runner\.md$/,
];

// A path that is GONE is a legitimate thing for a document to name - and docs/citations.md makes it
// a required one: when the target no longer exists, the repair is to point at the history that still
// holds it, `git show <sha>^:<path>`, which necessarily writes the dead path out. Without an escape,
// this gate would flag the very repair it tells authors to make.
//
// Line-scoped rather than file-scoped, because it must survive being read from a PATCH: in CI this
// module sees hunks, not files, so a marker at the top of a document would simply not be there. A
// reason is mandatory for the same reason the sibling gates demand one - an unexplained silence is
// indistinguishable from a mistake a year later.
// The reason must START WITH A LETTER, not merely be non-empty. Written inside an HTML comment -
// the natural place for it in prose - a bare `<!-- file-refs: N/A -->` ends in `-->`, and a
// "non-empty reason" rule reads that terminator as the reason and silences the line. The escape
// would then be exactly as strong as no escape, while looking like it had one.
const LINE_OPT_OUT = /file-refs:\s*N\/?A\b\s*-\s*[A-Za-z]/i;

// Illustrative names in worked examples: "name it `bin/check-foo.sh` and the reviewer can run it".
// Matched at segment and stem boundaries so `bin/foo.sh`, `bin/check-foo.sh` and `docs/bar.md` are
// all covered, while `parallel-consumer-examples/` - a real directory containing "example" - is
// not. Deliberately a short, closed list: anything longer starts swallowing real filenames.
const PLACEHOLDER = /(?:^|[/\-_.])(?:foo|bar|baz|qux|quux)(?:[/\-_.]|$)/i;

// Tokens that are patterns rather than paths: globs, `<placeholders>`, shell/CI interpolation, and
// the elided `.../` form docs use to shorten a long package path.
const NOT_A_PATH = /[*?<>${}|]|\.\.\./;

// A token is only a repo path if it STARTS one. These characters immediately before it mean it is
// the tail of something longer that this repo does not own: an absolute host path
// (`/usr/local/bin/reboot-into-windows.sh` in the runner docs), an interpolated one
// (`${GITHUB_WORKSPACE}/bin/build.sh`), or a home-relative one (`~/.m2/settings.xml`). The tail
// alone often resolves - `bin/build.sh` is real - so without this the gate would silently approve
// a path it never actually checked, which is the failure it exists to prevent.
const TAIL_OF_SOMETHING_ELSE = /[/~}$\\]/;

// `<rev>:<path>` is a POINTER INTO HISTORY, not a citation of the working tree - and the path is
// correct precisely because it no longer resolves. docs/citations.md does not merely permit this
// form, it PRESCRIBES it: "When the target is gone, point at the history holding it ...
// `git show <sha>^:<path>`". Reading it as a live path makes the gate fail the one repair it tells
// authors to make. Matched against the text immediately before the token, so `see: docs/x.md` -
// an ordinary colon in prose - is still checked.
// ONE grammar for what a git revision looks like, used by both readers below. They were written
// separately and drifted immediately: this one accepted `origin/master:` while the pointer reader
// did not, so `git show origin/master:docs/x.md` was correctly ignored inline yet failed to register
// as the document-wide repair pointer it plainly is. Caught in review on
// astubbs/parallel-consumer#320 before it bit anyone.
const REVISION = String.raw`(?:[0-9a-f]{7,40}|HEAD|master|origin/[\w.-]+)(?:[~^]\d*)*`;

const GIT_REVISION = new RegExp(`${REVISION}:$`);

// The PR-wide form. Deliberately looser than LINE_OPT_OUT - any non-blank reason will do - because
// the `-->` hole that rule guards against needs an HTML comment to open it, and this one must START
// its line.
const OPT_OUT = /^\s*file-refs:\s*N\/?A\b\s*-\s*\S[^\n]*/im;

function findOptOut(prBody) {
  return OPT_OUT.test(prBody || "");
}

function isExempt(path) {
  return EXEMPT_PATHS.some((re) => re.test(path));
}

// Only text files carry citations, and only their added lines are this PR's responsibility. A .java
// file's imports are the compiler's problem, not this gate's.
// `.html` IS A CITING FILE. It was excluded, so a path inside one was never checked - and a rename
// left `docs/ideation/2026-08-17-distributed-throttling-ideation.html` pointing at a note that no
// longer existed, silently, because the gate could not see the file at all (astubbs#323 review).
// The ideation documents cite notes and scripts the same way prose does; the format is not the point.
const CITING_FILE = /\.(md|adoc|txt|html)$/i;

function normalise(path) {
  const parts = [];
  for (const seg of path.split("/")) {
    if (seg === "." || seg === "") continue;
    if (seg === ".." && parts.length && parts[parts.length - 1] !== "..") parts.pop();
    else parts.push(seg);
  }
  return parts.join("/");
}

function dirname(path) {
  const i = path.lastIndexOf("/");
  return i === -1 ? "" : path.slice(0, i);
}

/**
 * Extracts the citations from one line of text. Exported so the self-test can pin the narrowing
 * rules directly, which is where every false positive this gate could produce is decided.
 */
function citationsIn(line) {
  const clean = line.replace(URLS, " ");
  // Deduplicated: a markdown link writes its target twice - [`docs/ci.md`](docs/ci.md) - and one
  // broken link is one finding, not two. Reported twice it reads as two separate defects, and the
  // count in the failure headline stops matching what a reader can see.
  const out = new Set();
  for (const m of clean.matchAll(TOKEN)) {
    const token = m[0];
    if (NOT_A_PATH.test(token) || PLACEHOLDER.test(token)) continue;
    if (m.index > 0 && TAIL_OF_SOMETHING_ELSE.test(clean[m.index - 1])) continue;
    if (GIT_REVISION.test(clean.slice(0, m.index))) continue;
    out.add(token);
  }
  return [...out];
}

/**
 * The oracle resolves() reads, built from `git ls-files`. It lives here because BOTH callers need
 * one - the CI job in pr-checklist.yml and bin/check-file-refs.sh - and two hand-copied versions are
 * how they would come to disagree about what exists, which is the single thing they cannot differ
 * on. Same reasoning that puts formatFailure here rather than at each call site.
 *
 * Directory prefixes are included, so a citation naming a directory resolves like one naming a file.
 *
 * A caller wanting more than the tracked set wraps the result rather than passing a flag:
 * bin/check-file-refs.sh ORs `fs.existsSync` into has(), so an added-but-not-yet-staged file counts
 * locally. CI has nothing to add - a runner's checkout holds the commit and nothing else.
 *
 * @param tracked  every tracked path, repo-relative
 */
function treeFrom(tracked) {
  const known = new Set(tracked);
  for (const p of tracked) {
    const parts = p.split("/");
    for (let i = 1; i < parts.length; i++) known.add(parts.slice(0, i).join("/"));
  }
  return {
    has: (p) => known.has(p),
    endsWith: (suffix) => tracked.some((p) => p.endsWith(suffix)),
  };
}

/**
 * Is this citation reachable? Three ways, and the last two are what keep the gate quiet enough to
 * be worth having:
 *
 *   1. from the repo root - the ordinary case, `bin/build.sh`
 *   2. relative to the citing file - `../ci.md` from inside docs/inflight/, the house style for
 *      neighbouring docs, and the form markdown links must use to work on GitHub
 *   3. as a path SUFFIX of some tracked file - `internal/ConsumerManager.java`, the package-tail
 *      shorthand AGENTS.md's "smallest distinctive greppable string" rule actively encourages. It
 *      names the file unambiguously without pinning the module prefix, so it survives a module
 *      being moved, and flagging it would push authors back toward brittle full paths.
 *
 * @param citation  the token as written
 * @param citingFile  path of the file it was written in
 * @param tree  { has(path): boolean, endsWith(suffix): boolean } - the filesystem oracle
 */
function resolves(citation, citingFile, tree) {
  const direct = normalise(citation);
  if (direct && tree.has(direct)) return true;

  const relative = normalise(`${dirname(citingFile)}/${citation}`);
  if (relative && tree.has(relative)) return true;

  return tree.endsWith(`/${direct}`);
}

// Every `<rev>:<path>` pointer in a document, as normalised paths.
//
// A document that repairs a citation the way docs/citations.md prescribes - naming the dead path AND
// the commit that still holds it - has done the required work, and the dead path is then a feature
// of the repair rather than a defect. Without this the gate reports a correctly repaired record as
// a pile of findings: 30 of the 31 remaining on master were `docs/inflight.md`, cited across eight
// documents that ALL carry `git show 0de96fc^:docs/inflight.md`. A gate that fires on the documented
// fix teaches people to stop applying it.
//
// Document-scoped, not line-scoped, because the pointer is usually written once at the top and the
// citations it repairs run through the whole document - which is exactly how those eight are
// written. It cannot excuse a path the document never points at: a pointer for docs/a.md says
// nothing about docs/b.md.
function historyPointersIn(lines) {
  const out = new Set();
  for (const line of lines || []) {
    for (const m of line.matchAll(new RegExp(`${REVISION}:([A-Za-z0-9_.@/-]+)`, "g"))) {
      out.add(normalise(m[1]));
    }
  }
  return out;
}

/**
 * Every citation in the tree that does not resolve.
 *
 * WHOLE TREE, NOT THE DIFF. The first version read added lines only, because the tree carried 87
 * dangling references and a gate demanding they be repaired first is a gate that never lands. They
 * are repaired, so the carve-out is gone - and with it a second rule that existed only to cover
 * what a diff cannot see. Deleting a file does not change a single line in the documents that cite
 * it, so an added-lines gate is blind to the commonest way a citation breaks; scanning the tree
 * catches it with no special case, because those citations simply stop resolving.
 *
 * The cost is that a branch is judged on the whole tree rather than on its own diff. That is only
 * affordable because master is clean and this gate keeps it clean: a red result on a branch means
 * that branch broke something, not that it inherited something.
 *
 * @param docs  [{ filename, lines }] every citing file in the tree
 * @param tree  { has, endsWith } oracle over the same tree
 * @returns [{ file, ref, text }]
 */
function danglingRefs(docs, tree) {
  const out = [];
  for (const doc of docs || []) {
    if (isExempt(doc.filename) || !CITING_FILE.test(doc.filename)) continue;
    const lines = doc.lines || [];

    // A pointer is read from the WHOLE document, wherever it sits - usually written once near the
    // top for citations running throughout. Computed here rather than passed in, so no caller can
    // forget it and quietly get a stricter gate than the other one.
    const pointers = historyPointersIn(lines);

    // The marker covers ITS OWN PARAGRAPH, reading UPWARD from where it sits.
    //
    // Per-line was the first cut and it does not survive contact with prose: a citation plus an
    // HTML comment does not fit in 120 columns, and the sentence carrying the citation usually
    // wraps, so the marker ends up two or three lines below the path it is about. Demanding one
    // line leaves the escape unused, and an unused escape is a gate people work around instead.
    //
    // Upward only, and never past a blank line. So a marker excuses the paragraph it closes, and
    // cannot reach the next one or silence a breakage that arrives below it later. That direction
    // is the one that matters: an escape covering text the author never looked at is no narrower
    // than having no gate.
    const exempted = (i) => {
      for (let j = i; j < lines.length && lines[j].trim() !== ""; j++) {
        if (LINE_OPT_OUT.test(lines[j])) return true;
      }
      return false;
    };

    lines.forEach((line, i) => {
      if (exempted(i)) return;
      for (const citation of citationsIn(line)) {
        if (resolves(citation, doc.filename, tree)) continue;
        if (pointers.has(normalise(citation))) continue;
        out.push({ file: `${doc.filename}:${i + 1}`, ref: citation, text: line.trim().slice(0, 120) });
      }
    });
  }
  return out;
}

// A finding's identity across two trees: the document it is in and the path it cites. Deliberately
// NOT the line number - inserting a paragraph above a citation must not turn an inherited finding
// into a new one, which is the same reasoning that makes AGENTS.md forbid citing a `file:line`.
function findingKey(hit) {
  return `${hit.file.replace(/:\d+$/, "")}\u0000${hit.ref}`;
}

/**
 * Every citing document in a git tree, read without checking it out.
 *
 * ONE COPY, because the tricky part is not the git plumbing but the SLICING. `cat-file --batch`
 * returns a byte length per object, and a document with any non-ASCII byte in it - this repo's docs
 * are full of arrows and dashes - has more bytes than characters. Reading the stream as `latin1`
 * makes one character exactly one byte so the length can be used as an offset, and each body is
 * re-decoded as UTF-8 afterwards. Written twice, that is two places for an off-by-one that would
 * silently truncate a document and drop the citations after the truncation - and it WAS written
 * twice: the local mirror and the CI step each carried their own copy for a day.
 *
 * @param rev  any revision - a SHA, `origin/master`, `FETCH_HEAD`
 * @param git  runs git and returns stdout: (args, opts) => string. Injected because CI runs it in
 *             the workspace and the local mirror in the current directory, and because it keeps
 *             this module free of process spawning.
 * @returns { names, docs } or null when the revision is not present (a shallow clone)
 */
function readTreeDocs(rev, git) {
  let names;
  try {
    names = git(["ls-tree", "-r", "--name-only", rev]).split("\n").filter(Boolean);
  } catch {
    return null;
  }

  const wanted = names.filter((f) => CITING_FILE.test(f));
  if (wanted.length === 0) return { names, docs: [] };

  const batch = git(["cat-file", "--batch"], {
    input: wanted.map((f) => `${rev}:${f}`).join("\n") + "\n",
    encoding: "latin1",
  });

  const docs = [];
  let pos = 0;
  for (const f of wanted) {
    const nl = batch.indexOf("\n", pos);
    if (nl === -1) break;
    // "<sha> <type> <size>", or "<object> missing" - a non-finite size is the missing case.
    const size = Number(batch.slice(pos, nl).split(" ")[2]);
    if (!Number.isFinite(size)) { pos = nl + 1; continue; }
    docs.push({
      filename: f,
      lines: Buffer.from(batch.slice(nl + 1, nl + 1 + size), "latin1").toString("utf8").split("\n"),
    });
    pos = nl + 1 + size + 1;             // + the newline cat-file writes after each body
  }
  return { names, docs };
}

/**
 * The findings this branch is answerable for: everything in its tree, minus what the base already
 * had.
 *
 * A RATCHET, NOT A CLEAN-TREE ASSUMPTION. The first cut of the whole-tree gate failed on any
 * finding, on the reasoning that the tree was clean and this gate would keep it clean. Master
 * disproved that within a day: it gained 90 dangling references from ordinary work, ~50 of them
 * documents describing modules and plans that live on FEATURE BRANCHES - `parallel-consumer-streams/`
 * paths, `docs/plans/` entries not yet on master. That is not sloppiness, it is how this repo
 * writes things down, and a gate failing every PR for it would be turned off within a week.
 *
 * The ratchet keeps everything the whole-tree scan buys and drops only the part that was wrong.
 * Deleting a file still fails the PR that deletes it: the citations pointing at it were resolving
 * in the base and stop resolving here, so they are new. Adding a bad citation still fails. What no
 * longer fails is inheriting someone else's.
 *
 * @param current  findings over this branch's tree
 * @param base     findings over the base tree
 */
function newFindings(current, base) {
  const inherited = new Set((base || []).map(findingKey));
  return (current || []).filter((h) => !inherited.has(findingKey(h)));
}

/**
 * The single copy of what an author is told when either rule fires - rendered by both callers, the
 * CI job in pr-checklist.yml and the local bin/check-file-refs.sh, so the two cannot tell different
 * stories. Its sibling gate learned that the hard way: hand-written copies of one message disagreed
 * in both directions within hours of the second being written.
 */
function formatFailure(dangling) {
  return (
    `${dangling.length} cited file path(s) do not resolve to anything in this repo.\n` +
    "A citation that looks right reads as right, so a wrong path is followed, not questioned.\n" +
    "A path resolves from the repo root, relative to the citing file, or as the tail of a real\n" +
    "path (`internal/ConsumerManager.java`). None of these matched.\n" +
    "\n" +
    "If a path is GONE, point at the history that still holds it - `git show <sha>^:<path>` -\n" +
    "which docs/citations.md prescribes and this gate accepts as the repair.\n" +
    "If a path is named deliberately - a proposal, a record of something deleted, a file on\n" +
    'another branch - close that PARAGRAPH with `<!-- file-refs: N/A - <reason> -->`. It covers\n' +
    "the paragraph above it, never anything below, and the reason must start with a letter.\n" +
    'To silence the gate for a whole PR, put "file-refs: N/A - <reason>" on its own line in the PR\n' +
    "body - but prefer the paragraph marker; the PR-wide form hides any real breakage beside it.\n" +
    "\n" +
    dangling.map((h) => `  ${h.file}: ${h.ref}\n      ${h.text}`).join("\n")
  );
}

module.exports = {
  danglingRefs, newFindings, findingKey, readTreeDocs, historyPointersIn, citationsIn, resolves, treeFrom, findOptOut, isExempt,
  formatFailure, normalise, EXEMPT_PATHS, EXTENSIONS, CITING_FILE, LINE_OPT_OUT,
};
