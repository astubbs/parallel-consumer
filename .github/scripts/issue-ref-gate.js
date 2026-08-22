// Copyright (C) 2026 Antony Stubbs and contributors
//
// Flags an UNQUALIFIED `#NN` on ADDED lines, where the number is low enough to be ambiguous.
//
// House convention (AGENTS.md -> Issue references): below #1000 a reference must say which repo it
// means - `astubbs#209` for this fork, `confluentinc#857` for confluentinc - because the fork's numbers
// sit entirely inside upstream's range and a bare number is a coin flip. "fork" is not used as the
// qualifier: this repo IS a fork, so it names nothing.
//
// WHY A THRESHOLD RATHER THAN "DOES IT RESOLVE HERE": resolving is a proxy for the real question and
// fails in the direction that matters. A bare `#200` resolves - to a fork issue about ManagedTruth -
// while the author meant confluentinc#200, the shared-nothing architecture. A wrong link that resolves
// is worse than a broken one, because nothing looks amiss. Checking the text instead is exact, needs
// no API calls, and cannot race issue creation.
//
// THE THRESHOLD IS AN ASSUMPTION WITH A DEADLINE. It holds only while confluentinc's numbering stays
// below 1000. That repo is dormant rather than archived, so it still creeps. Check the headroom:
//   gh api 'repos/confluentinc/parallel-consumer/issues?state=all&per_page=1&sort=created&direction=desc' \
//     --jq '.[0].number'   # state=all matters: the ceiling is often a merged PR
// `upstream-sweep.sh` warns when it gets thin - do not rely on noticing unaided.
//
// THE PR BODY IS IN SCOPE TOO, via prBodyEntry() below. The body is the surface a human actually
// reads on GitHub, and a bare `#200` renders there as a working link to the wrong issue - the exact
// failure this gate exists to prevent, on the one page where it is most visible. The gate already
// trusts the body enough to accept a *bypass* from it (`issue-refs: N/A`), so "bodies are out of
// scope" was never a principle, just an omission.
//
// Pulled out of the workflow so it can be unit tested; issue-ref-gate.test.js runs first in CI.

// Below this, a reference is ambiguous and must name its repo. At or above it, only this fork has
// such a number, so a bare `#NNNN` is unambiguous.
const QUALIFY_BELOW = 1000;

// Files where a bare #NN legitimately means upstream, so the rule must NOT fire.
const EXEMPT_PATHS = [
  // its own header says entries below 0.6.0.0 predate the fork and their #NN already mean upstream
  /(^|\/)CHANGELOG\.adoc$/,
  // every number in it is upstream by construction
  /(^|\/)src\/docs\/development\/upstream-map\.yaml$/,
  // editorial analysis written entirely in upstream terms
  /(^|\/)src\/docs\/development\/upstream-pr-analysis\.adoc$/,
  // this gate's own fixtures are deliberately full of unqualified refs
  /(^|\/)\.github\/scripts\/issue-ref-gate\.test\.js$/,
  // same rationale for the local checker's end-to-end harness: its fixtures write real opt-out
  // markers and bare refs on purpose, and a fixture that must dodge the gate stops testing it
  /(^|\/)bin\/test-check-issue-refs\.sh$/,
];

// Constructs that look like a ref but are not one.
const NOT_A_REF = [
  /\{@link\s+#\w/,        // javadoc member link: {@link #close()}
  /\{@linkplain\s+#\w/,
  /#\w+\(/,               // method reference: #poll(
];

const OPT_OUT = /^\s*issue-refs:\s*N\/?A\b\s*-\s*\S[^\n]*/im;

// IN-FILE opt-outs, three scopes, narrowest first. They exist for the ref that genuinely must stay
// bare - above all quoted source material, where qualifying the number edits the quotation - and
// unlike the PR-body OPT_OUT above they travel with the file: they hold for every future PR
// touching the text and for local runs on branches with no PR body to opt out in. The author
// writes the marker in whatever comment syntax the file already has (`// issue-refs: exempt` in
// code, `<!-- issue-refs: exempt -->` in markdown/HTML, where it is invisible when rendered).
//
//   `issue-refs: exempt`          this line only
//   `issue-refs: exempt-begin`    from here (added lines in the same patch) ...
//   `issue-refs: exempt-end`      ... to here
//   `issue-refs: exempt-file`     the whole file, judged against file CONTENT, not the patch
//
// MENTION IS NOT USE. Every marker test runs on a span-stripped view (stripCodeSpans below): a
// marker quoted in a backtick code span is documentation, not an instruction. Without that rule,
// this PR's own docs made five files - including the convention's defining doc - permanently
// self-exempt, and a doc line quoting the block markers opened a real, unclosed block. An
// UNQUOTED prose mention still matches (there is no way to tell it from use); write marker names
// in backticks when writing ABOUT them, as this comment does.
//
// Scopes are patch-shaped where they must be: suspectRefs judges added patch lines, so line and
// block markers take effect only when they are IN the patch - which they are, in the paste-a-
// quoted-block case they exist for. A lone later edit inside an old block re-carries its own
// marker. The file scope has no such caveat: hasFileOptOut is applied by both callers to the
// file's full text (local: disk; CI: contents API, fetched only for files with hits), so it holds
// however small the patch. A backslash-escape (`\#857`) was considered and rejected: it edits the
// text it is meant to preserve - so a quotation is no longer verbatim - renders visibly outside
// markdown, and is an illegal escape in a Java string literal.
//
// NB LINE_OPT_OUT's `exempt\b` also matches the longer markers (the `-` is a word boundary), so a
// begin/end/file marker line never flags its own bare refs either - harmless, and one less way to
// trip over the tool that is meant to be the escape hatch.
const LINE_OPT_OUT = /issue-refs:\s*exempt\b/i;
const BLOCK_BEGIN = /issue-refs:\s*exempt-begin\b/i;
const BLOCK_END = /issue-refs:\s*exempt-end\b/i;
const FILE_OPT_OUT = /issue-refs:\s*exempt-file\b/i;

// The mention-vs-use rule: markers are matched only on text OUTSIDE backtick code spans, the same
// judgment stripQualified applies to refs. Spans are single-line (a lone stray backtick must not
// pair across lines and swallow a real marker between them).
function stripCodeSpans(text) {
  return (text || "").replace(/`[^`\n]*`/g, " ");
}

function findOptOut(prBody) {
  return OPT_OUT.test(prBody || "");
}

// The file-scope test, applied to full file text by whichever caller can supply it.
function hasFileOptOut(text) {
  return FILE_OPT_OUT.test(stripCodeSpans(text));
}

/**
 * Drops every hit belonging to a file whose CONTENT carries the file-scope opt-out. The control
 * flow lives here so the two callers cannot drift - NO SECOND COPY OF THE RULE - and each caller
 * supplies only what genuinely differs: how a file is read (local disk vs the contents API) and
 * whether outcomes are logged. Read failures keep the hit (fail open here would let an unreadable
 * file silently pass, the wrong direction for a gate) and are reported via onError when given.
 *
 * @param hits      [{ file, ref, text }] from suspectRefs
 * @param readFile  async (path) => file text; may throw / reject
 * @param opts      { onDrop(path), onError(path, err) } - optional, for caller-side logging
 * @returns the hits that survive
 */
async function dropFileOptedOutHits(hits, readFile, opts = {}) {
  let out = hits;
  for (const path of [...new Set(hits.map((h) => h.file))]) {
    if (path === PR_BODY_LABEL) continue;
    try {
      if (hasFileOptOut(await readFile(path))) {
        out = out.filter((h) => h.file !== path);
        opts.onDrop?.(path);
      }
    } catch (e) {
      opts.onError?.(path, e);
    }
  }
  return out;
}

function isExempt(path) {
  return EXEMPT_PATHS.some((re) => re.test(path));
}

// Everything already unambiguous, removed before we look for bare refs.
function stripQualified(line) {
  return line
    // markdown link whose target is a URL: [#233](https://.../issues/233) - the URL qualifies it
    .replace(/\[[^\]]*\]\((https?:\/\/[^)]+)\)/g, " ")
    // html anchor, link text included: <a href="https://.../issues/329">Github issue #329</a>.
    // The href names the repo, so the number in the link text is already unambiguous - strip the
    // whole element, not just the URL, or the visible `#329` reads as an unqualified ref.
    .replace(/<a\s+href=["'][^"']*["'][^>]*>[\s\S]*?<\/a>/gi, " ")
    // The same anchor, wrapped across two lines. suspectRefs works a line at a time, so the rule
    // above cannot see it: the opening tag is on one line and `</a>` on the next. Strip each half
    // only when its counterpart is absent, so a complete anchor is still left to the rule above and
    // a `#NN` sitting OUTSIDE the element is still caught.
    .replace(/<a\s+href=["'][^"']*["'][^>]*>(?![\s\S]*<\/a>).*$/i, " ")
    .replace(/^(?!.*<a\s)[\s\S]*?<\/a>/i, " ")
    // asciidoc link macro, `URL[link text]` and the attribute-prefixed `{attr}/path[link text]`.
    // Same reasoning as the html anchor: the target names the repo, so a number in the link text is
    // already unambiguous. Must run BEFORE the bare-URL rule, which would otherwise eat the URL and
    // the opening `[` and leave the link text looking like loose prose.
    .replace(/(?:https?:\/\/|\{[\w-]+\})\S*?\[[^\]]*\]/g, " ")
    .replace(/https?:\/\/\S+/g, " ")            // bare URLs
    .replace(/`[^`]*`/g, " ")                   // code spans
    // Owner-qualified prose form, the house standard: astubbs#209 / confluentinc#857. The spaced
    // variants carry the issue-vs-PR distinction where it matters: "confluentinc PR #548".
    .replace(/\b(?:astubbs|confluentinc)\s*(?:PR\s+|issue\s+)?#\d+/gi, " ")
    // NOTE: `upstream #N` was accepted here until the tree-wide sweep removed its last use. It
    // named a role rather than a repo - and this repo is itself an upstream to anyone forking it -
    // so it is now flagged like any other unqualified reference. The tolerance is deliberately gone
    // rather than merely discouraged: left in, it comes back the moment someone copies old text.
    .replace(/[\w.-]+\/[\w.-]+#\d+/g, " ");     // fully qualified: owner/repo#N
}

/**
 * @param files  [{ filename, patch }] from the PR files API
 * @param opts   { qualifyBelow } - override the threshold (tests)
 * @returns [{ file, ref, text }]
 */
function suspectRefs(files, opts = {}) {
  const limit = opts.qualifyBelow ?? QUALIFY_BELOW;
  const out = [];
  for (const f of files || []) {
    if (!f.patch || isExempt(f.filename)) continue;
    // Block opt-out state, per file. An unclosed exempt-begin swallows the rest of this file's
    // added lines - the same direction prBodyEntry fails in for an unclosed fence, and for the
    // same reason: silently resuming after a marker the author forgot to close would flag text
    // they believed exempt, and a plausible-looking wrong answer is this gate's stated enemy.
    let exemptBlock = false;
    for (const raw of f.patch.split("\n")) {
      if (!raw.startsWith("+") || raw.startsWith("+++")) continue;
      const line = raw.slice(1);
      // Markers are matched on the span-stripped view, so quoting one never activates it.
      const markerView = stripCodeSpans(line);
      const opensBlock = BLOCK_BEGIN.test(markerView);
      const closesBlock = BLOCK_END.test(markerView);
      if (opensBlock && closesBlock) {
        // A line carrying BOTH is discussing the markers, not using them: no state change. Its own
        // refs are still spared by LINE_OPT_OUT below, same as any single unquoted mention.
      } else if (opensBlock) { exemptBlock = true; continue; }
      else if (closesBlock) { exemptBlock = false; continue; }
      if (exemptBlock) continue;
      if (NOT_A_REF.some((re) => re.test(line))) continue;
      if (LINE_OPT_OUT.test(markerView)) continue;

      for (const m of stripQualified(line).matchAll(/(?<![\w\/#])#(\d+)\b/g)) {
        const n = Number(m[1]);
        if (n < limit) {
          out.push({ file: f.filename, ref: `#${n}`, text: line.trim().slice(0, 120) });
        }
      }
    }
  }
  return out;
}

// What a PR-body hit is attributed to in the failure listing, where every other row is a file path.
// Angle brackets cannot occur in a path, so this can neither collide with a real file nor be caught
// by EXEMPT_PATHS, and an author reading `<PR body>: #857` knows at once that no file is at fault.
const PR_BODY_LABEL = "<PR body>";

// ``` or ~~~, optionally indented up to three spaces, per CommonMark. Group 1 is the delimiter run,
// group 2 the rest of the line: the info string on an opening fence, and on a closing fence the
// thing CommonMark does not permit at all.
const FENCE = /^ {0,3}((?:`{3,})|(?:~{3,}))(.*)$/;

/**
 * Presents a PR body to suspectRefs as if it were a diff, so the body is judged by the SAME rule as
 * every file - NO SECOND COPY OF THE RULE. This is an adapter, not a rule: it decides which text is
 * in scope, and suspectRefs still decides, alone, whether a piece of text is a violation.
 *
 * Two adaptations are needed, and both are about presentation rather than judgement:
 *
 * 1. EVERY LINE IS AN "ADDED" LINE. suspectRefs reads only `+` lines, because in a diff that is what
 *    this PR is responsible for; the whole body is what this PR is responsible for. The prefix is
 *    `"+ "` and not `"+"` so that a body line beginning `++` cannot become `+++`, which suspectRefs
 *    skips as a diff file header - a one-character hole an author could otherwise fall into (or hide
 *    in) without ever knowing it existed. The extra space is invisible downstream: hits are trimmed
 *    before display. Lines starting `-` need no such care; they are only diff markers in a diff.
 *
 * 2. FENCED CODE BLOCKS ARE DROPPED. GitHub does not autolink inside a fence, so `#123` there is
 *    literal text and cannot become the wrong link - the same reasoning by which stripQualified
 *    already drops code spans. Bodies routinely quote logs, `gh` output and this gate's own failure
 *    message, all of which are full of bare numbers. This lives here rather than in stripQualified
 *    because being fenced is a property of a whole document: a patch shows disconnected fragments,
 *    so no line-at-a-time rule over a diff can know. An UNCLOSED fence swallows the rest of the
 *    body, which is precisely how GitHub renders it. The open/close rules are CommonMark-exact
 *    rather than approximate - see the two comments in the loop for which direction each one
 *    fails in, and why "close enough" is not good enough for a rule whose entire premise is that a
 *    plausible-looking wrong answer is worse than an obviously wrong one.
 *
 * @param body  the PR body, possibly null
 * @returns { filename, patch } - one more entry for the files array suspectRefs already takes
 */
function prBodyEntry(body) {
  if (!body) return { filename: PR_BODY_LABEL, patch: "" };

  let fence = null;
  const lines = body.split(/\r?\n/).map((line) => {
    const m = line.match(FENCE);
    if (fence) {
      // A closing fence is the same character, at least as long as the opening run, and CommonMark
      // allows it to "be followed only by spaces or tabs". So "``` see the note" does NOT close the
      // block: GitHub goes on rendering the following lines as code, where nothing auto-links.
      // Treating it as a closer resumed scanning early and flagged text no reader ever sees as a
      // link - plausible, wrong, and quiet, which is the shape of failure this gate exists to end.
      if (m && m[1][0] === fence[0] && m[1].length >= fence.length && /^[ \t]*$/.test(m[2])) {
        fence = null;
      }
      return "+";
    }
    // The opening side has its own CommonMark rule, and it is the side that matters more: a BACKTICK
    // fence may not carry a backtick in its info string, so "```a`b" opens nothing. Accepting it
    // would drop the lines after it - and those lines DO auto-link on GitHub, so the miss would be
    // silent. Erring toward "not a fence" here is erring toward flagging, which is the safe way
    // round. A tilde fence has no such restriction.
    if (m && !(m[1][0] === "`" && m[2].includes("`"))) {
      fence = m[1];
      // A fence CLOSES any open exempt block. The fence's own contents are dropped wholesale
      // below, so a real exempt-end placed inside one would be blanked before suspectRefs ever
      // saw it - the block would never close and every ref in the rest of the body would silently
      // escape (fail-open). Emitting a synthetic end marker at the fence boundary flips that to
      // fail-closed: a block the author believed spanned a fence gets flagged, seen, and fixed.
      // With no block open, a stray end marker is a no-op by construction.
      return "+ issue-refs: exempt-end";
    }
    return "+ " + line;
  });

  return { filename: PR_BODY_LABEL, patch: lines.join("\n") };
}

/**
 * The single copy of what an author is told when the gate fires. Both callers render this - the CI
 * job in pr-checklist.yml and the local bin/check-issue-refs.sh - so the two cannot tell different
 * stories. They did exactly that once: hand-written copies disagreed in *both* directions within
 * hours of the second one being created, each carrying a correction the other lacked. The script's
 * own header already says "NO SECOND COPY OF THE RULE" about the matching logic; the message an
 * author actually reads is part of that rule.
 *
 * @param hits  [{ file, ref, text }] from suspectRefs
 * @param opts  { repo }        owner/name used in the mirror-lookup hint
 *              { readsPrBody } false when the caller could not read a PR body this run (no PR
 *                              yet, or gh unavailable): it could neither honour the
 *                              "issue-refs: N/A" opt-out nor scan the body for references
 * @returns string
 */
function formatFailure(hits, opts = {}) {
  const repo = opts.repo ?? "astubbs/parallel-consumer";
  const optOutTail = opts.readsPrBody === false
    ? "line in the PR body - the workflow honours that opt-out, and also scans the body itself;\n" +
      "this run could not read the PR body (no PR yet, or gh unavailable), so it did neither."
    : "line in the PR body.";

  // The advice above is right for a file and incomplete for the body, because the body is rendered
  // by GitHub: `astubbs#NN` satisfies this gate but is NOT cross-reference syntax, so it renders as
  // plain text and the body silently loses the link it had. An author told only "name the repo"
  // trades a wrong link for no link, which is not the trade this rule is asking for.
  const bodyNote = hits.some((h) => h.file === PR_BODY_LABEL)
    ? "\nIn the PR BODY, use the FULLY qualified form - `astubbs/parallel-consumer#NN` or\n" +
      "`confluentinc/parallel-consumer#NN`. It is the only form that both names the repo and still\n" +
      "auto-links on GitHub; `astubbs#NN` passes this gate but renders as plain text there. The same\n" +
      "goes for closing keywords: `Fixes astubbs#NN` closes nothing.\n"
    : "";

  // The guidance is repeated in one line AFTER the hits, because the hits are what survives
  // truncation: agents and humans alike read a failing tool's tail (or pipe it through
  // `tail`/`head`), so advice printed only above the list is advice that is never seen. On
  // 2026-08-17 an agent hand-qualified 25 refs, some inside quotations, without ever learning the
  // opt-out existed - the header had said so all along, 23 hits earlier.
  return (
    `${hits.length} reference(s) below #${QUALIFY_BELOW} do not say which repo they mean.\n` +
    "The fork's numbers sit inside confluentinc's range, so a bare number there is a coin flip.\n" +
    "Write `astubbs#NN` for this repo or `confluentinc#NN` for the original - or link it.\n" +
    "(`upstream #NN` is no longer accepted: it names a role, not a repo, and this fork is itself\n" +
    "an upstream to anyone who forks it. Use `confluentinc#NN`.)\n" +
    "Every confluentinc issue is mirrored here, and the mirror is usually the better number to cite:\n" +
    `  gh issue list -R ${repo} --label upstream-mirror --search "confluentinc#NN"\n` +
    "A ref that genuinely needs no qualifier (e.g. quoted source material) takes an IN-FILE opt-out,\n" +
    "written in the file's own comment syntax (`// ...` in code, `<!-- ... -->` in markdown/HTML,\n" +
    "invisible when rendered): `issue-refs: exempt` on the line itself, `issue-refs: exempt-begin` /\n" +
    "`issue-refs: exempt-end` around a pasted block, or `issue-refs: exempt-file` anywhere in a file\n" +
    "that is legitimately full of them. These travel with the file, so they keep holding in future\n" +
    "PRs and local runs. A marker inside a backtick code span (as written here) is a mention, not a\n" +
    "use - quoting a marker never activates it.\n" +
    'To opt the WHOLE PR out instead, put "issue-refs: N/A - <reason>" on its own\n' +
    `${optOutTail}\n` +
    bodyNote +
    "\n" +
    hits.map((h) => `  ${h.file}: ${h.ref}  ${h.text}`).join("\n") +
    "\n\n" +
    "Fix: qualify each ref (`astubbs#NN` / `confluentinc#NN`). Refs that must stay bare (quoted\n" +
    "source material): in a comment, `issue-refs: exempt` on the line, `issue-refs: exempt-begin`/\n" +
    "`-end` around a block, `issue-refs: exempt-file` for a whole file. Whole-PR opt-out:\n" +
    '"issue-refs: N/A - <reason>" in the PR body. Full guidance above the list.'
  );
}

module.exports = {
  suspectRefs, findOptOut, hasFileOptOut, dropFileOptedOutHits, isExempt, stripQualified,
  formatFailure, prBodyEntry,
  EXEMPT_PATHS, QUALIFY_BELOW, PR_BODY_LABEL,
};
