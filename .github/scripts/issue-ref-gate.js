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
];

// Constructs that look like a ref but are not one.
const NOT_A_REF = [
  /\{@link\s+#\w/,        // javadoc member link: {@link #close()}
  /\{@linkplain\s+#\w/,
  /#\w+\(/,               // method reference: #poll(
];

const OPT_OUT = /^\s*issue-refs:\s*N\/?A\b\s*-\s*\S[^\n]*/im;

function findOptOut(prBody) {
  return OPT_OUT.test(prBody || "");
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
    for (const raw of f.patch.split("\n")) {
      if (!raw.startsWith("+") || raw.startsWith("+++")) continue;
      const line = raw.slice(1);
      if (NOT_A_REF.some((re) => re.test(line))) continue;

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
 *              { readsPrBody } false when the caller cannot honour the "issue-refs: N/A" opt-out
 * @returns string
 */
function formatFailure(hits, opts = {}) {
  const repo = opts.repo ?? "astubbs/parallel-consumer";
  const optOutTail = opts.readsPrBody === false
    ? "line in the PR body - the workflow honours that opt-out; this script does not read the PR body."
    : "line in the PR body.";

  return (
    `${hits.length} reference(s) below #${QUALIFY_BELOW} do not say which repo they mean.\n` +
    "The fork's numbers sit inside confluentinc's range, so a bare number there is a coin flip.\n" +
    "Write `astubbs#NN` for this repo or `confluentinc#NN` for the original - or link it.\n" +
    "(`upstream #NN` is no longer accepted: it names a role, not a repo, and this fork is itself\n" +
    "an upstream to anyone who forks it. Use `confluentinc#NN`.)\n" +
    "Every confluentinc issue is mirrored here, and the mirror is usually the better number to cite:\n" +
    `  gh issue list -R ${repo} --label upstream-mirror --search "confluentinc#NN"\n` +
    'If a flagged reference genuinely needs no qualifier, put "issue-refs: N/A - <reason>" on its own\n' +
    `${optOutTail}\n\n` +
    hits.map((h) => `  ${h.file}: ${h.ref}  ${h.text}`).join("\n")
  );
}

module.exports = {
  suspectRefs, findOptOut, isExempt, stripQualified, formatFailure, EXEMPT_PATHS, QUALIFY_BELOW,
};
