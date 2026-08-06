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
    .replace(/https?:\/\/\S+/g, " ")            // bare URLs
    .replace(/`[^`]*`/g, " ")                   // code spans
    // Owner-qualified prose form, the house standard: astubbs#209 / confluentinc#857. The spaced
    // variants carry the issue-vs-PR distinction where it matters: "confluentinc PR #548".
    .replace(/\b(?:astubbs|confluentinc)\s*(?:PR\s+|issue\s+)?#\d+/gi, " ")
    // "upstream #N" and its variants - accepted, but astubbs/confluentinc is preferred: "upstream"
    // names a role rather than a repo, and this repo is itself an upstream to anyone forking it.
    .replace(/\bupstream\s+(?:PR\s+|issue\s+)?#\d+/gi, " ")
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

module.exports = {
  suspectRefs, findOptOut, isExempt, stripQualified, EXEMPT_PATHS, QUALIFY_BELOW,
};
