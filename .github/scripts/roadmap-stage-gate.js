// Makes the roadmap's stage ladder self-updating: when a PR is the carrier of a roadmap entry's
// artifact, merging it changes how real that entry is - so the same PR must move the entry's
// stage/stage_delivery, or say why not.
//
// docs/data/roadmap.yaml owns the rule (the `stages` block: "a PR that advances a track updates
// its entry's stage in the same change"); this gate is what catches a miss. Without it the rule is
// only documentation, and the stage field rots into exactly the stale second tracker the roadmap's
// own update_policy warns about.
//
// MECHANICS: the roadmap names its carriers - entries carry `pull_request: astubbs#NNN`. The gate
// reads the BASE branch's roadmap (the authority on what master currently claims), finds entries
// naming this PR, and requires the PR to either touch docs/data/roadmap.yaml or opt out with
// `roadmap-stage: N/A - <reason>` on its own line in the PR body (same convention as
// `changelog-ref: N/A` / `issue-refs: N/A`). Entries without a pull_request field are out of this
// gate's reach by design - a tracking issue is not a mergeable event.
//
// THE PARSE IS DELIBERATELY NARROW. No YAML library ships with the runner and the sibling gates
// are dependency-free, so this scans the two lines it needs: `- id:` opens an entry block,
// `pull_request:` inside it names the carrier. That holds for the file's actual shape (flat
// entries under `entries:`), and the unit tests pin it; a structural rewrite of roadmap.yaml
// should expect to revisit this.
//
// Pulled out of the workflow so it can be unit tested; roadmap-stage-gate.test.js runs first in CI.

const ROADMAP_PATH = "docs/data/roadmap.yaml";

const OPT_OUT = /^\s*roadmap-stage:\s*N\/?A\b\s*-\s*\S[^\n]*/im;

// Returns the opt-out line from a PR body, or null. The reason is mandatory: a bare "N/A" is not
// a judgment, it is a bypass.
function findOptOut(body) {
  const m = (body || "").match(OPT_OUT);
  return m ? m[0].trim() : null;
}

// Scans roadmap YAML text for entries whose pull_request names the given PR number.
// Returns [{id, ref}] - id for the failure message, ref as written in the file.
function entriesClaimingPr(roadmapText, prNumber) {
  const claims = [];
  let currentId = null;
  for (const line of (roadmapText || "").split(/\r?\n/)) {
    const id = line.match(/^\s*-\s*id:\s*(\S+)\s*$/);
    if (id) {
      currentId = id[1];
      continue;
    }
    const pr = line.match(/^\s*pull_request:\s*(\S+)\s*$/);
    if (pr && currentId) {
      const num = pr[1].match(/#(\d+)$/);
      if (num && Number(num[1]) === Number(prNumber)) {
        claims.push({ id: currentId, ref: pr[1] });
      }
    }
  }
  return claims;
}

// True when the PR's changed files include the roadmap.
function touchesRoadmap(files) {
  return (files || []).some((f) => f.filename === ROADMAP_PATH);
}

function formatFailure(claims, prNumber) {
  return (
    `This PR is the carrier of ${claims.length === 1 ? "a roadmap entry" : "roadmap entries"} - ` +
    `merging it changes how real ${claims.length === 1 ? "that entry is" : "they are"}, so the same ` +
    `PR must move the stage:\n` +
    claims.map((c) => `  - entry '${c.id}' names this PR (${c.ref}) in ${ROADMAP_PATH}`).join("\n") +
    `\n\nUpdate the entry's stage/stage_detail (and stage_delivery) in ${ROADMAP_PATH} in this PR - ` +
    `the 'stages' block there defines the ladder. If the stage genuinely does not move ` +
    `(a mid-flight fixup, say), put "roadmap-stage: N/A - <reason>" on its own line in the PR body.`
  );
}

module.exports = { ROADMAP_PATH, findOptOut, entriesClaimingPr, touchesRoadmap, formatFailure };
