// Copyright (C) 2026 Antony Stubbs and contributors

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
// naming this PR, and requires the CLAIMING ENTRY's stage block (stage / stage_delivery /
// stage_detail) to differ between the base and head versions of the file - merely touching the
// file elsewhere does not count, because a wording pass on entry B says nothing about entry A's
// stage. The escape hatch is `roadmap-stage: N/A - <reason>` on its own line in the PR body (same
// convention as `changelog-ref: N/A` / `issue-refs: N/A`). Entries without a pull_request field
// are out of this gate's reach by design - a tracking issue is not a mergeable event.
//
// THE PARSE IS DELIBERATELY NARROW. No YAML library ships with the runner and the sibling gates
// are dependency-free, so this scans the lines it needs: `- id:` opens an entry block, and inside
// it `pull_request:` names the carrier while `stage*` fields (with their folded continuations)
// form the compared block. That holds for the file's actual shape (flat entries under `entries:`),
// and the unit tests pin it - one against the real file - so a structural rewrite of roadmap.yaml
// fails the tests rather than silently blinding the gate.
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
// Only the fork's own qualified form counts: `astubbs#NNN`. A confluentinc#NNN (or any other
// prefix) names a different repository's PR, so a coinciding number must never claim this one.
// Returns [{id, ref}] - id for the failure message and stage comparison, ref as written.
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
      const num = pr[1].match(/^astubbs#(\d+)$/);
      if (num && Number(num[1]) === Number(prNumber)) {
        claims.push({ id: currentId, ref: pr[1] });
      }
    }
  }
  return claims;
}

// Returns the entry's stage block - its stage / stage_delivery / stage_detail lines, including
// folded-scalar continuation lines - or null when the entry does not exist in this text.
// Comparing this snapshot between base and head is what "the entry's stage moved" means.
function entryStageSnapshot(roadmapText, id) {
  const lines = (roadmapText || "").split(/\r?\n/);
  let inEntry = false;
  let capturing = false;
  let fieldIndent = 0;
  const out = [];
  for (const line of lines) {
    const idMatch = line.match(/^\s*-\s*id:\s*(\S+)\s*$/);
    if (idMatch) {
      if (inEntry) break; // the next entry starts - done
      inEntry = idMatch[1] === id;
      continue;
    }
    if (!inEntry) continue;
    if (/^\S/.test(line)) break; // dedent to a new top-level key ends the entries list
    const field = line.match(/^(\s*)(stage|stage_delivery|stage_detail):/);
    if (field) {
      out.push(line);
      capturing = true;
      fieldIndent = field[1].length;
      continue;
    }
    if (capturing) {
      const cont = line.match(/^(\s*)\S/);
      if (cont && cont[1].length > fieldIndent) {
        out.push(line); // folded-scalar continuation of the field above
        continue;
      }
      capturing = false;
    }
  }
  return inEntry || out.length > 0 ? out.join("\n") : null;
}

// True when the claiming entry's stage block differs between base and head - including the entry
// disappearing at head (a rename or removal is a change the PR is accountable for describing).
function stageMoved(baseText, headText, id) {
  const before = entryStageSnapshot(baseText, id);
  const after = entryStageSnapshot(headText, id);
  return before !== after;
}

function formatFailure(claims, prNumber) {
  return (
    `This PR is the carrier of ${claims.length === 1 ? "a roadmap entry" : "roadmap entries"} - ` +
    `merging it changes how real ${claims.length === 1 ? "that entry is" : "they are"}, but the ` +
    `entry's stage block is unchanged in this PR:\n` +
    claims.map((c) => `  - entry '${c.id}' names this PR (${c.ref}) in ${ROADMAP_PATH}`).join("\n") +
    `\n\nUpdate that entry's stage/stage_detail (and stage_delivery) in ${ROADMAP_PATH} in this PR - ` +
    `the 'stages' block there defines the ladder, and editing other parts of the file does not ` +
    `count. If the stage genuinely does not move (a mid-flight fixup, say), put ` +
    `"roadmap-stage: N/A - <reason>" on its own line in the PR body.`
  );
}

module.exports = {
  ROADMAP_PATH, findOptOut, entriesClaimingPr, entryStageSnapshot, stageMoved, formatFailure,
};
