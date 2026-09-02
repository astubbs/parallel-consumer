// Copyright (C) 2026 Antony Stubbs and contributors

// The quarantine lane's PR comment: what its payload MEANS, and how a change in it reads.
// `.github/workflows/quarantine-lane.yml` requires this and calls `post`; the generic sticky-comment
// mechanics live next door in sticky-report-comment.js, which this only configures.
//
// THE SPLIT IS THE POINT. Everything about finding, updating, retiring and stamping a comment is the
// same for every report and lives in one module. Everything about what a quarantine outcome is, and
// which transition a reader must be told about, is here - and it is in a module rather than inline in
// the workflow YAML because an inline `renderDelta` cannot be tested, and the transition it announces
// is the whole reason this report changed:
//
//   A DETERMINISTIC QUARANTINED TEST THAT STARTS PASSING MEANS ITS FIX LANDED. Its `@Quarantined`
//   annotation and its `docs/quarantined-tests.md` entry are now both wrong, and the registry gate
//   will not say so - a quarantined test is allowed to pass. Until this change, that transition
//   happened by silently editing a comment nobody was looking at.
//
// The producer is bin/quarantine-lane-report.sh, which writes the report into `target/` with a
// `quarantine-lane-data` payload. The two must agree on the marker's name and on the outcome
// vocabulary; quarantine-report-comment.test.js's END TO END case runs the real script and reads its
// real output back through here, so a rename on either side fails there rather than in production.
// `grep -rn quarantine-lane-data` and `grep -rn PASSED_ACTION` are still the lists to change.

"use strict";

const sticky = require("./sticky-report-comment.js");

const MARKER = "<!-- quarantine-lane-report -->";
// Its superseded twin, spelled out rather than derived - see sticky-report-comment.js.
const SUPERSEDED_MARKER = "<!-- quarantine-lane-report (superseded) -->";
const DATA_MARKER = "quarantine-lane-data";
// The `status` bin/quarantine-lane-report.sh writes when the registry has no entries at all. WRITTEN
// THERE AND READ HERE, with nothing enforcing the pair but this module's end-to-end self-test, which
// drives the real script against an empty registry - `grep -rn EMPTY_STATUS` is the list to change.
const EMPTY_STATUS = "empty";

// The four outcomes the reporter can record, in the reader's words. PASSED is split in two on
// purpose: a flapping test passing proves nothing, a deterministic one passing demands action, and a
// row moving between those two says something a reader needs. Collapsing them would make an
// annotation gaining `flapping = true` invisible here.
const LABELS = {
  FAILED: "🔴 failing",
  PASSED_ACTION: "🚨✅ passed - ACTION REQUIRED",
  PASSED_FLAPPER: "🟡🎲 passed (flapper)",
  NOT_RUN: "⚪ not run",
};

/** An outcome in the words the report's own table uses; unknown values pass through verbatim. */
function label(outcome) {
  return LABELS[outcome] ?? outcome;
}

/**
 * What moved since the previous push, as one line of prose.
 *
 * IT PRINTS EVEN WHEN NOTHING MOVED, unlike the throughput delta. "Did my push change anything?" is
 * the question people actually bring to this comment, and a silent delta cannot be told apart from a
 * delta that could not be computed. The one case it stays silent for is a genuinely absent payload on
 * either side - there, nothing is known, and saying "nothing changed" would be a claim.
 *
 * Rows entering and leaving the lane are reported too: a PR that quarantines a test, or deletes a
 * quarantine, has changed what this report is about.
 */
function renderDelta(prev, cur) {
  if (!prev?.outcomes || !cur?.outcomes) return "";
  const names = [...new Set([...Object.keys(prev.outcomes), ...Object.keys(cur.outcomes)])].sort();
  const bits = [];
  for (const name of names) {
    const before = prev.outcomes[name];
    const after = cur.outcomes[name];
    if (before === after) continue;
    if (!before) bits.push(`\`${name}\` entered the lane as ${label(after)}`);
    else if (!after) bits.push(`\`${name}\` left the lane (was ${label(before)})`);
    else bits.push(`\`${name}\`: ${label(before)} → ${label(after)}`);
  }
  if (!bits.length) return "\n\n_No quarantined test changed outcome since the previous push._";
  return `\n\n_Since the previous push: ${bits.join("; ")}._`;
}

/** Whether this run's payload is the lane-emptied one: the registry has no entries at all. */
function laneEmptied(payload) {
  return payload?.status === EMPTY_STATUS && Object.keys(payload.outcomes ?? {}).length === 0;
}

/**
 * Post or update the lane's sticky comment for this run.
 *
 * `body` is the file bin/quarantine-lane-report.sh wrote. The marker is UNCHANGED from the one that
 * script used to post under itself, so comments already live on open PRs are found and updated rather
 * than orphaned beside a fresh one.
 *
 * THE EMPTIED LANE IS A RETRACTION, AND IT IS TREATED AS ONE IN TWO PLACES.
 *
 * It POSTS FRESH rather than editing in place, because the same machinery already decides that: an
 * outcome digest of `empty` differs from any previous one, so the status changed. That is the right
 * answer here and not merely the incidental one. The comment being retracted told a reader to go and
 * delete an annotation and a registry entry, and it NOTIFIED them when it said so; withdrawing it by
 * silently rewriting a comment thirty scrolls up is exactly the failure astubbs/parallel-consumer#409
 * removed, and it would land on the one PR where the instruction has already been carried out. The
 * old comment is retired the way every superseded one is - live marker renamed, heading prefixed,
 * forward link - so a reader who arrives at the merged PR and lands on it is told it is stale rather
 * than being sent to delete something that is gone.
 *
 * It STAYS SILENT when there is nothing to retract. An empty lane is the healthy steady state, and a
 * comment on every PR announcing that nothing is quarantined is noise the stickiness exists to
 * prevent. `postWhenAbsent: false` is the one knob that distinguishes the two.
 */
async function post({ github, context, core, body, now }) {
  const emptied = laneEmptied(sticky.readPayload(body, DATA_MARKER));
  return sticky.postStickyReport({
    github, context, core,
    marker: MARKER,
    supersededMarker: SUPERSEDED_MARKER,
    dataMarker: DATA_MARKER,
    body,
    renderDelta,
    // This report's own heading is `## `, where the throughput report's is `### `.
    headingRe: /^## /m,
    // Not the generic "status changed to <digest>": the digest is every test's outcome joined, which
    // is unreadable in a heading and says nothing a reader can act on. The emptied lane gets its own
    // wording because the retired comment's HEADING is all a reader sees before deciding whether to
    // act on its body - "a quarantined test changed outcome" would leave them reading an ACTION
    // REQUIRED table to find out it no longer applies.
    supersededLabel: () => emptied ? "the quarantine lane is now empty" : "a quarantined test changed outcome",
    what: "quarantine lane report",
    postWhenAbsent: !emptied,
    ...(now ? { now } : {}),
  });
}

module.exports = { MARKER, DATA_MARKER, EMPTY_STATUS, LABELS, label, renderDelta, laneEmptied, post };
