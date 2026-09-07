// Copyright (C) 2026 Antony Stubbs and contributors

// The throughput report's PR comment: what its payload MEANS, and how a change in it reads.
// `.github/workflows/maven.yml` requires this and calls `post`; the generic sticky-comment mechanics
// live next door in sticky-report-comment.js, which this only configures.
//
// Extracted from that workflow, unchanged in behaviour, when the quarantine lane needed the same
// mechanics - see sticky-report-comment.js's header for why the mechanics are shared rather than
// copied. What is here is the half that is throughput's alone, and it is in a module rather than
// inline in YAML for one reason: inline JavaScript in a workflow cannot be tested, and this is the
// text a reviewer reads to decide whether their branch got slower.
//
// The producer is bin/check-throughput-regression.mjs, which writes target/throughput-report.md with
// a `pc-throughput-data` payload. Nothing enforces that the two agree on the marker's name, so
// `grep -rn pc-throughput-data` is the list to change if it moves. Tests:
// throughput-report-comment.test.js.

"use strict";

const sticky = require("./sticky-report-comment.js");
const { sanitiseForHeading } = sticky;

const MARKER = "<!-- pc-throughput-report -->";
// Spelled out rather than left to sticky-report-comment.js's default. The default derives the same
// string, and comments carrying this exact retired marker are already live on open PRs - pinning it
// means a change to that derivation cannot orphan them.
const SUPERSEDED_MARKER = "<!-- pc-throughput-report (superseded) -->";
const DATA_MARKER = "pc-throughput-data";

/**
 * What moved since the previous push, as one line of prose.
 *
 * A DELTA, NEVER A VERDICT. This test's run-to-run spread is wide enough that a single-push
 * difference is noise far more often than it is a change, and the sentence says so. It is shown at
 * all because an in-place update otherwise destroys the only record of what the previous push
 * measured.
 *
 * ORDERED AS THE REPORT'S OWN TABLE ORDERS THEM, most machine-independent first, with the rate LAST
 * because it is the one number the report itself labels "this machine only". `share` was the omission
 * worth fixing: it is the raw dimensionless shape the whole method rests on, it was already in the
 * payload, and leaving it out meant the delta showed two derived numbers and skipped the measurement
 * they come from.
 */
function renderDelta(prev, cur) {
  if (!prev || !cur) return "";
  const bits = [];
  if (prev.status !== cur.status) {
      // BOTH sides sanitised. `cur.status` went through the shared helper and `prev.status`
      // did not - and prev comes from a previous COMMENT body, which is the untrusted half.
      bits.push(`status **${sanitiseForHeading(prev.status)} -> ${sanitiseForHeading(cur.status)}**`)
    };
  if (typeof prev.ratio === "number" && typeof cur.ratio === "number") {
    bits.push(`ratio ${prev.ratio} -> ${cur.ratio}`);
  }
  if (typeof prev.share === "number" && typeof cur.share === "number") {
    bits.push(`share ${prev.share} -> ${cur.share}`);
  }
  if (typeof prev.rate === "number" && typeof cur.rate === "number" && prev.rate > 0) {
    const pc = ((cur.rate - prev.rate) / prev.rate) * 100;
    bits.push(`rate ${prev.rate} -> ${cur.rate} (${pc >= 0 ? "+" : ""}${pc.toFixed(1)}%)`);
  }
  if (!bits.length) return "";
  return `\n\n_Since the previous push: ${bits.join(", ")}. One push of difference sits inside this`
    + ` test's measured spread - read it as movement, not as a result._`;
}

/** Post or update the throughput report's sticky comment for this run. */
async function post({ github, context, core, body, now }) {
  return sticky.postStickyReport({
    github, context, core,
    marker: MARKER,
    supersededMarker: SUPERSEDED_MARKER,
    dataMarker: DATA_MARKER,
    body,
    renderDelta,
    what: "throughput report",
    ...(now ? { now } : {}),
  });
}

module.exports = { MARKER, SUPERSEDED_MARKER, DATA_MARKER, renderDelta, post };
