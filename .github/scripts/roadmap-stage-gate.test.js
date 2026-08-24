// Copyright (C) 2026 Antony Stubbs and contributors

// Unit tests for roadmap-stage-gate.js. Run by the PR Checklist job before the gate itself, so a
// broken rule fails loudly rather than silently passing - or failing - every PR.
// issue-refs: exempt-file - fixtures and check names are deliberately full of bare PR numbers;
// qualifying them would stop testing what the parser actually sees.
const assert = require("assert");
const {
  ROADMAP_PATH, findOptOut, entriesClaimingPr, entryStageSnapshot, stageMoved, formatFailure,
} = require("./roadmap-stage-gate.js");

let run = 0;
function check(name, fn) {
  fn();
  run++;
  console.log("  ok  " + name);
}

// A roadmap fragment in the file's real shape: flat entries, two of them carriers.
const ROADMAP = [
  "entries:",
  "  - id: web-gui",
  "    title: A web view showing what a running instance is actually doing",
  "    stage: limited-poc",
  "    stage_delivery: draft",
  "    stage_detail: >-",
  "      Phase one works and is tested - live page, offset ribbon - but it is a",
  "      brand-new surface nobody has operated against.",
  "    tracking: astubbs#215",
  "    pull_request: astubbs#268",
  "",
  "  - id: language-proxy-sidecar",
  "    stage: limited-poc",
  "    pull_request: astubbs#293",
  "",
  "  - id: distributed-throttling",
  "    stage: ideated",
  "    tracking: astubbs#228",
].join("\n");

check("finds the entry that names the PR", () => {
  assert.deepStrictEqual(entriesClaimingPr(ROADMAP, 268), [
    { id: "web-gui", ref: "astubbs#268" },
  ]);
});

check("a PR nothing names produces no claims", () => {
  assert.deepStrictEqual(entriesClaimingPr(ROADMAP, 999), []);
});

check("an entry with only a tracking issue is out of reach by design", () => {
  // astubbs#228 is distributed-throttling's tracking issue, not a pull_request - no claim.
  assert.deepStrictEqual(entriesClaimingPr(ROADMAP, 228), []);
});

check("the number must match exactly - #26 is not #268", () => {
  assert.deepStrictEqual(entriesClaimingPr(ROADMAP, 26), []);
});

check("only the fork's own qualified form claims - confluentinc#268 is another repo's PR", () => {
  const upstream = ROADMAP.replace("pull_request: astubbs#268", "pull_request: confluentinc#268");
  assert.deepStrictEqual(entriesClaimingPr(upstream, 268), []);
});

check("string and numeric PR numbers agree", () => {
  assert.strictEqual(entriesClaimingPr(ROADMAP, "293").length, 1);
});

check("empty or missing roadmap text claims nothing", () => {
  assert.deepStrictEqual(entriesClaimingPr("", 268), []);
  assert.deepStrictEqual(entriesClaimingPr(null, 268), []);
});

check("the stage snapshot captures stage fields with their folded continuations, nothing else", () => {
  const snap = entryStageSnapshot(ROADMAP, "web-gui");
  assert.ok(snap.includes("stage: limited-poc"));
  assert.ok(snap.includes("stage_delivery: draft"));
  assert.ok(snap.includes("brand-new surface"));
  assert.ok(!snap.includes("tracking"));
  assert.ok(!snap.includes("title"));
});

check("a missing entry snapshots to null, not empty", () => {
  assert.strictEqual(entryStageSnapshot(ROADMAP, "no-such-entry"), null);
});

check("an untouched file means the entry's stage did not move", () => {
  assert.strictEqual(stageMoved(ROADMAP, ROADMAP, "web-gui"), false);
});

check("editing a DIFFERENT entry does not count as moving this one - the coarse-gate hole", () => {
  const otherEdit = ROADMAP.replace("stage: ideated", "stage: planned"); // distributed-throttling
  assert.strictEqual(stageMoved(ROADMAP, otherEdit, "web-gui"), false);
  assert.strictEqual(stageMoved(ROADMAP, otherEdit, "distributed-throttling"), true);
});

check("a stage value change moves the entry", () => {
  const bumped = ROADMAP.replace("stage: limited-poc\n    stage_delivery: draft",
    "stage: poc\n    stage_delivery: pending-merge");
  assert.strictEqual(stageMoved(ROADMAP, bumped, "web-gui"), true);
});

check("a stage_detail rewording alone also counts as movement", () => {
  const reworded = ROADMAP.replace("brand-new surface nobody has operated against.",
    "operated in production by one team since 0.6.0.0.");
  assert.strictEqual(stageMoved(ROADMAP, reworded, "web-gui"), true);
});

check("an entry removed or renamed at head counts as moved - the PR answers for it", () => {
  const removed = ROADMAP.replace("  - id: web-gui", "  - id: web-gui-renamed");
  assert.strictEqual(stageMoved(ROADMAP, removed, "web-gui"), true);
});

check("opt-out needs a reason - a bare N/A is a bypass, not a judgment", () => {
  assert.ok(findOptOut("roadmap-stage: N/A - mid-flight fixup, stage moves with the final PR"));
  assert.ok(findOptOut("body text\nroadmap-stage: NA - carrier unchanged\nmore"));
  assert.strictEqual(findOptOut("roadmap-stage: N/A"), null);
  assert.strictEqual(findOptOut("roadmap-stage: N/A -"), null);
  assert.strictEqual(findOptOut("no marker here"), null);
  assert.strictEqual(findOptOut(""), null);
  assert.strictEqual(findOptOut(null), null);
});

check("the failure message names the entry, the file, and both ways out", () => {
  const msg = formatFailure([{ id: "web-gui", ref: "astubbs#268" }], 268);
  assert.ok(msg.includes("web-gui"));
  assert.ok(msg.includes(ROADMAP_PATH));
  assert.ok(msg.includes("roadmap-stage: N/A"));
  assert.ok(msg.includes("stage_delivery"));
  assert.ok(msg.includes("unchanged"));
});

check("the gate finds today's real carriers in the real roadmap", () => {
  // Pins the parse to the actual file, so a structural rewrite of roadmap.yaml fails here first
  // rather than silently blinding the gate.
  const fs = require("fs");
  const path = require("path");
  const real = fs.readFileSync(path.join(__dirname, "../../", ROADMAP_PATH), "utf8");
  const claims = entriesClaimingPr(real, 293);
  assert.deepStrictEqual(claims, [{ id: "language-proxy-sidecar", ref: "astubbs#293" }]);
  const snap = entryStageSnapshot(real, "language-proxy-sidecar");
  assert.ok(snap && snap.includes("stage:"));
});

console.log(`roadmap-stage-gate: ${run} checks passed`);
