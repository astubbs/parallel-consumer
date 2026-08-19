// Unit tests for roadmap-stage-gate.js. Run by the PR Checklist job before the gate itself, so a
// broken rule fails loudly rather than silently passing - or failing - every PR.
// issue-refs: exempt-file - fixtures and check names are deliberately full of bare PR numbers;
// qualifying them would stop testing what the parser actually sees.
const assert = require("assert");
const {
  ROADMAP_PATH, findOptOut, entriesClaimingPr, touchesRoadmap, formatFailure,
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

check("string and numeric PR numbers agree", () => {
  assert.strictEqual(entriesClaimingPr(ROADMAP, "293").length, 1);
});

check("empty or missing roadmap text claims nothing", () => {
  assert.deepStrictEqual(entriesClaimingPr("", 268), []);
  assert.deepStrictEqual(entriesClaimingPr(null, 268), []);
});

check("touchesRoadmap matches only the exact path", () => {
  assert.ok(touchesRoadmap([{ filename: ROADMAP_PATH }]));
  assert.ok(!touchesRoadmap([{ filename: "docs/data/schema.yaml" }]));
  assert.ok(!touchesRoadmap([]));
  assert.ok(!touchesRoadmap(null));
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
});

check("the gate finds today's real carriers in the real roadmap", () => {
  // Pins the parse to the actual file, so a structural rewrite of roadmap.yaml fails here first
  // rather than silently blinding the gate.
  const fs = require("fs");
  const path = require("path");
  const real = fs.readFileSync(path.join(__dirname, "../../", ROADMAP_PATH), "utf8");
  const claims = entriesClaimingPr(real, 293);
  assert.deepStrictEqual(claims, [{ id: "language-proxy-sidecar", ref: "astubbs#293" }]);
});

console.log(`roadmap-stage-gate: ${run} checks passed`);
