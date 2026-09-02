// Copyright (C) 2026 Antony Stubbs and contributors

// Unit tests for quarantine-report-comment.js. Plain node, no dependencies, no runner:
// `node .github/scripts/quarantine-report-comment.test.js`.
//
// THE ASSERTION THIS FILE EXISTS FOR is the last one: a quarantined test going from failing to
// passing must post a NEW comment. That is the transition the whole change is about - a deterministic
// quarantined test that passes means its fix landed, so its `@Quarantined` annotation and its
// `docs/quarantined-tests.md` entry are both wrong - and until now it was announced by silently
// editing a comment thirty scrolls up. Everything above it is there to make that one meaningful.
//
// THE PAYLOADS IN THE FIRST TWO SECTIONS ARE INVENTED, and that is a real limitation rather than a
// convenience: a payload this file writes agrees with the producer by construction, and would keep
// agreeing after bin/quarantine-lane-report.sh changed. The END TO END section at the bottom is the
// answer - it runs the real script and feeds its actual output through this module, so it is the one
// test that fails when the two drift. Both halves are needed: the invented payloads reach cases the
// script cannot easily be driven into, and the end-to-end one proves the vocabulary is shared.

"use strict";

const assert = require("assert");
const { execFileSync } = require("child_process");
const { mkdtempSync, mkdirSync, writeFileSync, rmSync } = require("fs");
const { tmpdir } = require("os");
const { join } = require("path");
const { label, renderDelta, laneEmptied, post, MARKER, DATA_MARKER, EMPTY_STATUS } =
  require("./quarantine-report-comment.js");

// Runner and fakes are shared - see report-comment-test-harness.js for why the shared fake is the
// RICHEST of the three rather than the smallest common one. `fakeGithub` keeps its positional
// signature here so no call site below has to change.
const harness = require("./report-comment-test-harness.js");
const { section, test, asyncTest, runAll } = harness.makeRunner();
const fakeGithub = (comments = []) => harness.fakeGithub({ comments });
const CONTEXT = harness.fakeContext();
const CORE = harness.fakeCore();

// A payload exactly as bin/quarantine-lane-report.sh builds it: the digest is the map, sorted and
// joined, so the two can never disagree here either.
const payloadFor = outcomes => ({
  status: Object.keys(outcomes).sort().map(k => `${k}=${outcomes[k]}`).join(";"),
  outcomes,
});
const bodyFor = outcomes =>
  `## 🧪🔒 Quarantine Lane Report\n\n(table)\n\n<!-- ${DATA_MARKER}: ${JSON.stringify(payloadFor(outcomes))} -->\n`;

// The lane-emptied payload, spelled the way bin/quarantine-lane-report.sh spells it. Invented here
// like the ones above; the END TO END section drives the real script into producing it.
const EMPTIED = { status: EMPTY_STATUS, outcomes: {} };

const A = "AlphaIT.someMethod";
const B = "BravoIT.otherMethod";

// =================================================================================================
section("label - the reader's words for each outcome");

test("failing", () => assert.strictEqual(label("FAILED"), "🔴 failing"));
test("a deterministic pass says ACTION REQUIRED", () =>
  assert.ok(label("PASSED_ACTION").includes("ACTION REQUIRED")));
test("a flapper pass does not", () =>
  assert.ok(!label("PASSED_FLAPPER").includes("ACTION REQUIRED")));
test("an outcome the reporter invents later passes through rather than vanishing", () =>
  assert.strictEqual(label("SOMETHING_NEW"), "SOMETHING_NEW"));

// =================================================================================================
section("\nrenderDelta - what moved, named by test");

test("a test that started passing is named, with both sides of the move", () => {
  const delta = renderDelta(payloadFor({ [A]: "FAILED" }), payloadFor({ [A]: "PASSED_ACTION" }));
  assert.ok(delta.includes(`\`${A}\``), delta);
  assert.ok(delta.includes("🔴 failing"), delta);
  assert.ok(delta.includes("ACTION REQUIRED"), delta);
});

// The flapper/deterministic split is not cosmetic: it is the difference between "delete the
// annotation" and "this proves nothing". Collapsing both to PASSED would make this move invisible.
test("a pass changing KIND is a change", () => {
  const delta = renderDelta(payloadFor({ [A]: "PASSED_FLAPPER" }), payloadFor({ [A]: "PASSED_ACTION" }));
  assert.ok(delta.includes("flapper"), delta);
  assert.ok(delta.includes("ACTION REQUIRED"), delta);
});

test("a test entering the lane is reported", () => {
  const delta = renderDelta(payloadFor({ [A]: "FAILED" }), payloadFor({ [A]: "FAILED", [B]: "FAILED" }));
  assert.ok(delta.includes(`\`${B}\` entered the lane`), delta);
  assert.ok(!delta.includes(`\`${A}\``), `an unchanged row was reported as moved: ${delta}`);
});

test("a test leaving the lane is reported", () => {
  const delta = renderDelta(payloadFor({ [A]: "FAILED", [B]: "FAILED" }), payloadFor({ [A]: "FAILED" }));
  assert.ok(delta.includes(`\`${B}\` left the lane`), delta);
});

// THE BRANCH THAT WAS UNREACHABLE. `left the lane` above needs a SURVIVING row to keep the report
// alive; for the LAST entry the producer used to write no report at all, so the one delta a reader
// most needs - the withdrawal of "delete the annotation and the registry entry" - could never be
// rendered.
test("the LAST test leaving the lane is reported, not swallowed with the report", () => {
  const delta = renderDelta(payloadFor({ [A]: "PASSED_ACTION" }), EMPTIED);
  assert.ok(delta.includes(`\`${A}\` left the lane`), delta);
  assert.ok(delta.includes("ACTION REQUIRED"), `the delta does not say what is being withdrawn: ${delta}`);
});

// UNLIKE THE THROUGHPUT DELTA, THIS ONE SPEAKS WHEN NOTHING MOVED. "Did my push change anything?" is
// the question people bring to this comment, and silence cannot be told apart from a delta that could
// not be computed.
test("no movement says so out loud", () =>
  assert.ok(renderDelta(payloadFor({ [A]: "FAILED" }), payloadFor({ [A]: "FAILED" }))
    .includes("No quarantined test changed outcome"), "a no-op delta was silent"));

// The one case it must stay silent for: nothing is known, so "nothing changed" would be a claim.
test("an absent previous payload is silence, not a claim that nothing changed", () =>
  assert.strictEqual(renderDelta(null, payloadFor({ [A]: "FAILED" })), ""));

test("an absent current payload is silence too", () =>
  assert.strictEqual(renderDelta(payloadFor({ [A]: "FAILED" }), null), ""));

// =================================================================================================
section("\nlaneEmptied - the payload that means there is nothing left to report on");

test("the producer's lane-emptied payload is recognised", () =>
  assert.strictEqual(laneEmptied(EMPTIED), true));

test("a lane with entries has not emptied", () =>
  assert.strictEqual(laneEmptied(payloadFor({ [A]: "FAILED" })), false));

// An unreadable payload is not evidence of anything, and reading it as "emptied" would silence the
// report on a PR that has entries.
test("no payload at all is not evidence the lane emptied", () =>
  assert.strictEqual(laneEmptied(null), false));

// =================================================================================================
section("\npost - the posting decision, which is the point of the whole change");

const existingComment = outcomes => ({
  id: 5, user: { type: "Bot" }, html_url: "https://example.test/c/5",
  body: `${MARKER}\n${bodyFor(outcomes)}`,
});
const run = (gh, outcomes) => post({
  github: gh, context: CONTEXT, core: CORE, body: bodyFor(outcomes), now: new Date("2026-09-02T03:04:05Z"),
});

asyncTest("an unchanged lane updates in place - it does not spam the PR", async () => {
  const gh = fakeGithub([existingComment({ [A]: "FAILED" })]);
  const result = await run(gh, { [A]: "FAILED" });
  assert.strictEqual(result.action, "updated");
  assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 0);
});

// THE ONE THAT MATTERS. A deterministic quarantined test passing means its fix landed; a silent edit
// is how that used to be announced.
asyncTest("failing -> PASSING posts a NEW comment and retires the old one", async () => {
  const gh = fakeGithub([existingComment({ [A]: "FAILED" })]);
  const result = await run(gh, { [A]: "PASSED_ACTION" });
  assert.strictEqual(result.action, "superseded");
  const created = gh.calls.find(c => c.op === "createComment");
  assert.ok(created, "no fresh comment was posted for a failing -> passing transition");
  assert.ok(created.body.includes("ACTION REQUIRED"), "the fresh comment does not say what changed");
  assert.ok(!gh.store.find(c => c.id === 5).body.includes(`${MARKER}\n`),
    "the old comment still carries the live marker - later runs would keep updating it");
});

asyncTest("the retired comment's heading says what changed, not a raw digest", async () => {
  const gh = fakeGithub([existingComment({ [A]: "FAILED" })]);
  await run(gh, { [A]: "PASSED_ACTION" });
  const retired = gh.store.find(c => c.id === 5).body;
  assert.ok(retired.includes("## [superseded - a quarantined test changed outcome]"), retired);
  assert.ok(!retired.includes("=PASSED_ACTION]"), `the digest leaked into the heading: ${retired}`);
  // An outcome change retires a report that was TRUE when written; only the emptied lane retires a
  // wrong one, and only that one is folded away (asserted in the END TO END section below).
  assert.ok(!retired.includes("<details>"), `an older-but-true report was hidden as if it were wrong: ${retired}`);
});

asyncTest("quarantining a test mid-PR is a change, so it announces itself", async () => {
  const gh = fakeGithub([existingComment({ [A]: "FAILED" })]);
  const result = await run(gh, { [A]: "FAILED", [B]: "FAILED" });
  assert.strictEqual(result.action, "superseded");
  assert.ok(gh.calls.find(c => c.op === "createComment").body.includes("entered the lane"));
});

asyncTest("the posted comment carries the stamp, so it is not mistaken for a stale one", async () => {
  const gh = fakeGithub([existingComment({ [A]: "FAILED" })]);
  await run(gh, { [A]: "FAILED" });
  const written = gh.calls.find(c => c.op === "updateComment").body;
  assert.ok(written.includes("`abcdef1`"), written);
  assert.ok(written.includes("2026-09-02 03:04 UTC"), written);
});

// =================================================================================================
section("\nEND TO END: the shell reporter's real output, through this module");

// NOTHING ELSE ENFORCES THAT THE PRODUCER AND THIS READER AGREE. bin/quarantine-lane-report.sh writes
// the payload; this module and .github/workflows/quarantine-lane.yml read it. The marker's name, the
// outcome vocabulary (`PASSED_ACTION`, `PASSED_FLAPPER`) and the JSON shape are a three-way contract
// with no gate behind it, so every assertion above this point is about a payload THIS FILE invented -
// which agrees with the producer by construction and would keep agreeing after the producer changed.
//
// So this one runs the real script, twice, against a fixture whose outcome flips between the runs,
// and feeds both of its actual output files through `post`. It is the only test that fails when the
// two sides drift, and the transition it drives is the one the whole change exists for: a
// quarantined test going from failing to passing must post a NEW comment.
//
// The fixture is the same shape the Java-side harness builds
// (parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/QuarantineLaneReportScriptTest.java):
// a registry entry, an annotated class, and one failsafe XML. `QUARANTINE_CHECK_ROOT` is the script's
// own test seam, so nothing here touches the real tree.
// `emptyLane` reproduces what the PR that removes the last quarantine actually does to the tree: it
// deletes the `@Quarantined` annotation AND the registry entry - the two things the previous run's
// comment demanded - leaving the registry with a heading and no entries. That transition is what no
// fixture covered, on either side: every suite here and in QuarantineLaneReportScriptTest assumes at
// least one entry, which is why the producer's early return went unnoticed.
function runReporter(root, failed, { emptyLane = false } = {}) {
  const annotated = join(root, "module/src/test-integration/java/SomeQuarantinedIT.java");
  mkdirSync(join(root, "module/src/test-integration/java"), { recursive: true });
  mkdirSync(join(root, "module/target/failsafe-reports"), { recursive: true });
  mkdirSync(join(root, "docs"), { recursive: true });
  if (emptyLane) {
    rmSync(annotated, { force: true });
    writeFileSync(join(root, "docs/quarantined-tests.md"),
      "# Quarantined tests\n\n## Currently quarantined\n\n_None._\n");
  } else {
    writeFileSync(annotated,
      'class SomeQuarantinedIT {\n'
      + '    @Quarantined(reason = "d", tracking = "t", fixedBy = "PR astubbs#999999")\n'
      + "    void someMethod() {}\n}\n");
    writeFileSync(join(root, "docs/quarantined-tests.md"),
      "# Quarantined tests\n\n## Currently quarantined\n\n"
      + "- [ ] `SomeQuarantinedIT.someMethod` - diagnosed. **Owner: PR astubbs#999999**\n");
    writeFileSync(join(root, "module/target/failsafe-reports/TEST-x.SomeQuarantinedIT.xml"),
      '<testsuite>\n  <testcase name="someMethod" classname="x.SomeQuarantinedIT" time="1">'
      + (failed ? '<failure message="boom">stack</failure>' : "")
      + "</testcase>\n</testsuite>\n");
  }
  execFileSync("bash", [join(__dirname, "../../bin/quarantine-lane-report.sh")],
    { env: { ...process.env, QUARANTINE_CHECK_ROOT: root, DRY_RUN: "1", PR_NUMBER: "" }, encoding: "utf8" });
  return require("fs").readFileSync(join(root, "target/quarantine-lane-report.md"), "utf8");
}

asyncTest("the shell reporter's own output drives a failing -> passing FRESH comment", async () => {
  const root = mkdtempSync(join(tmpdir(), "qlane-"));
  try {
    const failingBody = runReporter(root, true);
    // The contract, asserted before anything is done with it - a payload this module cannot read is
    // the drift this test exists to catch, and it must name itself rather than surfacing as
    // "posted fresh when it should have updated".
    const failingPayload = require("./sticky-report-comment.js").readPayload(failingBody, DATA_MARKER);
    assert.ok(failingPayload, `the shell reporter's payload is unreadable by this module: ${failingBody}`);
    assert.strictEqual(failingPayload.outcomes["SomeQuarantinedIT.someMethod"], "FAILED");

    // Run 1: no previous comment, so it posts.
    const gh = fakeGithub([]);
    const first = await post({
      github: gh, context: CONTEXT, core: CORE, body: failingBody, now: new Date("2026-09-02T03:04:05Z"),
    });
    assert.strictEqual(first.action, "created");

    // Run 2, same outcome: edits in place. This is the ordinary push, and it is asserted here rather
    // than only above because a producer whose digest was unstable would post fresh every time and
    // the invented-payload tests could never see it.
    const again = await post({
      github: gh, context: CONTEXT, core: CORE, body: runReporter(root, true), now: new Date("2026-09-02T03:05:05Z"),
    });
    assert.strictEqual(again.action, "updated");

    // Run 3, the test now passes: posts fresh, and says which test moved and where to.
    const passingBody = runReporter(root, false);
    const third = await post({
      github: gh, context: CONTEXT, core: CORE, body: passingBody, now: new Date("2026-09-02T03:06:05Z"),
    });
    assert.strictEqual(third.action, "superseded");
    const created = gh.calls.filter(c => c.op === "createComment").pop().body;
    assert.ok(created.includes("`SomeQuarantinedIT.someMethod`: 🔴 failing → 🚨✅ passed - ACTION REQUIRED"),
      `the fresh comment does not name the transition: ${created}`);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// =================================================================================================
section("\nEND TO END: the run that EMPTIES the lane, which used to be the one run that stayed silent");

// THE GAP THIS FILE WAS EXTENDED FOR. The comment being retracted here says a deterministic
// quarantined test that started passing means its fix landed, so delete its `@Quarantined`
// annotation AND its docs/quarantined-tests.md entry. A PR that does exactly that - removing the
// LAST entry - left that comment live on itself forever, demanding an action it had already taken,
// because the producer wrote no report for an empty registry and the workflow gated its posting step
// on the lane being non-empty. Both halves are driven here, through the real script.
asyncTest("emptying the lane RETRACTS the ACTION REQUIRED comment instead of leaving it live", async () => {
  const root = mkdtempSync(join(tmpdir(), "qlane-empty-"));
  try {
    const gh = fakeGithub([]);

    // Push 1: the last quarantined test passes, so the comment demands the annotation be deleted.
    const first = await post({
      github: gh, context: CONTEXT, core: CORE,
      body: runReporter(root, false), now: new Date("2026-09-02T03:04:05Z"),
    });
    assert.strictEqual(first.action, "created");
    assert.ok(gh.calls.find(c => c.op === "createComment").body.includes("ACTION REQUIRED"));

    // Push 2: the PR does what it was told - annotation and registry entry both gone. The stale
    // failsafe XML is deliberately left in the fixture: the empty path must not read it as a lane
    // leak, and must not need it to have a report to write.
    const emptied = runReporter(root, false, { emptyLane: true });
    const payload = require("./sticky-report-comment.js").readPayload(emptied, DATA_MARKER);
    assert.ok(payload, `the lane-emptied report carries no payload, so no delta can be rendered: ${emptied}`);
    assert.ok(laneEmptied(payload),
      `the producer's empty payload is not the one this module recognises: ${JSON.stringify(payload)}`);

    const second = await post({
      github: gh, context: CONTEXT, core: CORE, body: emptied, now: new Date("2026-09-02T03:05:05Z"),
    });
    assert.strictEqual(second.action, "superseded");

    // What the fresh comment says: the lane is empty, and WHICH row was withdrawn.
    const created = gh.calls.filter(c => c.op === "createComment").pop().body;
    assert.ok(created.includes("quarantine lane is empty"), `the retraction does not say so: ${created}`);
    assert.ok(created.includes("`SomeQuarantinedIT.someMethod` left the lane"),
      `the retraction does not name the row it withdraws: ${created}`);
    assert.ok(!/\|.*ACTION REQUIRED/.test(created),
      `the retraction still carries an ACTION REQUIRED row: ${created}`);

    // And the comment it replaces must stop being live AND say why, because a reader arriving at the
    // merged PR scrolls past it before they reach the retraction.
    const retired = gh.store.find(c => c.id === first.commentId).body;
    assert.ok(!retired.includes(`${MARKER}\n`),
      "the ACTION REQUIRED comment still carries the live marker - later runs would keep updating it");
    assert.ok(retired.includes("## [superseded - the quarantine lane is now empty]"),
      `the retired comment's heading does not say the instruction is withdrawn: ${retired}`);
    // And its table is FOLDED AWAY, not merely captioned: the heading says the instruction is
    // withdrawn, but a fully visible ACTION REQUIRED table under it is still the first thing a reader
    // sees. The heading stays above the fold and the payload survives inside it - the recovery test
    // below depends on reading it back.
    assert.ok(retired.indexOf("<details>") > retired.indexOf("[superseded - "), `the withdrawn table is not collapsed: ${retired}`);
    assert.ok(retired.indexOf("<details>") < retired.indexOf("ACTION REQUIRED"), "the ACTION REQUIRED row is above the fold");
    assert.deepStrictEqual(require("./sticky-report-comment.js").readPayload(retired, DATA_MARKER)?.outcomes,
      { "SomeQuarantinedIT.someMethod": "PASSED_ACTION" }, "the payload did not survive the fold");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// The other half of the decision, and the reason it is not simply "always post". An empty lane is the
// healthy steady state; announcing it on every PR that never had a report is the fifteen-comments
// problem the stickiness exists to prevent.
asyncTest("an empty lane says NOTHING on a PR it never spoke on", async () => {
  const root = mkdtempSync(join(tmpdir(), "qlane-quiet-"));
  try {
    const gh = fakeGithub([]);
    const result = await post({
      github: gh, context: CONTEXT, core: CORE,
      body: runReporter(root, false, { emptyLane: true }), now: new Date("2026-09-02T03:04:05Z"),
    });
    assert.strictEqual(result.action, "skipped");
    assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 0);
    assert.strictEqual(gh.calls.filter(c => c.op === "updateComment").length, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// A lane that is still empty on the next push must not re-announce itself either: the retraction is
// already there, and the digest has not moved.
asyncTest("a lane that stays empty updates in place rather than posting again", async () => {
  const root = mkdtempSync(join(tmpdir(), "qlane-stays-"));
  try {
    const gh = fakeGithub([]);
    await post({
      github: gh, context: CONTEXT, core: CORE,
      body: runReporter(root, false), now: new Date("2026-09-02T03:04:05Z"),
    });
    const emptied = runReporter(root, false, { emptyLane: true });
    await post({ github: gh, context: CONTEXT, core: CORE, body: emptied, now: new Date("2026-09-02T03:05:05Z") });
    const before = gh.calls.filter(c => c.op === "createComment").length;
    const again = await post({
      github: gh, context: CONTEXT, core: CORE,
      body: runReporter(root, false, { emptyLane: true }), now: new Date("2026-09-02T03:06:05Z"),
    });
    assert.strictEqual(again.action, "updated");
    assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, before);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// =================================================================================================
section("\nthe WORKFLOW's gating, which is the other half of the same defect");

// THE PRODUCER AND THE READER CAN BOTH BE RIGHT AND THE REPORT STILL NEVER APPEAR. Everything above
// drives the two modules directly; none of it runs `.github/workflows/quarantine-lane.yml`, and the
// original bug lived exactly in the seam - `renderDelta` had a `left the lane` branch the workflow's
// `if:` made unreachable. Nothing in this repo executes a workflow condition, so the `if:` lines are
// asserted as text: crude, but it fails on the one edit that would silently restore the defect.
const laneWorkflow = require("fs").readFileSync(
  join(__dirname, "../workflows/quarantine-lane.yml"), "utf8").split("\n");

/** The `if:` a named step is gated on. */
function gateFor(stepName) {
  const at = laneWorkflow.findIndex(l => l.includes(`- name: ${stepName}`));
  assert.ok(at >= 0, `no step named "${stepName}" in quarantine-lane.yml`);
  const gate = laneWorkflow.slice(at + 1, at + 4).find(l => /^\s+if:/.test(l));
  assert.ok(gate, `the step "${stepName}" has no if: within three lines of its name`);
  return gate;
}

for (const step of ["Classify lane outcomes (writes the report, opens threads on unexpected passes)",
  "Post the quarantine lane report to the PR"]) {
  test(`"${step.split(" (")[0]}" is reachable when the registry is empty`, () => {
    const gate = gateFor(step);
    // Either spelling of the output is the regression - `outputs.found` or `outputs['found']` - and a
    // substring match on the first would have let the second through green.
    assert.ok(!/steps\.any\.outputs(\.found|\[)/.test(gate),
      `gated on the lane being non-empty, so the run that EMPTIES it cannot retract: ${gate}`);
    assert.ok(gate.includes("steps.any.outcome == 'success'"),
      `not gated on the emptiness check having run, so a rejected registry could still report: ${gate}`);
  });
}

// The other direction: nothing to run means nothing should be run. Emptying the lane must not start
// a maven invocation, and this is what keeps the two gates from being "fixed" into one.
test("the test-running step is still gated on the lane having entries", () => {
  const gate = gateFor("Run quarantined tests (red step here is expected, not a gate)");
  assert.ok(gate.includes("steps.any.outputs.found == 'true'"),
    `an empty lane would run the suite: ${gate}`);
});

// =================================================================================================
runAll();
