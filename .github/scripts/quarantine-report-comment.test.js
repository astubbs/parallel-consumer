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
const { label, renderDelta, post, MARKER, DATA_MARKER } = require("./quarantine-report-comment.js");

let failures = 0;
const pending = [];
const section = name => pending.push(() => console.log(name));
const record = (name, error) => {
  if (!error) return console.log(`  ok  ${name}`);
  console.log(`FAIL  ${name}\n      ${error.message.replace(/\n/g, "\n      ")}`);
  failures++;
};
const test = (name, fn) => pending.push(() => { try { fn(); record(name); } catch (e) { record(name, e); } });
const asyncTest = (name, fn) => pending.push(async () => { try { await fn(); record(name); } catch (e) { record(name, e); } });

// A payload exactly as bin/quarantine-lane-report.sh builds it: the digest is the map, sorted and
// joined, so the two can never disagree here either.
const payloadFor = outcomes => ({
  status: Object.keys(outcomes).sort().map(k => `${k}=${outcomes[k]}`).join(";"),
  outcomes,
});
const bodyFor = outcomes =>
  `## 🧪🔒 Quarantine Lane Report\n\n(table)\n\n<!-- ${DATA_MARKER}: ${JSON.stringify(payloadFor(outcomes))} -->\n`;

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
section("\npost - the posting decision, which is the point of the whole change");

function fakeGithub(comments = []) {
  const calls = [];
  const store = comments.map(c => ({ ...c }));
  return {
    calls, store,
    paginate: async (fn, p) => fn(p),
    rest: {
      issues: {
        listComments: async () => store,
        updateComment: async ({ comment_id, body }) => {
          calls.push({ op: "updateComment", comment_id, body });
          const t = store.find(c => c.id === comment_id);
          if (t) t.body = body;
          return { data: { id: comment_id } };
        },
        createComment: async ({ body }) => {
          calls.push({ op: "createComment", body });
          const created = { id: 999, body, html_url: "https://example.test/c/999" };
          store.push({ ...created, user: { type: "Bot" } });
          return { data: created };
        },
      },
    },
  };
}
const CONTEXT = {
  repo: { owner: "astubbs", repo: "parallel-consumer" },
  issue: { number: 29 },
  serverUrl: "https://github.com",
  runId: 7,
  payload: { pull_request: { number: 29, head: { sha: "abcdef1234567890" } } },
};
const CORE = { warning: () => {} };
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
function runReporter(root, failed) {
  mkdirSync(join(root, "module/src/test-integration/java"), { recursive: true });
  mkdirSync(join(root, "module/target/failsafe-reports"), { recursive: true });
  mkdirSync(join(root, "docs"), { recursive: true });
  writeFileSync(join(root, "module/src/test-integration/java/SomeQuarantinedIT.java"),
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
(async () => {
  for (const run of pending) await run();
  console.log(failures ? `\n${failures} test(s) failed` : "\nAll tests passed");
  process.exit(failures ? 1 : 0);
})();
