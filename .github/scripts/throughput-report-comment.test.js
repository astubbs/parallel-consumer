// Copyright (C) 2026 Antony Stubbs and contributors

// Unit tests for throughput-report-comment.js. Plain node, no dependencies, no runner:
// `node .github/scripts/throughput-report-comment.test.js`.
//
// The delta text was inline in .github/workflows/maven.yml and therefore untestable; extracting it
// changed no behaviour, and these assertions pin the behaviour that was extracted so a later edit has
// something to break. The mechanics it configures are tested in sticky-report-comment.test.js;
// bin/test-check-throughput-regression.mjs tests the producer that writes the payload this reads.

"use strict";

const assert = require("assert");
const { renderDelta, post, MARKER, SUPERSEDED_MARKER, DATA_MARKER } = require("./throughput-report-comment.js");

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

const bodyFor = data => `### Throughput\n\n(table)\n\n<!-- ${DATA_MARKER}: ${JSON.stringify(data)} -->\n`;

// =================================================================================================
section("renderDelta - movement, ordered as the report's own table orders it");

test("a status change leads, in bold", () => {
  const d = renderDelta({ status: "green" }, { status: "regression" });
  assert.ok(d.includes("status **green -> regression**"), d);
});

// The order is the claim: dimensionless first, machine-dependent last. A reader who stops after the
// first number should have read the most transferable one.
test("ratio, then share, then rate - the rate is last because it is machine-only", () => {
  const d = renderDelta({ status: "g", ratio: 1.0, share: 0.3, rate: 100 },
    { status: "g", ratio: 1.1, share: 0.4, rate: 200 });
  assert.ok(d.indexOf("ratio") < d.indexOf("share"), d);
  assert.ok(d.indexOf("share") < d.indexOf("rate"), d);
});

test("the rate carries a percentage, signed", () => {
  assert.ok(renderDelta({ status: "g", rate: 100 }, { status: "g", rate: 150 }).includes("(+50.0%)"));
  assert.ok(renderDelta({ status: "g", rate: 100 }, { status: "g", rate: 50 }).includes("(-50.0%)"));
});

// A zero previous rate would divide by zero and print Infinity%, which reads as a catastrophic
// regression rather than as a missing measurement.
test("a zero previous rate is skipped rather than divided by", () =>
  assert.ok(!renderDelta({ status: "g", rate: 0 }, { status: "g", rate: 50 }).includes("rate")));

test("it always says the difference may be noise", () =>
  assert.ok(renderDelta({ status: "g", rate: 100 }, { status: "g", rate: 150 })
    .includes("measured spread")));

// UNLIKE THE QUARANTINE DELTA, THIS ONE NEVER ANNOUNCES THAT NOTHING MOVED - deliberately, and the
// distinction is finer than it first looks. Every number here is noisy, so "nothing changed" is a
// claim this measurement cannot support; what it does instead is print the numbers unchanged
// (`rate 100 -> 100 (+0.0%)`) and let the reader see that for themselves. Silence is reserved for
// having nothing comparable at all, which is a different statement.
test("identical numbers are still PRINTED - the delta never claims 'no change'", () => {
  const d = renderDelta({ status: "g", rate: 100 }, { status: "g", rate: 100 });
  assert.ok(d.includes("rate 100 -> 100 (+0.0%)"), d);
  assert.ok(!/no change|unchanged/i.test(d), `the delta claimed nothing changed: ${d}`);
});

test("nothing comparable at all is silence", () =>
  assert.strictEqual(renderDelta({ status: "g" }, { status: "g" }), ""));

test("no previous payload is silence", () =>
  assert.strictEqual(renderDelta(null, { status: "g", rate: 100 }), ""));

// =================================================================================================
section("\npost - the markers, which name comments already live on open PRs");

test("the live marker is unchanged from the one on existing comments", () =>
  assert.strictEqual(MARKER, "<!-- pc-throughput-report -->"));

// Pinned rather than derived: sticky-report-comment.js's default produces the same string today, and
// a change to that derivation must not orphan the retired comments already carrying this one.
test("the retired marker is pinned, not derived", () =>
  assert.strictEqual(SUPERSEDED_MARKER, "<!-- pc-throughput-report (superseded) -->"));

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
const existing = data => ({ id: 5, user: { type: "Bot" }, html_url: "u", body: `${MARKER}\n${bodyFor(data)}` });
const run = (gh, data) => post({
  github: gh, context: CONTEXT, core: CORE, body: bodyFor(data), now: new Date("2026-09-02T03:04:05Z"),
});

asyncTest("a moved number with the same verdict updates in place", async () => {
  const gh = fakeGithub([existing({ status: "green", rate: 100 })]);
  const result = await run(gh, { status: "green", rate: 150 });
  assert.strictEqual(result.action, "updated");
  assert.ok(gh.calls.find(c => c.op === "updateComment").body.includes("(+50.0%)"));
});

asyncTest("green -> regression posts fresh, and the retired heading names the new status", async () => {
  const gh = fakeGithub([existing({ status: "green", rate: 100 })]);
  const result = await run(gh, { status: "regression", rate: 50 });
  assert.strictEqual(result.action, "superseded");
  assert.ok(gh.store.find(c => c.id === 5).body
    .includes("### [superseded - status changed to regression]"), gh.store.find(c => c.id === 5).body);
});

// =================================================================================================
(async () => {
  for (const r of pending) await r();
  console.log(failures ? `\n${failures} test(s) failed` : "\nAll tests passed");
  process.exit(failures ? 1 : 0);
})();
