// Copyright (C) 2026 Antony Stubbs and contributors

// Unit tests for sticky-report-comment.js - the shared mechanics behind every CI report that posts
// one sticky PR comment. Plain node, no dependencies, no runner:
// `node .github/scripts/sticky-report-comment.test.js`. Exits non-zero on the first sign of trouble.
//
// WHAT THIS FILE IS ACTUALLY GUARDING. Four of the five behaviours here exist because they were
// WRONG in production, in two copies at once, and every one of the four failed SILENTLY - a
// duplicated comment, an overwritten human, an unannounced red, a stale marker winning forever. None
// of them turns anything red on its own. So the assertions below are written against the DEFECT, not
// against the feature: each one was red-proofed by reintroducing the original mistake in
// sticky-report-comment.js and watching this file flip. The reintroductions are named in each block
// so the next person can repeat that in seconds rather than trusting this sentence.
//
// AND THEY CHECK WHERE A THING LANDED, NOT THAT IT EXISTS. astubbs#407's first self-test grepped for a
// status literal in the reporter's source and passed on six corrupted messages, because the literal
// was present - inside the prose, in the wrong place. Assertions here name positions and orders:
// which call came first, which body the note is in, what is absent from a string.

"use strict";

const assert = require("assert");
const {
  readPayload, pickOurComment, findExisting, statusChanged, sanitiseForHeading,
  stampFor, retiredBody, postStickyReport,
} = require("./sticky-report-comment.js");

// Runner, fake GitHub client and fake core are shared - report-comment-test-harness.js. The
// can-never-fail guard in test() and the call-order recording in fakeGithub were BOTH originally
// only here; sharing is what gave them to the throughput and quarantine suites too.
const harness = require("./report-comment-test-harness.js");
const { section, test, asyncTest, runAll } = harness.makeRunner();
const fakeGithub = harness.fakeGithub;
const fakeCore = harness.fakeCore;



const fakeContext = {
  repo: { owner: "astubbs", repo: "parallel-consumer" },
  issue: { number: 29 },
  serverUrl: "https://github.com",
  runId: 12345,
  payload: { pull_request: { number: 29, head: { sha: "abcdef1234567890abcdef1234567890abcdef12" } } },
};

const MARKER = "<!-- pc-test-report -->";
const DATA = "pc-test-data";
const bodyWith = status => `### Test Report\n\nsome prose\n\n<!-- ${DATA}: ${JSON.stringify({ status })} -->\n`;
const botComment = (id, status) => ({
  id, user: { type: "Bot" }, html_url: `https://example.test/c/${id}`,
  body: `${MARKER}\n${bodyWith(status)}`,
});

// =================================================================================================
section("readPayload - the only store of the previous push's numbers");

test("reads the payload a report embedded in itself", () =>
  assert.deepStrictEqual(readPayload(bodyWith("green"), DATA), { status: "green" }));

test("a body with no payload reads as null, not as a throw", () =>
  assert.strictEqual(readPayload("### Report\n\nnothing machine-readable here", DATA), null));

test("undefined input (no previous comment at all) reads as null", () =>
  assert.strictEqual(readPayload(undefined, DATA), null));

// Malformed JSON is what a half-written or truncated comment looks like. Returning null puts the run
// on the "previous state unknown" path, which posts fresh - the safe direction. Throwing would fail
// the caller's step.
test("a malformed payload reads as null rather than throwing", () =>
  assert.strictEqual(readPayload(`<!-- ${DATA}: {not json} -->`, DATA), null));

test("a different report's payload is not picked up", () =>
  assert.strictEqual(readPayload("<!-- some-other-data: {\"status\":\"x\"} -->", DATA), null));

// =================================================================================================
section("\npickOurComment - the filter that stopped the bot overwriting people's writing");

// RED-PROOF: drop `&& c.user?.type === 'Bot'` from pickOurComment and this test goes red while every
// other test in this file stays green. That is the exact edit that was live in the SpotBugs step.
test("a HUMAN comment carrying the marker is never picked", () => {
  const human = { id: 1, user: { type: "User" }, body: `I read ${MARKER} and disagree` };
  const bot = botComment(2, "green");
  assert.strictEqual(pickOurComment([human, bot], MARKER).id, 2);
});

test("a human comment is not picked even when no bot comment exists", () => {
  const human = { id: 1, user: { type: "User" }, body: `quoting ${MARKER} here` };
  assert.strictEqual(pickOurComment([human], MARKER), undefined);
});

// The REST API can return a comment whose body is null - deleted between the page fetch and this
// read. `c.body.includes` throws there and fails the caller's step for an unrelated reason.
test("a null-bodied comment does not throw", () =>
  assert.strictEqual(pickOurComment([{ id: 1, user: { type: "Bot" }, body: null }], MARKER), undefined));

test("oldest match wins, which is what makes retiring the old marker load-bearing", () =>
  assert.strictEqual(pickOurComment([botComment(5, "a"), botComment(9, "b")], MARKER).id, 5));

// =================================================================================================
section("\nfindExisting - pagination, without which the stickiness silently stops sticking");

// RED-PROOF: replace the `github.paginate(...)` call with a bare
// `github.rest.issues.listComments({...})` and this test goes red on `per_page`.
asyncTest("the lookup paginates at 100 per page", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  const found = await findExisting({
    github: gh, owner: "astubbs", repo: "parallel-consumer", issue_number: 29, marker: MARKER,
  });
  assert.strictEqual(found.id, 7);
  const paginated = gh.calls.find(c => c.op === "paginate");
  assert.ok(paginated, "listComments was called without github.paginate - the report scrolls off page one");
  assert.strictEqual(paginated.params.per_page, 100);
});

asyncTest("no previous comment is undefined, not an error", async () => {
  const gh = fakeGithub({ comments: [] });
  assert.strictEqual(await findExisting({
    github: gh, owner: "astubbs", repo: "parallel-consumer", issue_number: 29, marker: MARKER,
  }), undefined);
});

// =================================================================================================
section("\nstatusChanged - and the case where the previous state is UNKNOWN");

test("the same status is not a change", () =>
  assert.strictEqual(statusChanged({ status: "green" }, { status: "green" }), false));

test("a different status is a change", () =>
  assert.strictEqual(statusChanged({ status: "green" }, { status: "regression" }), true));

// RED-PROOF: write it as `prev && cur && prev.status !== cur.status` - the obvious form, and the one
// that was live - and this single test flips. It is the whole difference between a green-to-red
// transition announcing itself and being edited into a comment thirty scrolls up.
test("NO previous payload counts as a change - unknown is not unchanged", () =>
  assert.strictEqual(statusChanged(null, { status: "regression" }), true));

// The mirror case: this run produced no payload, so there is no verdict to announce and editing in
// place is right. Posting fresh here would spam a PR every time a report failed to write its payload.
test("no CURRENT payload is not a change", () =>
  assert.strictEqual(statusChanged({ status: "green" }, null), false));

test("neither side has a payload", () =>
  assert.strictEqual(statusChanged(null, null), false));

// =================================================================================================
section("\nstampFor - what makes an in-place update legible as an update");

test("the sha is abbreviated to SEVEN characters, as GitHub abbreviates everywhere else", () => {
  const stamp = stampFor({
    serverUrl: "https://github.com", owner: "astubbs", repo: "parallel-consumer",
    prNumber: 29, headSha: "abcdef1234567890", runId: 42, now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.ok(stamp.includes("`abcdef1`"), `expected a 7-char sha, got: ${stamp}`);
  assert.ok(!stamp.includes("abcdef12`"), "the sha was abbreviated to more than seven characters");
});

test("the commit link keeps the PR framing rather than dropping into the repo at large", () => {
  const stamp = stampFor({
    serverUrl: "https://github.com", owner: "astubbs", repo: "parallel-consumer",
    prNumber: 29, headSha: "abcdef1234567890", runId: 42, now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.ok(stamp.includes("/astubbs/parallel-consumer/pull/29/commits/abcdef1234567890"),
    `expected a PR-context commit link, got: ${stamp}`);
});

test("the run is linked and the time is plain UTC to the minute", () => {
  const stamp = stampFor({
    serverUrl: "https://github.com", owner: "astubbs", repo: "parallel-consumer",
    prNumber: 29, headSha: "abcdef1234567890", runId: 42, now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.ok(stamp.includes("/actions/runs/42"), `expected a run link, got: ${stamp}`);
  assert.ok(stamp.includes("2026-09-02 03:04 UTC"), `expected a UTC stamp, got: ${stamp}`);
});

// The timeline anchor was measured to resolve zero times out of eight - see the comment on stampFor.
// Pinned so a future "helpful" change reintroducing it shows up here rather than in production.
test("no #commits-pushed anchor, which was measured never to resolve", () => {
  const stamp = stampFor({
    serverUrl: "https://github.com", owner: "astubbs", repo: "parallel-consumer",
    prNumber: 29, headSha: "abcdef1234567890", runId: 42, now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.ok(!stamp.includes("commits-pushed"), "a timeline anchor came back");
});

// =================================================================================================
section("\nretiredBody - a retired comment must stop being findable, and say so");

test("the live marker is replaced so later runs stop targeting it", () => {
  const out = retiredBody({
    body: `${MARKER}\n${bodyWith("green")}`, marker: MARKER,
    supersededMarker: "<!-- pc-test-report (superseded) -->", headingRe: /^### /m,
    label: "status changed to regression", note: "Superseded.",
  });
  assert.ok(!out.includes(`${MARKER}\n`), "the live marker survived - two comments would now match");
  assert.ok(out.includes("<!-- pc-test-report (superseded) -->"));
});

test("the heading says superseded, so a reader landing on it is not misled", () => {
  const out = retiredBody({
    body: `${MARKER}\n${bodyWith("green")}`, marker: MARKER,
    supersededMarker: "<!-- pc-test-report (superseded) -->", headingRe: /^### /m,
    label: "status changed to regression", note: "Superseded.",
  });
  assert.ok(out.includes("### [superseded - status changed to regression] Test Report"),
    `heading not marked: ${out}`);
});

// The label goes through `String.replace`, where `$&`, `$1` and friends are substitution syntax. A
// literal replacement string would expand them; the implementation uses a replacer function.
test("a label containing $& is not expanded as replacement syntax", () => {
  const out = retiredBody({
    body: `${MARKER}\n${bodyWith("green")}`, marker: MARKER,
    supersededMarker: "<!-- x -->", headingRe: /^### /m, label: "a $& b", note: "n",
  });
  assert.ok(out.includes("[superseded - a $& b]"), `replacement syntax leaked: ${out}`);
});

section("\nsanitiseForHeading");
test("a status is reduced to letters and hyphens", () =>
  assert.strictEqual(sanitiseForHeading("no-control<script>1"), "no-controlscript"));
test("a missing status reads as unknown", () =>
  assert.strictEqual(sanitiseForHeading(undefined), "unknown"));

// =================================================================================================
section("\npostStickyReport - update in place, post fresh on a status change");

asyncTest("no previous comment: one create, no update", async () => {
  const gh = fakeGithub({ comments: [] });
  const core = fakeCore();
  const result = await postStickyReport({
    github: gh, context: fakeContext, core, marker: MARKER, dataMarker: DATA,
    body: bodyWith("green"), now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.strictEqual(result.action, "created");
  assert.deepStrictEqual(gh.calls.filter(c => c.op === "updateComment"), []);
  assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 1);
});

asyncTest("same status: edits in place and posts nothing", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  const core = fakeCore();
  const result = await postStickyReport({
    github: gh, context: fakeContext, core, marker: MARKER, dataMarker: DATA,
    body: bodyWith("green"), now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.strictEqual(result.action, "updated");
  assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 0);
  assert.strictEqual(gh.calls.filter(c => c.op === "updateComment").length, 1);
});

asyncTest("the in-place update carries the stamp, so it does not look dead", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("green"), now: new Date("2026-09-02T03:04:05Z"),
  });
  const written = gh.calls.find(c => c.op === "updateComment").body;
  assert.ok(written.includes("`abcdef1`"), "no head sha on the updated comment");
  assert.ok(written.includes("2026-09-02 03:04 UTC"), "no timestamp on the updated comment");
});

// RED-PROOF: swap the two writes in postStickyReport so the create runs first and this goes red.
// The order is the whole safety property - create-then-retire leaves TWO live markers when the
// retire fails, and the oldest-first lookup then latches onto the stale one forever.
asyncTest("a status change RETIRES the old comment BEFORE creating the new one", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  const result = await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("regression"), now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.strictEqual(result.action, "superseded");
  const ops = gh.calls.filter(c => c.op !== "paginate" && c.op !== "listComments").map(c => c.op);
  assert.deepStrictEqual(ops, ["updateComment", "createComment", "updateComment"],
    "expected retire, create, then link forward");
});

// The half that matters when the create fails: exactly one comment may carry the live marker, and if
// the count is wrong it must be ZERO (recovers next run), never TWO (stale one wins forever).
asyncTest("a failed create leaves NO comment carrying the live marker, not two", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")], failCreate: true });
  await assert.rejects(() => postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("regression"), now: new Date("2026-09-02T03:04:05Z"),
  }));
  const live = gh.store.filter(c => c.body.includes(`${MARKER}\n`));
  assert.strictEqual(live.length, 0, "the old comment still carries the live marker after a failed create");
});

// The first note runs BEFORE the create, which can fail. It must not send a reader looking for a
// report that may never exist - and nothing ever revisits a retired comment to correct it.
asyncTest("the pre-create retirement note names no place", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")], failCreate: true });
  await assert.rejects(() => postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("regression"), what: "report", now: new Date("2026-09-02T03:04:05Z"),
  }));
  const note = gh.store.find(c => c.id === 7).body;
  assert.ok(note.includes("should follow for this push"), `unexpected note: ${note}`);
  assert.ok(!note.includes("Superseded by"), "the retired comment promises a link to a report that was never created");
  assert.ok(!/\bbelow\b|further down/i.test(note), "the retired comment points at a place that may not exist");
});

asyncTest("once the new comment exists, the retired one links forward to it", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("regression"), what: "report", now: new Date("2026-09-02T03:04:05Z"),
  });
  const retired = gh.store.find(c => c.id === 7).body;
  assert.ok(/Superseded by \[a newer report\]\(https:\/\/example\.test\/c\/\d+\)/.test(retired),
    `no forward link on the retired comment: ${retired}`);
});

// The forward link is the third write and is best-effort: by the time it runs the marker state is
// already correct, so losing it must not fail the caller.
asyncTest("a failed forward link warns rather than throwing", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")], failForwardLink: true });
  const core = fakeCore();
  const result = await postStickyReport({
    github: gh, context: fakeContext, core, marker: MARKER, dataMarker: DATA,
    body: bodyWith("regression"), what: "throughput report", now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.strictEqual(result.action, "superseded");
  assert.strictEqual(core.warnings.length, 1);
  assert.ok(core.warnings[0].includes("throughput report"), core.warnings[0]);
});

// The case that was live on three open PRs when the payload was introduced: an existing comment from
// before the payload existed. It must post FRESH, because unknown is not unchanged.
asyncTest("an existing comment with NO payload posts fresh rather than being edited silently", async () => {
  const payloadFree = { id: 7, user: { type: "Bot" }, html_url: "u",
    body: `${MARKER}\n### Test Report\n\nold, payload-free` };
  const gh = fakeGithub({ comments: [payloadFree] });
  const result = await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("regression"), now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.strictEqual(result.action, "superseded");
});

asyncTest("a human comment quoting the marker is never edited", async () => {
  const gh = fakeGithub({ comments: [{ id: 3, user: { type: "User" }, body: `look at ${MARKER} please` }] });
  await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("green"), now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.deepStrictEqual(gh.calls.filter(c => c.op === "updateComment"), [],
    "the bot edited a human's comment");
  assert.strictEqual(gh.store.find(c => c.id === 3).body, `look at ${MARKER} please`);
});

asyncTest("the delta a caller renders is appended below the body, above the stamp", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("green"),
    renderDelta: (prev, cur) => `\n\n_prev=${prev.status} cur=${cur.status}_`,
    now: new Date("2026-09-02T03:04:05Z"),
  });
  const written = gh.calls.find(c => c.op === "updateComment").body;
  assert.ok(written.includes("_prev=green cur=green_"), `delta missing: ${written}`);
  assert.ok(written.indexOf("_prev=green") < written.indexOf("Updated for"), "the delta landed below the stamp");
});

asyncTest("a caller's supersededLabel is what the retired heading says", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, dataMarker: DATA,
    body: bodyWith("regression"),
    supersededLabel: () => "a test outcome changed",
    now: new Date("2026-09-02T03:04:05Z"),
  });
  assert.ok(gh.store.find(c => c.id === 7).body.includes("[superseded - a test outcome changed]"));
});

// =================================================================================================
runAll();
