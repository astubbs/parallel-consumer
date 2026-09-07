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
  stampFor, retiredBody, postStickyReport, awaitingForwardLink,
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

const SUPERSEDED = "<!-- pc-test-report (superseded) -->";
const retire = collapse => retiredBody({
  body: `${MARKER}\n${bodyWith("green")}\n\n<sub>Updated for x</sub>`, marker: MARKER,
  supersededMarker: SUPERSEDED, headingRe: /^### /m,
  label: "the lane is now empty", note: "Superseded.", collapse,
});

// =================================================================================================
section("\nretiredBody with collapse - a WRONG report must not lead with its table");

// RED-PROOF: drop the `if (collapse)` block and the first two tests here go red; the rest stay green.
test("collapse folds everything below the heading into a details block", () => {
  const out = retire(true);
  const lines = out.split("\n");
  assert.strictEqual(lines[0], SUPERSEDED, `the superseded marker must stay on line one: ${out}`);
  assert.ok(lines[1].startsWith("### [superseded - the lane is now empty] Test Report"), lines[1]);
  assert.strictEqual(lines[2], "", "no blank line between the heading and the block");
  assert.ok(lines[3].startsWith("<details><summary>"), `the block does not open right under the heading: ${out}`);
  assert.ok(out.indexOf("<details>") < out.indexOf("some prose"), "the old body is outside the block");
  assert.ok(out.indexOf("some prose") < out.indexOf("</details>"), "the old body is outside the block");
  assert.ok(out.indexOf("</details>") < out.indexOf("<sub>Superseded.</sub>"), "the note landed inside the block");
});

// GitHub only renders markdown inside <details> after a blank line following </summary>; without it
// the old table comes out as one line of pipes.
test("the block leaves a blank line after the summary so the markdown inside still renders", () =>
  assert.match(retire(true), /<\/summary>\n\n/));

test("without collapse the body is untouched, because an older report is not a wrong one", () => {
  const out = retire(false);
  assert.ok(!out.includes("<details>"), `an ordinary retirement was collapsed: ${out}`);
  assert.ok(out.includes("### [superseded - the lane is now empty] Test Report\n\nsome prose"), out);
});

// The two lookups that later runs perform against a retired comment. Both must survive the fold, or
// the recovery path in postStickyReport cannot see the comment it exists to repair.
test("the data payload inside the block is still read", () =>
  assert.deepStrictEqual(readPayload(retire(true), DATA), { status: "green" }));

test("the collapsed comment is still found by its superseded marker", () => {
  const found = pickOurComment([{ id: 4, user: { type: "Bot" }, body: retire(true) }], SUPERSEDED);
  assert.strictEqual(found?.id, 4);
});

test("a body with no heading is folded from below the marker line rather than not at all", () => {
  const out = retiredBody({
    body: `${MARKER}\nno heading here\n\n<!-- ${DATA}: {"status":"g"} -->`, marker: MARKER,
    supersededMarker: SUPERSEDED, headingRe: /^### /m, label: "l", note: "n", collapse: true,
  });
  assert.ok(out.startsWith(`${SUPERSEDED}\n\n<details>`), out);
  assert.ok(out.includes("no heading here"), out);
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
section("\npostWhenAbsent - a body that is a CORRECTION rather than a report");

// The quarantine lane's emptied-lane body is the case: on a PR whose earlier push said "delete the
// annotation and the registry entry" it is the retraction and must be posted, but on a PR that never
// carried a report it is an announcement that nothing is quarantined - noise on every PR forever.

// Every correction in the sections below is the same call - our marker pair, `postWhenAbsent: false`,
// a fixed clock - against a different fake store and body. `options` overrides any of it, so a test
// that needs its own `core` or a `renderDelta` still reads as the one thing it varies.
const correction = (gh, body, options = {}) => postStickyReport({
  github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, supersededMarker: SUPERSEDED,
  dataMarker: DATA, body, postWhenAbsent: false, now: new Date("2026-09-02T03:05:05Z"), ...options,
});

asyncTest("no previous comment and postWhenAbsent false: nothing is written at all", async () => {
  const gh = fakeGithub({ comments: [] });
  const result = await correction(gh, bodyWith("green"));
  assert.strictEqual(result.action, "skipped");
  assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 0);
  assert.strictEqual(gh.calls.filter(c => c.op === "updateComment").length, 0);
});

// The half that makes the flag a correction rather than a mute: our own comment being there is
// exactly what it waits for.
asyncTest("postWhenAbsent false still corrects a comment we already posted", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  const result = await correction(gh, bodyWith("regression"));
  assert.strictEqual(result.action, "superseded");
  assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 1);
});

// A human comment quoting the marker is not ours, so it is not something to correct either - the
// author filter and this flag have to agree, or the bot answers a person's comment.
asyncTest("a human comment carrying the marker does not count as ours to correct", async () => {
  const gh = fakeGithub({ comments: [{ id: 3, user: { type: "User" }, body: `${MARKER}\nis this right?` }] });
  const result = await correction(gh, bodyWith("green"));
  assert.strictEqual(result.action, "skipped");
  assert.strictEqual(gh.store.find(c => c.id === 3).body, `${MARKER}\nis this right?`);
});

// =================================================================================================
section("\npostWhenAbsent false after a retire whose create FAILED - the gap that stayed silent forever");

// THE SEQUENCE. Run 1 is an ordinary status change: it retires the live comment (marker renamed, first
// note appended) and then the create fails - `continue-on-error` swallows that in production. Run 2
// is a correction: no live comment, so before this it returned `skipped`, and the retired comment sat
// under a heading promising a report that never came, permanently - nothing revisits a retired
// comment. RED-PROOF: make `unreplaced` always undefined and this test goes red on `action`.
const twoRuns = async ({ secondBody = bodyWith("empty"), secondOptions = {} } = {}) => {
  const gh = fakeGithub({ comments: [botComment(7, "green")], failCreate: true });
  await assert.rejects(() => postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, supersededMarker: SUPERSEDED,
    dataMarker: DATA, body: bodyWith("regression"), what: "report", now: new Date("2026-09-02T03:04:05Z"),
  }));
  gh.faults.failCreate = false;
  const core = fakeCore();
  const result = await correction(gh, secondBody, { core, what: "report", ...secondOptions });
  return { gh, core, result };
};

asyncTest("the correction is POSTED, because a retired comment of ours proves we have spoken", async () => {
  const { gh, result } = await twoRuns();
  assert.strictEqual(result.action, "recovered");
  const creates = gh.calls.filter(c => c.op === "createComment" && !c.body.includes("regression"));
  assert.strictEqual(creates.length, 1, "the correction was not posted");
  assert.ok(creates[0].body.startsWith(`${MARKER}\n`), "the fresh comment does not carry the live marker");
});

asyncTest("the retired comment is then linked forward to it, as the normal path would have done", async () => {
  const { gh, result } = await twoRuns();
  const retired = gh.store.find(c => c.id === 7).body;
  assert.ok(retired.includes(`Superseded by [a newer report](${result.url}).`), `no forward link: ${retired}`);
  assert.ok(!retired.includes("should follow for this push"), `the place-less note was left in place: ${retired}`);
  assert.ok(retired.startsWith(SUPERSEDED), "the retired marker was disturbed");
});

asyncTest("the retired comment's payload is the previous state, so the correction can render its delta", async () => {
  const { result, gh } = await twoRuns({
    secondOptions: { renderDelta: (prev, cur) => `\n\n_prev=${prev?.status} cur=${cur?.status}_` },
  });
  assert.deepStrictEqual(result.prev, { status: "green" });
  const posted = gh.calls.filter(c => c.op === "createComment").pop().body;
  assert.ok(posted.includes("_prev=green cur=empty_"), `the delta did not see the retired payload: ${posted}`);
});

// The forward link stays best-effort on this path too: the fresh comment already exists and is the
// only one carrying the live marker, so losing the link must not fail the step.
asyncTest("a failed forward link on the recovery path warns rather than throwing", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")], failCreate: true });
  await assert.rejects(() => postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, supersededMarker: SUPERSEDED,
    dataMarker: DATA, body: bodyWith("regression"), now: new Date("2026-09-02T03:04:05Z"),
  }));
  gh.faults.failCreate = false;
  gh.faults.failForwardLink = true;
  const core = fakeCore();
  const result = await correction(gh, bodyWith("empty"), { core });
  assert.strictEqual(result.action, "recovered");
  assert.strictEqual(core.warnings.length, 1, core.warnings.join("; "));
});

// The same half-done retirement, followed by an ORDINARY report rather than a correction: the lane
// emptied (create failed), a test was re-quarantined, and the next run has something to report. A
// report posts fresh whenever nothing is live, so this never went silent - but the first cut looked
// for the retired comment only under `postWhenAbsent: false`, so the report was created, the retired
// comment was never linked forward, and its "should follow for this push" note stood forever.
// Found by review on astubbs/parallel-consumer#415. RED-PROOF: restore the `correction ?` gate on
// `unreplaced` and this goes red on `action`.
asyncTest("an ordinary report after a half-done retirement also finishes it", async () => {
  const { gh, result } = await twoRuns({
    secondBody: bodyWith("regression"), secondOptions: { postWhenAbsent: true },
  });
  assert.strictEqual(result.action, "recovered");
  assert.deepStrictEqual(result.prev, { status: "green" }, "the previous state was not read from the retired comment");
  const retired = gh.store.find(c => c.id === 7).body;
  assert.ok(retired.includes(`Superseded by [a newer report](${result.url}).`), `no forward link: ${retired}`);
  assert.ok(!retired.includes("should follow for this push"), `the place-less note was left in place: ${retired}`);
});

// CONTROL: the recovery must not turn the flag back into "always post". With nothing of ours on the
// PR - live or retired - the correction stays silent.
asyncTest("CONTROL: with no retired comment either, the correction is still skipped", async () => {
  const gh = fakeGithub({ comments: [{ id: 3, user: { type: "User" }, body: "unrelated" }] });
  const result = await correction(gh, bodyWith("empty"));
  assert.strictEqual(result.action, "skipped");
  assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 0);
});

// A retired comment that already points forward was replaced successfully; the comment it points at
// is either live (then `existing` found it) or was retired in turn. It is not evidence of a half-done
// retirement, and posting for it would be the "empty lane on every PR" noise, once per PR that ever
// had a report. RED-PROOF: pick with `where: () => true` and this goes red.
asyncTest("a retired comment that ALREADY has a forward link does not trigger a post", async () => {
  const linked = retiredBody({
    body: `${MARKER}\n${bodyWith("green")}`, marker: MARKER, supersededMarker: SUPERSEDED, headingRe: /^### /m,
    label: "l", note: "Superseded by [a newer report](https://example.test/c/901).",
  });
  const gh = fakeGithub({ comments: [{ id: 7, user: { type: "Bot" }, body: linked }] });
  const result = await correction(gh, bodyWith("empty"));
  assert.strictEqual(result.action, "skipped");
  assert.strictEqual(gh.calls.filter(c => c.op === "createComment").length, 0);
  assert.strictEqual(gh.store.find(c => c.id === 7).body, linked, "a fully retired comment was edited");
});

// A human quoting the SUPERSEDED marker is no more ours than one quoting the live marker.
asyncTest("a human comment carrying the superseded marker and the pending note is not ours to repair", async () => {
  const gh = fakeGithub({ comments: [{ id: 3, user: { type: "User" },
    body: `${SUPERSEDED}\nwhy does it say <sub>Superseded - a fresh report should follow for this push.</sub>?` }] });
  const result = await correction(gh, bodyWith("empty"));
  assert.strictEqual(result.action, "skipped");
});

// Several retired comments can lack a forward link - the link is a best-effort third write - but only
// the NEWEST is the one whose replacement never existed; each older one was superseded by the comment
// retired after it. RED-PROOF: drop `newest: true` and this picks id 5.
asyncTest("of several unlinked retired comments, the NEWEST is the one repaired", async () => {
  const pending = id => ({ id, user: { type: "Bot" }, body: retiredBody({
    body: `${MARKER}\n${bodyWith(`s${id}`)}`, marker: MARKER, supersededMarker: SUPERSEDED, headingRe: /^### /m,
    label: "l", note: "Superseded - a fresh report should follow for this push.",
  }) });
  const gh = fakeGithub({ comments: [pending(5), pending(9)] });
  const result = await correction(gh, bodyWith("empty"), { what: "report" });
  assert.strictEqual(result.action, "recovered");
  assert.deepStrictEqual(result.prev, { status: "s9" });
  assert.ok(gh.store.find(c => c.id === 9).body.includes("Superseded by ["), "the newest was not linked");
  assert.ok(!gh.store.find(c => c.id === 5).body.includes("Superseded by ["), "the older one was linked past its real successor");
});

test("awaitingForwardLink tells the two notes apart, whatever the caller's noun was", () => {
  assert.strictEqual(awaitingForwardLink({ body: "<sub>Superseded - a fresh quarantine lane report should follow for this push.</sub>" }), true);
  assert.strictEqual(awaitingForwardLink({ body: "<sub>Superseded by [a newer report](u).</sub>" }), false);
  assert.strictEqual(awaitingForwardLink({ body: null }), false);
});

// The extra pick comes out of the SAME list. Once a lane is empty, the skipped path is the steady
// state on every PR, so a second pagination there would be the common case, not the rare one.
// RED-PROOF: replace the `unreplaced` pick with a second `findExisting` call and both halves go red.
asyncTest("every path paginates the comment list exactly once", async () => {
  for (const [comments, postWhenAbsent, expected] of [
    [[botComment(7, "green")], true, "superseded"],
    [[], false, "skipped"],
    [[], true, "created"],
  ]) {
    const gh = fakeGithub({ comments });
    const result = await postStickyReport({
      github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, supersededMarker: SUPERSEDED,
      dataMarker: DATA, body: bodyWith("regression"), postWhenAbsent, now: new Date("2026-09-02T03:05:05Z"),
    });
    assert.strictEqual(result.action, expected);
    assert.strictEqual(gh.calls.filter(c => c.op === "paginate").length, 1,
      `the ${expected} path paginated more than once`);
  }
});

// =================================================================================================
section("\na correction COLLAPSES the comment it retires; an ordinary status change does not");

// RED-PROOF: pass `collapse: false` (or nothing) from postStickyReport and the first goes red.
asyncTest("postWhenAbsent false folds the retired body under its heading", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  await correction(gh, bodyWith("empty"));
  const retired = gh.store.find(c => c.id === 7).body;
  assert.ok(retired.includes("<details><summary>"), `the wrong report is still fully visible: ${retired}`);
  assert.ok(retired.indexOf("[superseded - ") < retired.indexOf("<details>"), "the heading is inside the block");
  assert.ok(retired.indexOf("</details>") < retired.indexOf("Superseded by ["), "the forward link is inside the block");
});

asyncTest("an ordinary status change leaves the retired body readable", async () => {
  const gh = fakeGithub({ comments: [botComment(7, "green")] });
  await postStickyReport({
    github: gh, context: fakeContext, core: fakeCore(), marker: MARKER, supersededMarker: SUPERSEDED,
    dataMarker: DATA, body: bodyWith("regression"), now: new Date("2026-09-02T03:05:05Z"),
  });
  assert.ok(!gh.store.find(c => c.id === 7).body.includes("<details>"), "an older report was hidden as if it were wrong");
});

// =================================================================================================
section("\nthe harness's own guard against a test that can never fail");

asyncTest("test() rejects an async body, because its assertion would reject after 'ok' was logged", async () => {
  // DRIVEN IN A SUBPROCESS, against the real harness. The first cut of this re-implemented the
  // guard inline and asserted on the copy - which would have passed with the real guard deleted,
  // making it precisely the never-fail test the guard exists to prevent.
  const { execFileSync } = require("child_process");
  const { mkdtempSync, writeFileSync, rmSync } = require("fs");
  const { tmpdir } = require("os");
  const { join } = require("path");
  const dir = mkdtempSync(join(tmpdir(), "harness-guard-"));
  const probe = join(dir, "probe.js");
  writeFileSync(probe, `
    const h = require(${JSON.stringify(require.resolve("./report-comment-test-harness.js"))});
    const { test, runAll } = h.makeRunner();
    test("an async body handed to the sync runner", async () => { throw new Error("lands too late"); });
    runAll();
  `);
  let status = 0, out = "";
  try {
    out = execFileSync(process.execPath, [probe], { encoding: "utf8" });
  } catch (e) { status = e.status; out = `${e.stdout || ""}${e.stderr || ""}`; }
  rmSync(dir, { recursive: true, force: true });
  // The guard must turn a silently-passing test into a REPORTED FAILURE.
  assert.strictEqual(status, 1, "the probe suite must exit non-zero");
  assert.match(out, /use asyncTest, or it can never fail/);
});

section("\na payload that parses but carries no status");

test("statusChanged treats an unusable payload as unknown, not as unchanged", () => {
  // {} parses, and a truncation can leave valid JSON without `status`. Both sides then read
  // undefined, which compared EQUAL and suppressed the announcement this mechanism exists to make
  // - a silent failure in the safe-looking direction.
  assert.strictEqual(statusChanged({}, { status: "green" }), true, "no prior status -> announce");
  assert.strictEqual(statusChanged({ status: "green" }, {}), false, "no current status -> nothing to claim");
  assert.strictEqual(statusChanged({}, {}), false, "neither usable -> no announcement invented");
  assert.strictEqual(statusChanged({ status: "a" }, { status: "b" }), true, "a real change still announces");
  assert.strictEqual(statusChanged({ status: "a" }, { status: "a" }), false, "unchanged stays unchanged");
});

runAll();
