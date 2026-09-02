// Copyright (C) 2026 Antony Stubbs and contributors

// ONE STICKY PR COMMENT THAT CAN SAY WHAT CHANGED - the mechanics, shared by every CI report that
// posts one. Used today by the throughput report (.github/workflows/maven.yml) and the quarantine
// lane report (.github/workflows/quarantine-lane.yml).
//
// WHY IT IS A MODULE. Every one of these five behaviours was written for the throughput comment in
// astubbs/parallel-consumer#407, and every one of them is domain-free:
//
//   1. find our own previous comment by marker - PAGINATED, and filtered to `user.type === 'Bot'`
//   2. read a machine-readable payload back out of it, so a run can render a DELTA
//   3. update in place normally, but post a FRESH comment when the status changed
//   4. retire the old comment BEFORE creating the new one, then link it forward
//   5. stamp the head sha, a PR-context commit link, the run, and the time
//
// Only the payload's contents and the wording of the delta differ between callers, and those are
// parameters. The alternative - a second copy in the quarantine workflow - is how the ORIGINAL
// defects got into two places at once: astubbs#407 found the same unpaginated, unfiltered
// `listComments` lookup in the throughput step AND the SpotBugs step, because the second was copied
// from
// the first. A module cannot drift from itself.
//
// It is required from workflow YAML the way this repo's other gates are:
//
//   const sticky = require(`${process.env.GITHUB_WORKSPACE}/.github/scripts/sticky-report-comment.js`)
//
// `actions/github-script` runs with the workspace checked out, so `require` of a repo file works and
// is already the convention here (see .github/workflows/pr-checklist.yml, which requires four gate
// modules this way). Tests: .github/scripts/sticky-report-comment.test.js.

"use strict";

/**
 * The machine-readable payload a report embeds in its own body, or null.
 *
 * READING OUR OWN LAST COMMENT IS THE ONLY STORE AVAILABLE - no series is kept anywhere, by
 * decision - so this is what lets a run say what CHANGED rather than only what it measured. An
 * in-place update otherwise destroys the only record of the previous push.
 *
 * `dataMarker` is the payload's name (`pc-throughput-data`), not the whole comment: the producers
 * write `<!-- <name>: {json} -->`. Nothing enforces that a producer and this reader agree on the
 * name, so `grep -rn <name>` is the list to change if one ever moves.
 */
function readPayload(text, dataMarker) {
  const match = new RegExp(`<!-- ${dataMarker}: (.*?) -->`).exec(text ?? "");
  try {
    return match ? JSON.parse(match[1]) : null;
  } catch {
    return null;
  }
}

/**
 * Our own previous comment among `comments`, or undefined.
 *
 * TWO CONDITIONS, AND BOTH ARE LOAD-BEARING.
 *
 * The marker alone is not enough. The comment we find is an INPUT once its payload steers the
 * update-vs-post decision, so it must be restricted to comments a workflow could have written. In
 * astubbs#407 the SpotBugs step matched ANY comment containing "SpotBugs Report", oldest first:
 * somebody who asked "the SpotBugs Report shows 318, is that right?" before the bot first posted had
 * their
 * comment silently overwritten on the next push. That was live, and destroyed nothing only by luck.
 *
 * `c.body?.` rather than `c.body.` because the REST API can return a comment with a null body (a
 * comment deleted between the page fetch and this read); a throw there fails the caller's step for
 * a reason that has nothing to do with the report.
 *
 * The caller must pass a PAGINATED list - see `findExisting` below, which is the safe way in.
 */
function pickOurComment(comments, marker) {
  return comments.find(c => c.body?.includes(marker) && c.user?.type === "Bot");
}

/**
 * Our own previous comment on the PR, fetched with pagination.
 *
 * PAGINATE OR THE STICKINESS SILENTLY STOPS WORKING. `listComments` returns 30 per page. The bug is
 * latent while the report is an early comment - the API returns oldest-first, so a comment born
 * early stays on page one forever - and bites the moment one is born late on a busy PR: the lookup
 * misses it, every push posts a fresh comment instead of updating, the delta disappears, and the
 * fifteen-comments problem the stickiness exists to prevent comes back.
 */
async function findExisting({ github, owner, repo, issue_number, marker }) {
  const all = await github.paginate(github.rest.issues.listComments,
    { owner, repo, issue_number, per_page: 100 });
  return pickOurComment(all, marker);
}

/**
 * Whether this run's verdict differs from the previous comment's - the test for posting fresh.
 *
 * A COMMENT WITH NO PAYLOAD COUNTS AS A CHANGE, NOT AS "NO CHANGE". Written the obvious way
 * (`prev && cur && prev.status !== cur.status`) this is falsy whenever `prev` is null - which is
 * every comment posted before the payload existed, and such comments sat on three open PRs when
 * astubbs#407 landed. The first green-to-red after that merge would have been edited in place,
 * silently:
 * exactly the failure the fresh-comment rule removes, on the run where it matters most. Unknown is
 * not the same as unchanged.
 *
 * `cur` being null means THIS run produced no payload, so there is no verdict to announce and the
 * ordinary in-place update is right.
 */
function statusChanged(prev, cur) {
  // A USABLE STATUS IS A SCALAR, and an unusable one is not evidence of "unchanged". A payload that
  // parses but carries no `status` - `{}`, or a truncation that happens to stay valid JSON - left
  // both sides `undefined`, which compared EQUAL and suppressed the fresh announcement this whole
  // mechanism exists to make. The failure was silent and in the safe-looking direction: the engine
  // edited a comment in place while holding no verdict it could have compared.
  const usable = (p) => p != null && (typeof p.status === "string" || typeof p.status === "number");
  if (!usable(cur)) return false;          // nothing to announce, and nothing to claim
  if (!usable(prev)) return true;          // cannot prove it is unchanged -> announce, do not hide
  return prev.status !== cur.status;
}

/**
 * A status made safe to paste into a markdown heading. Letters and hyphens only - a status is a
 * short enum word (`green`, `regression`, `no-control`), and anything else in it is either a
 * formatting hazard or a sign the payload is not what this thought it was.
 */
function sanitiseForHeading(status) {
  return String(status ?? "unknown").replace(/[^a-zA-Z-]/g, "");
}

/**
 * The footer that makes "updated in place" legible as "updated".
 *
 * AN IN-PLACE UPDATE LOOKS DEAD. The body can be identical to last week's with nothing on it to say
 * a run happened - no timestamp, no commit, no link - so a reader cannot tell a fresh verdict from a
 * stale one, and the honest reaction to that is to stop trusting it.
 *
 * THE SHA IS A LINK, AND THAT IS THE WHOLE ANSWER TO "IS THIS COMMENT CURRENT?". A static comment
 * body cannot render the reader's local time - GitHub's sanitiser strips `<time>`, `<time-ago>` and
 * `<local-time>` to their text, and the REST API does not expose a user's timezone - so a UTC string
 * leaves every reader outside UTC doing arithmetic. The commit page does that work for them: it
 * carries GitHub's own localised, relative timestamp. One click beats any string printed here.
 *
 * POINTED AT THE COMMIT IN THIS PR, not at the bare commit. `/pull/N/commits/<sha>` resolves
 * (checked) and keeps the PR framing - which commit of THIS review it is, with its neighbours -
 * where `/commit/<sha>` drops the reader into the repository at large. The trade is that a commit
 * force-pushed off the branch later stops resolving there while the bare form would still work; that
 * is worth the framing, and the run link beside it survives either way.
 *
 * AND NOT AT THE CONVERSATION TIMELINE, WHICH WAS TRIED AND MEASURED RATHER THAN GUESSED. The
 * obvious wish is to scroll the reader to this commit's row in the conversation they are already on.
 * It cannot be done, and the evidence is one `curl` of the rendered PR page:
 *
 *   * THERE IS NO PER-COMMIT ANCHOR. The only fragment targets GitHub emits on a PR conversation are
 *     `#commits-pushed-<7char>`, `#issuecomment-<id>`, `#pullrequestreview-<id>`,
 *     `#ref-pullrequest-<id>` and the issue itself. The commit rows inside a push event carry no
 *     `id`, so the row a stamp would name is not addressable at all.
 *   * `#commits-pushed-` IS A PUSH ANCHOR KEYED ON THE PUSH'S OLDEST COMMIT, NOT THE HEAD - measured
 *     against a push whose contents were known exactly (three commits; the anchor is the first).
 *   * A SINGLE-COMMIT PUSH HAS NO ANCHOR EITHER, which is the half that kills the idea. It renders
 *     as a lone commit row badged `octicon-git-commit` with NO `id`. So the fragment does not merely
 *     key on the wrong commit; for the one shape of push where head and oldest COINCIDE, it does not
 *     exist. Across the branch measured, the head sha was an anchor ZERO times out of 8 commits.
 *
 * Grouping is also GitHub's rendering choice rather than a record of pushes - consecutive pushes can
 * be coalesced - so even "the oldest commit of my push" is not a stable key after the fact.
 *
 * SEVEN CHARACTERS, because GitHub abbreviates to seven everywhere else on the page. At nine the
 * comment showed a second, different-looking abbreviation of the same commit, so a reader had to stop
 * and check it was even the same one.
 *
 * The UTC timestamp stays, and stays plain: it is the one thing the links cannot do, which is show at
 * a glance that a run happened without navigating anywhere.
 */
function stampFor({ serverUrl, owner, repo, prNumber, headSha, runId, now = new Date() }) {
  const runUrl = `${serverUrl}/${owner}/${repo}/actions/runs/${runId}`;
  const commitUrl = `${serverUrl}/${owner}/${repo}/pull/${prNumber}/commits/${headSha}`;
  return `\n\n<sub>Updated for [\`${String(headSha).slice(0, 7)}\`](${commitUrl})`
    + ` · [run ${runId}](${runUrl})`
    + ` · ${now.toISOString().replace("T", " ").slice(0, 16)} UTC</sub>`;
}

/**
 * The retired body for a superseded comment: marker renamed so later runs stop targeting it, heading
 * prefixed so a reader who lands on it knows, and a note appended.
 *
 * THE NOTE IS PASSED WHOLE rather than as a noun slotted into a fixed "Superseded by X" sentence,
 * because the two writes that use this cannot honestly promise the same thing - see `postStickyReport`.
 */
function retiredBody({ body, marker, supersededMarker, headingRe, label, note }) {
  return `${body
    .replace(marker, supersededMarker)
    .replace(headingRe, match => `${match}[superseded - ${label}] `)
    }\n\n<sub>${note}</sub>`;
}

/**
 * Upsert the sticky report comment, posting fresh on a status change.
 *
 * Returns `{ action, commentId, url, statusChanged, prev, cur }` where `action` is one of
 * `updated` (edited in place), `created` (no previous comment), `superseded` (retired the old one
 * and posted fresh because the status changed), or `skipped` (nothing was written - only reachable
 * under `postWhenAbsent: false`).
 *
 * Required: `github`, `context`, `core` (the github-script globals), `marker`, `dataMarker`, `body`.
 * Optional: `supersededMarker` (defaults to the marker with ` (superseded)` before the `-->`),
 * `renderDelta(prev, cur)` returning text appended below the body, `headingRe` locating the report's
 * own heading in the retired copy, `supersededLabel(prev, cur)`, `what` (a noun used in warnings),
 * `postWhenAbsent` (see below), and `now` for tests.
 *
 * `postWhenAbsent: false` MAKES THIS BODY A CORRECTION RATHER THAN A REPORT: if we have not spoken
 * on this PR, it stays quiet and returns `{ action: "skipped" }`. The quarantine lane's
 * lane-emptied body is the case - "there is nothing quarantined" is the healthy steady state and
 * saying it on every PR is the fifteen-comments problem in a new costume, but on a PR whose earlier
 * push demanded an annotation be deleted the same body is the retraction, and must be posted. The
 * flag is here rather than in the caller because the only way to know is the lookup this function
 * already does; doing it in the caller means paginating the comment list twice.
 */
async function postStickyReport({
  github, context, core,
  marker,
  // REQUIRED, not derived. This used to default to `marker.replace(/\s*-->$/, ...)`, which CodeQL
  // flags high as js/bad-tag-filter: that regex treats `-->` as THE html comment terminator when
  // `--!>` is also one. Here `marker` is a literal we own, so it was not exploitable - but a marker
  // and its superseded twin are two constants of a report, not one computed from the other, and
  // deriving them by parsing html was the weak part regardless of reachability. The throughput
  // consumer already passed both explicitly; now everyone does.
  supersededMarker,
  dataMarker,
  body,
  renderDelta = () => "",
  headingRe = /^### /m,
  supersededLabel = (prev, cur) => `status changed to ${sanitiseForHeading(cur?.status)}`,
  what = "report",
  postWhenAbsent = true,
  now = new Date(),
}) {
  const { owner, repo } = context.repo;
  const issue_number = context.issue.number;
  const pr = context.payload.pull_request;

  const existing = await findExisting({ github, owner, repo, issue_number, marker });
  const prev = readPayload(existing?.body, dataMarker);
  const cur = readPayload(body, dataMarker);

  // Nothing of ours to correct, and this caller only wanted to correct - so say nothing.
  if (!existing && !postWhenAbsent) {
    return { action: "skipped", commentId: null, url: null, statusChanged: false, prev: null, cur };
  }

  const delta = renderDelta(prev, cur) || "";
  const stamp = stampFor({
    serverUrl: context.serverUrl, owner, repo,
    prNumber: pr.number, headSha: pr.head.sha, runId: context.runId, now,
  });
  const payload = `${marker}\n${body}${delta}${stamp}`;

  const changed = statusChanged(prev, cur);

  // NORMALLY UPDATE IN PLACE: a PR with fifteen throughput comments is a PR where nobody reads the
  // throughput comment.
  //
  // EXCEPT WHEN THE STATUS CHANGES. Editing a comment thirty scrolls up produces no notification and
  // no visible change to anyone reading the PR top to bottom, so a run that went green to red used to
  // announce itself by silently rewriting a comment nobody was looking at. On a status change a fresh
  // comment is posted at the bottom, and the old one is retired. Movement that is not a status change
  // still updates in place; only the VERDICT earns a new comment.
  if (existing && !changed) {
    await github.rest.issues.updateComment({ owner, repo, comment_id: existing.id, body: payload });
    return { action: "updated", commentId: existing.id, url: existing.html_url, statusChanged: false, prev, cur };
  }

  // RETIRE FIRST, THEN CREATE - the order is the whole safety property, and in astubbs#407 it was
  // written the wrong way round first. There is no transaction across two API calls, so one can fail after
  // the other succeeded, and the two orders fail very differently:
  //
  //   create then retire  -> the retire fails and TWO comments carry the live marker. `listComments`
  //                          is oldest-first and the lookup takes the first match, so every later run
  //                          latches onto the STALE one and updates it forever while the newer report
  //                          sits orphaned. Silent, self-perpetuating, and worst on the status change
  //                          it exists to announce.
  //   retire then create  -> the create fails and NO comment carries the live marker. The next run
  //                          simply posts a fresh one. Degraded, obvious, and it recovers by itself.
  //
  // The forward link costs a third write, so it is best-effort and last: by the time it runs, the
  // marker state is already correct whether or not it succeeds.
  //
  // The first note must NOT NAME A PLACE. It is written BEFORE the create, and the create can fail -
  // the callers' `continue-on-error` swallows it by design - so a retired comment reading "further
  // down this conversation" when nothing was posted sends a reader looking for a report that is not
  // there, permanently: the retired marker no longer matches the lookup, so no later run revisits it.
  // Only the second note, written once `created` demonstrably exists, may point at it.
  const label = supersededLabel(prev, cur);
  const retire = note => retiredBody({
    body: existing.body, marker, supersededMarker, headingRe, label, note,
  });

  if (existing) {
    await github.rest.issues.updateComment({ owner, repo, comment_id: existing.id,
      body: retire(`Superseded - a fresh ${what} should follow for this push.`) });
  }
  const created = await github.rest.issues.createComment({ owner, repo, issue_number, body: payload });
  if (existing) {
    try {
      await github.rest.issues.updateComment({ owner, repo, comment_id: existing.id,
        body: retire(`Superseded by [a newer ${what}](${created.data.html_url}).`) });
    } catch (e) {
      core.warning(`could not link the retired ${what} forward: ${e.message}`);
    }
  }
  return {
    action: existing ? "superseded" : "created",
    commentId: created.data.id,
    url: created.data.html_url,
    statusChanged: changed,
    prev,
    cur,
  };
}

module.exports = {
  readPayload,
  pickOurComment,
  findExisting,
  statusChanged,
  sanitiseForHeading,
  stampFor,
  retiredBody,
  postStickyReport,
};
