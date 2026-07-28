# Upstream backlink plan (dry-run) & freshness-checker design

This documents what the backlink mechanism would post upstream, and what would
happen if we ran it. **Nothing here has been posted.** All of this is driven by
`scripts/upstream-backlink.sh` (dry-run by default) reading
`src/docs/development/upstream-map.yaml`.

## Why backlink at all

`confluentinc/parallel-consumer` is effectively archived, but users keep arriving
at its issues/PRs unaware a maintained fork exists (see upstream #907, *"Is the
project still actively maintained?"*). A short, respectful cross-repo comment:

1. tells people hitting a bug that a fix already exists in the fork, and
2. makes the maintained fork discoverable from the place people actually land.

GitHub renders a `owner/repo#123` mention as a genuine bidirectional link visible
in both timelines. We deliberately use plain references, **not** `Fixes/Closes`
keywords - those do not auto-close across repos anyway, and we are not trying to
close anyone's issue. One comment per item, no spam.

## What would happen if we ran it (per comment)

Running `scripts/upstream-backlink.sh --post <id>` for one target would:

- add one public comment to the named upstream issue/PR under the authenticated
  `gh` account;
- create a cross-repo reference that appears in **both** the upstream item's
  timeline and our fork's referenced PR/commit;
- send a notification to everyone subscribed to that upstream item;
- **not** change labels, state, or close anything;
- print the new comment URL and the exact snippet to paste into the entry's
  `forwarded:` list in `upstream-map.yaml` (write-back is manual on purpose, to
  preserve the manifest's comments/formatting).

It is public and hard to undo (you can delete a comment, but notifications and
cross-references have already fired). That is why the script is dry-run unless
`--post` is passed, and prompts for confirmation even then.

## The queue (already-fixed / carried work)

Preview any row with `scripts/upstream-backlink.sh --target <t> <id>`
(dry-run). `--target both` is the default; the table narrows it where only one
side is relevant.

| Manifest id | Template | Target | Dry-run command | Post when |
|---|---|---|---|---|
| `bug-857-stall-after-rebalance` | fix-backlink | issue 857 | `upstream-backlink.sh --target issues bug-857-stall-after-rebalance` | after the branch is PR'd (currently no fork PR #) |
| `bug-859-pcmetrics-leak` | fix-backlink | issue 859 | `upstream-backlink.sh --target issues bug-859-pcmetrics-leak` | ready now (fork PR #57 exists) |
| `cherry-pick-893-offset-reset` | fix-backlink | PR 893 | `upstream-backlink.sh --target prs cherry-pick-893-offset-reset` | ready now (tell the PR author it's carried in fork #57) |
| `cherry-pick-905-max-shard-metric` | fix-backlink | PR 905 | `upstream-backlink.sh --target prs cherry-pick-905-max-shard-metric` | ready now (carried in fork #57) |
| `fix-909-stale-container` | fix-backlink | PR 909 | `upstream-backlink.sh --target prs fix-909-stale-container` | hold until the regression test lands / it's PR'd |
| `bug-912-vertx-stream-leak` | fix-backlink | issue 912 | `upstream-backlink.sh --target issues bug-912-vertx-stream-leak` | hold until fixed & PR'd (status `in-progress`) |
| `issue-907-maintenance-signal` | fork-awareness | issue 907 | `upstream-backlink.sh --template fork-awareness issue-907-maintenance-signal` | ready now (highest-value awareness post) |

Recommended first posts once you choose to go live: **#859** (concrete fix,
PR #57) and **#907** (the maintenance question - answer it by pointing at the
fork). The carried-PR notes (#893/#905) are courteous but lower urgency.

## After posting

1. Paste the printed `forwarded:` snippet into the matching entry in
   `upstream-map.yaml` (url + date). This is also what makes future runs skip the
   target (idempotency), so it is not optional book-keeping.
2. Bump the entry's `upstream.last_checked` if you re-checked state while there.

## Anti-spam guarantees (backlink helper)

`scripts/upstream-backlink.sh` is built so it cannot nag upstream:

- **Dry-run by default** — posts nothing without `--post`, which also prompts.
- **Idempotent** — skips any target already recorded in a manifest `forwarded`
  url (from `scripts/upstream-map.py posted-refs`), so re-running never
  double-posts. The dry-run preview marks each target `[would post]` /
  `[SKIP: already commented]`.
- **Per-run cap** — `--max` (default 3) limits comments per invocation.
- **Delay** — `--delay` (default 3s) between posts.
- **Status guard** — `fix-backlink` only fires when the fix is in a PR or landed
  (`fork.status` `pr-open` | `merged` | `released`); it refuses `none` |
  `in-progress` | `ready` (use `--force`, or the `fork-awareness` template which
  claims no fix). This is why #909/#912 (`in-progress`) and #857 (`ready`, no PR
  yet) can't be announced as fixed by accident.

One comment per item, plain cross-repo reference, never `Fixes/Closes`.

- **Tailored wording in the source of truth** — for items where the generic
  template is too blunt, set a per-entry `backlink:` field in `upstream-map.yaml`
  (supports `{{FORK_REPO}}` / `{{FORK_REF}}` / `{{SUMMARY}}` / `{{ID}}`). The helper
  renders the comment from that field instead of the template, so the public
  explanation lives in one place. `bug-859-pcmetrics-leak` uses this to explain
  the two-cause leak and how our fix complements the already-merged #892.

## Built: "new activity since last sweep" checker

`scripts/upstream-sweep.sh` — **read-only by default**. It reads `last_swept`
from the manifest and:

- lists upstream issues **and** PRs updated since then
  (`gh {issue,pr} list --state all --search "updated:>=<since> sort:updated-desc"`),
  formatted via `gh --jq`;
- re-checks every primary ref the manifest tracks
  (`scripts/upstream-map.py tracked`) and flags **drift** — anything recorded
  `open` that upstream now reports `closed`/`merged`;
- prints a Markdown report. `--since <date>` overrides the window;
  `--update-swept` prints the `last_swept` bump to apply.

Anti-spam on the sweep side: it posts nothing unless `--publish` is passed, and
even then it updates a **single** fork tracking issue (found by title) rather
than creating new issues each run.

Deferred (thin wrapper left to add): a scheduled GitHub Action that runs
`upstream-sweep.sh --publish` weekly. No off-the-shelf bot does fork↔upstream
issue-state tracking; the fork-sync Actions only sync code.

### Triage from the first live sweep (2026-07-28)

The first read-only run surfaced upstream items not yet in the manifest — fold
these in when convenient:

- **#892 (MERGED)** "Keep instantiated OffsetMapCodecManager … #859" — an
  *upstream* fix for #859 landed; reconcile with our `bug-859-pcmetrics-leak`
  entry (upstream may no longer be `open` for this).
- **#917** kafka-clients 3.9.2 [SECURITY] — new; add to the security-batch entry.
- **#918 / #919** log-noise trims (#919 "fixes #640") — new; relate to the
  logging-cleanup opportunity.
- **#920** "document JDK 17 build requirement / Jabel Java 8 bytecode" — relates
  to `java-17-baseline-kafka4`.
- **#902** "Always process the freshest record when ordered by key" — new issue.
- **#921 / #922 (MERGED)** README maintenance notice / "Add link to fork" —
  fork-awareness already partly present upstream; check before re-posting.
