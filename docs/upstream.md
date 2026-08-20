# Upstream tracking and issue mirrors

This is a maintained hard fork of the effectively-archived `confluentinc/parallel-consumer`. This
doc owns everything about that relationship: the manifest that maps fork work to upstream PRs, the
editorial analysis that ranks and plans that work, the mirrors that stand in for upstream issues,
the commit trailers that carry provenance, and the sweep that watches upstream for new activity.
AGENTS.md carries only the pointer and the one-line rule that manifest upkeep is the agent's job.

## The two sources, and which wins

We keep a durable, machine-readable cache of the fork↔upstream relationship so it never has to be
re-derived from scratch:

- [**`src/docs/development/upstream-map.yaml`**](../src/docs/development/upstream-map.yaml) - the
  **state tracker**, and the **source of truth** for the *facts*: which fork branch/PR maps to
  which upstream issue/PR, its work group, and current status. Its header documents the schema.
  Validate and render with `scripts/upstream-map.py {validate,table,refs}`. Design follows Debian
  DEP-3, Yocto `Upstream-Status:` and OpenShift's `UPSTREAM:` fork conventions.
- [**`src/docs/development/upstream-pr-analysis.adoc`**](../src/docs/development/upstream-pr-analysis.adoc) -
  the **plan**: *editorial* analysis with rankings, verdicts, and the recommended merge order. When
  prose and manifest disagree, **the manifest wins for facts**. Manifest entries link back to
  `.adoc` section anchors via `adoc_anchor`.
- **`docs/inflight/`** - *transient* cross-branch working notes only, one file per item.

The manifest maps upstream **PRs**. **If the work maps to an upstream *issue*, the fork mirror is
where status goes** - diagnosis, labels, and closing all belong on the mirror.

**But the manifest may cache an issue's *frozen* facts** - its number, the closure event that swept
it, and the mirror that owns it (`fork_issue`, or `fork_issues` when one entry groups a cohort).
That is a read-path optimisation, not a second tracker: grepping one local file is instant, while
the same answer from the mirrors is dozens of API round-trips against a rate limit every agent here
shares. The usual objection - the copy drifts from its source - needs a source that can still move,
and an archived upstream's closed issue numbers and closure events cannot. **Cache what is frozen;
never mirror what is still moving.** The line is *status*: the moment an answer can change, it
belongs to the mirror and the manifest must not hold a copy.

This is also what makes a cohort record possible. A set derived from live mirror titles is whatever
the mirrors say today; the point of `sweep-2023-admin-closure` is a fixed cutoff - *these* are the
items upstream closed administratively - which is exactly the thing no query can reconstruct once
the mirrors move on.

## Keeping the manifest in sync is the agent's job

**When you start work that maps to an upstream PR, add or update its entry in `upstream-map.yaml`**
- do not just note it in prose. And **it does not stop at "start work"**.

Nothing automated checks the *fork* side: `upstream-map.py validate` only checks the schema, and
`upstream-sweep.sh` only watches upstream - so a manifest that says `prs: []` while a fork PR is
open still passes every check, and the mapping quietly rots (a 2026-08-04 audit found five such
entries). Update the entry **at every lifecycle transition of your own work, in the same commit that
causes it**: opening a PR (`prs:` + `status: pr-open`), finishing on a branch without a PR
(`status: ready`), merging (`merged`), releasing (`released`), abandoning
(`superseded`/`wontfix`).

**For `merged`, write it in the branch and push it *before* you merge.** That reads like claiming
something untrue and is not: branch content is visible to nobody until it lands, and the moment it
lands the entry is correct. Leaving it until after the merge is the expensive order - your branch is
gone, the fix is a commit straight to master, and until someone remembers, the manifest is wrong.
Three entries were found stale exactly this way (astubbs#204, astubbs#31, astubbs#258 - one of them
for a week). `.claude/hooks/check-upstream-map-merged.sh` refuses a `gh pr merge` while the entry
still says `pr-open`, and goes quiet once it does not.

Loose ends do **not** go in this manifest - it has no `todo:` field. Anything a command can answer
("how far behind is PR #N?" - `git rev-list --left-right --count`) should be asked of the command
rather than cached here, where it rots. Record what no command knows in `docs/inflight/`; keep this
manifest to the mapping itself.

**The `upstream:` half needs the same discipline, and nothing else maintains it.**
`upstream-sweep.sh` reports drift; it never writes. When it flags an entry, correct
`upstream.status` and bump `last_checked` in the same pass - an item left unfixed is re-reported
every run, which teaches you to skim past the report, and then it stops working at all. Verify
against GitHub rather than against the entry: `confluentinc#548` sat recorded `open` while it had
been merged since 2023, and a header note in the same file asserted a third answer.

## Commit trailers

Commits that relate to upstream carry DEP-3-style trailers so provenance lives in the commit itself:

```
Upstream-Issue: confluentinc/parallel-consumer#857
Upstream-PR: confluentinc/parallel-consumer#548
Forwarded: <upstream comment URL | no | not-needed>
Applied-Upstream: <no | commit:SHA | VERSION>
```

**Trailers are not enforced** - they only fit upstream-related commits, not fork-only work
(rebrand, release, dependabot, formatting). Use judgement. The subject convention for upstream
references is governed by AGENTS.md, Commits; branch names encode the upstream number
(`bugs/857-...`, `cherry-pick/893-...`) and match the manifest's `fork.branches`.

## Issue mirrors

Every open upstream issue has a **fork mirror** here (astubbs#44, astubbs#117-astubbs#195, label
`upstream-mirror`), so a reference has a fork-local number a reader can click. Find one with
`gh issue list -R astubbs/parallel-consumer --label upstream-mirror --search "confluentinc#NNN"`.

Mirrors astubbs#227-254 carry `upstream-admin-closed` alongside `upstream-mirror`: the 28 issues
upstream bulk-closed as COMPLETED on 2023-07-07 without triage. The full record - what the two 2023
sweeps were, why they were invisible to our tooling for three years, and why the cohort was
mirrored in full including the low-value items - lives in the `THE 2023 ADMINISTRATIVE SWEEPS`
section header of `upstream-map.yaml`; keep it there, not here. The practical rule it leaves
behind: **do not trust the closure state of any 2023-era upstream item** - several of those issue
bodies claim "implemented in #NNN" where #NNN was itself closed unmerged.

### Mirror format

When you create or edit a mirror:

- **Title `confluentinc#NNN: <description>`.** The prefix is the join to upstream and never
  changes; it uses the owner form for the reasons in
  [`docs/issue-references.md`](issue-references.md). It read `upstream #NNN:` until astubbs#196: the
  bulk import deviated from its own plan, which had specified the owner form, and the deviation was
  written up afterwards as if it were the intent. Neither form auto-links in a title, so nothing was
  gained by the role word; all 78 mirrors were retitled. The description half started as upstream's
  own title, but it is **ours to rewrite** - many upstream titles name only where a failure surfaced
  ("Error in onPartitionsAssigned") and contain no term anyone would search for. Retitle once the
  cause is actually known.
- **Always record the upstream title verbatim in the body header**, whether or not the mirror's
  title still matches it: `> Upstream title: *"..."*`. Unconditional on purpose - the obvious rule
  ("record it when you retitle") needs whoever retitles to remember, and it leaves a reader unable
  to tell whether any given mirror's title is upstream's words or ours without opening upstream to
  compare. Recording it always makes the mirror self-describing and the mapping lossless.
- **The body is a summary that captures the original**, not a verbatim copy - a landing page that
  preserves the substance and links out. **No `@mentions` in mirrored content**, or the import
  notifies people who never opted in.
- **Labels**: `upstream-mirror`, one area label, one type label.
- **Cross-repo references in the body are fully qualified** - `confluentinc/parallel-consumer#NN`.
  This is the one place the house prose form does not apply: `confluentinc#NN` does not auto-link on
  GitHub - only `owner/repo#NN` does - and a bare `#NN` resolves against the fork's own numbering.
  Titles are different again: nothing auto-links there, which is why they use the short owner form.

The header block:

```markdown
> **Mirror of [confluentinc/parallel-consumer#NNN](https://github.com/confluentinc/parallel-consumer/issues/NNN)**
> Upstream title: *"<upstream's own title, verbatim>"*
> Opened by [<author>](https://github.com/<author>), <YYYY-MM-DD> ·
> <N> comments upstream · last upstream activity <YYYY-MM-DD>
> Summarised, not copied. Discussion belongs here - upstream is unmaintained and may be archived.

## Summary
<2-6 sentences: symptom, conditions, what the thread established>

## Fork status
<fixed-in / investigating / not started, with links to fork PRs and docs/solutions/ entries>
```

Mirrors created before this convention may lack the upstream-title line. Add it when you touch one;
there is no value in a bulk backfill pass.

[`docs/plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md`](plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md)
carries the original bulk-import plan and what the run taught. Read it for *why*, not *how* - it is
a dated record, so its own copy of this format has since drifted. This doc is the live one.

### Read the upstream original before acting on a mirror

Body **and** comments: `gh issue view <N> -R confluentinc/parallel-consumer --json body,comments`.
Every mirror says "Summarised, not copied", which makes it one agent's reading of the issue rather
than the issue. Verify the summary against the original and against the code before fixing or
documenting anything.

This is not hypothetical. astubbs#194's summary said the Mutiny dependency "requires a higher
bytecode level", while confluentinc#906's reporter had written *"I think the compiler target for
that dependency is 17"* - the detail that actually mattered. A fix followed the summary, set
`release.target=9`, compiled green, and shipped a jar that died with `UnsupportedClassVersionError`
on Java 8 and 11. astubbs#171 shows the same failure in its **Fork status** notes rather than its
summary - "`shutdownTimeout` and `drainTimeout` (default 30s)" reads as one shared default, where
the code has two (10s and 30s) - so check a mirror's added commentary as sceptically as its
summary.

When the mirror turns out to be wrong, say so in the PR **and correct the mirror**, or the next
reader inherits the same error.

## Discussions are not mirrored

Upstream also has ~74 GitHub Discussions - a third content type, outside both the manifest and the
mirror set. Decided 2026-08-07: **we do not mirror them.** Discussions are not a queue to be
converted - there is no pipeline turning threads into issues, and most should never become one:
questions, doc gaps and chat stay exactly where they are.

The rule is a judgement, not a mapping: **if reading a discussion makes us think there is an
issue, we raise one** - a normal fork issue on its own merits, no `upstream-mirror` label and no
"Mirror of" header, citing the discussion as where it came from so the thread can be linked
forward to it. The issue exists because we believe the problem is real, not because a discussion
existed.

That also keeps `upstream-mirror` meaning one precise thing - "an upstream *issue* we carry" -
which is what makes the mirror set verifiable against upstream.

`scripts/upstream-sweep.sh --audit` lists the zero-reply discussions. The unread backlog and the
threads worth acting on are tracked in
[`docs/inflight/upstream-discussions-unanswered.md`](inflight/upstream-discussions-unanswered.md).

## Backlinking upstream

**Done, and there is no tooling for it.** All 78 open upstream issues are mirrored here and every
one carries a backlink comment pointing at its mirror. Seven that are fixed in a *released* version
carry a second comment naming the version. This is a finished, one-off job, not a recurring chore.

**If you need to comment upstream again**, do it directly with `gh`, and:

- Put a hidden marker in the body (`<!-- pc-mirror:issue-NNN -->` or similar) and check for it
  before posting. That is the idempotency record - not a field in a manifest, which goes stale the
  moment anyone comments and, when the field was removed, silently failed *open*.
- Use **plain cross-repo references, never `Fixes`/`Closes`** - they do not auto-close cross-repo
  and we are not closing anyone's issue.
- Comment a **second** time only when there is something to act on: a fix in a published version, or
  a question answered. One meaningful notification beats a stream of empty ones.
- Fully qualify references in anything posted to GitHub - `confluentinc/parallel-consumer#NN` -
  since `upstream #NN` does not auto-link and a bare `#NN` resolves against whichever repo it lands
  in.

## Checking upstream for new activity

`scripts/upstream-sweep.sh` (read-only) lists upstream issues and PRs with activity since the
manifest's `last_swept`, and flags drift on tracked refs (recorded `open` but now closed or merged
upstream). `--since <date>` overrides the window; `--publish` updates a single fork tracking issue
(guarded, never spams). Run it periodically to catch new reports from users who do not know the fork
exists - that is its whole purpose, since upstream is otherwise static.

**It ignores our own comments, and must.** Every mirrored issue carries a backlink comment from us,
and posting one bumps that issue's `updated` timestamp - so a plain `updated:>=` search returns all
78 and the signal disappears entirely. An item is reported only when it was opened inside the
window, or someone who is not us commented inside it. Keep that filter if you touch the script;
without it the report is the tracker read back to you.

`last_swept` lives at the top of `upstream-map.yaml`. Bump it after acting on a sweep, or every run
re-reports the same items.

### `--audit` - closures the window cannot see

The default sweep is windowed (`updated:>=last_swept`), so anything last touched before the window
can **never** appear in it - which is exactly how upstream's 2023 administrative closures stayed
invisible for three years. `--audit` takes no window. It asks which *closed* upstream issues are
neither tracked in the manifest nor mirrored in the fork, flags days where an implausible number of
items closed at once, and lists zero-reply discussions (excluding release-announcement threads by
title). Bots are filtered from the PR analysis: dependabot self-closes superseded bumps in batches
that look identical to a sweep, and unfiltered they bury the real ones. Run against the live repo it
rediscovers both 2023 sweeps from scratch; "zero unaccounted" doubles as an end-to-end check that
the manifest's `fork_issues` linkage is correct.

Known blind spots, recorded so a clean audit is not mistaken for completeness: a PR closed *alone*
on a quiet day never trips the bulk-day heuristic, and a discussion with one dismissive reply is not
"zero reply". The audit narrows the field; only reading discharges the coverage obligation, which is
tracked in
[`docs/inflight/upstream-coverage-completeness.md`](inflight/upstream-coverage-completeness.md).

### Swept PR heads that only upstream had - now preserved as tags

A mirror records what a closed PR *said*. It does not keep the code. The 35 PRs closed in the
2023-06-15 sweep were reachable through `refs/pull/<n>/head` **in the upstream repository**, which is
not a copy we control: if that repository goes, the commits go with it, and a mirror describing work
whose diff no longer exists is close to useless.

Two things are **not** loss events, and both are easy to assume are. Deleting the *branch* behind a
PR does not lose the commit - `refs/pull/<n>/head` is held by the base repository and outlives its
branch. Nor does a contributor's fork vanishing: all 35 heads were raised from branches in
`confluentinc/parallel-consumer` itself (every `head.label` is `confluentinc:<branch>`), so no third
party holds any of them. **The single exposure is loss of `confluentinc/parallel-consumer`.**

There is a second, narrower risk, and it belongs to this fork rather than upstream: the 29 heads that
are safe are safe only because some `origin/*` branch *contains* them. Deleting one of those fork
branches can orphan a head that reads as preserved today. That - not any upstream branch - is what
the recurring check below has to watch.

Checked 2026-08-14 per PR with

```bash
git branch -r --contains <head> --list 'origin/*'
```

reconciled against a live `git ls-remote --heads origin`, because three stale local tracking refs
would otherwise have counted as safe. **Restricting to `origin/*` is the whole point of the check**:
a full clone of this fork also carries `upstream` (see AGENTS.md - `gh` defaults to the wrong repo
here), and a head contained only in `upstream/*` is exactly the case being looked for. A bare
`git branch -r --contains` searches every remote, so it would report the upstream-only heads as
preserved and the archive would never have been created. **29 were reachable from a branch that
still exists on this fork** -
note *reachable from*, not *raised from*: confluentinc#271's own branch is long gone and its head
survives only because an unrelated branch contains it, while confluentinc#22, confluentinc#270 and
confluentinc#405 have same-named fork branches that do **not** contain their heads, which is why they
are in the table below. **Six were reachable from nothing on this fork.** They are now pinned as
annotated tags:

| Upstream PR | Author | What it was |
|---|---|---|
| confluentinc#22 | astubbs | Dynamic concurrency control (WIP) |
| confluentinc#204 | astubbs | Run user functions on a Vert.x verticle instead of a Java thread pool |
| confluentinc#270 | astubbs | Shared-nothing architecture - partition events |
| confluentinc#405 | astubbs | Remove static state |
| confluentinc#443 | **Robbie-Palmer** | Python support |
| confluentinc#506 | astubbs | Fix chart links |

Each is tagged `archive/upstream-pr-<n>`. **The tag name, target SHA and check date are deliberately
not repeated here** - they live only in `sweep-2023-admin-closure.preserved_heads` in
[`upstream-map.yaml`](../src/docs/development/upstream-map.yaml), this repo's owner of fork-upstream
facts. A corrected SHA updated in one copy while the other still read as authoritative is exactly the
drift this section exists to prevent; the table above carries only what does not change.

Each tag's message carries the upstream title, author, head branch name and closure date, so the
provenance survives without the upstream thread. confluentinc#443 is worth one note: it is the only
one raised by an outside contributor, that contributor's own fork is already gone, and it made no
difference - the head was on `confluentinc:pyallel-consumer` like every other, which is why the
exposure above is stated as upstream-repository loss and nothing else.

Tagging is deliberate over branching: tags are not swept by branch-cleanup tooling and read as
archival rather than live work. An annotated tag is also fetched by every clone, which
`refs/pull/<n>/head` is not - so the copy actually propagates. Note the tags do **not** put the
objects outside the GitHub fork network; that is acceptable because deleting a public parent re-roots
the network to a surviving fork rather than destroying its objects. If an out-of-network copy is ever
wanted, `git bundle` is the tool, and nobody has decided it is needed.

Recording the SHAs in the manifest is what makes the check redoable without re-querying upstream; the
PR numbers alone did not allow it.

Their objects are not reachable from any branch, so a plain `git fetch` in a clone made before they
were pushed will not bring them down - use `git fetch origin --tags` to get the commits locally.
Name the remote: on a branch tracking `upstream`, a bare `git fetch --tags` fetches from there and
leaves all six fork-only tags unavailable. Verifying
the tags still exist needs no fetch at all: `git ls-remote --tags origin 'archive/upstream-pr-*'`
asks the remote directly.

**The same pass, run over branch tips instead of swept PR heads (2026-08-17):** every
`upstream/*` branch tip was checked for containment in any origin branch *or tag* (the tag half is
what the 2026-08-14 command missed - `git branch -r --contains` cannot see the archive tags, so
re-running it verbatim would report four already-preserved heads as lost). Ten non-bot tips were
reachable from nothing on this fork and are now pinned as `archive/upstream-branch/<name>` annotated
tags: the release-line branches (`0.5.3.x`, `v0.5.2.x-dev`, `v0.6.x`), upstream's final `master`,
`docs/back-pressure` (the swept confluentinc#508 head, out of the 2026-08-14 pass's scope),
`features/batching`, `PL-176/DontDrainIssue` (content unassessed - flagged in
`docs/inflight/branch-audit-orphans.md`), `python-cd-pipeline`, `correct-failing-license-check`,
and `DP-12547` (already ruled out as content below; pinned so the ruling stays checkable). The 18
dependabot/renovate/chore branches were deliberately not preserved - recreatable version bumps, not
work. Tag names, SHAs and check date live only in `preserved_branch_tips` in
[`upstream-map.yaml`](../src/docs/development/upstream-map.yaml), same contract as
`preserved_heads`.

**Re-running this is not yet automated.** Branches get deleted, so a head safe today can be orphaned
tomorrow - but no script checks it: `--audit` covers tracking and mirroring, not reachability, and
would report clean with every tag above deleted. Until a containment check is wired into
`upstream-sweep.sh`, this is a manual step to repeat whenever the sweep cohort is revisited. Tracked
with the rest of the decision backlog in astubbs#300.

### Surfaces checked and ruled out

Recorded here so they are not re-investigated (established 2026-08-07):

- **Wiki** - disabled upstream (`has_wiki: false`).
- **Security advisories** - none published.
- **Milestones** - three still open (0.3.1, 0.5.1, 0.6); their only open issues (confluentinc#27,
  confluentinc#192, confluentinc#78) are all already mirrored.
- **Orphan branches never attached to a PR** - `v0.6.x`, `v0.6.x-dev`, `0.5.3.x`, `v0.5.2.x-dev`,
  `DP-12547`. `v0.6.x-dev` is 78 non-release commits of the lambda-actor-bus work already captured
  via the swept PRs confluentinc#325 and confluentinc#524. `0.5.3.x`'s regression fix
  (confluentinc#362, state truncation vs commit order) **is** on master as `a908e1663` - verified,
  not a lost fix. `DP-12547` shares no ancestor with master; it is Confluent-internal service
  config.
- **"Upstream pushed today"** - misleading. `pushed_at` moves on branch and tag activity; the
  newest actual commit is 2026-05-28 (`rmoff`, "Add link to fork"). No new upstream code activity.
