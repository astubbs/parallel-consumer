# Mirror the upstream issue tracker into the fork

**Status:** **done for issues** (2026-08-05). All 78 upstream issues are mirrored, diagnosed and backlinked; 7 are closed against a released version. Phases 3-5 are done too; nothing outstanding in this plan. See *What actually happened* below - the execution diverged from the plan in ways worth knowing.
**Date:** 2026-08-04
**Scope:** all 78 open issues in `confluentinc/parallel-consumer` - **all 78 mirrored 2026-08-05**
**Ledger entry:** retired - the work landed, so the `docs/inflight/` entry was deleted per the rule that
inflight files do not outlive their work. The durable half now lives in `AGENTS.md` → *Mirror format*
(how to write a mirror) and *Backlinking upstream* (that the bulk run is finished).
**Related:** `src/docs/development/upstream-map.yaml`

## Why

The fork tracks its relationship to upstream in `upstream-map.yaml`: a third store, reconciled by hand,
holding facts that also live in two issue trackers. Every audit of it is manual archaeology. That is
parallel state and it will keep costing.

Mirroring collapses it. Afterwards **the fork's own tracker is the tracker** - contributors do not need
to know upstream exists to work the backlog, and users can subscribe and be notified on fixes in the repo
that actually ships releases.

The second driver is preservation. Upstream is judged **likely to be archived**, timing unknown. Once
archived it becomes read-only: the issues stay readable, but nobody can add anything to them ever again.

The upstream tracker is not a live system to stay in sync with - 40 of its 78 open issues have had no
activity since 2023. **That is when the maintainer left Confluent, not when the problems stopped.**
Dormancy here means unattended, not resolved, so **age is deliberately not used as a triage signal
anywhere in this plan.** What it does mean is that a one-off mirror will not meaningfully drift.

## The archive asymmetry - this sets the ordering

Archival kills writes, not reads:

| | Survives archival? | Implication |
|---|---|---|
| Reading upstream issue content | **Yes** | The mirror can be built at any time, even years after archival. Not time-critical. |
| Commenting on an upstream issue | **No** | Backlinks are only possible while upstream is open. Time-critical. |

And a per-issue backlink has to name the mirror it points at, so **the mirror gates the half that has a
deadline**. That is the argument against deferring this indefinitely once 0.6.0 is out.

If archival starts looking imminent before the mirror is ready, the hedge is a short fork-awareness
comment on the high-traffic issues - it needs no mirror to exist and still survives.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Scope | All 78 open issues, in bulk | An unconditional import needs no per-issue judgement, which is what made the incremental version a mess |
| Body | **Summary capturing the original**, not verbatim | A placeholder landing page that preserves substance; also avoids copying others' text wholesale |
| `@mentions` in mirrors | **None** | A summary has none to strip; nothing is added |
| Backlink upstream | **One comment per upstream issue**, naming its fork mirror number | The durable cross-repo link, and the thing archival takes away |
| Already-fixed issues | Mirrored too | Gives a fork-side landing page to tag people against when announcing the fix |
| Notifying authors | Only when actionable - a fix shipped, a question answered | One meaningful notification beats 78 empty ones |
| Blocking 0.6.0 | **No.** Manual comments on affected issues until then | The release is higher-value and directly answers [`confluentinc#906`](https://github.com/confluentinc/parallel-consumer/issues/906)/`#907`/`#880`/`#885` |
| Renumbering code refs | **No** - see below | |

### Why we are not renumbering the codebase

An earlier framing paired the mirror with rewriting every issue reference in the tree to fork-local
numbers. Dropped:

- **Git history is immutable.** Four months of commits, the branch
  `bugs/857-paused-consumption-multi-consumers-bug`, merged PR titles reading `#857 family`, and the
  32-comment upstream thread all say 857. Renumbering the working tree leaves dual vocabulary anyway - it
  adds a translation step without removing one.
- ~~**It fights upstream-PR compatibility.**~~ **Withdrawn 2026-08-05.** This originally read as a
  stated fork goal, but nothing is going to be sent upstream - upstream is unmaintained and heading
  for archival, so there is no patch-compatibility constraint to preserve. The decision below stands
  on the other three reasons; this one no longer carries weight and should not be cited again.
- **A mechanical remap is unsafe.** Of ~1,348 `#NNN` refs across 77 files, the Java-source hits include
  `#999`, `#123`, `#35`, `#200` - test offsets and record counts, not issues. Demonstrated on 2026-08-05:
  a regex sweep over one PR's touched files rewrote 77 refs and got three classes of them wrong -
  `CHANGELOG.adoc` entries below `0.6.0.0` (which its own header says already mean upstream), every
  number in `upstream-map.yaml` (all upstream by construction, so the prefix is noise), and an
  `AGENTS.md` commit-subject convention shown *by example*, where rewriting the example changes the
  documented rule. Reverted and redone as explicit reviewed replacements.
- **Fork numbers will collide with upstream's range.** The fork is at astubbs#112; upstream reaches `#920`. Fork
  `#857` will eventually exist and mean something else entirely.

The underlying problem - an unqualified number resolving against the wrong repo - is already solved by
the reference convention this repo adopted (then at docs/inflight/AGENTS.md "Reference convention" -
heading retired in astubbs#324, readable via `git show e7af9e897^:docs/inflight/AGENTS.md`;
[`docs/issue-references.md`](../issue-references.md) owns the rule today): bare `#NN` is the fork,
`upstream #NN` is upstream. Phase 4 extends it rather than replacing it.

## Label scheme

Three orthogonal axes. Nothing encodes a fact that decays.

**Provenance:** `upstream-mirror` on all 78.

**Area** - exactly one per issue, 12 labels:

| Label | n | Upstream issues |
|---|---|---|
| `area/reliability` | 14 | 326, 402, 546, 597, 777, 803, 809, 833, 843, 857, 859, 875, 894, 912 |
| `area/compat` | 7 | 299, 825, 862, 874, 878, 896, 904 |
| `area/release-health` | 6 | 172, 177, 880, 885, 906, 907 |
| `area/build-test` | 7 | 103, 130, 162, 259, 290, 526, 861 |
| `area/error-handling` | 7 | 196, 304, 310, 391, 550, 718, 887 |
| `area/batching-ordering` | 6 | 266, 314, 321, 551, 560, 902 |
| `area/observability` | 6 | 27, 71, 192, 629, 631, 640 |
| `area/api` | 7 | 78, 170, 186, 520, 782, 860, 879 |
| `area/modules` | 4 | 180, 480, 484, 543 |
| `area/performance` | 4 | 322, 394, 540, 884 |
| `area/internals` | 4 | 200, 233, 241, 903 |
| `area/docs` | 6 | 109, 115, 171, 178, 622, 642 |

Total 78. All numbers above are **upstream** issue numbers.

**Type** - exactly one: `bug` · `feature` · `question` · `chore`. (Renamed from `enhancement` on
2026-08-05, so the label matches the vocabulary the changelog and commit convention already use -
`* feature:` and `feat:` - which matters because releases after 0.6.0.0 are generated from the commit
log.) Upstream's own labels are too
sparse to inherit (only 14 of 78 carry any), but where they exist (`blocker`, `1.0` - renamed from `ver:1.0` - `verified bug`,
`good first issue`, `not-a-bug`, `wait for info`, `help wanted`) they are preserved. Upstream's
`high`/`medium`/`low` priority labels are deliberately dropped: stale triage calls from an
unmaintained tracker.

**No `stale` label** - it is a claim about a date, so it rots on first comment. Upstream created/updated
dates and comment count go in the mirrored body, where they stay accurate by construction.

## Mirror format

> **Superseded as a spec - kept as the record of what was proposed.** The live convention is
> `AGENTS.md` → *Mirror format*; follow that when creating or editing a mirror. Two ways this section
> no longer matches reality: the title prefix below was **not** what shipped (the import used
> `upstream #NNN:`, which all 78 mirrors carry), and the body template predates the rule that a mirror
> records its **upstream title verbatim** in the header, which was added on 2026-08-06 after
> astubbs#118 was retitled. Left unedited otherwise - a dated plan is a record of the decision, not a
> place to retcon it.

Title:

```
confluentinc#857: Paused consumption across multiple consumers
```

Body:

```markdown
> **Mirror of [confluentinc/parallel-consumer#857](https://github.com/confluentinc/parallel-consumer/issues/857)**
> Opened by [rbokade-rbk](https://github.com/rbokade-rbk), 2025-03-14 ·
> 32 comments upstream · last upstream activity 2026-04-13
> Summarised, not copied. Discussion belongs here - upstream is unmaintained and may be archived.

## Summary
<2-6 sentences: symptom, conditions, what the thread established>

## Fork status
<fixed-in / investigating / not started, with links to fork PRs and docs/solutions/ entries>
```

Two mechanical rules for generated text:

1. **No `@mentions` anywhere.** Author attribution is a profile *link*, which renders identically and
   notifies nobody. The summary must not reproduce mentions from the thread.
2. **Cross-repo references are fully qualified** - `confluentinc/parallel-consumer#NN`, not the
   repo's prose form `upstream #NN`. This is the one place the house convention does not apply:
   `upstream #NN` is prose that GitHub does not auto-link, and a bare `#NN` in a fork issue silently
   resolves against the fork's own numbering. Content *posted to GitHub* needs the qualified form;
   content *in this repository* keeps `upstream #NN`.

## Backlink comment

One comment per upstream issue, posted after its mirror exists:

```markdown
Tracked in the maintained fork as astubbs/parallel-consumer#<N>.

This project appears unmaintained; the fork ships releases and continues this issue there.
Nothing here is closed or changed by this comment.
```

72 further comments will notify each issue's subscribers. That is defensible - it is the "something to action"
case, and it is precisely the link archival would make impossible - but it is a real notification event
and should be paced, not fired as a burst.

## Executed: the six-issue rehearsal (2026-08-05)

**This part is no longer proposed - it has been done.** Phase 2 asks for a rehearsal on three issues
before the bulk run; this is that rehearsal, run on the six issues the fork has actually worked on,
so it validates the scheme against real content rather than throwaways.

| Upstream | Fork mirror | Area | Fork work | Closes on merge? |
|---|---|---|---|---|
| [`confluentinc#233`](https://github.com/confluentinc/parallel-consumer/issues/233) | astubbs#117 | `area/internals` | astubbs#57 (partial) | **No** - only one instantiation site removed, not the refactor |
| [`confluentinc#326`](https://github.com/confluentinc/parallel-consumer/issues/326) | astubbs#118 | `area/reliability` | `afde8c5e` | **No** - the landed fix *relates to* this, unconfirmed as the same failure |
| [`confluentinc#857`](https://github.com/confluentinc/parallel-consumer/issues/857) | astubbs#119 | `area/reliability` | astubbs#80, astubbs#100, astubbs#108 merged | **No** - the original deadlock is still open in astubbs#29 |
| [`confluentinc#859`](https://github.com/confluentinc/parallel-consumer/issues/859) | astubbs#120 | `area/reliability` | astubbs#57 | **Yes** |
| [`confluentinc#894`](https://github.com/confluentinc/parallel-consumer/issues/894) | astubbs#121 | `area/reliability` | astubbs#57 | **Yes** |
| [`confluentinc#912`](https://github.com/confluentinc/parallel-consumer/issues/912) | astubbs#122 | `area/reliability` | astubbs#116 | **Yes** |

Labels created: `upstream-mirror`, `area/reliability`, `area/internals`, `chore`. Four, not the full
twelve - the rest are created by the script when it first needs them.

### What the rehearsal changed in this plan

**Auto-close replaces the post-merge step.** `Fixes #NNN` in the fixing PR's body means GitHub closes
the mirror on merge. This removed a whole artefact: `docs/runbooks/pr57-post-merge.md` existed only
because closing and announcing had to be remembered and done by hand. Phase 2 step 5 ("close the
already-fixed mirrors") is therefore only needed for issues whose fix has *already* merged; anything
still in review should be wired with `Fixes` instead.
<!-- file-refs: N/A - the sentence records that this artefact was removed -->

**"Already fixed" needs three states, not two.** Of the six, only three could honestly be closed.
[`confluentinc#233`](https://github.com/confluentinc/parallel-consumer/issues/233) is partially addressed, [`confluentinc#857`](https://github.com/confluentinc/parallel-consumer/issues/857) is two-thirds fixed with the original defect
still open, and [`confluentinc#326`](https://github.com/confluentinc/parallel-consumer/issues/326) has a fix that merely *might* be the same bug. A binary
fixed/not-fixed field would have produced three wrong closures. The `## Fork status` section must
therefore state which, and the script must never infer "fixed" from the existence of a linked PR.

**`fork_issue` works and is worth doing eagerly.** Phase 3 defines it; two entries in
`upstream-map.yaml` now carry it. Adding it as each mirror is created, rather than as a migration at
the end, keeps the map honest in the interim.

**Upstream PRs need comments even though they are not mirrored.** [`confluentinc#893`](https://github.com/confluentinc/parallel-consumer/issues/893) and `#905` are
carried in astubbs#57. They get no mirror - the plan is right that PRs stay in the map - but the *authors*
deserve to know their work ships somewhere, and a reader of those PRs deserves the pointer. Add to
Phase 1: the script must comment on carried and superseded upstream **PRs** as well as issues, with
text that makes clear authorship is unchanged.

**Posting to the upstream repo needs a human.** Agent tooling refused to bulk-post into
`confluentinc/parallel-consumer`, so the eight rehearsal comments went out via a reviewed script run
by hand. Phase 2 step 4 should not assume the remaining comments can be fired by automation.

**Write the comment for the reader, not the tracker.** Five review passes over the eight bodies cut
roughly a third: PR base branches and commit-counts, "so the fork mirror stays open" (our
bookkeeping), a walkthrough of `LinkedHashSet` vs `List`, and hedging like "a maintained community
fork" and "one of its original authors" - the fork is *mine*, ~99% of the commits, and distancing
language just muddies it. Two rules for the generator: link the PR rather than describe its state,
and never write anything that will be stale in a week. Also learned: "this project is no longer
maintained" reads on an upstream issue as *Parallel Consumer is dead* - name the Confluent-hosted
version specifically, and say the project continues.

## What actually happened (2026-08-05)

The whole issue half ran in a day, by hand and with parallel subagents rather than the script this
plan specified. Phase 1 was never written.

### Result

| | |
|---|---|
| Upstream issues mirrored | **78 of 78** (fork astubbs#44, astubbs#117-astubbs#195) |
| Backlink comment posted upstream | **78 of 78** |
| Mirrors carrying a code-backed diagnosis | **78** |
| Closed against a released version | **7** - astubbs#125, astubbs#140, astubbs#166, astubbs#176, astubbs#182, astubbs#184, astubbs#188 |
| Targeted `0.6.0.0` | 22 |
| `next-feature-release` / `next-breaking-release` | 14 / 14 |
| `pr-available` | 16 (7 of them orphaned upstream PRs) |

### How the labels evolved past this plan

The three axes held (`upstream-mirror` + one `area/*` + one type), but the type label was renamed
`enhancement` → **`feature`** to match the vocabulary the changelog and commit convention already use,
and four axes were added that this plan did not anticipate:

- **`fixed-in/<version>`** and **`partially-fixed-in/<version>`** - a bare version number cannot
  distinguish "reported against" from "fixed in", and both get said.
- **`affects/<version>`** - the version a reporter named.
- **`pr-available`** - an implementation exists, either an open fork PR or one orphaned upstream.
- **`next-feature-release` / `next-breaking-release`** instead of `0.6.1.0` / `0.7.0.0`, so a target
  does not commit to a version number that has not been decided. A `next` label was tried and dropped
  as meaningless.

### The title format evolved past this plan, twice

The import shipped `upstream #NNN: <title>`, not the `confluentinc#NNN:` this plan proposed - so every
one of the 78 carries the `upstream` form. Nothing recorded that at the time; it surfaced on
2026-08-06 while retitling astubbs#118.

Then the description half stopped being upstream's words. astubbs#118 was mirrored as *"Error in
onPartitionsAssigned in parallel consumer"*, which names where a failure surfaced and nothing a
sufferer would search for; once astubbs#217 established the cause it was retitled to lead with the
error string. That made the upstream title worth keeping, so mirrors now record it verbatim in the
header block - unconditionally, not only when retitled, so a reader never has to open upstream to
learn whose words a title is. See `AGENTS.md` → *Mirror format*, which is the live spec.

### The manifest-driven backlink tooling was retired

`scripts/upstream-backlink.sh`, its two templates (`fix-backlink`, `fork-awareness`) and
`src/docs/development/upstream-backlink-plan.md` are **deleted**. So are `upstream-map.py`'s
`posted-refs` and `todo` subcommands, which existed only to serve that script, and the `backlink:`,
`forwarded:` and `todo:` fields it read.
<!-- file-refs: N/A - the sentence states these are deleted -->

It commented on one upstream issue per manifest entry. After the mirror there are no issue entries -
the map tracks the upstream **PRs** we may carry, and every upstream issue already has its backlink.
The tool had nothing left to point at.

It also had a live hazard. Its anti-spam guard called `posted-refs`, which read the `forwarded:`
field removed in the map slim - a guard reading a field that no longer exists **fails open**, and
would have permitted a second comment on someone else's issue. That was fixed first (marker in the
template, checked against the live thread) and then the whole thing was retired, because the fix
made it clearer that the tool was doing a job the mirror had already absorbed.

What replaces it is written into AGENTS.md: comment directly with `gh`, put a hidden marker in the
body and check for it before posting, never `Fixes`/`Closes` cross-repo, and comment a second time
only when there is something to act on.

### What we got wrong, and what it cost

**A grep sweep is not a diagnosis.** The first attempt at "is this already implemented?" probed the
codebase for strings related to each issue and reported 11 as IMPLEMENTED. At least 9 were false: the
probe found *the code the issue is about* - the log line being complained about, a class merely
mentioned - and read its presence as a fix. Several matched a commit called `Rename: Script` from
0.1.0, which should have been the tell. **Discarded entirely and redone by reading each issue.**

The probes that *did* work shared one property: the string could only exist if the request had been
implemented (`class PCMetrics`, `pc-control-<id>`, the escape-hatch option name). That distinction is
the rule for any future automation here.

**Attributing a fix to a released version**, in strength order - worth reusing rather than
re-deriving:

1. A `CHANGELOG.adoc` section that mentions the issue number gives the version directly.
2. `git log --oneline -S "<code that could only exist if implemented>"` and take the **oldest** line -
   `-S` with `-1` returns the *newest* touch, which is the wrong end and will date a fix years late.
   Then `git tag --contains <sha>` and take the lowest `0.x` tag: that is the first release carrying it.
3. Failing both, probe the code for the artefact and say plainly that the version is unknown.

And one that cost three wrong mirror bodies: **check the artefact repository, not the issue thread.**
`repo1.maven.org/maven2/io/confluent/parallelconsumer/parallel-consumer-core/` settles "was this ever
published" in one request. A reporter's account of what is on Central is accurate on the day they
write it and goes stale silently.

**Upstream's own "fixed by" annotations are unreliable.** Verified with `gh pr view`: [confluentinc#346](https://github.com/confluentinc/parallel-consumer/pull/346), [#270](https://github.com/confluentinc/parallel-consumer/pull/270), [#325](https://github.com/confluentinc/parallel-consumer/pull/325), [#181](https://github.com/confluentinc/parallel-consumer/pull/181),
[#179](https://github.com/confluentinc/parallel-consumer/pull/179) and [#197](https://github.com/confluentinc/parallel-consumer/pull/197) are all closed with `mergedAt=null` (note that confluentinc#197 is a
different thing entirely from this fork's astubbs#197 - precisely the collision the convention prevents). Around 40 of 53 open PRs were closed on
**2023-06-15**, when the maintainer left Confluent - an administrative sweep, not review outcomes. So
`confluentinc#186` says "Fixed by: confluentinc#346" and nothing was fixed. Seven mirrors now carry an
**Unmerged work** section saying which PR exists, its revival rank from
`upstream-pr-analysis.adoc`, and that it was orphaned rather than rejected.

**Do not trust an issue's own account of what is published.** Three mirrors said 0.5.3.3 "was never
published to Maven Central" - true when the issue was written, false by the time we repeated it.
0.5.3.3 reached Central on **2025-08-28**, three weeks after `confluentinc#885` asked where it was.
Corrected, and the people blocked on it were told. **Check the artefact repository, not the
conversation.**

**Auto-close is silently fragile.** `Fixes #NNN` only closes on PRs targeting the **default branch** -
astubbs#29 had the keyword and GitHub ignored it because it targeted `master-confluent`. And a later body
rewrite on astubbs#116 dropped its `Fixes` block entirely without anyone noticing. Verify with
`gh pr view N --json closingIssuesReferences`; never assume it took.

### What worked

**Fan-out for diagnosis, single voice for writing.** Six subagents took eight issues each, investigated
the code and returned structured reports; all edits were applied centrally. That kept 78 write-ups
consistent and reviewable, and meant one reviewer caught the mistakes above rather than six agents
each committing their own.

**Notify only when actionable, and it pays.** Seven upstream threads got a second comment naming a
released version - the bar this plan set. Four people who reported problems between 2022 and 2025 got
a usable answer, three of them able to act immediately.

**Diagnosing found bugs nobody had reported.** Six defects surfaced from reading code to answer
something else, now in `docs/inflight/release-0.6.0.0.md` - including a WARN that fires permanently for
anyone following the README's own tuning advice, and an offset-encoding forward-compatibility hazard
where an unknown magic byte throws before the configured policy is consulted.

## Phases

### Phase 0 - manual comments — **DONE**

Comment manually where there is already something real to say. Highest value first.

**The first six, plus the two carried PRs, were posted on 2026-08-05.** Comment URLs are recorded
in `upstream-map.yaml` under `forwarded:` for every entry the map covers ([`confluentinc#233`](https://github.com/confluentinc/parallel-consumer/issues/233) and
`#326` have no map entry - their mirrors are the record).

- **[`confluentinc#857`](https://github.com/confluentinc/parallel-consumer/issues/857)** - the 2026-04-13 comment still says *"I think I fixed it"* and points at fork
  PR astubbs#29, which never merged. astubbs#100, astubbs#80 and astubbs#108 have since landed. Four months stale on a 32-comment
  thread with users waiting. **Posted** - mirror astubbs#119.
- **[`confluentinc#859`](https://github.com/confluentinc/parallel-consumer/issues/859)** + carried PRs [`confluentinc#893`](https://github.com/confluentinc/parallel-consumer/issues/893) / `#905` - fork PR astubbs#57. **Posted** - mirror astubbs#120.
- **[`confluentinc#912`](https://github.com/confluentinc/parallel-consumer/issues/912)** - vertx leak, fixed in astubbs#116. **Posted** - mirror astubbs#122.
- **[`confluentinc#326`](https://github.com/confluentinc/parallel-consumer/issues/326)**, **[`confluentinc#233`](https://github.com/confluentinc/parallel-consumer/issues/233)** - **Posted** - mirrors astubbs#118, astubbs#117.
- **[`confluentinc#907`](https://github.com/confluentinc/parallel-consumer/issues/907)** - "is the project still actively maintained?" Answer by pointing at the fork.
  Not yet mirrored.
- **[`confluentinc#909`](https://github.com/confluentinc/parallel-consumer/issues/909)** - fork PR astubbs#31. Not yet mirrored.

### Phase 1 - the script — **NOT BUILT** (superseded)

The 78 issues were created and diagnosed without it, so the script was never written. Kept for
reference in case a second bulk import is ever needed; if so, note that the summaries were the
expensive part and they do not automate - the mechanical rules below still apply.

`scripts/upstream-mirror.py` would have followed the conventions of the (now retired)
`upstream-backlink.sh`:
<!-- file-refs: N/A - describes a script that was never written -->

- **dry-run by default**, `--post` to execute, confirmation prompt even then
- `--limit N` / `--only <numbers>` for staged rehearsal
- **idempotent** - skips any upstream number that already has a fork issue titled `upstream #N:`, so a
  partial run resumes safely
- ascending upstream-number order, so fork numbers ascend predictably
- **paced**: ~3s between writes. ~72 creates plus ~72 comments will otherwise hit GitHub's secondary
  content-creation limit
- emits `src/docs/development/mirror-map.tsv` (`upstream# → fork# → title`) as the durable migration record
- creates labels first, idempotently
<!-- file-refs: N/A - a planned output of a design that was not built -->

Summaries are editorial, so they are drafted into a curated file the script only renders, and reviewed
before posting. Unit tests cover the two mechanical rules (no `@`, qualified cross-repo refs).

### Phase 2 - rehearse, then run — **DONE**, by hand

1. Dry-run the remaining 72; read the generated summaries. The 6 already mirrored
   ([`confluentinc#233`](https://github.com/confluentinc/parallel-consumer/issues/233), `#326`, `#857`, `#859`, `#894`, `#912`) are skipped by the idempotency rule -
   they already have a fork issue titled `upstream #N:` - so no special-casing is needed.
2. `--post --limit 3` on low-traffic self-authored issues ([`confluentinc#241`](https://github.com/confluentinc/parallel-consumer/issues/241), `#290`, `#322` - zero
   comments each). Inspect the rendered result.
3. Full `--post`. Verify 72 created (78 mirrors total), one `area/*` and one type label each, no
   `@` in any body.
4. Post the backlink comments.
5. Close the already-fixed mirrors ([`confluentinc#857`](https://github.com/confluentinc/parallel-consumer/issues/857) if astubbs#29 has landed, `#859`, `#912`) and any answered
   by 0.6.0 (`#906`, `#907`, `#885`, `#880`), each with a pointer to the fix.

### Phase 3 - slim `upstream-map.yaml` — **DONE**

The map is a PR-tracking table, not a parallel store: 24 entries down to 14. The mirror covers
issues, so the map holds only what the mirror cannot -

- the **upstream PRs** we may cherry-pick ([confluentinc#893](https://github.com/confluentinc/parallel-consumer/pull/893),
  [#905](https://github.com/confluentinc/parallel-consumer/pull/905),
  [#909](https://github.com/confluentinc/parallel-consumer/pull/909),
  [#915](https://github.com/confluentinc/parallel-consumer/pull/915),
  [#908](https://github.com/confluentinc/parallel-consumer/pull/908),
  [#866](https://github.com/confluentinc/parallel-consumer/pull/866),
  [#867](https://github.com/confluentinc/parallel-consumer/pull/867),
  [#901](https://github.com/confluentinc/parallel-consumer/pull/901), the security batch, the dep
  bumps) - not issues, so not mirrored
- `adoc_anchor` links into `upstream-pr-analysis.adoc`
- fork branch names and `last_swept`

Surviving entries carry `fork_issue: N`; the issue-only entries collapsed into their mirrors and were
deleted. `scripts/upstream-map.py` and `upstream-sweep.sh` cover PRs only, and the AGENTS.md file-map
table matches.

### Phase 4 - enforce the reference convention — **DONE**

The convention itself already exists (docs/inflight/AGENTS.md's then "Reference convention" section -
retired in astubbs#324, `docs/issue-references.md` owns it now -
`CHANGELOG.adoc`, `docs/refactoring.md`): bare `#NN` is the fork, `upstream #NN` is upstream. What is
missing is enforcement and the GitHub-rendering carve-out:

- lint in the existing `PR Checklist` gate: flag a bare `#NN` on changed lines where `NN` is outside the
  fork's current number range and not already written as `upstream #NN`
- document the carve-out from the mirror format above - content *posted to GitHub* uses
  `confluentinc/parallel-consumer#NN` because `upstream #NN` does not auto-link there
- existing references are corrected opportunistically as files are touched, never bulk-rewritten

### Phase 5 - fork mirror number in PR titles — **DONE**

Titles carry both numbers on the five single-mirror PRs: astubbs#29, astubbs#31, astubbs#51, astubbs#57, astubbs#116.

astubbs#53 and astubbs#106 span or only partly address several mirrors, so instead of an unwieldy title they
carry a **Related issues** section listing the mirrors **without** a `Fixes` keyword - verified
0 closing references on each. A title listing three mirror numbers helps nobody, and `Fixes` on
partial work is the error the astubbs#117, astubbs#118 and astubbs#119 mirrors already document.

Now that every upstream issue has a fork mirror, an open PR that fixes one should say so in its
**title**, in fork terms - that is the number a reader of this repo can click, and the number that
`Fixes #NNN` acts on. The upstream number **stays**: it is what four months of commits, branch names
and the upstream threads all use, and dropping it would break the trail.

So a title carries both, e.g.
`fix(core): paused consumption after rebalance (#119, confluentinc#857)`.

- Sweep the open PRs and add the mirror number where one exists (astubbs#29, astubbs#31, astubbs#51, astubbs#53, astubbs#8, astubbs#106, astubbs#116
  are the candidates - each maps to a mirror via `mirror-map.json` or the issue title).
- Where the PR genuinely *closes* the mirror, also add `Fixes #<mirror>` to the body. **Careful:
  GitHub only honours that on PRs targeting the default branch** - discovered on astubbs#29, which targets
  `master-confluent`, so its `Fixes` lines are inert until it is retargeted to `master`.
- Do **not** add `Fixes` for partial fixes. astubbs#106 and astubbs#51 only partly address their mirrors, and the
  astubbs#117, astubbs#118 and astubbs#119 cases already showed that auto-closing on "a PR mentions it" closes issues that
  are not actually resolved.

## Risks

| Risk | Mitigation |
|---|---|
| Upstream archived before backlinks land | Phase 0 comments go out now, independent of the mirror; fork-awareness hedge if archival looks imminent |
| Burst of 72 notifications reads as spam | Paced writes; one comment per issue, never a second |
| Summaries misrepresent the original | Dry-run review before posting; the body links the source so anyone can check |
| Copied references linking to the wrong repo | Qualification rule + unit test + post-run grep |
| Bad full run | Rehearse on 3 first. Issues are admin-deletable, but the fork's number sequence does **not** reclaim the gap - a bad run permanently burns ~78 numbers |
