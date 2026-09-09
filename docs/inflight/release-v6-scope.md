# v6 (0.6.0.0) release scope and burn-down

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

**The source of truth for 0.6.0.0**: the owner's decisions, the burn-down to the tag, every open
question, and the tag-day checks. [`release-0.6.0.0.md`](release-0.6.0.0.md) holds the published
wording and answers "is it ready?"; this note answers "is it enough?" and, since the answer came
back "yes, once the fixes already built are merged", carries the list. Its own file rather than a
section, so it sits in the session-start index where an agent trips over it. Named
`release-when-is-v6-good-enough.md` until 2026-09-08, when it stopped being a question.

## Every v6 note, and what each one owns

**This file is the source of truth for the release.** astubbs#197 is the tracking *handle* - the
thing to link from PRs, mirrors and upstream threads - and since 2026-09-09 its body says only that
and points here; nothing is maintained on the issue. Everything else below owns one thing:

| Where | Owns |
|---|---|
| [`release-v6-scope.md`](release-v6-scope.md) (this file) | Scope, the burn-down, every open decision, the tag-day checks, the upstream survey |
| [`release-0.6.0.0.md`](release-0.6.0.0.md) | The published wording: the release-note draft and the breaking-change record; "is it ready?" |
| [`release-0.6.0.0-issue-response-drafts.md`](release-0.6.0.0-issue-response-drafts.md) | Issue replies held until the release note exists; posted in the after-it-ships sweep |
| [`release-three-mirrors-label-undecided.md`](release-three-mirrors-label-undecided.md) | Three closed mirrors that may belong under the `0.6.0.0` label - owner's call |
| [`release-groom-1-0-train-issue.md`](release-groom-1-0-train-issue.md) | The 1.0 train issue, groomed against the roadmap data after v6 ships |
| [`release-experimental-module-records.md`](release-experimental-module-records.md) | Feature records for the modules that do NOT ship in v6 (deferred, 2026-09-07 decision) |
| [`process-candidate-ranking.md`](process-candidate-ranking.md) | Candidate ranking, the vetting sweep's reading of what gates v6, and the dated disposition of every open bug note |
| [`upstream-items-with-no-fix-and-no-response.md`](upstream-items-with-no-fix-and-no-response.md) | The survey of upstream items nothing addresses yet; after the tag |
| [`bug-857-family.md`](bug-857-family.md) | The register the release note's confluentinc#857 wording is read from |
| [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) | The flake register - a tag needs a green master |
| [`docs/releasing.md`](../releasing.md) | The mechanics: strip `-SNAPSHOT`, merge, `publish.yml` deploys and tags, `release.yml` cuts the GitHub release from the curated changelog section |
| [`docs/data/roadmap.yaml`](../data/roadmap.yaml), [`docs/data/module-maturity.yaml`](../data/module-maturity.yaml) | The claims rendered into the README; the maturity value is a tag-day recheck |
| `CHANGELOG.adoc` | Generated from the commit log at release time; working text until the tag |
| [`docs/plans/2026-07-28-release-pipeline-hardening.md`](../plans/2026-07-28-release-pipeline-hardening.md) | The dated plan for the publish pipeline |
| [`release-v6-announcement.md`](release-v6-announcement.md) | The announcement theme and plan, qualified on 2026-09-09 as 6.1 material with its figures from an experimental branch; what it lends 0.6.0.0 is the theme, the ordering and the experimental-claims rule |
| `release-v6-merge-order.md`, `release-0600-blockers.md` (deleted) | Folded into this file on 2026-09-08; `git show 2c874ecac:docs/inflight/release-0600-blockers.md` for the history |

**astubbs#197's body was shed to a pointer on 2026-09-09.** Everything it carried is either done on
master (its two artefact claims, its not-blocking list) or in the tiers and the tag-day list below
(the after-it-ships sweep, including the announcing comments on the three upstream threads that
asked for a release). The old body is in the issue's edit history.

## The decisions - 2026-09-07 by the owner, confirmed and extended 2026-09-08

**v6 is a bug release. It is overdue. The bar is the one already stated** in
[`release-0.6.0.0.md`](release-0.6.0.0.md) under *"This release is a stability release, and that is
the point"* - nothing else has to be true. The owner took four decisions on 2026-09-07 (recorded on
<!-- post-merge: checked - names astubbs#475 as the PR that merged the decisions branch, which stays true after it lands -->
branch `docs/v6-scope-decisions`, merged into astubbs#475) and confirmed the bar on 2026-09-08:

- **The bar is the stability release, and nothing else.** "Every known critical defect resolved,
  with a guard" is the whole bar. Anything not fixing an open bug or clearing a release gate is out
  of v6, however finished it looks. That one sentence settles the feature PRs without arguing each:
  the commit-failure seam, virtual threads, residence time, the proxy stack, the perf campaign.
- **Streams and Connect do not ship in v6.** Both moved to the `next-0x` horizon in
  `docs/data/roadmap.yaml`; the announcement carries them as what is coming. The feature-record
  note that waited on their modules is deferred.
- **The transactional revoke wait (astubbs#44) is outside v6 for now, and the claim names it.** Its
  fix (astubbs#408) is stacked on producer-fencing recovery (astubbs#410) by design, not by
  chronology: declining the lock is only safe once a fenced producer is recoverable. So the choice
  was the whole fencing work in v6 or the claim naming the exception, and the owner chose the
  exception. `release-0.6.0.0.md`'s release condition carries the amended claim, with the
  instruction to delete that paragraph if astubbs#410 lands before the tag.
- **The `0.6.0.0` issue label means "closes when the release ships".** Swept the same day; the
  features and the decision-only mirrors lost it.
- **Which open items are genuinely v6?** (2026-09-08) The merge queue below, and only it. A thing
  being ready is not a reason to ship it in this one.
- **Does the roadmap announcement have to be simultaneous?** (2026-09-08) No. Its plan is
  astubbs#446; it can land before the tag and take pressure off sooner. The announcement is what
  lets the first release be a bug release without being the only thing anyone sees.

**Why a stability release is the right first release.** The scope question carried an unstated
assumption: that the first release of a revived fork must be a feature release to justify the
attention it gets. The record does not support that. The change set since 0.5.3.3 is the largest
this codebase has shipped in one version: the confluentinc#857 commit-path deadlock, the torn-read
family, the metrics leak, offset accuracy on assignment, the async-commit acknowledgement, the
package rename, MDC propagation, the log-noise fixes, and the chaos and Lincheck lanes that guard
each of them. For the users this project serves, that is the release that matters. What people
learn about the direction of the project comes from the roadmap announcement, which is decoupled
from the tag, so the release does not need to carry it. The cost of the alternative is already on
record: the 26 August target passed with nothing shipped, and the previous version of this note had
named "never shipping" as the failure mode to avoid.

<!-- post-merge: checked-begin - the paragraph names astubbs#475 as the PR that merged and removed the note, which stays true after it lands -->
**Prior art this supersedes, and what it still gets right.** A merge-order note from 2026-08-08,
`release-v6-merge-order.md`, sat on the never-pushed branch `docs/v6-merge-order`; that branch was
merged into astubbs#475 and the file removed there, so the note is in history
(`git log --all --oneline -- docs/inflight/release-v6-merge-order.md`) and nowhere live. It ordered a far larger v6: the
transactional-atomicity trio first, then the loss and confluentinc#857 fixes, then **new surface**
(the health check, MDC, the mock consumer in the main jar), then **new opt-in modules** (the
dashboard, the Streams and Connect proofs of concept) and the examples rewrite, on the argument that
a first release whose job is to make "actively maintained" credible should ship the surface. Its
first two tiers have all merged. **The 2026-09-08 decision overrides its tiers three, seven and
eight**: new surface and new modules are not defects, so they do not gate a bug release, and the
announcement plan carries the "maintained, and past where upstream stopped" claim instead. Two of
its points survive: astubbs#197's body reads as more blocked than it is (since shed to a pointer), and
astubbs#199 - which it called the one item that cannot follow the tag - can, because the release
page body is posted by hand on the day (tier 3) and astubbs#199 only automates that. Its open question
of which modules v6 publishes is moot under this decision, since no module PR is in the queue.
<!-- file-refs: N/A - the merge-order note was merged and removed by astubbs#475; the path is cited as history -->
<!-- post-merge: checked-end -->

**"Draft" on a fork PR means "needs the owner to review and merge", not "unfinished".** Every PR in
the queue below is implemented, tested and green on everything except the human-LGTM gate and, where
it stacks, the dependency gate. `gh pr list -R astubbs/parallel-consumer` shows near enough every
PR as a draft, so the state flag carries no information here; what a PR still needs is written
against it below, because that is the part `gh` cannot say.

## Proposed cut-off and order of work

**Confirmed by the owner, 2026-09-09: the merge queue is closed as of today.** Nothing joins it that
is not already a fix for an open defect with a PR. Anything a soak or a chaos lane finds after
2026-09-09 is a 0.6.0.x unless it is data loss on a default configuration, and then it joins tier 1.
Tag when tier 3 is done, not when the "can follow" list is empty.

The order is chosen so that every merge is independently shippable - if the queue stops anywhere,
what is on master is still a release.

### Tier 1 - self-contained fixes, any order, each blocked only on LGTM

Data-shaped and stall-shaped, no design question open, no stack. These are the release.

- [x] **astubbs#470** - merged 2026-09-08. An async commit counts as committed when the broker
  answers, not when it is sent; silent loss on the shipped default commit mode. Serves astubbs#248.
- [x] **astubbs#466** - merged 2026-09-08. The revoke-path commit drains the work mailbox first; in
  transactional mode a rebalance could publish a transaction whose offsets omitted records it
  contained. Its proof left quarantine with it. Its commit body says it **collides with astubbs#408
  on `tryCommitOffsetsOnRevoke`**, so astubbs#408 now carries that resolution.
- [x] **astubbs#468** - merged 2026-09-08. `WorkContainer` equality is identity, so the stale
  sweep removes only the container it inspected, never a fresh replacement racing in from the
  controller. Marked breaking (`fix(core)!`) for the equality change; the release note carries it.
  Of the two further by-key removals astubbs#483 found, astubbs#468 dismissed one
  (`ProcessingShard.onSuccess` - the third staleness checkpoint drops a stale result first) and
  astubbs#492 fixed the other, the revoke sweep, merged 2026-09-09.
- [x] **astubbs#469** - merged 2026-09-08. The two remaining `PartitionState` flags that cross
  threads, measured and then fenced or redesigned; the follow-on astubbs#349 deliberately left.
- [x] **astubbs#481** - merged 2026-09-08. The poll thread never touches the retry queue; the
  controller collects what it leaves. The owner's own PR, superseding astubbs#431 (closed - correct
  and proven, but more machinery than the defect needed). Same stall from the confluentinc#857
  defect-class sweep: an unbounded fair write-lock wait inside the rebalance callback, spent out of
  `max.poll.interval.ms`. astubbs#483 (the shard-displacement reachability verdict) stacked on it
  and is being brought level.
- [x] **astubbs#473** - merged 2026-09-08. Cleared the two remaining quarantine entries by fixing
  what they were about; `docs/quarantined-tests.md` is empty and the release guard no longer blocks
  on it. It also moved the capacity profiles behind a `capacity` tag with scheduled runners, so
  their pass rate is measured rather than gating.
- [x] **astubbs#480** - merged 2026-09-09. An offset map whose run or bitset extends past the partition's log end
  offset is an unreadable payload, not a completed range. Silent skip of real records on a corrupt
  or foreign payload; proven red through the real assignment path, green with the guard, mutation
  lane green on the bound. Reworked off the wire before merge: the claim is settled lazily at the
  first poll batch against a watermark read without blocking from the consumer's own position and
  lag, so no broker round trip sits inside the rebalance callback. Promoted from the
  owner's-decision list on 2026-09-08.
- [x] **The `batchSize` validation bound** (astubbs#496, merged 2026-09-09) - `batchSize(0)` silently
  processed nothing and a negative value failed obscurely; one `validate()` bound in the options,
  in the style of its neighbours, now rejects zero, a negative and null (astubbs#311, the
  validation half only - the over-request arithmetic stays deferred, so the issue stays open). A
  startup exception where there was silence, so it carries `!` and the release note names it under
  breaking.
- [ ] **The gate-latch warning** (astubbs#497, draft, in review; decided 2026-09-09) - a WARN when
  the record-intake load gate has read loaded across many consecutive control-loop ticks while
  nothing retired: the state astubbs#487 measured, today exported only as a paused-partition gauge
  and logged nowhere. A log line, no semantic change; the last item to join the queue before it
  closed. The Claude review is answered; the Codex review's findings are being worked through.
- [x] **astubbs#477** - merged 2026-09-08. A dead broker-poll thread now closes the consumer in
  the consumer-commit modes, the shipped default among them, so the group rebalances at once instead
  of after `max.poll.interval.ms`. One derived predicate and one condition, proven red on every
  consumer-commit mode and green on the transactional control arm; the review's close-time
  subtlety was characterised by running it (no second close - the ownership guard declines the
  retry). Retires its inflight note into `docs/solutions/`.

### Tier 2 - the producer-recovery stack: OUTSIDE v6 by the 2026-09-07 decision, named in the claim

astubbs#225 (survive producer fencing rather than dying) is a feature, and two open **defects** are
stacked on it: the transactional revoke wait (astubbs#44, upstream's verified-bug label) and the
poisoned-transaction wedge. The owner chose on 2026-09-07 not to pull the whole fencing stack into a
bug release, and to name astubbs#44 as the one known critical defect outside scope instead. **That
call stands unless the owner reopens it**; this tier records the stack and its order so that if it
is reopened - or if the stack simply lands before the tag - nothing has to be re-derived.

The order, bottom-up, each rung re-cut on 2026-09-07 so it can be reviewed against pieces already
reviewed: astubbs#472 (vocabulary and plumbing, no behaviour change), astubbs#474 (keep every
completed record until the commit that carries it succeeds), astubbs#410 (recovery itself, closes
astubbs#225), astubbs#434 (abort a transaction poisoned by a terminal send failure), astubbs#408
(a revocation declines the transaction lock, closes astubbs#44 - the one PR in the stack with real
reds, and it must now resolve the `tryCommitOffsetsOnRevoke` collision with the merged
astubbs#466), then astubbs#420 (producer-ownership polish, after v6 in any case).

**Decided 2026-09-09: the poisoned-transaction wedge is the second named exception**, beside
astubbs#44 in `release-0.6.0.0.md`; astubbs#434 stays in tier 2. The wedge ([`bug-poisoned-transaction-not-aborted-while-running.md`](bug-poisoned-transaction-not-aborted-while-running.md),
which astubbs#476's vetting sweep merged its sibling note into)
is fixed by astubbs#434, which also stacks on astubbs#410. With the stack outside v6 the claim
has two named exceptions: a single oversized record stops its partition for the life of the
process in transactional mode. The astubbs#476 vetting sweep read the pair as **not gating**
("today's behaviour is strictly better than what it replaced"); the owner named it anyway rather
than carry a standalone abort.

### Tier 3 - release plumbing, then tag

- [ ] **Post the release page body by hand.** `release.yml` tries to build the notes from the
  `CHANGELOG.adoc` section, but its heading match is exact and the section is headed
  `== 0.6.0.0 (unreleased)`, so on master it matches nothing and falls back to generated notes.
  On the day: convert the curated section and `gh release edit v0.6.0.0 --notes-file <file>`
  after `release.yml` has cut the release. astubbs#199 fixes the match and can follow.
- [x] **astubbs#446** - merged 2026-09-09. Lift the announcement plan onto master, so the announcement is not being
  written from a branch nobody merges.
- [ ] The tag-day artefact checks in the section of that name below.
- [ ] Amend the release claim, not the standard, for what is still open. `release-0.6.0.0.md`
  already names astubbs#44 as the exception (2026-09-07) and the poisoned-transaction wedge as the
  second (2026-09-09); say which confluentinc#857 mechanisms are closed and which sightings remain
  unattributed.
- [ ] Post the drafted issue responses (`ls docs/inflight/issue-response-*.md` and
  [`release-0.6.0.0-issue-response-drafts.md`](release-0.6.0.0-issue-response-drafts.md)) in the
  pre-release sweep [`docs/releasing.md`](../releasing.md) describes.
- [ ] Tag. Then the after-it-ships items below, and the rest of astubbs#197.

### Can follow - finished or nearly, and deliberately not v6

Named so nobody re-argues them in: the producer-recovery stack in tier 2 (by the 2026-09-07
decision, unless reopened); astubbs#199 (the changelog heading match in `release.yml` - the release
page body is posted by hand on the day, so this follows the tag); astubbs#352 (commit-failure seam - a feature, even though
confluentinc#833's reporter patched the library for it), astubbs#226 (health check), astubbs#306
(offset density), astubbs#360 (virtual threads), astubbs#405 (the torture harness - test
infrastructure; astubbs#471's soak has merged and its finding is in the confluentinc#857 list), astubbs#479 (the
God-class decomposition plan and the five classes below it - a refactor track whose own notes say
which open PRs must merge before each cut, so it follows the release rather than gating it), and
every Streams, proxy, perf, rate-limiting and dashboard stack.

## What v6 must say about the confluentinc#857 family

[`bug-857-family.md`](bug-857-family.md) owns the evidence; its retirement rule is that every
mechanism is individually explained or closed, so the family does not close with the release.
What v6 ships, and what the release note has to be honest about:

**Closed on master** (each with its guard): the poll/control commit deadlock (astubbs#29), the
poller death on `RebalanceInProgressException` (astubbs#100), the draining busy-spin (astubbs#80),
the orphaned retry entry (astubbs#346), the poll-thread NPE (astubbs#345), the load-gate phantom
counts (astubbs#336), the sign-reversed shard count (astubbs#373), the retry-queue orphan window
(astubbs#437), a revoke surviving a failed assignment (astubbs#451). The lag-stagnation line was
demoted to a timing proxy, and astubbs#444 measured the large-instance residual as group-protocol
churn rather than a PC defect.

**Still open - the release note names each, and each has an agent on it (2026-09-08).** A ticked
box means the v6 action for that line is done, not that the defect is closed:

- [x] **astubbs#408 (tier 2, after v6)** - the transactional revoke wait, astubbs#44
  (confluentinc#803). Outside v6 by the 2026-09-07 decision; the release claim names it as the
  known exception. The decision predates astubbs#466, which replaced the unbounded spin with a wait
  bounded by `commitLockAcquisitionTimeout`, so what astubbs#408 still owns is the measurement of
  that bound and the held decline seam - astubbs#466 refuted declining as the fix, so it is only the
  deadline fallback. The v6 action is the release-note sentence, and
  [`release-0.6.0.0.md`](release-0.6.0.0.md) now says "bounded, not yet declined" rather than
  "unbounded". astubbs#408's own probe still reads the callback over the poll-interval budget on
  today's master, which is the defect reproduced against the bounded design; its title and whether
  it still closes astubbs#44 are the owner's, after v6.
- [x] **astubbs#488 (merged 2026-09-09)** - `INSTANCE_STALL` and `ZOMBIE_MEMBER` sightings that replay clean on
  idle runners, so they read as starvation rather than a wedge. The idle replay was the weak
  direction: a load-shaped stall needs the load. astubbs#488 ran the load arm - three seeds, idle
  and under CPU burners, one term differing - and on one seed both arms crossed the bound that raised
  every gating firing on CI, with the detector reporting the member busy in user code and no stall
  violation or dump in any arm, every window proven open. Starvation, confirmed from the load side.
  It proposes a PROPOSED close for the `INSTANCE_STALL` line only, owner-gated; the `ZOMBIE_MEMBER`
  arm never fired, so nothing there moves; its two recorded seeds and the busy-observation
  calibration are what the experiment runner stays for, and its retire condition is in the
  runner's row. Merged.
- [x] **astubbs#487 (merged 2026-09-09)** - **the intake stall under an always-failing key, found by
  astubbs#471's soak, is the load gate, and head-of-line blocking is not why.** Three arms, one
  term each, predictions written first, every one confirmed. Under KEY ordering with half the keys
  poisoned, the gate `WorkManager#isSufficientlyLoaded` latched true on the first fetch, under a
  second in, and never unlatched: every partition paused, successes frozen at the same count
  astubbs#471 saw after thirty minutes, failures retrying at full worker throughput. UNORDERED
  stalls identically, which refutes the head-of-line half, and the KEY arm's own arithmetic refutes
  it harder - one burst over a thousand distinct keys leaves at most one record per key, so nothing
  was queued behind any head. Raising only the gate's threshold (`messageBufferSize`) flips the
  outcome: gate false, nothing paused, successes rise - the positive control that says the gate is
  the latch. What latches it is records that are themselves workable, retried forever, never
  retiring. Offset-encoding back pressure re-eliminated in all three logs.
  **There is no gate fix.** The threshold arm exposes the second bound: lifting the intake bound
  doubles successes and then plateaus while the held population climbs without limit, so a bigger
  threshold trades a hard stall for an unbounded-memory slow starve, and "count only what is
  selectable" would let a healthy instance fetch without bound. The property that discriminates is
  liveness of the shard head, not decidable from shard state. **The fix bounds the failures, not
  the buffer: astubbs#149's dead-letter queue (confluentinc#310), after v6.** One same-class
  instance found and pinned by assertion: `drain()` gates on the same over-count. No product code
  changed. astubbs#487 carries the flag that reaches the gate's DEBUG line, the gate's operands on
  the soak's progress line, one knob per arm, the accounting gap as a characterisation test, and
  the working note. confluentinc#833's flat processed-records counter is this state.
  **For the release note, and it is stronger than "a poison-record workload":** the stall does not
  need a high failure rate, and it does not need saturated workers either. The gate reads
  `inShards - parkedForRetry`, and the parked term is throughput times retry delay, not a
  population property - it sat at the same value across a thirty-fold change in population. So the
  unparked count is the poison population minus what the retry service is holding in back-off, and
  it crosses the gate's threshold as soon as the population outgrows that. A fourth arm at a low
  poison rate, prediction first, latched the gate at under a hundred held records about a minute
  in, with three of fourteen workers busy: the instance stopped fetching while mostly idle and
  looking healthy, successes flowed for minutes and then froze for the rest of the run. A slower
  retry service latches sooner, since fewer records are parked and more read as workable. So any
  long-lived instance with no user-side terminal handling and any poison at all gets there
  eventually, silently, and for good: the latch is exported only as a paused-partition count and
  logged nowhere, and no poller wakeup is ever attempted because the wakeup is itself gated on the
  same reading. That is the best explanation yet for confluentinc#809 and confluentinc#833's flat
  processed-records counters. The owner decided on 2026-09-09 not to ask those reporters (assume
  no reply); the drafted question stays unposted. One thing the arms did not explain, flagged as the
  next arm: the observed retry cadence is about three times the configured delay, and the latch
  point is a function of it. **Decided 2026-09-09: the interim warning is v6-sized** - a WARN when
  the gate has read loaded across many consecutive ticks while nothing retired, no semantic change -
  and joins tier 1 as its own item, still to be built. astubbs#487 itself is merged; the
  retry-cadence arm is what remains of the measurement.

**Resolved or reassigned - one line each, so the release note can say what was ruled out:**

- The eager-mode stall on trees carrying astubbs#29's fix - **withdrawn, astubbs#478 (merged):** a
  timing bound crossing that flips with the processor count; the grid it was opened on was never a
  one-term A/B. What survives is the per-shard liveness gap, narrowed by astubbs#491.
- The async-unordered rebalance stall, the family's fifth item - **the group protocol, astubbs#486
  (merged):** a join phase the coordinator holds open, during which every member's poll returns
  nothing, then recovers; PC holds nothing during it (control arm). The `ZOMBIE_MEMBER` line does
  not discriminate a PC hold from the coordinator, so a sighting is told apart by the closing
  members' threads.
- A dead broker-poll thread leaving the consumer open in consumer-commit modes - **fixed,
  astubbs#477 (merged).**

## What v6 must say about data loss and duplicates

- **Fixed on master, 2026-09-08:** the async-commit acknowledgement (astubbs#470).
- **Fixed on a branch outside v6 scope, and named as the second exception (2026-09-09):** the
  poisoned-transaction wedge (astubbs#434, stacked on the producer-recovery work) - see tier 2.
- **Fixed on master:** the revoke-path transaction omitting offsets (astubbs#466); the torn-read family
  ([`bug-torn-read-family.md`](bug-torn-read-family.md) - astubbs#337, astubbs#344, astubbs#345,
  astubbs#346, astubbs#349); a terminally failed send publishing half a result set (astubbs#261);
  the produce-lock double release (astubbs#257); `InvalidPidMappingException` looping
  (astubbs#429).
- **By design, needs a documentation reply, not a fix:** in-flight work at revocation is redelivered
  (confluentinc#777, [`upstream-173-revocation-duplicate-processing.md`](upstream-173-revocation-duplicate-processing.md)).
  One chaos cell (cooperative plus draining) was predicted and never run; a revocation grace period
  is an owner decision.
- **Refuted, astubbs#484 (merged 2026-09-08):** the "reset to earlier offset" replay branch behind
  confluentinc#546 (its note retired by astubbs#494 into
  [`absence-of-commit-data-was-inferred-from-a-sentinel-that-a-real-commit-shares-2026-09-09.md`](../solutions/logic-errors/absence-of-commit-data-was-inferred-from-a-sentinel-that-a-real-commit-shares-2026-09-09.md)).
  One fixture, one differing term, both arms' predictions held: polled-below-expected discards every
  loaded incomplete and rewinds the commit frontier, which is duplicates by construction, never
  loss; and the note's own hypothesis - that `committed()` races the client's position resolution -
  is refuted against the kafka-clients source, where the listener runs strictly before the fetcher
  resolves positions. The only PC-owned route to that branch was astubbs#337's defect, now pinned
  by a four-shape round-trip test. What remains under astubbs#162 is the **false-truncation WARN**:
  a new group, and every partition recovered through the foreign-metadata path, logs "truncating"
  having truncated nothing. Misdirection operators may alert on; not a data risk. **Decided
  2026-09-09:** absent commit data does not warn - a distinct INFO line saying no committed offset
  was found and the partition starts from the polled offset, no truncation branch taken, and the
  WARN kept for the genuine truncation cases. Fixed by astubbs#494 (merged 2026-09-09), which closes
  astubbs#162: absence of commit data is now recorded from the codec's default entry rather than
  inferred from a sentinel that a real commit at offset zero shares, all three defects behind the
  warning string are settled, and the mirror's fork-status text is drafted for the pre-release
  sweep.
- **Never reproduced:** the commit-response timeout (confluentinc#809, confluentinc#833). astubbs#471
  (merged) is the first experiment that hunts it; its first runs could not reach the timeout because
  the instance stalled first - see the intake-stall item in the confluentinc#857 list above, which
  is now the live lead. Not a v6 gate in itself; the release note says the symptom's known causes
  are fixed and the reports were never reproduced.

## Known unknowns the release note should not paper over

**What is still unknown at the cut, so the release note names it rather than implies it is closed.**
Each was surfaced by the work that resolved the items under "Unknowns made known" below, and none is
a v6 gate; the note must simply not claim more than the suite or the code can show.

- **A single wedged shard is invisible to everything that gates - narrowed to the shard half by
  astubbs#491 (merged 2026-09-09).** astubbs#478's surviving finding, owned by
  [`test-per-shard-liveness-has-no-gate.md`](test-per-shard-liveness-has-no-gate.md). The commit
  half is closed: a red control (one partition's commits answered and dropped, every existing
  gating detector shown green on it with an armed control so the silence is not vacuous) came
  first, and the gate that came second compares two positions - a member's own next offset to
  commit against what the group has committed - held across samples with the group stable, so the
  demoted timing bound's false positive is excluded structurally. Both replay seeds the demotion
  was argued from re-ran across the old bound with the new gate silent. The note's own prescription
  was refuted on the way: "completions advancing" is instance-wide and does not discriminate. What
  remains is the shard half: a key-order shard that will never be dispatched again inside an
  otherwise healthy partition still gates nothing, any incomplete offset pins the local watermark,
  and no red control exists for it - astubbs#483 found the nearest mechanism unreachable, so it is a
  reachability question first. The release note says the suite cannot see that shape. One sibling
  found by the sweep is recorded, not fixed: the ledger's duplicate allowance is fleet-wide while
  redelivery is per-partition.
- ~~**One unconditional by-key shard removal remains on master**~~ - **fixed, astubbs#492 (merged
  2026-09-09).** The revoke sweep in `ShardManager.removeWorkFromShardFor`, the second of the two
  astubbs#483's defect-class sweep found, now declines to evict an occupant that is both from a
  different registration and still live; red first on a fresh container that had displaced the
  stale one, with an ablation arm per leg of the guard. No unconditional by-key removal of a
  container remains in main. The retry queue's by-key removal is the same shape but is that queue's
  keying model, reported by astubbs#483 and left. Cost before the fix was misdirection bounded to
  one control-loop tick by astubbs#481's purge, never loss.
- **The other rows of [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md)** - astubbs#482
  closed the most-sighted row, and astubbs#490 (merged 2026-09-09) worked the rest: the
  `processInKeyOrder` sanity-check row was already fixed on master by astubbs#29's merge (every
  sighting predates it - the wait counted control-loop cycles while the poll thread fills the
  batch); a shutdown test whose "in flight" stand-in was two loop cycles is fixed with a control arm;
  the ambient-probe extension test's process-global log capture is fixed; rows whose owners merged
  without retiring them are retired; four rows stay open with their reasons, none meeting the
  quarantine bar. Nothing quarantined. A tag needs a green master, so what is left is tag-day work
  rather than scope.
- The maturity claim itself: `docs/data/module-maturity.yaml` carries a bare `production-use` next
  to a conditional support posture, and a renderer can lift the bare value without its condition.
  The tag-day checks below carry the recheck.

## Unknowns made known, 2026-09-08

Each was a known unknown at the start of 2026-09-08 and was pursued by its own agent on its own
branch; the replay-shaped ones ran one at a time on one machine. One line each; the PR and its
solutions write-up hold the evidence.

- Whether the six deadlock captures ever replay clean with astubbs#29's fix - **unanswerable by
  replay, and proven another way, astubbs#485 (merged):** the fix replaced the monitor the captures
  identify the defect by, so a clean replay would say nothing; the control arm on the deterministic
  probe passes with the fix and fails every run with it restored. PROPOSED close for the owner.
- Whether the shard-displacement orphan window is reachable - **unreachable, astubbs#483 (merged):**
  four-arm test, one per ordering mode plus same-key cross-partition; the one caveat is an
  in-generation replay, which no main code produces and astubbs#481 bounds to misdirection.
- Whether "rejoin" after producer fencing is expressible - **yes, by reading the astubbs#472,
  astubbs#474 and astubbs#410 diffs:** the instance never leaves the group, so rejoin reduces to
  abort, rebuild the producer, replay under the write lock. The one open question, declining the
  lock during a rebalance, stays with tier 2.
- Which flakes are load-shaped and which real - **the batch test's own key draw, astubbs#482
  (merged),** and the rest of the register worked by astubbs#490 (merged).

## What the vetting sweep read, and every open bug note's disposition - moved out

Two records that were here until 2026-09-09 now live in
[`process-candidate-ranking.md`](process-candidate-ranking.md): the astubbs#476 vetting sweep's
reading of what gates v6, and the disposition of every open `bug-` note against the bar as of
2026-09-08. Both are dated agent readings, not decisions, and the tiers above are what overrode them
where they disagree. What this note kept from them is already in the tiers: the poisoned-transaction
wedge as the second exception, the `batchSize` bound and the gate-latch warning in tier 1, the
withdrawn eager stall, the false-truncation warning on astubbs#494. Re-derive the open-bug list
before the tag rather than trusting either copy: `ls docs/inflight/bug-*.md`, and the state and
impact markers each note carries.

The survey of upstream items with no fix PR and no prepared response is
[`upstream-items-with-no-fix-and-no-response.md`](upstream-items-with-no-fix-and-no-response.md);
it is research for after the tag, not scope.

## Tag-day artefact checks - are the things we publish true on the day we cut?

Folded in from the register that was `release-0600-blockers.md`. Scope: `CHANGELOG.adoc` and
`README.adoc` as published. Release mechanics stay in [`release-0.6.0.0.md`](release-0.6.0.0.md);
the tracker is astubbs#197.

- **Master must be green, and five lanes are known to lie or to fail on their own.** The autopsy
  that omits fleet violations, the perf lane's wall-clock deadline, the churn scenario's
  thirty-second no-progress window (being settled by replay on
  `test/857-no-progress-window-replay`), the codecov flags that are not like-for-like, and the
  broker container's undiagnosable exit. Each has its own note; read the flake register and those
  notes before believing a red or a green on the day.
- **The package rename shipped (astubbs#294); keep the release notes honest about it.** The
  `== 0.6.0.0` changelog section is rebuilt from the commit log when the tag is cut, and generation
  cannot notice that it dropped a claim the current text makes. After regenerating, confirm the
  opening paragraph and the `=== Breaking` bullet still name **both** the Maven `groupId` and the
  Java packages every import names, not just the `groupId`. Reasoning and the Apache 2.0 analysis:
  [`docs/plans/2026-08-11-001-refactor-package-rename-plan.md`](../plans/2026-08-11-001-refactor-package-rename-plan.md).
- **Recheck the documentation data after tiers 1 and 2 land.** The published claim is "every known
  **critical** defect resolved and evidenced". Nothing verifies it automatically -
  `bin/check-docs-data.sh` checks structure only, on purpose. `docs/data/module-maturity.yaml` is
  only half conditional: each shipped module carries a bare `maturity: production-use` and,
  separately, a `support_posture` line that is qualified ("when the release validation passes").
  Whether the bare value is a claim the still-open confluentinc#857 items falsify, or a label the
  posture line conditions, is the owner's call - start from
  `grep -n 'maturity:\|support_posture' docs/data/module-maturity.yaml`, not from "already
  conditional". A first pass on 2026-09-05 established the state and changed no value; it did
  correct a stale claim in `docs/data/roadmap.yaml`'s `known-defects-cleared` entry (it said
  astubbs#29 was unmerged). The staged Streams and Connect rows in
  `docs/data/staging/module-maturity-rows.yaml` and the records in `docs/features/staging/` stay
  staged: under the bug-release decision no module PR is in the queue, and each moves with the PR
  that lands its module.
- **The release page must carry the curated notes.** `release.yml` already builds a notes file from
  the `CHANGELOG.adoc` section (astubbs#72) - the 2026-09-07 vet of the old blockers note was right
  that "the body is empty" was never the whole story - but its heading match is exact and the
  section is headed `== 0.6.0.0 (unreleased)`, so it matches nothing and falls back to generated
  notes - so the body is posted by hand on the day (tier 3), and astubbs#199, which fixes the match,
  follows the tag. The rest of astubbs#197's triage list has been picked
  up: the magic-byte hazard in astubbs#217, the load-factor WARN in astubbs#201, and MDC in
  astubbs#205 (`MdcPropagation` on master captures and restores the caller's context; the
  2026-09-08 vet that called the gap "real" grepped for a name the class does not use). The
  tracker's own checklist boxes lag the work.
- **Three `3.9.1` references, and only one was wrong.** The CI description that named the default
  Kafka version was fixed in astubbs#272 by dropping the number. The `bin/ci-build.sh 3.9.1` command
  examples in `AGENTS.md` and in `src/docs/README_TEMPLATE.adoc` (which reaches the published
  README) demonstrate that the script takes a version argument and assert nothing about CI's
  default. Do not "fix" either.
- **After it ships:** the mirrors that describe 0.6.0.0 in the future tense need the real
  coordinate (`gh issue list -R astubbs/parallel-consumer --label 0.6.0.0` finds them);
  astubbs#186 and astubbs#195 close with a pointer to the release (astubbs#188 already is); one
  announcing comment each on upstream confluentinc#880, confluentinc#885 and confluentinc#907, the
  deliberate exception to one-backlink-per-issue; the `issue-response-*.md` drafts are posted in the
  same sweep; and astubbs#197, the tracking handle, closes with the tag.

Context worth inheriting on the day:

- **`README.adoc` is generated - never hand-edit it.** Edit `src/docs/README_TEMPLATE.adoc` and
  regenerate with `./mvnw -N asciidoc-template:build`. A PR that touches only the template has
  silently not changed the published README.
- **`CHANGELOG.adoc`'s `== 0.6.0.0` section is working text until the tag** - it is regenerated from
  the commit log, so do not quote it as the release notes, and when agents work in parallel exactly
  one holds that file; it is the highest-collision file in the repo.
- **The README's trademark wording claims nothing it does not have.** The 2026-08-11 branding
  rename put "KAFKA ... has been licensed for use by Antony Stubbs and contributors" at the top of
  the README and in the attribution section - the Foundation's boilerplate for a licence nobody
  holds. astubbs#495 removes the top note and rewrites the attribution sentence as nominative use:
  a registered mark of the Foundation, an independent library that works with Apache Kafka, no
  affiliation and no endorsement. On the day: reread the attribution section against the
  Foundation's third-party naming guidance, and confirm no "licensed" claim has crept back in.
- **A dependency version in prose drifts silently.** The `3.9.1`/`3.9.2` mismatch came from a
  Dependabot group bump moving `kafka.version` after the note was written. Re-read the
  `=== Dependencies` section against `pom.xml` immediately before cutting, not weeks earlier.

## Delete when

The tag is cut. Migrate first: the family and data-loss dispositions above go into the release note
text and `docs/data/roadmap.yaml`'s `known-defects-cleared` entry; the "context worth inheriting"
bullets go to [`docs/releasing.md`](../releasing.md) if it does not already carry them; the
upstream survey's residue goes to
[`upstream-coverage-completeness.md`](upstream-coverage-completeness.md) if any of it is still
unanswered after the sweep.
