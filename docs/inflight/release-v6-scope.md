# v6 (0.6.0.0) release scope and burn-down

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

**Deliberately its own file, not a section inside
[`release-0.6.0.0.md`](release-0.6.0.0.md).** A section is invisible until someone opens that file;
a note gets its own line in the session-start index. The scope decision needs to be *met*, not
looked up - so it is filed where an agent trips over it. Named `release-when-is-v6-good-enough.md`
until 2026-09-08, when it stopped being a question.

[`release-0.6.0.0.md`](release-0.6.0.0.md) is the content and the breaking-change record, and
answers "is it ready?". **This one asks "is it enough?"** - and, since 2026-09-08, it also carries
the burn-down that gets from here to the tag, because the answer to "is it enough" turned out to be
"yes, once the fixes already built are merged", and that is a list. The artefact-correctness
register that was `release-0600-blockers.md` is folded in below as the tag-day checks, so the
release has one note to burn down rather than three.

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
its points survive unchanged and are in tier 3 below: astubbs#199 is the one item that cannot be
applied after the tag, and astubbs#197's body reads as more blocked than it is. Its open question
of which modules v6 publishes is moot under this decision, since no module PR is in the queue.
<!-- file-refs: N/A - the merge-order note was merged and removed by astubbs#475; the path is cited as history -->
<!-- post-merge: checked-end -->

**"Draft" on a fork PR means "needs the owner to review and merge", not "unfinished".** Every PR in
the queue below is implemented, tested and green on everything except the human-LGTM gate and, where
it stacks, the dependency gate. `gh pr list -R astubbs/parallel-consumer` shows near enough every
PR as a draft, so the state flag carries no information here; what a PR still needs is written
against it below, because that is the part `gh` cannot say.

## Proposed cut-off and order of work

**Proposal, for the owner to confirm or amend:** the merge queue closes today. Nothing joins it that
is not already a fix for an open defect with a PR. Anything astubbs#471's soak or a chaos lane finds
after today is a 0.6.0.x unless it is data loss on a default configuration, and then it joins tier 1.
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
  The two further by-key removals astubbs#483 found were left for astubbs#468's identity-`equals`
  change to make fixable - check that PR's body for whether it took them.
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
- [ ] **astubbs#480** - an offset map whose run or bitset extends past the partition's log end
  offset is an unreadable payload, not a completed range. Silent skip of real records on a corrupt
  or foreign payload; proven red through the real assignment path, green with the guard, mutation
  lane green on the bound. Promoted from the owner's-decision list on 2026-09-08.
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

**One thing the 2026-09-07 decision did not name, and the owner should:** the poisoned-transaction
wedge ([`bug-poisoned-transaction-not-aborted-while-running.md`](bug-poisoned-transaction-not-aborted-while-running.md),
which astubbs#476's vetting sweep merged its sibling note into)
is fixed by astubbs#434, which also stacks on astubbs#410. If the stack is outside v6, the claim
has two named exceptions, not one - a single oversized record stops its partition for the life of
the process in transactional mode. Either name it beside astubbs#44 in `release-0.6.0.0.md`, or
carry a smaller standalone abort for v6. The astubbs#476 vetting sweep read the pair as **not
gating** ("today's behaviour is strictly better than what it replaced"); that is an agent's reading,
recorded in the sweep's list below, and the call is still the owner's.

### Tier 3 - release plumbing, then tag

- [ ] **astubbs#199** - publish the curated changelog section as the GitHub Release body. Without it
  the release page is empty.
- [ ] **astubbs#446** - lift the announcement plan onto master, so the announcement is not being
  written from a branch nobody merges.
- [ ] The tag-day artefact checks in the section of that name below.
- [ ] Amend the release claim, not the standard, for what is still open. `release-0.6.0.0.md`
  already names astubbs#44 as the exception (2026-09-07); add the poisoned-transaction wedge if the
  owner confirms it is the second, and say which confluentinc#857 mechanisms are closed and which
  sightings remain unattributed.
- [ ] Post the drafted issue responses (`ls docs/inflight/issue-response-*.md` and
  [`release-0.6.0.0-issue-response-drafts.md`](release-0.6.0.0-issue-response-drafts.md)) in the
  pre-release sweep [`docs/releasing.md`](../releasing.md) describes.
- [ ] Tag. Then the after-it-ships items below, and the rest of astubbs#197.

### Can follow - finished or nearly, and deliberately not v6

Named so nobody re-argues them in: the producer-recovery stack in tier 2 (by the 2026-09-07
decision, unless reopened); astubbs#352 (commit-failure seam - a feature, even though
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
- [ ] **astubbs#488 (draft)** - `INSTANCE_STALL` and `ZOMBIE_MEMBER` sightings that replay clean on
  idle runners, so they read as starvation rather than a wedge. The idle replay was the weak
  direction: a load-shaped stall needs the load. astubbs#488 ran the load arm - three seeds, idle
  and under CPU burners, one term differing - and on one seed both arms crossed the bound that raised
  every gating firing on CI, with the detector reporting the member busy in user code and no stall
  violation or dump in any arm, every window proven open. Starvation, confirmed from the load side.
  It proposes a PROPOSED close for the `INSTANCE_STALL` line only, owner-gated; the `ZOMBIE_MEMBER`
  arm never fired, so nothing there moves. Box closes when astubbs#488 merges.
- [ ] **astubbs#487 (draft, arms run)** - **the intake stall under an always-failing key, found by
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
  processed-records counters. A draft question for those reporters is in the session scratchpad,
  unposted. One thing the arms did not explain, flagged as the next arm: the observed retry cadence
  is about three times the configured delay, and the latch point is a function of it. Owner's
  calls: whether to ask those reporters, and whether the cheap interim - a warning when the gate
  latches with nothing retiring, no semantic change - is v6-sized. Box closes when astubbs#487
  merges.

**Resolved or reassigned since this list was written - kept so the release note can say what was ruled out:**

- ~~An eager-mode (`PERIODIC_CONSUMER_SYNC`) stall that reproduces on trees carrying astubbs#29's
  fix (the family note's "fourth open item")~~ - **withdrawn 2026-09-08, astubbs#478 (merged).** Four replays
  of the recorded seed on today's master all drained completely with zero loss; the "stall" was the
  Class 2 timing bound, whose crossing flips with the processor count at a fixed seed and tree. The
  grid the item was opened on was also never a one-term A/B. What survives from this line is the
  per-shard liveness gap, already tracked and deferred with a stated bar - a blind spot to name,
  not a bug to fix before cutting.
- ~~A rebalance stall in async unordered mode from `MultiInstanceRebalanceTest` (the "fifth open
  item"), blocked on progress-tracker instrumentation that does not exist yet~~ - **attributed
  2026-09-08, astubbs#486: the consumer-group protocol, not PC, and it was never blocked.** The
  progress-tracker diagnostic has been wired since astubbs#444 and the mechanism was already
  measured (a join phase the coordinator holds open, during which `poll()` returns nothing to any
  member, then recovers - the write-up astubbs#473 promoted to master); the family note's section
  had two claims that were false against master, which is why the item kept being picked up. The
  freeze is real and fleet-wide but it recovers, no work is stranded, and no instance stays wedged;
  what reads as red is the detector's no-progress window closing inside a real protocol freeze. The
  PC half - that PC holds nothing during it - was re-verified with a one-term control arm on
  `ClosingMemberRebalanceIT` after astubbs#451, astubbs#466 and astubbs#468 moved the revoke and close seam. Not a
  defect; the release note need not name it. One new fact worth keeping: the
  `ZOMBIE_MEMBER/REBALANCE_BLOCKED` probe line does not discriminate a PC-side hold from a
  coordinator holding its join phase open, so a future sighting is told apart by what the closing
  members' threads are in. astubbs#486 merged 2026-09-08.
- A dead broker-poll thread leaving the consumer open in consumer-commit modes, no LeaveGroup until
  `max.poll.interval.ms` - **fixed in the queue, astubbs#477, tier 1.**
## What v6 must say about data loss and duplicates

- **Fixed on master, 2026-09-08:** the async-commit acknowledgement (astubbs#470).
- **Fixed on a branch outside v6 scope, and needing a named exception or an owner decision:** the
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
  confluentinc#546 ([`bug-162-offset-state-truncation.md`](bug-162-offset-state-truncation.md)).
  One fixture, one differing term, both arms' predictions held: polled-below-expected discards every
  loaded incomplete and rewinds the commit frontier, which is duplicates by construction, never
  loss; and the note's own hypothesis - that `committed()` races the client's position resolution -
  is refuted against the kafka-clients source, where the listener runs strictly before the fetcher
  resolves positions. The only PC-owned route to that branch was astubbs#337's defect, now pinned
  by a four-shape round-trip test. What remains under astubbs#162 is the **false-truncation WARN**:
  a new group, and every partition recovered through the foreign-metadata path, logs "truncating"
  having truncated nothing. Misdirection operators may alert on; cheap once decided, and it wants
  the owner's call on message and level. Name it in the release note; not a data risk.
- **Never reproduced:** the commit-response timeout (confluentinc#809, confluentinc#833). astubbs#471
  (merged) is the first experiment that hunts it; its first runs could not reach the timeout because
  the instance stalled first - see the intake-stall item in the confluentinc#857 list above, which
  is now the live lead. Not a v6 gate in itself; the release note says the symptom's known causes
  are fixed and the reports were never reproduced.

## Known unknowns the release note should not paper over

**What is still unknown at the cut, so the release note names it rather than implies it is closed.**
Each was surfaced by the work that resolved the items under "Unknowns made known" below, and none is
a v6 gate; the note must simply not claim more than the suite or the code can show.

- **A single wedged shard is invisible to everything that gates** - astubbs#478's surviving finding,
  owned by [`test-per-shard-liveness-has-no-gate.md`](test-per-shard-liveness-has-no-gate.md)
  (deferred: a new gate needs a red control before it can be trusted). `INSTANCE_STALL` is
  per-instance, so a watermark frozen by a commit that never landed, on an instance whose other
  shards keep completing, is caught by no chaos detector; every 857 replay this month drained with
  full key coverage, which is what a false negative of that shape looks like. The release note
  should say the suite cannot see it, not that no stall is known. Not a v6 gate: the gate is
  test-suite work, and building it without the red control first is the trap the note names.
- **One unconditional by-key shard removal remains on master** - the revoke sweep in
  `ShardManager.removeWorkFromShardFor`, the second of the two astubbs#483's defect-class sweep
  found. astubbs#468's identity-`equals` change made conditional removal possible and fixed the
  stale sweep; this one was reported, not fixed. Cost is misdirection bounded to one control-loop
  tick by astubbs#481's purge, never loss. A small fix, not a v6 gate.
- **The other rows of [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md)** - astubbs#482
  closed the most-sighted row; the register still names several, one of them
  (`processInKeyOrder` failing its own input sanity check) undiagnosed. A tag needs a green master,
  so these are tag-day work rather than scope.
- The maturity claim itself: `docs/data/module-maturity.yaml` carries a bare `production-use` next
  to a conditional support posture, and a renderer can lift the bare value without its condition.
  The tag-day checks below carry the recheck.

## Unknowns made known, 2026-09-08

**Every item here was a known unknown at the start of 2026-09-08 and was pursued by its own agent on
its own branch (owner's instruction).** Code-shaped questions ran in parallel; the replay-shaped
ones ran one at a time, because several replay agents on one machine produce exactly the starvation
artefacts they are meant to rule out. The replay queue is drained into PRs: the eager-mode stall
(withdrawn, astubbs#478), the six deadlock captures (proven by control arm, astubbs#485), the
async-unordered rebalance stall (the group protocol, astubbs#486), the `INSTANCE_STALL` load arm
(astubbs#488), and the intake stall (astubbs#487) - the last two are tracked in the "Still open"
list above. Kept here so the release note can say what was asked and how it was settled.

- ~~Whether the six deadlock captures that verified astubbs#29's mechanism ever replay clean with
  the fix applied~~ - **known, 2026-09-08, astubbs#485: the question was unanswerable by replay, and
  the fix is proven another way.** The captures identify the defect as the poll thread `BLOCKED` on
  an `AtomicBoolean` monitor; astubbs#29 replaced that monitor with a `ReentrantLock`, and a thread
  waiting on a lock parks rather than blocks - so a clean replay would have said "no BLOCKED frame"
  with or without the deadlock, and the family note's own header already warned that replaying
  captured seeds does not reproduce it. Proven instead with a control arm on the deterministic
  probe: with the fix, both assignors pass every run; with the deadlock deliberately restored
  (`tryLock` back to `lock`), every run fails and every dump shows the waiting frame. Merged to
  master 2026-09-08. The solutions doc's "Unproven" section is superseded in place, and the
  deadlock line carries a `PROPOSED
  closed` marker for the owner. Two side findings worth keeping: the JVM's deadlock detector cannot
  see this cycle (its other edge is a queue poll, not a lock), and one replay in two was VOID
  because the window never opened - check the discriminator fired before banking a green.
- ~~Whether the shard-displacement orphan window is reachable in production~~ - **known,
  2026-09-08, astubbs#483 (merged): unreachable**, with a four-arm regression test, one per ordering mode plus a same-key cross-partition case, and an ablation that goes
  red only when both sweeps are removed. The caveat is the finding: the last leg of the proof is a
  Kafka property, not this engine's - the consumer's fetch position never goes backwards within a
  generation - so an in-generation replay of an offset whose resident was **fenced but not swept**
  would reopen the window and nothing goes red. Narrower than it first read: a non-stale resident
  makes the replayed record dropped, not displaced (`addWorkContainer` returns on "already exists"),
  and astubbs#484 showed PC's own bootstrap truncation path cannot produce such a replay - it runs
  only inside `onPartitionsAssigned`, after the stale sweep has emptied the shards and the queue. A
  backwards `seek` on a running assignment is the remaining route, and main has none today.
  astubbs#481's controller purge bounds any such orphan to one control-loop tick, so the cost would
  be misdirection, not a stall. The sweep for the
  same shape found two more by-key removals (`ProcessingShard.onSuccess`, the revoke sweep in
  `ShardManager.removeWorkFromShardFor`), left for astubbs#468 whose identity-`equals` change is
  what makes conditional removal possible. Not a v6 gate; astubbs#483 stacked on astubbs#481 and merged after it.
- ~~Whether "rejoin" after producer fencing is expressible in PC's lifecycle~~ - **known,
  2026-09-08, by a read of the astubbs#472/#474/#410 diffs against the engine's ownership rules:**
  it is, and the stack expresses it, with the correction that the question dissolves - PC's
  instance never leaves the group (no file in the stack touches the consumer, the poll system or
  the subscription), so "rejoin" reduces to aborting the open transaction under the write lock,
  building and adopting a replacement producer on the control thread, and restoring the
  completed-but-uncommitted records for replay, drain-then-replay inside the same lock. All five
  broker invalidation conditions are covered on both the commit and produce paths; the
  `@GuardedBy` ledger, thread confinement and the produce/commit lock pair are respected and
  asserted. What the stack does not answer is astubbs#420's territory (the derived
  `transactional.id`, redaction), one wire-level test nobody wrote (a fence induced by real
  consumer-generation loss; both ITs use a rogue producer under the same id), and the plan's one
  open question - whether recovery should decline the write lock while a rebalance is in progress,
  which astubbs#410 does not check and the measurement meant to settle was not taken. Bounded, and
  review-sized; it does not change the tier 2 decision.
- ~~Which of the flakes in `test-untracked-ci-flakes.md` are load-shaped and which are real - the
  three module `simpleBatchTest` failures have the most sightings and no diagnosis~~ - **known,
  2026-09-08, astubbs#482: neither a flake nor a defect.** The test computed its expected batch
  count from the record count while drawing keys with replacement; under KEY ordering a shard
  yields one record per retrieval round, so a three-way key collision deterministically forces a
  fourth batch - the `2+1+1+1` shape every sighting carried, at about the rate the sightings
  showed, and only on the KEY parameter, which a contention reading could never explain. Reproduced
  red with a forced collision, green with keys drawn without replacement; the exact assertion is
  kept, and a new core test covers the collision case the old one can no longer reach. The other
  rows of [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) are untouched by this. The
  automated review found nothing blocking; astubbs#482 squash-merged 2026-09-08.

## What the astubbs#476 vetting sweep read as gating

Moved here from `process-candidate-ranking.md` on 2026-09-08 (it was written by the six-agent sweep
on 2026-09-07 and is the agents' reading, with their stated confidence - not the owner's decision).
Where it disagrees with the tiers above, the tiers say so: the poisoned-transaction pair (the sweep:
not gating; the owner's call is still open in tier 2), the transactional revoke wait (the sweep read
astubbs#466 as having replaced the unbounded wait, which is right, and astubbs#408 as owning the
bound), and the `batchSize` validation bound (the sweep: cheapest real fix; the triage below filed
it as 0.6.0.x - it could ride in tier 1). Item 2 in its list, the dead poll thread, is
astubbs#477, merged.

The bar above is "the bugs that are already open". Six area sweeps each named what they read as gating (the owner's pass over
the sweep's proposals is done - [`process-inflight-vet-sweep.md`](process-inflight-vet-sweep.md)
records it); this is the union,
ordered by user-visible consequence, with the confidence each agent stated. The mechanical gate
comes first because nothing else matters until it clears.

- **The quarantine registry is non-empty, and every entry is unowned.** `release.yml` refuses the
  cut while [`docs/quarantined-tests.md`](../quarantined-tests.md) lists anything; read that file,
  not this line.
- **Verified defects, in the code as written today:**
  1. `bug-857-transactional-revoke-wait.md` - was the unbounded wait inside the revoke callback,
     with a user report carrying upstream's verified-bug label. astubbs#466 (merged the day the sweep
     ran) replaced the spin with a wait bounded by `commitLockAcquisitionTimeout`; whether that bound
     is right is what is left, and astubbs#408 holds it. The sweep also read
     `core-revoke-commit-skips-the-work-mailbox-drain.md` as gating - a deterministic exactly-once
     break with C9 refuted - and the same commit fixed it; the note is gone and the record is in
     `docs/solutions/logic-errors/`.
  2. A dead poll thread holding its partitions for `max.poll.interval.ms` in the default commit
     mode - read as gating, traced end to end but untested and unfixed when the sweep ran. Now
     tested and fixed by astubbs#477 (merged): `maybeCloseConsumer` gained an arm for a poll thread
     that ended without closing the consumer; the note is gone and the record is in
     `docs/solutions/logic-errors/`.
  3. `pr-431-must-pair-its-queue-removal-with-the-shard-removal.md` with
     `bug-retry-queue-write-lock-on-the-rebalance-path.md` - the retry-queue orphan window; master
     is still shard-first and astubbs#431 is a draft. *(Since overtaken: astubbs#431 closed as
     superseded by the owner's astubbs#481, ready and green, which is what tier 1 lists.)*
  4. `bug-unvalidated-batchsize.md` - `batchSize(0)` silently processes nothing; one `validate()`
     bound closes all three shapes (astubbs#311). The cheapest real fix in the set.
  5. `bug-max-failure-history-is-inert.md` - a public option that does nothing; removing it is
     breaking, so it is settled before the major or carried forever.
  6. `bug-offset-commit-timeout-does-two-jobs.md` - the default makes a retry unreachable; the fix is
     a design choice among three.
  7. `bug-162-offset-state-truncation.md` - a WARN operators alert on, firing falsely for every new
     group; decision 5 in the section above.
  8. `bug-unbounded-log-lines.md` - record keys and values printed at WARN on a line that asks to be
     pasted into a public issue; cheap to fix.
- **Contract and compatibility, where a major is the only window:**
  `core-bytearray-encodings-have-no-codec.md` (two magic bytes),
  `core-pc-owns-the-clients-it-uses.md` (the consumer-instance option). The sweep also listed
  `core-139-public-api-thread-safety-contract.md` here; the owner ruled astubbs#139 out of v6 scope
  on 2026-09-08 and the note is deferred after v6.
- **Instruments the release decision is read through, currently lying or unproven:**
  `test-chaos-autopsy-omits-fleet-violations.md` (a clean autopsy after a fleet-violation kill,
  confirmed in code), `test-perf-lane-asserts-a-deadline-on-a-varying-machine.md` (a required check
  that fails on arithmetic), `test-no-progress-window-may-not-transfer-to-w1.md` (sightings at the
  bound, none replayed), `ci-codecov-flags-not-like-for-like.md` (proposal 9),
  `ci-broker-container-exit-126-is-undiagnosable.md`.
- **Decisions, not engineering:** the astubbs#161 and astubbs#181 replies (items 1 and 2 at the top
  of this file); the "is it enough?" call, whose own target date has passed; and astubbs#257's
  changelog wording, which has one window because the section is generated from the log.
- **Read as not gating, by the agent that vetted each:** the new modules (astubbs#271, astubbs#269,
  astubbs#268 - capabilities, not defects); the `deps-` majors; every `issue-response-*` draft; the
  `static-` registers (advisory lanes); the `branch-` notes; the `test-debt` and feature notes; the
  unfenced `PartitionState` booleans and the plain-int counter (real, unmeasured, possibly absorbed
  by the shared-nothing rework); and the poisoned-transaction pair, where today's behaviour is
  strictly better than what it replaced.

## Open defects with no PR - each one's disposition against the bar

The sweep's reading above agrees with the look-at items below and adds one this section had filed
as 0.6.0.x: `batchSize(0)` silently processes nothing, and the sweep calls the `validate()` bound
"the cheapest real fix in the set" (astubbs#311). Its list of instruments the release decision is
read through that are currently lying or unproven is worth reading before trusting a green.

"Gate on open bugs" only works if every open bug has a disposition, so this is every `bug-` note on
master that no queue PR addresses (`ls docs/inflight/bug-*.md` is the list; the impact tag on each
is the sort key). Re-derive it before the tag rather than trusting it: a note can gain a PR or lose
its subject at any merge.

**Look at before the tag - these contradict the release claim if left silent:**

- ~~The eager-mode stall that reproduces with the fixes applied~~ - **withdrawn, astubbs#478 (merged)**
  (2026-09-08): not a defect, a timing bound crossing on processor count. Nothing to ship or name.
- ~~Poller death leaves the consumer open in consumer-commit modes~~ - **now astubbs#477 in tier 1**
  (2026-09-08). The fix was as small as the note proposed, and its defect-class sweep - cleanup gated
  on "am I the role-holder?" where the holder may be dead - found no other instance across the four
  modules' `close()` paths.

**Owner's decision - taken 2026-09-08: in v6, as astubbs#480 (tier 1).**

- ~~Run-length plausibility ceiling~~ - a readable but absurd run length marked a vast range complete
  and PC silently skipped it; data-loss class, reachable only through a corrupt or foreign payload,
  which is why astubbs#207 did not cover it. astubbs#480 bounds every decoded run and bitset by the
  partition's log end offset - the one bound that cannot reject a real map, since PC only encodes
  offsets it polled - routed through `invalidOffsetMetadataPolicy` with no parallel policy, failing
  open with a warn if the broker will not answer. The same defect class was found and fixed in the
  bitset decoder; the simple serialisation has no declared count and is clean. The inflight note is
  retired into `docs/solutions/`.

**0.6.0.x - open, real, not a gate for a bug release:**

- Config lies: `maxFailureHistory` is read nowhere; `offsetCommitTimeout` bounds two different
  waits; `batchSize` is unvalidated (astubbs#311, deferred with its sibling - but see the sweep's
  "cheapest real fix" reading above; a one-line `validate()` bound could ride in tier 1).
- Blind spots: the racy and uncalled pause API; no metric for a discarded offset map under the
  default `IGNORE` policy; the worker future swallowing framework exceptions.
- Misdirection: the plain-`int` out-for-processing counter; the module's processor reference
  overwritten before the owner guard; the rest of the unbounded-log-lines class; the two 857 mirror
  attributions never verified against the reporter's environment.
- The shutdown teardown race; the test-only `close()` shadowing; and
  [`bug-shared-collections-across-the-poll-boundary.md`](bug-shared-collections-across-the-poll-boundary.md),
  which is mostly stale - the metrics set and the shared empty set it names are both fixed on master
  and the note needs shrinking to whatever remains.

**Not a bug note, but a signal - settled:** the `simpleBatchTest` failures across the Reactor,
Mutiny and Vert.x modules were the test's own randomised key draw, not the batcher (astubbs#482,
see the known-unknowns section). The register's most-sighted row is retired.

## Tag-day artefact checks - are the things we publish true on the day we cut?

Folded in from the register that was `release-0600-blockers.md`. Scope: `CHANGELOG.adoc` and
`README.adoc` as published. Release mechanics stay in [`release-0.6.0.0.md`](release-0.6.0.0.md);
the tracker is astubbs#197.

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
  notes; astubbs#199 fixes the match (tier 3). The rest of astubbs#197's triage list has been picked
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
  astubbs#186, astubbs#188 and astubbs#195 close with a pointer to the release; and the
  `issue-response-*.md` drafts are posted in the same sweep.

Context worth inheriting on the day:

- **`README.adoc` is generated - never hand-edit it.** Edit `src/docs/README_TEMPLATE.adoc` and
  regenerate with `./mvnw -N asciidoc-template:build`. A PR that touches only the template has
  silently not changed the published README.
- **`CHANGELOG.adoc`'s `== 0.6.0.0` section is working text until the tag** - it is regenerated from
  the commit log, so do not quote it as the release notes, and when agents work in parallel exactly
  one holds that file; it is the highest-collision file in the repo.
- **A dependency version in prose drifts silently.** The `3.9.1`/`3.9.2` mismatch came from a
  Dependabot group bump moving `kafka.version` after the note was written. Re-read the
  `=== Dependencies` section against `pom.xml` immediately before cutting, not weeks earlier.

## Upstream items with no fix PR and no prepared response - surveyed 2026-09-08

Every upstream open issue carries a fork reply from the 2026-08-05 mirror sweep pointing at its
mirror, so nothing upstream is silent. What follows is what has **no fix PR, open or merged, and no
draft response beyond that pointer**. Reproduce the survey rather than trusting this list: join
`gh issue list -R confluentinc/parallel-consumer --state open`, the mirrors
(`gh issue list -R astubbs/parallel-consumer --state all --label upstream-mirror`), fork PRs citing
`confluentinc#N` (`gh pr list -R astubbs/parallel-consumer --state all --json title,body`), the
manifest `src/docs/development/upstream-map.yaml`, and `scripts/upstream-sweep.sh --audit`.
[`upstream-coverage-completeness.md`](upstream-coverage-completeness.md) owns the standing
obligation; this section is one dated pass at it, kept here because the pre-release sweep is when
these get answered.

**Bugs with no fix PR:**

- confluentinc#843 (astubbs#178) - same key on two threads across a rebalance. A contract question,
  wait-for-info; [`core-178-key-order-across-a-rebalance.md`](core-178-key-order-across-a-rebalance.md).
- confluentinc#546 (astubbs#162) - truncating state; the replay branch is refuted (astubbs#484, merged),
  the false-truncation WARN is what remains and wants an owner decision.
- confluentinc#551 (astubbs#164) - batching not as expected; the fork verified the over-request as
  astubbs#311, no PR.
- confluentinc#887 (astubbs#189) - a poison record re-forms the identical batch on every retry;
  manifest says none.
- confluentinc#777 (astubbs#173) - settled as by-design; the documentation reply and the grace-period
  decision are what remain.
- confluentinc#597 (astubbs#166, mirror closed as fixed) - the poller-death residual above.
- confluentinc#803 (astubbs#44) - has astubbs#408, addressed on paper only until it merges.

**Feature requests with nothing behind them** (no branch, PR or note beyond the mirror):

- API surface: confluentinc#78 executor customisation, confluentinc#170 `CompletableFuture`,
  confluentinc#520 safe consumer-API exposure, confluentinc#782 seek to offset, confluentinc#860
  managed-executor params, confluentinc#879 no-commit option.
- Error handling: confluentinc#304, confluentinc#391 and confluentinc#550 - deserialization failures,
  the largest cluster of user asks with no design;
  [`core-163-poll-path-has-no-error-seam.md`](core-163-poll-path-has-no-error-seam.md) confirms there
  is no seam. confluentinc#718 terminate processing.
- Batching and ordering: confluentinc#314 combine queues across partitions, confluentinc#560 min
  batch plus max wait (roadmap: ideated), confluentinc#902 freshest record per key, confluentinc#321
  large-message chunking.
- Performance: confluentinc#322 disk-backed produce queue, confluentinc#394 least-loaded broker,
  confluentinc#540 per-partition backpressure.
- Docs and examples: confluentinc#171 Spring Boot example, confluentinc#178 fan-out with DLQ,
  confluentinc#180 vert.x POST, confluentinc#115 tombstones javadoc.

Two mirror labels overstate coverage: confluentinc#314 and confluentinc#394 carry `pr-available` on
their mirrors, and no fork PR cites either.

**Upstream open PRs with no fork action or comment:**

- confluentinc#915 batch construction strategy - manifest none; the roadmap's batch-composition
  decision is pending. A contributor is waiting on this one.
- confluentinc#867 Vert.x 5 major - nothing in the fork.
- confluentinc#908 virtual threads - the fork went its own way in astubbs#360 and replied on the
  issue, never on the contributor's PR.
- confluentinc#918, confluentinc#919, confluentinc#920, confluentinc#901 - each absorbed by fork work
  (the log-noise fixes, `docs/building.md`, dropping the licence plugin), and none of the four PRs
  has a comment saying so.

**Unanswered conversations:**

- confluentinc#894 got a follow-up on 2026-09-01 asking where interactive replay should live. Partly
  vendor marketing; the only unanswered direct question upstream.
- Upstream discussions with zero replies - `scripts/upstream-sweep.sh --audit` lists them;
  [`upstream-discussions-unanswered.md`](upstream-discussions-unanswered.md) defers them to after v6.

**Drafted and waiting, not gaps:** the `issue-response-*.md` drafts and the astubbs#337 drafts for
confluentinc#894. [`upstream-tell-809-833-the-hang-is-fixed.md`](upstream-tell-809-833-the-hang-is-fixed.md)
is stale in one respect: both reports already carry the 2026-08-05 fork reply, though neither has
been told the fixes merged.

## Delete when

The tag is cut. Migrate first: the family and data-loss dispositions above go into the release note
text and `docs/data/roadmap.yaml`'s `known-defects-cleared` entry; the "context worth inheriting"
bullets go to [`docs/releasing.md`](../releasing.md) if it does not already carry them; the
upstream survey's residue goes to
[`upstream-coverage-completeness.md`](upstream-coverage-completeness.md) if any of it is still
unanswered after the sweep.
