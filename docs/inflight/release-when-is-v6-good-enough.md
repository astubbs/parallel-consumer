# When do we ship v6? Decided: a bug release, overdue - and the burn-down to cut it

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

**Deliberately its own file, not a section inside
[`release-0.6.0.0.md`](release-0.6.0.0.md).** A section is invisible until someone opens that file;
a note gets its own line in the session-start index. This question needs to be *met*, not looked up
- so it is filed where an agent trips over it.

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

**The mental hurdle, named so it stops steering.** The owner's instinct is that the first fork
release has to be impressive. The record says otherwise: the delta since 0.5.3.3 is the largest
this codebase has ever shipped in one version - the confluentinc#857 commit deadlock, the torn-read
family, the metrics leak, offset accuracy on assignment, the async-commit acknowledgement, the
package rename, MDC propagation, the log-noise fixes, and the chaos and Lincheck lanes that guard
them. A second release gets a smaller launch, but the roadmap announcement is what people read for
"what is coming", and that is decoupled from the tag. Perfect is what has kept this release from
shipping since the 26 August date passed; the previous version of this note said the failure mode
was never shipping, and that is the failure mode that occurred.

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

- [ ] **astubbs#470** - an async commit counts as committed when the broker answers, not when it is
  sent. Silent loss on the shipped default commit mode. Closes the
  [`bug-async-commit-marked-successful-before-broker-ack.md`](bug-async-commit-marked-successful-before-broker-ack.md)
  note; serves astubbs#248.
- [x] **astubbs#466** - merged 2026-09-08. The revoke-path commit drains the work mailbox first; in
  transactional mode a rebalance could publish a transaction whose offsets omitted records it
  contained. Its proof left quarantine with it. Its commit body says it **collides with astubbs#408
  on `tryCommitOffsetsOnRevoke`**, so astubbs#408 now carries that resolution.
- [ ] **astubbs#468** - the stale sweep removes only the container it inspected, never a fresh
  replacement racing in from the controller. Rebalance-shaped.
- [ ] **astubbs#469** - the two remaining `PartitionState` flags that cross threads, measured and then
  fenced or redesigned. The follow-on astubbs#349 deliberately left.
- [ ] **astubbs#431** - the rebalance callbacks decline the retry queue's write lock instead of waiting
  for it. A stall from the confluentinc#857 defect-class sweep; its three prerequisites merged
  2026-09-07.
- [ ] **astubbs#473** - clears the two remaining quarantine entries by fixing what they were about.
  The release guard blocks while `docs/quarantined-tests.md` has any.
- [ ] **astubbs#477** - a dead broker-poll thread now closes the consumer in the consumer-commit
  modes, the shipped default among them, so the group rebalances at once instead of after
  `max.poll.interval.ms`. Promoted from the no-PR triage on 2026-09-08: the fix is one derived
  predicate and one condition, proven red on every consumer-commit mode and green on the
  transactional control arm. Retires its inflight note into `docs/solutions/`.

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
(offset density), astubbs#360 (virtual threads), astubbs#471 and astubbs#405 (soak and torture
harnesses - test infrastructure, unless a run finds a data-loss defect), and every Streams, proxy,
perf, rate-limiting and dashboard stack.

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

**Still open, and the release note names each:**

- The transactional revoke wait, astubbs#44 (confluentinc#803) - outside v6 by the 2026-09-07
  decision; the release claim names it as the known exception. The decision predates astubbs#466,
  which replaced the unbounded spin with a wait bounded by `commitLockAcquisitionTimeout`, so what
  astubbs#408 (tier 2) still owns is declining instead of waiting, and whether that bound is right.
  The exception the claim names should say "bounded, not yet declined", not "unbounded".
- ~~An eager-mode (`PERIODIC_CONSUMER_SYNC`) stall that reproduces on trees carrying astubbs#29's
  fix (the family note's "fourth open item")~~ - **withdrawn 2026-09-08, astubbs#478.** Four replays
  of the recorded seed on today's master all drained completely with zero loss; the "stall" was the
  Class 2 timing bound, whose crossing flips with the processor count at a fixed seed and tree. The
  grid the item was opened on was also never a one-term A/B. What survives from this line is the
  per-shard liveness gap, already tracked and deferred with a stated bar - a blind spot to name,
  not a bug to fix before cutting.
- A rebalance stall in async unordered mode from `MultiInstanceRebalanceTest` (the "fifth open
  item"), blocked on progress-tracker instrumentation that does not exist yet. Unattributed.
- `INSTANCE_STALL` and `ZOMBIE_MEMBER` sightings that replay clean on idle runners, so they read as
  starvation rather than a wedge. Not a confirmed defect; not ruled out either.
- A dead broker-poll thread leaving the consumer open in consumer-commit modes, no LeaveGroup until
  `max.poll.interval.ms` - **fixed in the queue, astubbs#477, tier 1.**

## What v6 must say about data loss and duplicates

- **Fixed in the queue:** the async-commit acknowledgement (astubbs#470).
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
- **Open, no PR:** the "reset to earlier offset" replay branch behind confluentinc#546
  ([`bug-162-offset-state-truncation.md`](bug-162-offset-state-truncation.md)) is an untested
  hypothesis. **Proposed: 0.6.0.x**, and say so in the release note rather than claim it.
- **Never reproduced:** the commit-response timeout (confluentinc#809, confluentinc#833). astubbs#471
  is the first experiment that hunts it, and its first runs found a stall. Not a v6 gate; the
  release note says the symptom's known causes are fixed and the reports were never reproduced.

## Known unknowns the release note should not paper over

**Every item below is being pursued (owner's instruction, 2026-09-08), each by its own agent on its
own branch; `gh pr list -R astubbs/parallel-consumer` shows the PRs as they open.** Code-shaped
questions run in parallel; the replay-shaped ones (chaos and soak) run one at a time, because
several replay agents on one machine produce exactly the starvation artefacts they are meant to
rule out. Order of the replay queue: the eager-mode stall (done - withdrawn, astubbs#478), then the six deadlock captures
with the fix applied, then the async-unordered rebalance stall with its progress-tracker
instrumentation, then the `INSTANCE_STALL`/`ZOMBIE_MEMBER` idle-versus-loaded replay, then the
commit-response-timeout stall astubbs#471's soak found.


- Whether the six deadlock captures that verified astubbs#29's mechanism ever replay clean **with
  the fix applied** - the owning solutions doc still says "unproven".
- Whether the shard-displacement orphan window
  ([`bug-shard-displacement-orphans-the-retry-queue-entry.md`](bug-shard-displacement-orphans-the-retry-queue-entry.md))
  is reachable in production.
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
- Which of the flakes in [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) are
  load-shaped and which are real - the three module `simpleBatchTest` failures have the most
  sightings and no diagnosis.
- The maturity claim itself: `docs/data/module-maturity.yaml` carries a bare `production-use` next
  to a conditional support posture, and a renderer can lift the bare value without its condition.
  The tag-day checks below carry the recheck.

## What the astubbs#476 vetting sweep read as gating

Moved here from `process-candidate-ranking.md` on 2026-09-08 (it was written by the six-agent sweep
on 2026-09-07 and is the agents' reading, with their stated confidence - not the owner's decision).
Where it disagrees with the tiers above, the tiers say so: the poisoned-transaction pair (the sweep:
not gating; the owner's call is still open in tier 2), the transactional revoke wait (the sweep read
astubbs#466 as having replaced the unbounded wait, which is right, and astubbs#408 as owning the
bound), and the `batchSize` validation bound (the sweep: cheapest real fix; the triage below filed
it as 0.6.0.x - it could ride in tier 1). Item 2 in its list, the dead poll thread, is now
astubbs#477 in tier 1.

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
  2. `bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md` - in the default commit
     mode, a dead poll thread holds its partitions for `max.poll.interval.ms`; traced end to end,
     untested, unfixed.
  3. `pr-431-must-pair-its-queue-removal-with-the-shard-removal.md` with
     `bug-retry-queue-write-lock-on-the-rebalance-path.md` - the retry-queue orphan window; master
     is still shard-first and astubbs#431 is a draft.
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

- ~~The eager-mode stall that reproduces with the fixes applied~~ - **withdrawn, astubbs#478**
  (2026-09-08): not a defect, a timing bound crossing on processor count. Nothing to ship or name.
- ~~Poller death leaves the consumer open in consumer-commit modes~~ - **now astubbs#477 in tier 1**
  (2026-09-08). The fix was as small as the note proposed, and its defect-class sweep - cleanup gated
  on "am I the role-holder?" where the holder may be dead - found no other instance across the four
  modules' `close()` paths.

**Owner's decision:**

- **Run-length plausibility ceiling** -
  [`bug-run-length-plausibility-ceiling.md`](bug-run-length-plausibility-ceiling.md). A readable but
  absurd run length marks a vast range complete and PC silently skips it. Data-loss class, reachable
  only through a corrupt or foreign payload, which is why astubbs#207 did not cover it. A decode-side
  ceiling is small; either it ships in v6 or the release note names it.

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

**Not a bug note, but a signal:** the `simpleBatchTest` flake across the Reactor, Mutiny and Vert.x
modules has the most sightings in [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) and
no diagnosis. The same batch test failing the same way in three modules is not noise. Not a gate.

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
- confluentinc#546 (astubbs#162) - truncating state; the replay branch above.
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
