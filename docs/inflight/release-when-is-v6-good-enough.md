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

## The decision, 2026-09-08

**v6 is a bug release. It is overdue. The bar is the one already stated** in
[`release-0.6.0.0.md`](release-0.6.0.0.md) under *"This release is a stability release, and that is
the point"* - nothing else has to be true. The three questions the previous version of this note
left open are answered:

- **What is the bar?** Stability release, and that is the whole bar.
- **Which open items are genuinely v6?** The merge queue below, and only it. Everything that is
  finished but does not fix a currently-open defect follows in a later release. A thing being ready
  is not a reason to ship it in this one.
- **Does the roadmap announcement have to be simultaneous?** No. Its plan is astubbs#446; it can
  land before the tag and take pressure off sooner. The announcement is what lets the first release
  be a bug release without being the only thing anyone sees.

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

### Tier 2 - the producer-recovery stack, bottom-up, in this order

astubbs#225 (survive producer fencing rather than dying) is a feature, but two open **defects** are
stacked on it and cannot land without it: the poisoned-transaction wedge and the transactional
revoke wait that carries upstream's verified-bug label. That is why the stack is in a bug release.
Each rung was re-cut on 2026-09-07 so it can be reviewed against pieces already reviewed.

- [ ] **astubbs#472** - the vocabulary and plumbing: what the broker reports, how PC builds another
  producer. Changes no behaviour.
- [ ] **astubbs#474** - keep every completed record until the commit that carries it succeeds. The
  exactly-once argument of recovery, on its own.
- [ ] **astubbs#410** - recovery itself. Closes astubbs#225.
- [ ] **astubbs#434** - abort a transaction poisoned by a terminal send failure. The wedge in
  [`bug-wedged-after-poisoned-transaction.md`](bug-wedged-after-poisoned-transaction.md) and
  [`bug-poisoned-transaction-not-aborted-while-running.md`](bug-poisoned-transaction-not-aborted-while-running.md).
- [ ] **astubbs#408** - a revocation declines the transaction lock instead of waiting on it. Closes
  astubbs#44 (confluentinc#803). **The one PR in the queue with real reds** - checklist, hygiene,
  the macOS shell lane and the heavy integration shard - so it needs work, not just a merge. Its
  design question was settled by stacking on astubbs#410, and it must now resolve the
  `tryCommitOffsetsOnRevoke` collision with the merged astubbs#466.
- **astubbs#420** - derive the `transactional.id`, the enforced factory, config redaction. Producer
  ownership polish stacked above recovery. **Proposed: after v6.** It fixes no open defect.

### Tier 3 - release plumbing, then tag

- [ ] **astubbs#199** - publish the curated changelog section as the GitHub Release body. Without it
  the release page is empty.
- [ ] **astubbs#446** - lift the announcement plan onto master, so the announcement is not being
  written from a branch nobody merges.
- [ ] The tag-day artefact checks in the section of that name below.
- [ ] Amend the release claim, not the standard, for what is still open in the confluentinc#857
  family below. The claim is "every known **critical** defect resolved and evidenced", and the
  family is not closed - say which mechanisms are, and which sightings remain unattributed.
- [ ] Post the drafted issue responses (`ls docs/inflight/issue-response-*.md` and
  [`release-0.6.0.0-issue-response-drafts.md`](release-0.6.0.0-issue-response-drafts.md)) in the
  pre-release sweep [`docs/releasing.md`](../releasing.md) describes.
- [ ] Tag. Then the after-it-ships items below, and the rest of astubbs#197.

### Can follow - finished or nearly, and deliberately not v6

Named so nobody re-argues them in: astubbs#352 (commit-failure seam - a feature, even though
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

- The transactional revoke wait, astubbs#44 (confluentinc#803) - astubbs#408 in tier 2.
- An eager-mode (`PERIODIC_CONSUMER_SYNC`) stall that reproduces on trees carrying astubbs#29's fix
  (the family note's "fourth open item"). Unattributed.
- A rebalance stall in async unordered mode from `MultiInstanceRebalanceTest` (the "fifth open
  item"), blocked on progress-tracker instrumentation that does not exist yet. Unattributed.
- `INSTANCE_STALL` and `ZOMBIE_MEMBER` sightings that replay clean on idle runners, so they read as
  starvation rather than a wedge. Not a confirmed defect; not ruled out either.
- A dead broker-poll thread leaves the consumer open in consumer-commit modes, no LeaveGroup until
  `max.poll.interval.ms` -
  [`bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md`](bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md).
  Diagnosed 2026-09, no PR. **Decide: v6 or 0.6.0.x** - the no-PR triage below argues for looking
  now, because the shipped default commit mode is exposed.

## What v6 must say about data loss and duplicates

- **Fixed in the queue:** the async-commit acknowledgement (astubbs#470); the poisoned-transaction
  wedge (astubbs#434).
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

- Whether the six deadlock captures that verified astubbs#29's mechanism ever replay clean **with
  the fix applied** - the owning solutions doc still says "unproven".
- Whether the shard-displacement orphan window
  ([`bug-shard-displacement-orphans-the-retry-queue-entry.md`](bug-shard-displacement-orphans-the-retry-queue-entry.md))
  is reachable in production.
- Whether "rejoin" after producer fencing is expressible in PC's lifecycle - flagged in
  [`core-recoverable-producer-fencing.md`](core-recoverable-producer-fencing.md) as needing
  investigation; astubbs#410 is the answer under review.
- Which of the flakes in [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) are
  load-shaped and which are real - the three module `simpleBatchTest` failures have the most
  sightings and no diagnosis.
- The maturity claim itself: `docs/data/module-maturity.yaml` carries a bare `production-use` next
  to a conditional support posture, and a renderer can lift the bare value without its condition.
  The tag-day checks below carry the recheck.

## Open defects with no PR - each one's disposition against the bar

"Gate on open bugs" only works if every open bug has a disposition, so this is every `bug-` note on
master that no queue PR addresses (`ls docs/inflight/bug-*.md` is the list; the impact tag on each
is the sort key). Re-derive it before the tag rather than trusting it: a note can gain a PR or lose
its subject at any merge.

**Look at before the tag - these contradict the release claim if left silent:**

- **The eager-mode stall that reproduces with the fixes applied** - the "fourth open item" in
  [`bug-857-family.md`](bug-857-family.md): two seeds, `PERIODIC_CONSUMER_SYNC`, reproduces every
  time, undiagnosed. Time-box a diagnosis alongside tier 1; if it is not understood when tier 3 is
  done, ship and name it in the release note rather than wait.
- **Poller death leaves the consumer open in consumer-commit modes** -
  [`bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md`](bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md).
  The shipped default commit mode is one of them, so a dead poll thread idles the partition until
  the broker evicts the member. The proposed fix shape is small; if it is, this joins tier 1.
  Otherwise 0.6.0.x, named.

**Owner's decision:**

- **Run-length plausibility ceiling** -
  [`bug-run-length-plausibility-ceiling.md`](bug-run-length-plausibility-ceiling.md). A readable but
  absurd run length marks a vast range complete and PC silently skips it. Data-loss class, reachable
  only through a corrupt or foreign payload, which is why astubbs#207 did not cover it. A decode-side
  ceiling is small; either it ships in v6 or the release note names it.

**0.6.0.x - open, real, not a gate for a bug release:**

- Config lies: `maxFailureHistory` is read nowhere; `offsetCommitTimeout` bounds two different
  waits; `batchSize` is unvalidated (astubbs#311, already deferred with its sibling).
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
- **The release page must not be empty** - astubbs#199, tier 3 above. The rest of astubbs#197's
  triage list has been picked up (the magic-byte hazard in astubbs#217, the load-factor WARN in
  astubbs#201, MDC in astubbs#205); the tracker's own checklist boxes lag the work.
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
