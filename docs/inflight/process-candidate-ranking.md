# Next candidates, ranked

<!-- inflight-type: register -->
<!-- inflight-vetted: 2026-09-07 - all eight ranked decisions are still open issues awaiting the same reply (astubbs#161, astubbs#181, astubbs#163, astubbs#189, astubbs#162, astubbs#241, astubbs#173, astubbs#178) and every note they name is present, so the ranking stands unchanged; four settled lines removed instead - the astubbs#155/astubbs#169/astubbs#170 scheduling sentence and the logging-verbosity pick (merged as astubbs#203 and astubbs#428), the astubbs#40 dedup pick (astubbs#206), and confluentinc#906 out of the contributor-friction pick (astubbs#194 closed) -->


## Decisions waiting on the maintainer, ranked

**These outrank everything below, and the reason is cost rather than importance.** Each is a note
whose engineering is already done or already unnecessary - what is left is one reply, and until it
arrives the note cannot close and the work behind it cannot be scheduled. Ranked by how little input
each needs, not by the size of what it unblocks: an item needing a yes/no beats one needing a policy,
and a policy beats a bound that has to be argued for.

They came out of the 2026-08-20 mirror triage, which produced one note per issue - each carrying the
verification, the draft answer, and the collision list. **Do not re-derive any of it here**; the note
named on each line owns it.

1. **astubbs#161** (confluentinc#543), `upstream-161-reactor-scheduler-rationale.md` - the reply is
   written and postable as-is, so the only decision is to post it and close. Two code findings ride
   along that nothing else records: the scheduler supplier is resolved per wrapped invocation rather
   than once, so a factory-shaped supplier leaks a `Scheduler` and its threads every batch, and the
   two-argument `ReactorProcessor` constructor has no test.
2. **astubbs#181** (confluentinc#862), `deps-181-java-24-compatibility.md` - close it on the
   kafka-clients 3.9.2 rationale and let astubbs#128 carry the CI proof, or hold it open until that
   lane exists. The note has the evidence and states the one caveat (`MockConsumer` cannot exercise
   SASL, which is the path that broke).
3. **astubbs#163** (confluentinc#550), `core-163-poll-path-has-no-error-seam.md` - post the drafted
   answer, then close as a duplicate of astubbs#153 with astubbs#148 as the contained step. **This
   one has a deadline the others do not**: it corrects open question 6 of the DLQ prior-art report on
   astubbs#313, which currently assumes deserialization failures can ride along with the DLQ work.
   They cannot, on the mechanism. Deciding DLQ requirements before this is answered settles them on a
   false premise.
4. **astubbs#189** (confluentinc#887), `core-189-batch-failure-granularity.md` - go/no-go on jitter in
   the default retry delay. A small change, but it moves retry timing for every existing deployment,
   which is why it is a call rather than a commit. Nothing else in the poison-isolation ladder waits
   on the answer.
5. **astubbs#162** (confluentinc#546), `bug-162-offset-state-truncation.md` - should absent commit
   data warn at all? It is the normal state of a new group or an expired offset, so the honest
   handling is a quieter distinct message and no truncation branch - against which operators alert on
   the current line.
6. **astubbs#241** (confluentinc#144), `core-241-tx-commit-failure-taxonomy.md` - agree the issue's
   stated premise died in confluentinc#355, then keep it open with a rewritten `## Fork status` and
   relabel `bug` to `feature`. No defect is demonstrated; what survives is a policy design.
7. **astubbs#173** (confluentinc#777), `upstream-173-revocation-duplicate-processing.md` - should PC
   offer a revocation grace period at all? Upstream declined it. **If the answer is no, confluentinc#777
   is a documentation obligation rather than a defect** and the close is unblocked - at the cost of a
   README section and one chaos cell that must be run rather than predicted.
8. **astubbs#178** (confluentinc#843), `core-178-key-order-across-a-rebalance.md` - is an undrained
   old-epoch delivery a violation of the README's "strong ordering by key", or legitimate
   at-least-once? Last because it is the only one that needs a *bound* argued for rather than a
   yes/no, and `KeyOrderLedger`'s javadoc already says picking that number is the whole job.

**What is NOT on this list, from the same triage, and why:** astubbs#139 is a 1.0 blocker with a
four-step definition of done in `core-139-public-api-thread-safety-contract.md` - real work, not a
call. astubbs#175 has no decision left in it; its one live strand is the AB-BA wedge that
<!-- post-merge: checked - names that PR as the work the strand belonged to, in the past tense, so it reads the same once it has landed -->
astubbs/parallel-consumer#29 carried.

## Proposals from the 2026-09-07 vetting sweep, ranked by how little input each needs

The first grooming sweep ([`docs/grooming.md`](../grooming.md)) re-read every open note against the
tree. Where a note was owner-gated - a `bug` at `stall` or worse, a `release-` note, a public-API
contract - the agent wrote what it found into the note's `inflight-vetted` marker as `PROPOSED` and
changed nothing else. `grep -l 'inflight-vetted:.*PROPOSED' docs/inflight/*.md` is the live list;
each marker carries the evidence in full, so this section is only the order and the ask. Accepting
one means applying the outcome and replacing the marker with a plain stamp; declining means a stamp
saying so. Like the section above, the order is cost of the reply, not importance.

**Yes/no, the evidence is complete and the outcome is mechanical:**

1. **Close** `ci-merge-guard-fails-open-on-bsd-stat.md` - both halves shipped (the hook probes the
   platform and fails closed; `repo-hygiene.yml` has a macOS lane).
2. **Close** `ci-the-coverage-uploads-still-use-the-inert-glob.md` - its own delete-when holds: the
   two flags now report different figures on master.
3. **Close** `bug-shared-collections-across-the-poll-boundary.md` - both named defects fixed on
   master; the branch it tracks is far behind with no PR.
4. **Correct the counts** in `static-sneaky-throws-blind-the-analysers.md`, title included - the
   marker holds today's figures and the command that yields them.
5. **Rewrite Blocker 2** of `static-infer-threadsafe-is-blocked-by-third-party-interfaces.md` - the
   method it rests on no longer exists; the map is installed once at construction.
6. **Shrink** `test-retry-queue-behaviour-untested.md` to its two unasserted bullets - four
   `RetryQueue*` test classes now exist and the first bullet is fixed.
7. **Shrink** `bug-torn-read-family.md` to the racing-double unification and the next hunt - every
   candidate it tracks is fixed and verified in the tree.
8. **Shrink** `bug-177-commit-response-timeout-unreproduced.md` and
   `bug-857-mirror-attributions-unconfirmed.md` to their astubbs#175 halves - astubbs#177 closed on
   2026-09-01 with no comment naming both candidates, which is the outcome both notes warned about.
9. **Shrink** `ci-codecov-flags-not-like-for-like.md` - the first item is discharged and the two-band
   jumps stopped after astubbs#464; the mechanism is still unproven, which is what remains.
10. **Shrink** `ci-bsd-portability-gaps.md` - item 3 landed as `gnu-bsd` rows in the shell-hazards
    gate; items 1 and 2 stand.
11. **Shrink** `bug-stale-sweep-iterator-evicts-fresh-replacement.md` - the remove-by-key defect is
    live; the iterator half was closed by astubbs#336 and the note's middle section contradicts it.
12. **Re-premise** `bug-857-transactional-revoke-wait.md` - astubbs#466 landed the day the sweep ran
    and removed the unbounded spin it describes; what survives is whether the new bound is right
    against confluentinc#803, and astubbs#408's amendment. Its stale citations are listed in the
    marker.
13. **Shrink** `bug-retry-queue-write-lock-on-the-rebalance-path.md` - the defect is intact; "not
    started, no design agreed" is false, astubbs#431 is the open fix and `RetryQueue.remove`'s own
    javadoc names it.
14. **Shrink** `upstream-173-revocation-duplicate-processing.md` to the unposted draft - the adoc
    misdirection it was filed against has already been corrected to REFUTED.
15. **Shrink** `core-unmailboxed-container-recovery.md` - the "where it surfaces" half is overtaken
    (`failFatallyOnUnmailboxableRecord` shipped); the recovery blind spot itself is still real.
16. **Shrink** `test-chaos-teardown-double-close.md` - item 2 is half fixed (`markStopRequested()`
    is called; `closePending` is still never set for a drain).

**A judgement, not a yes/no:**

17. **Merge** `bug-wedged-after-poisoned-transaction.md` into
    `bug-poisoned-transaction-not-aborted-while-running.md` - its own question is answered in-file
    and the residue duplicates the sibling. Which note survives is the call.
18. **Shrink** `bug-857-family.md` (astubbs#29 merged with its fix; the fourth and fifth strands are
    still unexplained) **and decide its type** - it describes itself as a register and is typed
    `bug`, which is load-bearing for the owner gate.
19. **Re-premise** `core-139-public-api-thread-safety-contract.md` - `state` is volatile now, so the
    note's central claim moved; what remains is the non-atomic pause/resume transitions and the
    absent per-method contract, which is the 1.0 blocker proper.
20. **Shrink** `release-0.6.0.0.md` and `release-0600-blockers.md` - the mirror paragraph is stale
    (only astubbs#161 and astubbs#181 still need a reply), astubbs#444 has merged, and the quarantine
    registry is the enforced copy of what still blocks; one plainly false line was corrected in
    place.

## What gates v6, as the sweep read it

[`release-when-is-v6-good-enough.md`](release-when-is-v6-good-enough.md) set the bar as "the bugs
that are already open". Six area sweeps each named what they read as gating; this is the union,
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
  `core-139-public-api-thread-safety-contract.md` (see proposal 19),
  `core-bytearray-encodings-have-no-codec.md` (two magic bytes),
  `core-pc-owns-the-clients-it-uses.md` (the consumer-instance option).
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

## Ready picks

Collisions are in `pr-blockers-and-collisions.md`. The ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; these are the ready picks:

- **Commit-failure seam ([astubbs#317](https://github.com/astubbs/parallel-consumer/issues/317))** -
  **highest-demand item on this list, and the only one with a user shipping a patched build to get
  it.** On confluentinc#833 `ndqvinh2109` reported patching `controlLoop` with a try/catch so the
  exception would not reach `supervisorLoop` and close PC. That is not a feature request in a
  backlog - it is someone maintaining a private fork of the library because the decision PC makes
  for them is the wrong one for their deployment. Kafka's client throws a retriable exception and
  lets the caller choose; PC only terminates. Research, both sides of the upstream argument, and why
  fixing astubbs#177 does not close it: `core-commit-failure-seam.md`.
- **Auto-scaling (astubbs#227)** - runtime-discovered per-instance concurrency; candidate killer
  feature alongside key ordering, priority raised 2026-08-18 (`core-auto-scaling.md`). Spec
  stage; two bitrotted prototypes to mine; async-timing metrics fix is the prerequisite.
- **Contributor-friction build fixes** - `confluentinc#162` (mvn compile without test-jar) and
  `confluentinc#861` (`ManagedTruth` not found). The third, `confluentinc#906` (pom version
  mismatch), is settled - astubbs#194 is closed.
- **Security dependency bumps** - `confluentinc#851` (postgres), `confluentinc#913` (assertj); pom-only.
- **`confluentinc#915` batch construction strategy** - cherry-pick, closes the 4-year-old
  `confluentinc#266`. Medium effort.
- **Point ArchUnit at main code** (`static-archunit-main-code-rules.md`) - the harness is already
  wired into all four modules with a shared rule library, but polices only three test conventions.
  Post-v6: it is what would hold the boundaries the God-class decomposition creates.
- **DLQ** (`confluentinc#310`, or revive `confluentinc#366`) - the most-demanded missing feature. Large, and
  spec-stage only.
