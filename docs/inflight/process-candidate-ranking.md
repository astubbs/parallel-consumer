# Next candidates, ranked

<!-- inflight-type: register -->
<!-- inflight-vetted: 2026-09-09 - the astubbs#162 decision was made and shipped, so that line is removed and the ranking is seven; the rest are still open issues awaiting the same reply (astubbs#161, astubbs#181, astubbs#163, astubbs#189, astubbs#241, astubbs#173, astubbs#178) and every note they name is present. 2026-09-07: four settled lines removed - the astubbs#155/astubbs#169/astubbs#170 scheduling sentence and the logging-verbosity pick (merged as astubbs#203 and astubbs#428), the astubbs#40 dedup pick (astubbs#206), and confluentinc#906 out of the contributor-friction pick (astubbs#194 closed) -->


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
5. **astubbs#241** (confluentinc#144), `core-241-tx-commit-failure-taxonomy.md` - agree the issue's
   stated premise died in confluentinc#355, then keep it open with a rewritten `## Fork status` and
   relabel `bug` to `feature`. No defect is demonstrated; what survives is a policy design.
6. **astubbs#173** (confluentinc#777), `upstream-173-revocation-duplicate-processing.md` - should PC
   offer a revocation grace period at all? Upstream declined it. **If the answer is no, confluentinc#777
   is a documentation obligation rather than a defect** and the close is unblocked - at the cost of a
   README section and one chaos cell that must be run rather than predicted.
7. **astubbs#178** (confluentinc#843), `core-178-key-order-across-a-rebalance.md` - is an undrained
   old-epoch delivery a violation of the README's "strong ordering by key", or legitimate
   at-least-once? Last because it is the only one that needs a *bound* argued for rather than a
   yes/no, and `KeyOrderLedger`'s javadoc already says picking that number is the whole job.

**What is NOT on this list, from the same triage, and why:** astubbs#139 is a 1.0 blocker with a
four-step definition of done in `core-139-public-api-thread-safety-contract.md` - real work, not a
call. astubbs#175 has no decision left in it; its one live strand is the AB-BA wedge that
<!-- post-merge: checked - names that PR as the work the strand belonged to, in the past tense, so it reads the same once it has landed -->
astubbs/parallel-consumer#29 carried.

## What gates v6, as the sweep read it

[`release-when-is-v6-good-enough.md`](release-when-is-v6-good-enough.md) set the bar as "the bugs
that are already open". Six area sweeps each named what they read as gating (the owner's pass over
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
  2. A dead poll thread holding its partitions for `max.poll.interval.ms` in the default commit mode -
     read as gating, and traced end to end but untested and unfixed when the sweep ran. Now tested
     and fixed: `maybeCloseConsumer` gained an arm for a poll thread that ended without closing the
     consumer, the note is gone, and the record is in
     `docs/solutions/logic-errors/a-duty-assigned-by-role-is-unassigned-when-the-role-holder-dies-2026-09-08.md`.
  3. `pr-431-must-pair-its-queue-removal-with-the-shard-removal.md` with
     `bug-retry-queue-write-lock-on-the-rebalance-path.md` - the retry-queue orphan window; master
     is still shard-first and astubbs#431 is a draft.
  4. `batchSize(0)` silently processing nothing - read as the cheapest real fix in the set, and it
     was. Now fixed: `validate()` rejects zero, a negative and null, which is astubbs#311's
     validation half; the arithmetic half stays open in `bug-batch-quantity-over-request.md`. The
     note is gone and the record is in
     `docs/solutions/logic-errors/an-unvalidated-option-failed-in-whichever-way-the-rest-of-the-configuration-decided-2026-09-09.md`.
  5. `bug-max-failure-history-is-inert.md` - a public option that does nothing; removing it is
     breaking, so it is settled before the major or carried forever.
  6. `bug-offset-commit-timeout-does-two-jobs.md` - the default makes a retry unreachable; the fix is
     a design choice among three.
  7. A WARN operators alert on, firing falsely for every new consumer group - the bootstrap
     truncation warning of astubbs#162. The decision it waited on was made (absent commit data
     reports at INFO and takes no truncation branch); the note is gone and the record is in
     `docs/solutions/logic-errors/absence-of-commit-data-was-inferred-from-a-sentinel-that-a-real-commit-shares-2026-09-09.md`.
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
