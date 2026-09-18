# Next candidates, ranked

<!-- inflight-type: register -->
<!-- inflight-vetted: 2026-09-18 - the seven decisions are made and posted; the ranking section is retired to a pointer. Earlier: 2026-09-09 - the astubbs#162 decision was made and shipped, so that line is removed and the ranking is seven; the rest were still open issues awaiting the same reply (astubbs#161, astubbs#181, astubbs#163, astubbs#189, astubbs#241, astubbs#173, astubbs#178) and every note they name is present. 2026-09-07: four settled lines removed - the astubbs#155/astubbs#169/astubbs#170 scheduling sentence and the logging-verbosity pick (merged as astubbs#203 and astubbs#428), the astubbs#40 dedup pick (astubbs#206), and confluentinc#906 out of the contributor-friction pick (astubbs#194 closed) -->


## Decisions waiting on the maintainer - all seven made, 2026-09-18

The ranked list that stood here from the 2026-08-20 mirror triage is decided and posted: each mirror
body corrected, each reporter answered on the upstream original, every decision recorded on the
issue itself. astubbs#161 answered and closed; astubbs#181 closed on the kafka-clients 3.9.2
rationale with astubbs#128 carrying the CI proof; astubbs#163 answered, held open for the fluent API
(astubbs#502) rather than closed as a duplicate; astubbs#189 answered with default-on jitter as the
first rung; astubbs#241 rewritten and relabelled `feature`; astubbs#173 closed as by-design, no
revocation grace period; astubbs#178 ruled a violation and fixed in astubbs#517. The posting list
and what the release still owes is
[`upstream-v6-release-responses.md`](upstream-v6-release-responses.md). The version of this
section that carried the ranking: `git show 2e6f13ef1:docs/inflight/process-candidate-ranking.md`.

## What gates v6, as the sweep read it

Moved into the burn-down note on 2026-09-08 and back here on 2026-09-09 (that note, `release-v6-scope.md`, retired at the tag - `git show e6a429d87:docs/inflight/release-v6-scope.md`), because it is the
sweep's dated reading rather than a decision; the burn-down's tiers override it where they
disagree and say so there. Kept whole as the record.

### The sweep's list, 2026-09-07

Moved here from `process-candidate-ranking.md` on 2026-09-08 (it was written by the six-agent sweep
on 2026-09-07 and is the agents' reading, with their stated confidence - not the owner's decision).
Where it disagrees with the tiers above, the tiers say so: the poisoned-transaction pair (the sweep:
not gating; the owner named it the second exception on 2026-09-09), the transactional revoke wait (the sweep read
astubbs#466 as having replaced the unbounded wait, which is right, and astubbs#408 as owning the
bound), and the `batchSize` validation bound (the sweep: cheapest real fix; the triage had filed
it as 0.6.0.x, and the owner moved it into tier 1 on 2026-09-09). Item 2 in its list, the dead poll thread, is
astubbs#477, merged.

The bar above is "the bugs that are already open". Six area sweeps each named what they read as gating (the owner's pass over
the sweep's proposals is done - [`process-inflight-vet-sweep.md`](process-inflight-vet-sweep.md)
records it); this is the union,
ordered by user-visible consequence, with the confidence each agent stated. The mechanical gate
comes first because nothing else matters until it clears.

- ~~**The quarantine registry is non-empty, and every entry is unowned.**~~ *The sweep's reading on
  2026-09-07; the registry is empty on master (astubbs#80 emptied it and nothing has joined since),
  which this note says in its tag-day checks.* `release.yml` refuses the cut while
  [`docs/quarantined-tests.md`](../quarantined-tests.md) lists anything; read that file, not this
  line.
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
  astubbs#268 - capabilities, not defects); the `deps-` majors; every `issue-response-*` draft (all posted 2026-09-17 and deleted); the
  `static-` registers (advisory lanes); the `branch-` notes; the `test-debt` and feature notes; the
  unfenced `PartitionState` booleans and the plain-int counter (real, unmeasured, possibly absorbed
  by the shared-nothing rework); and the poisoned-transaction pair, where today's behaviour is
  strictly better than what it replaced.

### Every open bug note's disposition against the v6 bar, as of 2026-09-08

The sweep's reading above agrees with the look-at items below and added one this section had filed
as 0.6.0.x: `batchSize(0)` silently processes nothing, and the sweep calls the `validate()` bound
"the cheapest real fix in the set" (astubbs#311) - put in tier 1 by the owner's 2026-09-09 decision and merged as astubbs#496. Its list of instruments the release decision is
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
  waits; `batchSize`'s over-request arithmetic (astubbs#311's other half, deferred) - its validation
  bound merged as astubbs#496 on 2026-09-09.
- Blind spots: the racy and uncalled pause API; no metric for a discarded offset map under the
  default `IGNORE` policy; the worker future swallowing framework exceptions.
- Misdirection: the plain-`int` out-for-processing counter; the module's processor reference
  overwritten before the owner guard; the rest of the unbounded-log-lines class; the two 857 mirror
  attributions never verified against the reporter's environment.
- Deferred with a reason in the note, and outside a bug release: the shard's available-work
  counter undercounting after a stale replacement
  ([`bug-processing-shard-available-work-undercount.md`](bug-processing-shard-available-work-undercount.md)
  - a gauge inaccuracy that loses no records; the decision is whether the counter is worth keeping);
  the deferred-commit WARN naming no offsets
  ([`bug-deferred-commit-warn-names-no-offsets.md`](bug-deferred-commit-warn-names-no-offsets.md) -
  astubbs#352 owns the method and adds the field the fix needs); and batching requesting a full
  extra in-flight target ([`bug-batch-quantity-over-request.md`](bug-batch-quantity-over-request.md)
  - throughput only).
- The shutdown teardown race; the test-only `close()` shadowing; and
  [`bug-shared-collections-across-the-poll-boundary.md`](bug-shared-collections-across-the-poll-boundary.md),
  which is mostly stale - the metrics set and the shared empty set it names are both fixed on master
  and the note needs shrinking to whatever remains.

**Not a bug note, but a signal - settled:** the `simpleBatchTest` failures across the Reactor,
Mutiny and Vert.x modules were the test's own randomised key draw, not the batcher (astubbs#482,
see the known-unknowns section). The register's most-sighted row is retired.

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
