# UX modernisation - the fluent API and park in place, and what each phase waits on

<!-- inflight-type: feature -->

The tracking issue is astubbs#504; implementation commits cite it. The requirements are `docs/plans/2026-09-09-002-feat-ux-modernisation-plan.md`: a fluent API over
today's engine, one typed route per topic with one function, outcomes instead of exceptions, park in
place as the dead-letter of first resort with export at a payload fraction, a broker-free sandbox that
serves both APIs, and milestones cut by how much of the engine each needs (tiny, small, medium,
large). Read the plan for what is decided; this note carries only what changes as other branches
land, which the plan cannot track and `gh` cannot tell you.

## The producer stack, and which phase waits on which rung

The fluent API builds its own producer from the connection properties, and export sends on the
produce-many path, so this work sits on the producer-ownership and recovery stack for astubbs#225.
The rungs, in the order they must merge, and what each one unlocks here:

<!-- post-merge: checked-begin - every rung is named by PR number, which outlives the branch, and each
     sentence reads the same once that rung has landed -->
- **astubbs#426, merged.** PC builds the producer from `producerConfig`. This is all the tiny tier
  needs: the facade hands the same map with raw-bytes serialisers. Nothing else in the stack gates
  the first milestone.
- **astubbs#420** derives the `transactional.id` and enforces a producer-factory contract. Not a
  dependency, but until it lands a transactional definition must carry a `transactional.id` in its
  properties, and the README example under that mode has to show it. The natural point: merge it
  before the README rewrite (R21) so the transactional example is written once.
- **astubbs#420 also breaks a fluent refusal, and the break is silent.** This is a code change, not
  only the documentation one above. `ParallelConsumerDefinition`'s `validateTransactionalId` holds
  two refusals; the second, *"The commit mode is {} but there is no {} in the connection
  properties"*, is true only while PC needs the caller to supply that id. astubbs#420 derives it
  (`internal/TransactionalIdDerivation`, `prefixFor`/`derive`/`resolve`) and **removes a caller-set
  one with a WARN, in every mode** - so under that PR the fluent API would refuse to start a
  definition unless the user sets a key PC then strips and complains about. Nothing fails until a
  user hits it: both sides pass their own suites, and the two PRs touch different files.
  - **Which side changes: the fluent one, and whichever of astubbs#502 / astubbs#420 merges second
    does it.** Delete that second refusal and its test,
    `DefinitionRefusalTest#theTransactionalCommitModeWithNoTransactionalIdNamesTheSetting`. The
    first refusal - an id declared under a non-transactional commit mode - stays true either way and
    keeps its test, `aTransactionalIdUnderANonTransactionalCommitModeNamesBoth`.
  - **The two seams are not the same job and are not being unified.** `ClientRuntime` builds the
    **consumer** and splits one property bag per client; it deliberately declines producer
    construction - `KafkaClientRuntime.producer()` returns empty and the definition then hands the
    producer properties to the options, so PC builds it (astubbs#426, already merged). astubbs#420's
    `ProducerFactory` sits on the other side of `ParallelConsumerOptions`, inside `PCModule`, which
    is why an adapter between them would violate KTD3 - the facade holds no client in a field.
    `transactional.id` is the one key both claim, and it belongs to the derivation, not the facade.
  - **Back-pointer worth adding from the 225 side when that stack is next touched**:
    `docs/inflight/core-recoverable-producer-fencing.md` names no fluent dependency, and astubbs#420
    carries a note arguing PC should build the **consumer** from configuration too - for which
    `KafkaClientRuntime.consumer()` is already a working, unstable-annotated precedent. That is a
    milestone of its own, not this stack's work.
- **astubbs#472, astubbs#474, astubbs#410, astubbs#434**, in that order, are producer recovery
  itself: the plumbing, the ledger that puts an aborted transaction's work back, recovery of an
  invalidated producer, and the abort of a transaction an unsendable record poisoned. **R14's
  medium-tier entry, export under the transactional commit mode, waits on all four**; until they
  land the plan refuses a dead-letter destination under that mode at definition time, so nothing in
  the tiny tier depends on them. astubbs#434 is the one easy to miss: an export record too large
  for the dead-letter topic would leave the transaction abortable with nothing aborting it.
- **astubbs#410 also decides two things the facade inherits.** A record put back because its
  transaction aborted does not count as an attempt (R10). Recovery never runs on the instance path,
  so a definition that supplies a pre-built producer (R1's Java-only sugar) forgoes recovery and,
  under the transactional mode, export.
- **astubbs#352**, the commit-failure seam, is independent of the stack. It gives the instance a
  decision other than terminating when a commit exhausts its budget. When it lands, the fluent API
  carries its policy as data, the one other instance-wide setting beside the commit mode (KD11, R6;
  owner decision, 2026-09-10). Merging it before R21 means the README error-handling section is
  written once.
<!-- post-merge: checked-end -->

Re-read this section whenever one of those merges: `gh pr view <n> -R astubbs/parallel-consumer`
is the status, this note is the consequence.

## The compatibility gate's exclusion

The fluent package `bz.stub.parallelconsumer.fluent` is incubating and its shape will churn before it settles, so it is excluded from the API-compatibility gate until then (plan KTD1). The gate is astubbs#315, not yet merged; when it lands, or when that branch is next touched, add the package exclusion to its japicmp configuration and name this note in the commit. Until the gate exists on master, nothing enforces the exclusion and nothing needs it.

## When the direct-pull engine merges: the pause purge has nothing to purge

<!-- post-merge: checked - the PR numbers below outlive the branches -->
The pause in the shipped engine stops work in two places: the controller stops submitting, and on its next pass it pulls the batches still queued in the worker pool out of that queue and abandons their claims, so nothing queued before the pause starts after the controller acts (plan KTD14; the window between a worker requesting the pause and the controller's next pass is accepted and documented on the purge). That purge exists only because the shipped engine pre-fills the pool queue ahead of the workers. Under direct pull (astubbs#361, `perf/shard-occupancy-scan-v2`, draft) workers take their own next record from the shards and there is no queue, so a pause is simply a take that refuses, and the purge has nothing to do. When astubbs#361 merges after the UX modernisation (astubbs#502), or when astubbs#502 is rebased over it: make the purge a no-op for the direct-pull pool rather than leaving it to find an empty queue, keep the pause test's upper bound on what ran (that branch's `pausingDrainsThePreLoadedExecutorQueueAsWellAsTheInFlightRecords` skips itself for queue-less engines and the unconditional bound stays), and re-read the stop path in `ParallelConsumerInstance`, which relies on the purge for "no new work after the controller acts". Owner direction, 2026-09-10.

**The same branch is also where the drain-first close's park defect gets cheap.** Gated on the same merge. Under
`KEY` or `PARTITION` ordering, records
queued behind a parked shard head are work the engine can never take, so a close that drains waits
out its whole drain timeout and then reports the timeout - about five seconds on the README's
quickstart, ten on a small run. It is documented rather than hidden: see the README's park section,
"A close that drains waits for records queued behind a parked key". The fix is a shard whose head is
parked yields nothing, and the drain ends when no shard could yield. Written against today's master
that is a new per-shard walk, because
`ShardManager.getNumberOfWorkQueuedInShardsAwaitingSelection()` is an O(1) counter with a documented
skew the drain depends on - and astubbs#361 then rewrites that walk. That branch already carries a
per-shard walk for the ordered modes in `ShardManager.getUpperBoundOnSelectableWork()`, so after it
merges the fix is one clause on a walk that already exists. Owner direction, 2026-09-10.

## The classic API as an adapter over the fluent one: a later milestone's question

Raised by the owner on 2026-09-10 and deliberately **not** Milestone A. Today the classic API is the
engine's public surface and the fluent package sits over it; inverting that - the classic verbs
re-expressed as a thin adapter over the fluent definition - would put the stable, long-lived API on
top of one every public type of which still carries Kafka's `@InterfaceStability.Unstable` (plan
KTD1, and the compatibility-gate exclusion above says the same thing from the gate's side). A surface
users depend on cannot rest on one whose shape is expected to churn.

So this is recorded as a decision to revisit **once the fluent API stabilises**, not as work. What
would make it worth doing is not new: it is the duplication the two surfaces will otherwise carry,
and the per-topic design cluster is where that is already being thought about -
`docs/inflight/next-multi-topic-multi-function.md`, which exists only on branches that have not
merged (`node bin/inflight.mjs prior-art per-topic` finds it; print it with
`node bin/inflight.mjs docs show <path>`). astubbs#254, the fork mirror of confluentinc#372, is the
issue the typed routes already answer, and the one whose classic-API counterpart this question is
really about - R26 deliberately gives the classic API no per-topic verb.
<!-- file-refs: N/A - next-multi-topic-multi-function.md is branch-only; print it with bin/inflight.mjs docs show -->

## Related notes

On master, each named for what it lends this work:

- `docs/inflight/core-recoverable-producer-fencing.md` - the stack above, from its own side.
- `docs/inflight/bug-poisoned-transaction-not-aborted-while-running.md` - the liveness half
  astubbs#434 fixes; the terminal-failure concept it says is missing is the outcome model here.
- `docs/inflight/core-163-poll-path-has-no-error-seam.md` - why the classic API's deserialisation
  policy (R25) is a third arm on an existing seam and the fluent API keeps decoding off the poll path.
- `docs/inflight/core-189-batch-failure-granularity.md`, `docs/inflight/core-batching-enhancements.md`,
  `docs/inflight/bug-batch-quantity-over-request.md` - batch mode (R32) and the defect that must land
  first.
- `docs/inflight/core-retry-queue-needs-a-runtime-controller-ownership-guard.md`,
  `docs/inflight/test-retry-queue-behaviour-untested.md` - the retry queue that park in place is
  built on (R27), and the tests it lacks.
- `docs/inflight/core-237-continuous-offset-encoding.md` - the export percentage default is provisional
  on today's approximate encoding.
- `docs/inflight/branch-ks-streams-workstream.md` - the stateful API this surface must not contradict
  about what an outcome means.
- `docs/inflight/branch-proxy-http-ideation.md` - the language proxy; one callback per route and
  policy as data (R18) exist for it.
- `docs/inflight/pr-strategy-doc-merge-triggers.md` - `STRATEGY.md` must record the returning
  developer and the surface work when the fluent API ships.

On branches not yet merged, printed with `node bin/inflight.mjs docs show <path>`:

- `docs/inflight/next-multi-topic-multi-function.md` - the per-topic design cluster; R2 to R4 settle
  the attachment shape, cross-topic key identity and topic priority stay there.
- `docs/inflight/core-decompose-abstract-parallel-eos-stream-processor.md` - the God-class cut the
  large tier waits for.
- `docs/inflight/next-select-retries-from-the-retry-queue.md` - the retry queue is time-ordered with a
  hash index and nothing selects from it; park is that state with the re-attempt withheld.
- `docs/inflight/web-control-plane.md` - the blocked-frontier panel whose buttons R28's parked-set
  commands are the engine API for.
- `docs/inflight/core-work-identity-model.md` - the disposition vocabulary a parked record is
  classified in.
- `docs/inflight/core-spring-kafka-integration.md` - the framing the Spring example (R36) follows.
<!-- file-refs: N/A - the six notes above live on unmerged branches; print them with bin/inflight.mjs docs show -->
