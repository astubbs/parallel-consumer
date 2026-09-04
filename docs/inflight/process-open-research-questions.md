# Open research questions - what we believe and have not checked

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->

**The sibling of [`process-prior-art-research-targets.md`](process-prior-art-research-targets.md),
asking the other question.** That register asks *does this already exist somewhere*. This one asks
**is what we believe actually true** - unchecked assumptions, claims with no falsifier, constants
that came from reasoning rather than measurement, and decisions nothing is scheduled to resolve.
Keeping them apart matters: one is answered by reading other people's work, the other by running
something here.

**Impact is `misdirection` deliberately.** A corpus that states reasoned claims in the same
typographic voice as measured ones is an instrument that lies, and this repository ranks that above
data loss because everything else is judged through it.

**Every entry names the check that would settle it.** A finding with no proposed check is an opinion,
and does not belong here.
<!-- post-merge: checked-begin -->
Produced by a review of astubbs/parallel-consumer#367 on 2026-09-05 which
read the whole `core-*` engine spine and about half the corpus
<!-- post-merge: checked-end --> - the harness half, the preserved
handoffs under `docs/ideation/` and a dozen smaller notes were not read, so **absence from this list
is not evidence of soundness.**

## The one to settle first

**A build is running ahead of the decisions that gate it.**
[`core-distributed-throttling.md`](core-distributed-throttling.md) lists *"Decisions that gate any
build"* and says they *"stay open until the owner adopts them"*;
[`../w2-vision.md`](../w2-vision.md) agrees the micro-MVP *"remains gated on the two decisions"* -
and its own composition-test section says astubbs/parallel-consumer#392, the navigator micro-MVP, is
in flight. Either the code has implicitly taken the enforcement-fork and standalone-versus-controller
decisions, or it is about to.

**Check:** ask the owner directly, then record them as taken. Otherwise the vision doc goes on
describing as open what the implementation has already decided - which is the corpus lying about
itself, in the direction that is hardest to notice.

## Claims with no evidence and no falsifier

- **Controller interference has never been observed**, and it is the entire justification for moving
  the adaptive boundary onto the resource:
  [`core-shared-execution-resources.md`](core-shared-execution-resources.md) states that independent
  adaptive controllers on one shared database *"see each other's probes as noise and fight"*. No run,
  no seed, no measurement anywhere. **Check:** run astubbs/parallel-consumer#333's controller at one,
  two, four and eight instances against a single shared bounded downstream and record whether
  discovered targets oscillate or diverge. Existing machinery, cheaper than the twenty-node test it
  currently sits behind, and it gates more.
- **That a 429 seen by one participant is evidence about the resource** - true only if the limit is
  resource-scoped, where per-API-key, per-tenant and per-IP are the common cases and no note records
  that a 429 carries a scope. **Check:** enumerate limit scoping for three named downstreams from
  their published docs, then add a scope field to the resource contract. If scope is not declarable,
  the feature is wrong more often than right.
- **That a blocked external caller is the strongest demand signal anything can supply**
  ([`core-non-kafka-participants.md`](core-non-kafka-participants.md)) - a caller can equally be
  blocked by its own retry loop, a speculative prefetch, or a bug, and the allocator cannot tell.
  **Check:** make grant-to-execution conversion a first-class metric in the micro-MVP; falsify with a
  synthetic client that blocks on credit while doing no real work, and confirm it visibly loses
  allocation.
- **That capacity fingerprinting survives outside the process** - the whole standalone tier rests on
  one hedged line in [`core-standalone-deployment.md`](core-standalone-deployment.md). **Check:** take
  one real Prometheus export from a live deployment and try to compute a fingerprint from it alone.
  One desk day decides whether that tier is a product or a brochure.

## Constants that came from reasoning, not measurement

Each is load-bearing, and each currently reads as a measured design value:

- **The coordination frequency** in [`core-shared-execution-resources.md`](core-shared-execution-resources.md),
  which sets overshoot, staleness and control-topic traffic at once. **Check:** derive the analytic
  bound from quantum, round-trip time and instance count, then have the conservation test sweep a
  range and publish the overshoot-versus-control-bytes curve *instead of* a constant.
- **The pacing interval rule and slot width** in [`core-distributed-throttling.md`](core-distributed-throttling.md).
  The note even states a testable prediction - that a token-bucket downstream means smoothing gains
  nothing - and nothing runs it. **Check:** a three-shape downstream simulator (fixed window, sliding
  window, token bucket with burst), measuring refusal rate with and without phase assignment. Decides
  whether pacing is a default or an option.
- **The vote clamp and cooldown** in [`core-auto-scaling.md`](core-auto-scaling.md), where fleet
  convergence is asserted AIMD-style. **Check:** simulate the vote-sum loop against the real
  autoscaler algorithm including its tolerance band and stabilisation window, across clamp and
  cooldown variants; report time-to-converge and overshoot. No cluster needed.
- **The scale-in decrement** in [`core-scale-in-proof.md`](core-scale-in-proof.md). **Check:** measure
  controller-target variance on a steady workload first, and set the minimum informative decrement
  from it. *A proof whose step sits inside the noise band is a coin flip wearing the words "proven
  safe".*
- **The instance count the acceptance tests use.** **Check:** name the failure mode that appears at
  that scale and not at a quarter of it, or run a ladder and show where measured overshoot changes.

## Kafka, JVM and library behaviour assumed rather than verified

- **That the fencing vocabulary comes from machinery Kafka already has.** **NARROWED 2026-09-05 from
  evidence already in this repository, and the claim is half right in a way that matters.**
  `ProducerFencedException` appears in `ProducerWrapper` on **transactional paths only** -
  `beginTransaction`, `commitTransaction`, `abortTransaction`, `sendOffsetsToTransaction`. So Kafka's
  fencing machinery is real but it is **transactional-producer fencing keyed on `transactional.id`
  plus epoch**, not consumer-generation fencing of an arbitrary produce. A revoked owner holding a
  plain producer can still write to the control topic and nothing rejects it.
  **What that leaves:** the mechanism exists and can be used, but only by giving each fenced writer
  its own `transactional.id` - so the real question is no longer *does Kafka fence this* (it does
  not, for plain produces) but **is per-resource-shard `transactional.id` cardinality affordable**,
  which is a cost question about transaction coordinator state and initialisation latency.
  **Check, now sharper:** measure `initTransactions` cost and coordinator state at the shard
  cardinality the design implies, and confirm the small negative case - two consumers, forced
  rebalance, old owner produces non-transactionally to the control topic after revocation, nothing
  rejects it. The second is cheap and closes the claim; the first decides whether the fix is
  affordable.
- **That two consumer groups will agree on the partition assignment**
  ([`core-frontier-handover.md`](core-frontier-handover.md)). Two coordinators with different member
  sets generally do not produce the same mapping, so an ownership claim can land on a member that
  does not hold that partition. **Check:** run two groups over one topic with differing member counts
  and diff the assignments; measure the shadow group's broker-side fetch cost over a handover window.
- **That Kafka Streams state gives fleet-wide fingerprint access for free**
  ([`core-capacity-fingerprinting.md`](core-capacity-fingerprinting.md)) - stores are partition-local,
  so a fleet-wide subscriber needs a global store or interactive queries, with real size and rebuild
  cost. **Check:** prototype the read path and establish whether an allocator can subscribe without
  co-partitioning.

## Systems asserted about but never studied

None of these are in the prior-art register, which is why they are here rather than there:

- **Hazelcast embedded mode**, cited in [`core-standalone-deployment.md`](core-standalone-deployment.md)
  as a precedent *with an attributed historical cause* for why client/server mode was added. **Check:**
  verify against its own docs and release history, and add Ignite and Infinispan - same shape, same
  lifecycle collision.
- **A universal negative about limiter products** in
  [`core-distributed-throttling.md`](core-distributed-throttling.md) - that phase coordination across
  shards is something *no limiter product offers*. Never searched for. **Check:** search specifically
  for slot or phase assignment in distributed limiters, including published designs from the large
  API providers. **Until then the honest form is "not found"** - the same distinction the prior-art
  register insists on for its own weakest angle.
- **Dapr**, named as an adoption surface in two notes and never examined. **Check:** run the
  ecosystem-adapter seam classification against its pub/sub component API - does it hand over
  execution, or only consumption?
- **The ecosystem-adapter targets**, where [`core-ecosystem-adapters.md`](core-ecosystem-adapters.md)
  is called the strategic main line in the risks register **with no seam actually classified**.
  **Check:** classify exactly one, against the question *who owns dispatch?* One classification
  validates the campaign premise or kills it.
- **Nile**, where [`core-nile-boundary.md`](core-nile-boundary.md) builds a joint-loop hypothesis on
  an asserted present capability. **Check:** verify from its docs or a trial whether that telemetry
  exists today. If not, the joint loop is a feature request to a third party and the note should say
  so.

## Contradictions to resolve

- **RESOLVED 2026-09-05 in law 3: "no cluster" means no cluster YOU OPERATE.** The strong form is an
  open question this corpus carries, never a property to advertise. **And the sweep it called for was then run, and came back clean** -
  every live use is already the honest form (*no cluster to operate*), the two remaining hits are a
  frozen record quoting the claim as it was put to a research sweep and a register row using it as a
  claim label, and `SOUND_BITES.md`, the README and the docs sources contain no form of it at all. So
  the contradiction was in the *laws*, not in the copy. Original finding:
  **"No cluster" had three readings and no authoritative one** - [`../w2-vision.md`](../w2-vision.md)'s
  law 3 read to its limit, the preserved handoff's *globally coordinated runtime with no runtime
  cluster*, and [`core-standalone-deployment.md`](core-standalone-deployment.md)'s third topology
  which the note itself calls an inversion of the record. **Check:** write the reconciling sentence
  into law 3 - *no cluster you operate* versus *no cluster at all* - then sweep the corpus for every
  place the strong form appears in positioning copy.
- **[`core-auto-scaling.md`](core-auto-scaling.md) contradicts itself on prior art** - an early line
  claims no known competitor does runtime-discovered per-instance adaptive concurrency, while a later
  line records Envoy's filter as live prior art in exactly that family. **Check:** correct the earlier
  sentence. The register's standing rule is *name theirs first*, and this is the file where it is
  broken.
- **Three notes each own the same predicate** for when the engine votes to scale out. **Check:**
  nominate [`core-auto-scaling.md`](core-auto-scaling.md) as owner and reduce the others to links.
  The risks register predicts note-corpus drift; this is it, already materialised.

## Open decisions blocking other work

- **In-band versus out-of-band frontier**, marked *unresolved - record, do not assume* in
  [`core-frontier-handover.md`](core-frontier-handover.md), blocking a staircase rung and the
  generation cutover that names it as its mechanism. **Check:** prototype the rung with the in-log
  marker variant first - it needs no offset prediction - and let the prototype settle the design
  question as a byproduct.
- **The resource contract has no schema**, and several notes depend on fields it does not define -
  limit semantics, burst tolerance, the pacing default, scope, fencing-token support. **Check:** write
  the one-page schema **before the micro-MVP grows an API surface**; it is a one-way door on
  semantics.
- **The Prescience read-path measurement has no owner**, though
  [`core-prescience-and-spice.md`](core-prescience-and-spice.md) names the exact experiment and a
  whole vision beat rests on it. **Check:** put it on the falsification staircase - it is cheaper than
  the rung above it and gates more.

## Claims about users, with no user having said it

The weakest evidence class in the corpus, and the one most likely to be believed because it sounds
like product sense:

- **The refusal persona** - *uses Kafka, will not hand over the consumer* - from which a five-rung
  adoption ramp is derived. **Check:** read the tracked issues for whether a real user says it, then
  ask three users directly. There is real recorded demand evidence for throttling and none for this.
- **That service owners will publish capacity contracts** because it gives them control - the supply
  side of the entire fleet layer. **Check:** five conversations with platform or service owners: would
  you publish one, and what would you want in return? If the supply side does not volunteer, the fleet
  layer needs a different acquisition story.
- **That the cost framing is understood instantly** where a speed framing is not. **Check:** put both
  in front of a handful of users and record which they repeat back correctly, or publish both and
  compare.

## The rule this register runs under

**A reasoned claim and a measured one must not look the same.** Where an entry here is settled, the
owning note gains the evidence *and* loses the hedge; where it is refuted, the note says so where the
claim was. Nothing is deleted from here silently - an entry leaves by being answered, and the answer
goes to the note that made the claim.
