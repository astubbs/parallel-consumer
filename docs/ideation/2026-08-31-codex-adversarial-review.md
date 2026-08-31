<!-- issue-refs: exempt-file - preserved verbatim external review; numeric tokens are not issue references -->
> Provenance: an adversarial cross-model review of this branch's strategy corpus, run 2026-08-31
> at the owner's request via codex-cli (gpt-5.6-sol, high reasoning, read-only sandbox), briefed
> to attack CONTENT rather than mechanics and to verify prior-art claims against primary sources
> (it web-checked the Calvin paper, CockroachDB admission-control docs, KIP-932, DRF and the
> C/D-RAS literature). Preserved verbatim below; local absolute paths are as emitted. This is the
> adversarial pressure the original conversations lacked. Every finding was folded into its
> owning note in the same commit that adds this file, and the owner's ruling on the two hardest
> findings is recorded in the vision map's risks register: even if hard global rate ceilings and
> frontier-based cutover never work, the remainder of the architecture stands on its own.

## Ranked findings

1. **The capacity-lease safety claim is not established.**  
   File: [core-shared-execution-resources.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-shared-execution-resources.md:64)

   Claim: delegated credits make “failure waste capacity, never violate the constraint,” and aggregate rate never exceeds the contract during churn.

   Kafka ownership fences Kafka operations, not calls that a paused or partitioned old process makes to Salesforce or another external resource. A successor can mint credits while stale holders still spend theirs. Clock skew and window-boundary bursts also make “never exceed” meaningless until the contract is defined as fixed-window, sliding-window, or token-bucket average-plus-burst.

   Fix/test: specify a durable issuance ledger and overlap-free epoch transition. A successor must wait for or account for every prior-epoch credit. Test long GC pauses, network partitions, delayed grants, coordinator replacement, and skew at quantum boundaries. Until then, promise bounded overshoot—not a hard ceiling—unless the downstream validates fencing tokens.

2. **The Future Frontier Agreement is incompatible with the key-level execution model.**  
   Files: [core-frontier-handover.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-frontier-handover.md:24), [core-partition-virtualization.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-partition-virtualization.md:27)

   Claim: after checking `highestAdmitted < F`, “offset F-1 runs on v7, offset F runs on v8”; topic generations can likewise cut over at agreed frontiers.

   PC deliberately permits sparse, out-of-offset execution. Crossing scalar offset F does not prove that an earlier record for the same key has completed. Across partitions there is not even one F. For stateful handlers, a successor cannot both “never speculatively execute” and arrive at F with state fully warmed. Partition virtualization adds another hole: an old producer can keep writing to the retired generation after cutover.

   Fix/test: start with stateless handlers. Define a partition-vector frontier plus per-ordering-domain completion transfer, producer epoch fencing, old-generation sealing, dual-generation reads, and state snapshot/catch-up. Inject old/new concurrent producers, delayed retries, transactions, and failover during cutover.

3. **The confidence split is drawn far too generously—and hides the most important missing risk.**  
   Files: [vision map](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/plans/2026-08-31-001-hasten-vision-map.md:116), [core-admission-scheduling-model.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-admission-scheduling-model.md:28), [core-distributed-throttling.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-distributed-throttling.md:1)

   Claim: admission, resources, attribution, DLQ-less failure, and Prescience are the grounded local half.

   Shipped PC code grounds the dispatch seam, key independence, sparse completion, and accounting. It does not ground complete resource declarations, lease conservation, exhaustive lookahead, or durable provenance. Arbitrary handlers discover dependencies conditionally and may misdeclare them; propagated priority, tenant, and resource metadata can also be stale, wrong, or malicious. Those errors corrupt admission and attribution before the fleet layer exists.

   There is also a settled/open contradiction: the map names Navigator as the next build, while the throttling owner says the direction is unchosen and two decisions “gate any build.”

   Fix/test: use three evidence grades—shipped, locally testable extension, distributed hypothesis. Add “contract fidelity and control-plane trust” to the risk register. Measure declaration recall against observed calls, retain a safe opaque-work lane, authorize control metadata, and settle the throttling gates before calling Navigator scheduled.

4. **The named falsifiers are not falsifiers, and the sequencing cannot isolate failures.**  
   Files: [core-lighthouse-mvp.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-lighthouse-mvp.md:10), [vision map](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/plans/2026-08-31-001-hasten-vision-map.md:108)

   Claim: the four claims are pre-registered falsifiers, tested by Navigator followed by a twelve-dimensional, deliberately hollow lighthouse.

   They are hypothesis labels without baselines, thresholds, or stop rules. The lighthouse surface does not actually include a deep-versus-modest Prescience comparison or a failure-injected generation cutover. Navigator’s two-instance, two-token demonstration proves shared plumbing and attribution, not advantage over a conventional limiter or safety under churn. The resource note itself expects a twenty-instance conservation test before broader composition.

   Fix/test: turn each claim into a separate controlled experiment with a null arm, metric, rejection threshold, and stop decision. Run the lease-conservation rung before the lighthouse. This is also why the risks register’s “falsifiers are written down” correction is inadequate: names are not preregistration.

5. **“100% Prescience is plausible today” prices the postings list and ignores the system around it.**  
   File: [core-prescience-and-spice.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-prescience-and-spice.md:27)

   Kafka still transfers and decompresses complete record batches. A separate index reader needs ownership, checkpoints, freshness, retention-loss behavior, rebalance recovery, and a hydration protocol. Body dropping creates random refetch amplification; rebuilding a “disposable” complete index may take hours and may be impossible after source retention expires. This is probably the first supposedly local feature to become operationally expensive.

   Fix/test: prototype the actual read path before choosing storage tiers. Compare bounded buffers against full-horizon indexing on skewed retained backlogs. Measure broker bytes, decode allocation, disk, catch-up/rebuild time, random refetches, rebalance downtime, and useful-throughput gain. “100%” should remain a hypothesis unless that curve wins.

6. **Decision lineage is the clearest false “nearly free” claim.**  
   File: [core-decision-lineage.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-decision-lineage.md:10)

   Retaining one scheduler reason is cheap. A durable graph of attempts, state versions, effects, rejected candidates, historical reads, and causal ancestry is another state system: it needs atomicity with application state, its own restoration source, retention and garbage collection, privacy controls, and a query substrate. Moving provenance into a sidecar avoids contaminating values; it does not remove write amplification. “Never build a tracing backend” also does not support months-later authoritative causal queries through ordinary telemetry alone.

   Fix/test: split ephemeral decision explanation from durable audit lineage. Make the latter opt-in and independently budgeted. Measure bytes and writes per state mutation, restore consistency, graph-query cost, cardinality, and retention before treating it as a projection.

7. **The categorical uniqueness claim does not survive prior art.**  
   File: [core-engine-thesis.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-engine-thesis.md:119)

   Claim: this is “the only thing” scheduling at ordering-domain granularity; the unusual position is durable intent plus pre-dispatch admission and key ordering.

   No single competitor matches the entire proposed intersection, but important neighbors are omitted:

   - Pulsar Key_Shared dispatches the same ordering key to one consumer while allowing other keys to progress.  
   - Restate durably queues writes per object key and runs different keys concurrently.  
   - Temporal reserves worker slots before polling durable task queues and supports resource-based admission.  
   - Flink guarantees same-key invocation order and provides bounded async execution.

   [Pulsar](https://pulsar.apache.org/docs/2.5.0/concepts-messaging/), [Restate](https://docs.restate.dev/foundations/services), [Temporal](https://javadoc.io/static/io.temporal/temporal-sdk/1.26.0/io/temporal/worker/tuning/SlotSupplier.html), [Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state_v2/).

   ksqlDB, Faust, and Goka are not exact counterexamples: ksqlDB is a Kafka Streams server, while Faust and Goka remain fundamentally partition-based—even though Goka promises key-wise sequential updates. [ksqlDB architecture](https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/how-it-works.html), [Faust](https://faust.readthedocs.io/en/latest/userguide/kafka.html), [Goka](https://github.com/lovoo/goka/wiki/Introduction).

   Fix: replace “only” with a capability matrix. The defensible candidate novelty is narrower: Kafka-compatible, application-embedded, resource-aware selection across independent ordering domains with deep backlog visibility.

8. **The literature analogies are useful leads but are written as transferred guarantees.**  
   File: [core-shared-execution-resources.md](/Users/astubbs/github/parallel-consumer/.claude/worktrees/codex-strategy-inflight/docs/inflight/core-shared-execution-resources.md:202)

   Conservative 2PL is characterized correctly as deadlock-free preclaiming, but “this system says we do [know the full set]” assumes away dynamic application dependencies and preclaiming’s utilization/starvation costs. Calvin uses a predetermined transaction order, deterministic locking, replication, and known access sets; a single Kafka claim topic gives a total sequence but does not by itself solve grants, authority failure, cancellation, or atomic release. [Calvin paper](https://www.cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf).

   DRF is genuinely relevant to multi-resource fairness, but it equalizes dominant shares; it does not optimize marginal utility, replenishing rates, priorities, alternative bundles, or temporal reservations. [DRF paper](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-18.html). C/D-RAS supplies deadlock/liveness theory for conjunctive/disjunctive resource systems, not an implementation protocol.

   CockroachDB and Impala are broadly characterized correctly, although Cockroach already covers more of the proposed policy surface—slots and tokens, dynamic adjustment, priorities, and multitenancy—than the note acknowledges. [CockroachDB admission control](https://www.cockroachlabs.com/blog/admission-control-in-cockroachdb/), [Impala admission control](https://impala.apache.org/docs/build/html/topics/impala_admission.html).

   Fix: label these as analogies and write down the assumptions required to inherit each result. Anything not proved under leases, failures, dynamic declarations, and indivisible work remains original research.

## Overall verdict

The architecture has a credible kernel: PC’s key-level independence makes pre-dispatch admission unusually promising. It is credible as a research program, not yet as an architecture whose later layers “fall out” from that kernel. The first likely break is lease fencing; the first likely cost explosion is Prescience; the first likely correctness cliff is frontier-based topology migration.

The single change I would make is to replace `Navigator → broad lighthouse` with a falsification staircase: local admission A/B test, then twenty-node lease conservation under churn, then one stateless frontier cutover. Each rung gets quantitative stop rules, and the twelve-dimensional lighthouse is built only if all three survive.

Coverage note: coherence, feasibility, and adversarial reviews completed; the interrupted product/scope and independent cross-model passes did not return.
