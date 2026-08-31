# The Hasten vision map: how the notes string together

Dated binder, 2026-08-31. **Thin on purpose**: the spine, then each beat of the vision in a
sentence or two with the notes that own it - no content of its own, so it cannot drift from the
notes. Point-in-time per `docs/plans/` convention; the file-refs gate forces whoever moves a
linked note to update this map. The preserved primary sources are the handoff documents
([`2026-08-29-hasten-compound-engineering-handoff.md`](../ideation/2026-08-29-hasten-compound-engineering-handoff.md),
[`2026-08-30-hasten-handoff-supplement.md`](../ideation/2026-08-30-hasten-handoff-supplement.md))
and the vision fiction
([`2026-08-29-the-story-of-hasten.md`](../ideation/2026-08-29-the-story-of-hasten.md));
[`SOUND_BITES.md`](../../SOUND_BITES.md) carries the compressed intent.

## The spine

One question at successively larger scheduling domains:

```
Kafka          which machine owns this partition?
PC             which record inside that partition may run?
local engine   which runnable record is most useful to run?
fleet          which application should receive scarce capacity?
infrastructure which resource should receive additional capacity?
economics      where should the next dollar go?
```

## The beats

**1. The thesis.** Kafka ownership and execution are independent; PC proved it at the key level,
and following the observation to its conclusion makes execution itself programmable. Waiting is a
scheduling state, not an execution state - every mechanism that makes work wait is one eligibility
model. Work has identity, position and incarnation, and everything else is a projection of one
opportunity model.
[`core-engine-thesis.md`](../inflight/core-engine-thesis.md) ·
[`core-admission-scheduling-model.md`](../inflight/core-admission-scheduling-model.md) ·
[`core-execution-opportunity-model.md`](../inflight/core-execution-opportunity-model.md) ·
[`core-work-identity-model.md`](../inflight/core-work-identity-model.md)

**2. The local engine learns.** The controller discovers useful concurrency experimentally
(astubbs#333 implements it), classifies *why* more stops helping, proves what could be removed,
and holds a declared objective instead of a configured number.
[`core-auto-scaling.md`](../inflight/core-auto-scaling.md) ·
[`core-bottleneck-attribution.md`](../inflight/core-bottleneck-attribution.md) ·
[`core-scale-in-proof.md`](../inflight/core-scale-in-proof.md) ·
[`core-slo-objective-api.md`](../inflight/core-slo-objective-api.md)

**3. The future becomes legible.** The committed backlog is indexed by execution meaning
(an inverted index, not a cache), demand and capacity get horizons, queue disciplines become
policy over one primitive, and the causal past is the same graph pointed backward.
[`core-prescience-and-spice.md`](../inflight/core-prescience-and-spice.md) ·
[`core-temporal-horizons.md`](../inflight/core-temporal-horizons.md) ·
[`core-queue-disciplines.md`](../inflight/core-queue-disciplines.md) ·
[`core-decision-lineage.md`](../inflight/core-decision-lineage.md) ·
[`core-record-semantic-tracing.md`](../inflight/core-record-semantic-tracing.md)

**4. Capacity becomes shared.** Named resources own capacity; renewable pieces are delegated and
spent locally; per-function arbitration, tenant quotas, priorities and the partition advisor fall
out as policy. This is the buildable centre - the navigator micro-MVP and the twenty-instance
conservation test live here.
[`core-shared-execution-resources.md`](../inflight/core-shared-execution-resources.md) ·
[`core-distributed-throttling.md`](../inflight/core-distributed-throttling.md) ·
[`core-per-function-capacity-arbitration.md`](../inflight/core-per-function-capacity-arbitration.md) ·
[`core-partition-advisor.md`](../inflight/core-partition-advisor.md)

**5. The fleet, without a cluster.** Coordination rides Kafka; frontier agreements make drains,
deployments and topology evolution boring; partitions, records and topics virtualize; scheduled
intent generalises what an obligation is; and the boundary with specialist substrates stays
explicit.
[`core-fleet-capacity-coordination.md`](../inflight/core-fleet-capacity-coordination.md) ·
[`core-frontier-handover.md`](../inflight/core-frontier-handover.md) ·
[`core-partition-virtualization.md`](../inflight/core-partition-virtualization.md) ·
[`core-scheduled-intent.md`](../inflight/core-scheduled-intent.md) ·
[`core-nile-boundary.md`](../inflight/core-nile-boundary.md)

**6. Many faces, one engine.** Facades, ecosystem adapters, runtime services and compatibility
APIs are the adoption surface; internal machinery becomes product through the polyglot
multiplier; the manifest stays on the right side of the platform line.
[`core-alternate-api-facades.md`](../inflight/core-alternate-api-facades.md) ·
[`core-ecosystem-adapters.md`](../inflight/core-ecosystem-adapters.md) ·
[`core-spring-kafka-integration.md`](../inflight/core-spring-kafka-integration.md) ·
[`core-runtime-services-and-compat.md`](../inflight/core-runtime-services-and-compat.md) ·
[`core-internal-machinery-as-features.md`](../inflight/core-internal-machinery-as-features.md) ·
[`core-function-manifest.md`](../inflight/core-function-manifest.md) ·
[`release-certified-execution-semantics.md`](../inflight/release-certified-execution-semantics.md)

**7. Seeing and steering.** Observe/Explain/Act with expiring interventions; the cheap instruments
(gap explainer, hot keys, retry economics, true lag); fingerprints remembered over time; replay
and canarying as safe experimentation.
[`web-control-plane.md`](../inflight/web-control-plane.md) ·
[`web-gui-observability-ideas.md`](../inflight/web-gui-observability-ideas.md) ·
[`core-retry-economics.md`](../inflight/core-retry-economics.md) ·
[`core-ordering-profiler.md`](../inflight/core-ordering-profiler.md) ·
[`core-capacity-fingerprinting.md`](../inflight/core-capacity-fingerprinting.md) ·
[`perf-workload-replay-simulator.md`](../inflight/perf-workload-replay-simulator.md) ·
[`core-scheduler-canarying.md`](../inflight/core-scheduler-canarying.md)

**8. Proving and telling.** The lighthouse exists to falsify; one staged application feeds every
presentation and demo; measurements publish including the refuted ones; the archaeology grounds
it; the cost model says where attention goes.
[`core-lighthouse-mvp.md`](../inflight/core-lighthouse-mvp.md) ·
[`docs-executable-progression.md`](../inflight/docs-executable-progression.md) ·
[`web-three-reveal-demo.md`](../inflight/web-three-reveal-demo.md) ·
[`docs-research-program.md`](../inflight/docs-research-program.md) ·
[`docs-content-series.md`](../inflight/docs-content-series.md) ·
[`perf-benchmark-cost-to-slo.md`](../inflight/perf-benchmark-cost-to-slo.md) ·
[`process-agentic-cost-model.md`](../inflight/process-agentic-cost-model.md) ·
[`process-csid-repo-archaeology.md`](../inflight/process-csid-repo-archaeology.md)

## Sequencing, in one line each

v6 and the open PR stack are untouched by all of the above. The build order is the
**falsification staircase** the cross-model review substituted for navigator-then-lighthouse
(finding 4): (1) a local admission A/B against a conventional limiter, (2) the
twenty-node lease-conservation test under churn measuring overshoot bounds, (3) one stateless
frontier cutover with failure injection - each rung with a null arm, metric, threshold and stop
rule; the twelve-dimension lighthouse only if all three survive. The navigator micro-MVP is the
*candidate* first rung, not a scheduled build: it remains gated on the two decisions
[`core-distributed-throttling.md`](../inflight/core-distributed-throttling.md) says gate any
build (the review caught this map stating it as settled while the owning note says unchosen).
STRATEGY.md adoption waits for the owner's triage and the ce-strategy run.

## Risks register

Recorded at capture time so future sessions can correct course, not just admire the map. A
cross-model adversarial review ran 2026-08-31; its eight findings
are folded into the owning notes and reflected below. Each
entry: the risk, the tell that it is materialising, and the correction already on the record (or
named here for the first time). Consult this before expanding any track.

- **Frictionless-derivation bias.** The corpus was produced in a medium with no adversarial
  pressure - the reviewing model never once said "bad idea"; every correction came from the owner
  or from checking the repo. The local half (admission, resources, attribution, DLQ-less failure,
  Prescience-as-PC-feature) is grounded in shipped code; the global half (fleet, economics,
  demand shaping) is pure derivation with materially lower survival odds, and the notes give both
  equal typographic confidence. *Tell:* global-half notes accumulating detail while no falsifier
  has run. *Correction, upgraded by the cross-model review (finding 3): use THREE evidence grades* -
  **shipped** (the dispatch seam, key independence, sparse completion, accounting), **locally
  testable extension** (admission, attribution, lease mechanics, Prescience read path), and
  **distributed hypothesis** (fleet, economics, frontier migration) - because "grounded local
  half" was itself too generous: shipped code grounds the seam, not lease conservation or
  exhaustive lookahead. The staircase in Sequencing gates investment
  ([`core-lighthouse-mvp.md`](../inflight/core-lighthouse-mvp.md)).
- **Shipping displacement.** The vision makes everything feel connected, so everything feels
  worth doing - in a repo that already accumulates open PRs faster than it lands releases.
  *Tell:* another strategy weekend before the navigator demo exists; v6 slipping for any
  vision-shaped reason. *Correction:* v6 is untouched by all of this by construction; the next
  built thing is the smallest demo (2 tokens/sec, two instances, bottleneck correctly attributed),
  and "no subsystem gets completed for its own sake" cuts both ways - including the note corpus.
- **Identity fusion resists falsification.** The owner has named this work as a career
  culmination; pre-registered falsifiers are emotionally easy to write and hard to honour.
  *Tell:* a falsifier outcome reframed as "needs more work" rather than accepted; benchmarks
  chosen after seeing results. *Correction:* names are not preregistration (cross-model review, finding 4) - each falsifier
  carries a null arm, metric, rejection threshold and stop rule per the staircase in Sequencing;
  the research-program discipline (publish refuted hypotheses,
  [`docs-research-program.md`](../inflight/docs-research-program.md)) applies to the architecture
  itself; the measurement campaign that withdrew its own headline claims is the precedent to keep.
- **Naming exuberance as a displacement tell.** A dozen codenames accreted in one weekend; fun is
  fine, but naming is cheaper than building and feels like progress. *Tell:* new vocabulary
  outpacing new measurements. *Correction:* one product name (after trademark clearance - open
  decision below), plain words everywhere else; codenames stay quarantined in the glossary.
- **Market-niche mismatch.** The workloads that *need* ordering-domain scheduling today
  (key-skewed, downstream-limited, ordering-sensitive) are real but a niche; intellectual
  superiority historically loses to distribution. *Tell:* engine features landing while adoption
  numbers stay flat; positioning arguments winning debates and no users. *Correction:* the
  layer-2 adapter strategy ([`core-ecosystem-adapters.md`](../inflight/core-ecosystem-adapters.md))
  is the strategic main line, not a side quest - "just change your import" is the distribution
  answer, and the fleet layer is the eventual market-widener, sequenced after a falsifier
  survives.
- **Kafka absorbs the substrate faster than the higher layers mature.** Share Groups, KIP-1277
  and successors keep implementing pieces of the mechanics. *Tell:* a KIP shipping something a
  note still describes as a differentiator. *Correction:* the recorded robustness position -
  multiple acquisition engines, "as Kafka gains primitives the runtime gets smaller, not less
  useful" - plus actively tracking the KIP landscape rather than discovering it in a launch-week
  comment thread.
- **Coordination-plane correctness debt.** The global half implies leases, epochs, fencing and
  authority transfer - the hardest failure-mode class there is, and this repo's own bug ledger
  shows what client-side concurrency already cost. *Tell:* a distributed feature growing beyond
  its conservation-law safety argument; an in-memory stub whose semantics a distributed
  implementation cannot honour. *Correction:* failure-wastes-capacity-never-violates-the-contract
  as the design bias, the architectural-stubs rule, and the twenty-instances-under-churn
  acceptance test before anything clever.
- **The agentic cost model can rationalise unbounded breadth.** "Price conceptual complexity, not
  code" is only valid while the breadth genuinely collapses onto the small primitive set; agents
  making code cheap makes sprawl cheap too. *Tell:* a new "product" needing its own distributed
  coordination mechanism (the recorded litmus test firing); adapter count growing while the
  conformance matrix does not. *Correction:* the litmus test in
  [`core-internal-machinery-as-features.md`](../inflight/core-internal-machinery-as-features.md),
  and the falsifiers again - if the collapse is illusory, the cost model is void.
- **Solo-maintainer concentration.** One person's attention is the ordering key; the vision
  multiplies the surface that only the owner can explain. *Tell:* questions only answerable by
  the owner accumulating; contributors bouncing off the corpus. *Correction:* the lighthouse as
  onboarding path, the docs-site work, and the owner's own stated goal - explainable without the
  owner in the critical path.
- **Complexity leaking to users.** Every layer added above the consumer risks the
  change-your-import promise. *Tell:* a feature that requires users to learn scheduler vocabulary
  to get the old behaviour; a facade needing configuration the wrapped API never needed.
  *Correction:* progressive declaration (discover what is safe, declare only the unknowable,
  delegate explicitly) and the adapter pass/fail test - if a compatibility layer still builds its
  own HA/retries/DLQ, the substrate failed.
- **Note-corpus drift.** ~45 notes restate facts the duplication scanners cannot see; cached
  external-world claims (Share Groups versions, competitor facts) go stale silently. *Tell:* two
  notes disagreeing about a fact neither owns; a note citing an external state older than its
  reader assumes. *Correction:* this binder owns no content; the file-refs gate polices links;
  verify-before-citing caveats are already in the notes and must be honoured, not tidied away;
  the owner's triage pass is the scheduled cleanup.
- **Contract fidelity and control-plane trust** (added by the cross-model review, finding 3).
  Admission, attribution and Spice all consume *declared* metadata - resource requirements,
  priorities, tenant identity - and declarations can be incomplete, stale, wrong or malicious;
  garbage in corrupts admission and attribution long before any fleet layer exists. *Tell:*
  attribution blaming a resource the handler never calls; a tenant quota bypassed by an
  undeclared dependency. *Correction:* measure declaration recall against observed calls (the
  declared-vs-observed feedback loop in
  [`core-prescience-and-spice.md`](../inflight/core-prescience-and-spice.md)), keep a safe
  opaque-work lane for undeclared handlers, and treat control metadata as an authorization
  surface, not trusted input.
- **The two hard-mode features, ring-fenced by the owner's ruling (2026-08-31).** The review's
  two deepest findings - hard global rate ceilings (lease fencing against external services) and
  frontier-based cutover (scalar F vs sparse execution) - may be unachievable as originally
  stated. The owner's recorded position: *if those two never work, the remainder still stands on
  its own.* *Tell:* another track's design quietly depending on a hard ceiling or a seamless
  cutover. *Correction:* both notes now carry the corrected, weaker promises (bounded overshoot;
  stateless-first vector frontiers -); no other note may assume the strong forms.
- **Positioning misread as abandonment.** To an outside user, "PC is now the kernel of a
  scheduler runtime" can read as "the consumer library is deprecated". *Tell:* issues or comments
  asking whether PC is still maintained; v6 messaging leading with the vision. *Correction:* v6
  ships as a consumer-library release on its own merits; the vision is introduced as what the
  library was always becoming, never as what replaces it.

## Open decisions, all the owner's

Product name (trademark clearance first) and the W2/Voice codename question; the OSS/enterprise
split hypothesis; STRATEGY.md adoption; PC-inside-Streams timing (ruled post-lighthouse);
"Merge 367" disposition. Each is recorded where it arose - this map only lists them.
