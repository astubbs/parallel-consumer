# The W2 vision: the laws, the beats, and how the notes string together

**W2** is the project-level working codename - *"Why Wait?"* - deliberately used here so the product
name stays adjustable; [`inflight/core-admission-scheduling-model.md`](inflight/core-admission-scheduling-model.md)
owns the naming glossary, including why W2-the-codename is not the W2 the handoff used for the
control plane (now *Voice*).

**Live document, not a record.** It lived under `docs/plans/` as the dated vision map until
2026-09-01 - `git log --follow -M30% -- docs/w2-vision.md` walks through the move, and the threshold
is needed because the promotion rewrote enough that git's default 50% similarity does not pair the
two - and was misfiled there: a `docs/plans/` document says what was known on one day and may
not be rewritten ([`citations.md`](citations.md)), whereas this binds a note set that keeps moving.
Statements here that *are* point-in-time carry their own date inline, so the record survives the
move.

## What this owns, and the rule that keeps it DRY

It owns **the generative laws and the connections between notes** - which law a claim follows from,
what implies what, what must come before what, and what contradicts what. It owns **no fact a note
owns**. That is the whole DRY protection, and it is narrower than the rule this file used to carry
(*"no content of its own"*), which prevented drift by preventing content - and left the laws
stranded in a frozen document.

The distinction is not fussy: a law is not a duplicate because no note owns it, and a connection is
not a duplicate because **no note can own the relation between two notes**. When a note and this
file disagree about a fact, the note wins and this file is wrong.

**Nothing enforces the rule, so here are its tripwires**, each naming its own fix - the same
treatment [`../AGENTS.md`](../AGENTS.md) gives its own belongs-here test, which it likewise says no
check can make:

- **A paragraph that names no note.** It is a fact in the wrong file; move it to the note that
  should own it, or write that note.
- **A claim restated rather than linked.** Cut it to the link. If the link is not enough, the note
  is the thing to fix.
- **This file growing faster than the notes it binds.** Content is migrating the wrong way.
- **A law with no notes beside it.** Either it generates nothing - retire it - or the work it
  governs is untracked.

What *is* enforced: the file-refs gate breaks when a linked note is moved or renamed, which is the
drift that would otherwise go silent.

## Sources

The preserved primary sources are the handoff documents
([`2026-08-29-hasten-compound-engineering-handoff.md`](ideation/2026-08-29-hasten-compound-engineering-handoff.md),
[`2026-08-30-hasten-handoff-supplement.md`](ideation/2026-08-30-hasten-handoff-supplement.md))
and the vision fiction
([`2026-08-29-the-story-of-hasten.md`](ideation/2026-08-29-the-story-of-hasten.md));
[`SOUND_BITES.md`](../SOUND_BITES.md) carries the compressed intent. Those are dated records of
conversations and stay frozen - **where one of them and a note disagree, flag it, never silently
reconcile.**

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

## The architectural laws

Ten rules the rest of this is generated from, from the compound-engineering handoff's section 2.
They lived only in that document until 2026-09-01 - a dated record that may not be updated - so the
layer the whole corpus is derived from sat where it could only get further from the truth. Each line
is the law, then what it **governs**; what any governed claim actually says belongs to the note
named with it.

1. **Ownership and execution are independent.** Partitions answer where work is owned, not what may
   run concurrently. Beat 1 is this law stated as a thesis, and it is why the spine above has more
   rungs than Kafka has.
   [`core-engine-thesis.md`](inflight/core-engine-thesis.md) ·
   [`core-execution-opportunity-model.md`](inflight/core-execution-opportunity-model.md)

2. **Programming model: yours. Execution model: ours.** Insert at the execution seam; Streams, plain
   Consumer, Spring, actors and workflows stay recognisable. This is why beat 6 is a set of facades
   rather than a set of products, and it is the premise the integration filter tests
   (*what unnecessary coupling does this remove?*).
   [`core-ecosystem-adapters.md`](inflight/core-ecosystem-adapters.md) ·
   [`core-alternate-api-facades.md`](inflight/core-alternate-api-facades.md) ·
   [`core-spring-kafka-integration.md`](inflight/core-spring-kafka-integration.md)

3. **Global intelligence, local execution.** Coordinate constraints and capacity globally; dispatch
   on the hot path locally, from delegated state. **The most load-bearing law here, and the one that
   had no live owner at all before this file took it.** Fleet coordination's *"every runtime decides
   locally, collectively one scheduler"* is this law at the fleet; delegated credits are it at a
   resource; the line between forcing and offering a per-call decision is it at the deployment. Its
   failure mode is the law with the global half removed: the local controller keeps discovering
   alone, which is what a conventional adaptive limiter does, so losing the fleet degrades rather than
   stalls - for participants that have a local controller to fall back to. Read to its limit the
   law allows *no cluster at all* - the broker inside the application - which is the open question
   the standalone note's third topology carries.
   [`core-fleet-capacity-coordination.md`](inflight/core-fleet-capacity-coordination.md) ·
   [`core-shared-execution-resources.md`](inflight/core-shared-execution-resources.md) ·
   [`core-auto-scaling.md`](inflight/core-auto-scaling.md) ·
   [`core-standalone-deployment.md`](inflight/core-standalone-deployment.md) ·
   [`core-non-kafka-participants.md`](inflight/core-non-kafka-participants.md)

4. **One implementation of intelligence. Many implementations of ergonomics.** Distributed
   correctness belongs in the shared runtime; language SDKs are native surfaces over it. This is the
   multiplier that makes beat 6's exposures cheap - build a primitive once and it exists in every
   bound language - so it is a precondition of the AWS move, not a consequence of it.
   [`core-internal-machinery-as-features.md`](inflight/core-internal-machinery-as-features.md) ·
   [`core-native-rewrite.md`](inflight/core-native-rewrite.md)

5. **Discover what is safe; declare what is unknowable; delegate control explicitly.** Infer
   mechanics and observations, require declaration only for business intent, require granted
   authority for external control. It decides where every feature's input comes from, and it is why
   the manifest stays on the right side of the platform line. The authority ladder that implements
   its third clause is owned by
   [`core-engine-thesis.md`](inflight/core-engine-thesis.md) (*Observe → Recommend → Shadow →
   Enforce*, with progressive declaration).
   [`core-function-manifest.md`](inflight/core-function-manifest.md) ·
   [`core-prescience-and-spice.md`](inflight/core-prescience-and-spice.md) ·
   [`core-slo-objective-api.md`](inflight/core-slo-objective-api.md)

6. **No unexplained waiting.** Every prevented execution has a binding constraint and a reason
   describing what would let it proceed. Beat 1's *waiting is a scheduling state* is the modelling
   half; this is the obligation half, and it is what turns one eligibility model into a user-visible
   promise.
   [`core-admission-scheduling-model.md`](inflight/core-admission-scheduling-model.md) ·
   [`core-temporal-horizons.md`](inflight/core-temporal-horizons.md)

7. **The scheduler's decision state is the observability.** Preserve the reasons rather than
   reconstructing them from generic telemetry later. This is why beat 7 is a projection of beat 1
   rather than an instrumentation project, and why lineage and OTel export are *exports* of
   something already known.
   [`core-decision-lineage.md`](inflight/core-decision-lineage.md) ·
   [`core-record-semantic-tracing.md`](inflight/core-record-semantic-tracing.md) ·
   [`web-control-plane.md`](inflight/web-control-plane.md)

8. **Internal machinery should become public capability when reusable.** If a primitive is needed
   anyway, prefer exposing it to deploying a separate subsystem. Law 4 is what makes this cheap; the
   AWS test is what stops it becoming a product line.
   [`core-internal-machinery-as-features.md`](inflight/core-internal-machinery-as-features.md) ·
   [`core-non-kafka-participants.md`](inflight/core-non-kafka-participants.md)

9. **Architectural stubs preserve future seams.** A designed capability may start local or
   in-memory, but only if it obeys the eventual distributed semantics. This is what lets the
   lighthouse keep sophisticated implementations hollow without the hollowness becoming a rewrite.
   [`core-shared-execution-resources.md`](inflight/core-shared-execution-resources.md) ·
   [`core-lighthouse-mvp.md`](inflight/core-lighthouse-mvp.md)

10. **The canonical example must compose.** Every major capability extends the same logistics
    application; one that needs a new conceptual universe is a scope challenge. It has a merge-gate
    edge - no new standalone demo codebase when a stage of the progression could carry it - and it
    is the rule the lighthouse's *no subsystem gets completed for its own sake* enforces in the
    build.
    [`docs-executable-progression.md`](inflight/docs-executable-progression.md) ·
    [`core-lighthouse-mvp.md`](inflight/core-lighthouse-mvp.md)

## The four decisions

Four decisions Kafka applications routinely conflate, separated. The spine above is the question
each one answers; this is who answers it. Also from the handoff's section 3, and likewise without a
live owner until now - [`core-engine-thesis.md`](inflight/core-engine-thesis.md) names the model as
candidate `STRATEGY.md` material but does not state it.

| Decision | Question | Owner |
|---|---|---|
| **Partitions** | Where is work owned? | Kafka / group ownership |
| **Keys and ordering domains** | What may execute independently? | The scheduler (law 1) |
| **Engine** | How much useful parallelism should run? | The adaptive local controller (beat 2) |
| **Infrastructure** | How much engine capacity should exist? | An external actuator, informed (beat 2, dimension 2) |

Read down the Owner column and the *embedded, not cluster* positioning falls out: three of the four
are answered inside something that already exists, and the fourth is answered by advising something
that already exists.

## The beats

**1. The thesis.** Kafka ownership and execution are independent; PC proved it at the key level,
and following the observation to its conclusion makes execution itself programmable. Waiting is a
scheduling state, not an execution state - every mechanism that makes work wait is one eligibility
model. Work has identity, position and incarnation, and everything else is a projection of one
opportunity model.
[`core-engine-thesis.md`](inflight/core-engine-thesis.md) ·
[`core-admission-scheduling-model.md`](inflight/core-admission-scheduling-model.md) ·
[`core-execution-opportunity-model.md`](inflight/core-execution-opportunity-model.md) ·
[`core-work-identity-model.md`](inflight/core-work-identity-model.md)

**2. The local engine learns.** The controller discovers useful concurrency experimentally
(astubbs#333 implements it), classifies *why* more stops helping, proves what could be removed,
and holds a declared objective instead of a configured number.
[`core-auto-scaling.md`](inflight/core-auto-scaling.md) ·
[`core-bottleneck-attribution.md`](inflight/core-bottleneck-attribution.md) ·
[`core-scale-in-proof.md`](inflight/core-scale-in-proof.md) ·
[`core-slo-objective-api.md`](inflight/core-slo-objective-api.md)

**3. The future becomes legible.** The committed backlog is indexed by execution meaning
(an inverted index, not a cache), demand and capacity get horizons, queue disciplines become
policy over one primitive, and the causal past is the same graph pointed backward.
[`core-prescience-and-spice.md`](inflight/core-prescience-and-spice.md) ·
[`core-temporal-horizons.md`](inflight/core-temporal-horizons.md) ·
[`core-queue-disciplines.md`](inflight/core-queue-disciplines.md) ·
[`core-decision-lineage.md`](inflight/core-decision-lineage.md) ·
[`core-record-semantic-tracing.md`](inflight/core-record-semantic-tracing.md)

**4. Capacity becomes shared.** Named resources own capacity; renewable pieces are delegated and
spent locally; per-function arbitration, tenant quotas, priorities and the partition advisor fall
out as policy. This is the buildable centre - the navigator micro-MVP and the twenty-instance
conservation test live here.
[`core-shared-execution-resources.md`](inflight/core-shared-execution-resources.md) ·
[`core-distributed-throttling.md`](inflight/core-distributed-throttling.md) ·
[`core-per-function-capacity-arbitration.md`](inflight/core-per-function-capacity-arbitration.md) ·
[`core-partition-advisor.md`](inflight/core-partition-advisor.md)

**5. The fleet, without a cluster.** Coordination rides Kafka; frontier agreements make drains,
deployments and topology evolution boring; partitions, records and topics virtualize; scheduled
intent generalises what an obligation is; the boundary with specialist substrates stays
explicit; and participants who never run Kafka enter the resource graph through the telemetry they
already emit and a credit-vending surface, which is where the "beyond Kafka" product decision was
taken - and asking whether the runtime must be inside a process at all turns out to be where
"without a cluster" has to be defined precisely rather than asserted.
[`core-fleet-capacity-coordination.md`](inflight/core-fleet-capacity-coordination.md) ·
[`core-frontier-handover.md`](inflight/core-frontier-handover.md) ·
[`core-partition-virtualization.md`](inflight/core-partition-virtualization.md) ·
[`core-scheduled-intent.md`](inflight/core-scheduled-intent.md) ·
[`core-nile-boundary.md`](inflight/core-nile-boundary.md) ·
[`core-non-kafka-participants.md`](inflight/core-non-kafka-participants.md) ·
[`core-standalone-deployment.md`](inflight/core-standalone-deployment.md)

**6. Many faces, one engine.** Facades, ecosystem adapters, runtime services and compatibility
APIs are the adoption surface; internal machinery becomes product through the polyglot
multiplier; the manifest stays on the right side of the platform line.
[`core-alternate-api-facades.md`](inflight/core-alternate-api-facades.md) ·
[`core-ecosystem-adapters.md`](inflight/core-ecosystem-adapters.md) ·
[`core-spring-kafka-integration.md`](inflight/core-spring-kafka-integration.md) ·
[`core-runtime-services-and-compat.md`](inflight/core-runtime-services-and-compat.md) ·
[`core-internal-machinery-as-features.md`](inflight/core-internal-machinery-as-features.md) ·
[`core-function-manifest.md`](inflight/core-function-manifest.md) ·
[`release-certified-execution-semantics.md`](inflight/release-certified-execution-semantics.md)

**7. Seeing and steering.** Observe/Explain/Act with expiring interventions; the cheap instruments
(gap explainer, hot keys, retry economics, true lag); fingerprints remembered over time; replay
and canarying as safe experimentation.
[`web-control-plane.md`](inflight/web-control-plane.md) ·
[`web-gui-observability-ideas.md`](inflight/web-gui-observability-ideas.md) ·
[`core-retry-economics.md`](inflight/core-retry-economics.md) ·
[`core-ordering-profiler.md`](inflight/core-ordering-profiler.md) ·
[`core-capacity-fingerprinting.md`](inflight/core-capacity-fingerprinting.md) ·
[`perf-workload-replay-simulator.md`](inflight/perf-workload-replay-simulator.md) ·
[`core-scheduler-canarying.md`](inflight/core-scheduler-canarying.md)

**8. Proving and telling.** The lighthouse exists to falsify; one staged application feeds every
presentation and demo; measurements publish including the refuted ones; the archaeology grounds
it; the cost model says where attention goes.
[`core-lighthouse-mvp.md`](inflight/core-lighthouse-mvp.md) ·
[`docs-executable-progression.md`](inflight/docs-executable-progression.md) ·
[`web-three-reveal-demo.md`](inflight/web-three-reveal-demo.md) ·
[`docs-research-program.md`](inflight/docs-research-program.md) ·
[`docs-content-series.md`](inflight/docs-content-series.md) ·
[`perf-benchmark-cost-to-slo.md`](inflight/perf-benchmark-cost-to-slo.md) ·
[`process-agentic-cost-model.md`](inflight/process-agentic-cost-model.md) ·
[`process-csid-repo-archaeology.md`](inflight/process-csid-repo-archaeology.md)

## The adoption staircase

The handoff's section 21, and **not the same ladder as the build order below** - this is the order a
*user* climbs, the falsification staircase is the order *we* build in, and conflating them is easy
because both are numbered lists of increasing ambition. A rung here is only reachable once the
capability exists; a rung there is only attempted once the previous one survived.

```
1  key concurrency (Parallel Consumer, today)
2  language-native Kafka-compatible client
3  Kafka Streams wrapper
4  decision telemetry, observe-only
5  named resources and global coordinated limits
6  horizontal scaling recommendations
7  vertical scaling recommendations
8  delegated scaling (Kubernetes, specialist optimizers)
9  coordinated shared infrastructure across workloads
10 global cost/SLO optimization across the dependency graph
```

Three things it settles that are otherwise argued from scratch each time:

- **Rung 4 before rung 5 is the whole authority argument.** Observation earns the right to restrain,
  which is law 5's third clause and the ladder
  [`core-engine-thesis.md`](inflight/core-engine-thesis.md) owns.
- **It predicts where a non-Kafka participant enters**, and the answer is not rung 1. The mechanisms
  in [`core-non-kafka-participants.md`](inflight/core-non-kafka-participants.md) map onto rungs 4-6
  with the client rungs skipped entirely, which is why
  [`core-standalone-deployment.md`](inflight/core-standalone-deployment.md) reads the same four
  mechanisms as an adoption ramp of their own.
- **Rungs 6 and 7 are separate on purpose.** Beat 2's dimension 2 is rung 6 only; recommending
  *vertical* capacity is a distinct claim that no note yet owns.

## Sequencing, in one line each

v6 and the open PR stack are untouched by all of the above. The build order is the
**falsification staircase** the cross-model review substituted for navigator-then-lighthouse
(finding 4): (1) a local admission A/B against a conventional limiter, (2) the
twenty-node lease-conservation test under churn measuring overshoot bounds, (3) one stateless
frontier cutover with failure injection - each rung with a null arm, metric, threshold and stop
rule; the twelve-dimension lighthouse only if all three survive. The navigator micro-MVP is the
*candidate* first rung, not a scheduled build: it remains gated on the two decisions
[`core-distributed-throttling.md`](inflight/core-distributed-throttling.md) says gate any
build (the review caught this file stating it as settled while the owning note says unchosen).
STRATEGY.md adoption waits for the owner's triage and the ce-strategy run.

## Risks register

Recorded at capture time (2026-08-31) so future sessions can correct course, not just admire the
landscape. A
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
  ([`core-lighthouse-mvp.md`](inflight/core-lighthouse-mvp.md)).
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
  [`docs-research-program.md`](inflight/docs-research-program.md)) applies to the architecture
  itself; the measurement campaign that withdrew its own headline claims is the precedent to keep.
- **Naming exuberance as a displacement tell.** A dozen codenames accreted in one weekend; fun is
  fine, but naming is cheaper than building and feels like progress. *Tell:* new vocabulary
  outpacing new measurements. *Correction:* one product name (after trademark clearance - open
  decision below), plain words everywhere else; codenames stay quarantined in the glossary.
- **Market-niche mismatch.** The workloads that *need* ordering-domain scheduling today
  (key-skewed, downstream-limited, ordering-sensitive) are real but a niche; intellectual
  superiority historically loses to distribution. *Tell:* engine features landing while adoption
  numbers stay flat; positioning arguments winning debates and no users. *Correction:* the
  layer-2 adapter strategy ([`core-ecosystem-adapters.md`](inflight/core-ecosystem-adapters.md))
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
  [`core-internal-machinery-as-features.md`](inflight/core-internal-machinery-as-features.md),
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
  [`core-prescience-and-spice.md`](inflight/core-prescience-and-spice.md)), keep a safe
  opaque-work lane for undeclared handlers, and treat control metadata as an authorization
  surface, not trusted input - with ownership/version authorization on contract IDs, validated
  parameter binding, and fail-closed defaults (GitHub Codex review, 2026-08-31).
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
split hypothesis, and the hosted-offering shape beside it; STRATEGY.md adoption; PC-inside-Streams
timing (ruled post-lighthouse);
"Merge 367" disposition. Each is recorded where it arose - this file only lists them.
