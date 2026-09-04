# Hasten prior-art sweep: what is already occupied, and what is only "not found"

Dated record of a deep-research sweep run 2026-09-05 against the Hasten / W2 engine design. **A
`docs/plans/` document is a record of what was known on one day and may not be rewritten**
([`../citations.md`](../citations.md)); the living view is
[`../inflight/core-hasten-adjacent-systems-register.md`](../inflight/core-hasten-adjacent-systems-register.md),
which owns what changes from here.

Run as a fan-out of five search angles with adversarial 3-vote verification per extracted claim: a
claim needed two of three verifiers to refute it before it was killed. Ten claims were killed in
verification, and those are recorded below beside the survivors, because a claim that failed
verification is a result.

**Why it was run.** [`../inflight/core-engine-thesis.md`](../inflight/core-engine-thesis.md) already
carried the instruction - a single external model's novelty search, *"recorded as a lead and not a
survey; do the prior-art sweep before any public 'first' claim"* - and nobody had executed it. The
owner asked for it directly after the equivalent InFlight sweep, saying he would hate to discover an
existing project like Hasten later.

## Framing, corrected the same day by the owner - read before the findings

**This corpus was never claiming novelty; it was exploring product space.** The sweep was run in
adversarial mode - *try to disprove this* - because that is what makes a language model search hard
instead of agreeably. **"Refuted" and "disproved" throughout the findings below are the instrument's
verdict language, not a record of anything we believed and lost.** The right reading of a REFUTED row
is *this question already has a shipped answer, and here is whose* - never *we thought we invented
this*. The first version of this section was written the other way round and imputed claims that were
never made; it is corrected here rather than reworded silently, because a dated record that
misdescribes its own history is worse than one that shows the correction.

**And idea-novelty is close to irrelevant to the actual question**, which is whether the
*implementation* is novel and useful **in its placement**. The primary audience is teams already
running Kafka who never set out to adopt a scheduler; for them the value is capabilities arriving
without a new cluster, a second system to operate, or an application rewrite. Doorman having invented
lease-vending does not help that team, because they were never going to deploy Doorman's server tree.
Attracting teams already on a scheduler is a byproduct, not the strategy.

## The one-paragraph answer

Most of the design's individually-explored ideas already have shipped answers elsewhere, and two are
answered emphatically: pre-execution admission as a scheduling state, and the
globally-coordinated/locally-decided split. The lease-delegation mechanism of claim 2 is Doorman's
and is nineteen-year-old territory; what is unanswered there is the **substrate** - divisible leases
carried on a durable log to embedded instances with no coordination cluster and nothing per-call in
the hot path, which Doorman (server tree plus etcd), DRL (UDP gossip), Kueue (API server) and DBOS
(Postgres) each miss on a different axis. Also unanswered: claim 4's index over a committed backlog
keyed by producer-declared requirements, and the composite of claim 5.

**Convergence is a good sign, not a bad one.** Four independent teams reaching the same shapes is
evidence the shapes are right. What it changes is how the work should be *described* - name theirs
first - and where effort is best spent: not on re-deriving admission control, but on the substrate,
on Prescience, and on the one difference the sweep positively confirmed rather than merely failed to
find a counterexample to: **no application rewrite and nothing in the per-call hot path.**

**The conclusion is not "nobody does this".** It is *nobody does it from this position*, and the
position is what makes the rest cheap - which is a testable claim about arrangement, not about
invention.

## The findings

### 1. Claim 1 (admission as a scheduling state) - REFUTED AS NOVEL

*Confidence: high · verification vote: 3-0 (x4)*

REFUTED AS NOVEL — Claim 1 ("waiting is a scheduling state, not an execution state") is fully
occupied prior art in at least four independent production systems. Kueue holds a Workload in a
queue and creates no Pods at all until quota admits it; CockroachDB's own tech note defines
admission control as deciding "when work submitted to that system begins executing" with work
"queued until admitted"; Impala queues queries that "wait to begin execution" and admits them when
resources free; Restate v1.7 holds invocations in virtual queues under a scheduler "that decides
which invocation runs next". The only part of claim 1 that survives is its scoping clause: none of
these admit records that are already durably resident in an external log owned by someone else.

**Evidence and corrections.** Four unanimous 3-0 verified claims, each against a primary source (project reference docs, in-repo
design tech note, ASF docs, vendor docs + release notes). Kueue is the strictest instantiation — a
suspended Job's Pods are never created, so a queued Workload is not even a Pending pod, stricter
than the Kubernetes-unschedulable-pod case the brief already listed as adjacent. Verifier note on
CockroachDB: elastic CPU tokens and the `Pacer` type mean some CockroachDB work re-acquires tokens
*during* execution, which extends rather than contradicts the framing. Impala collapses ADMITTED
and RUNNING into one transition, so it instantiates a three-state machine, not the literal four-
state KNOWN -> ADMISSIBLE -> ADMITTED -> RUNNING — write this up as "occupies the pre-execution
admission distinction", not "occupies the four-state framing". Restate is shipped-but-experimental
(gated behind `experimental_enable_vqueues`, fresh clusters only in 1.7).

Sources:
- <https://kueue.sigs.k8s.io/docs/concepts/>
- <https://kueue.sigs.k8s.io/docs/tasks/run/jobs/>
- <https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md>
- <https://impala.apache.org/docs/build3x/html/topics/impala_admission.html>
- <https://docs.restate.dev/services/flow-control>

### 2. Claim 3 (global intelligence, local execution) - REFUTED AS NOVEL, most thoroughly of the set

*Confidence: high · verification vote: 3-0 (Doorman, DRL); 2-1 (Impala)*

REFUTED AS NOVEL — Claim 3 ("global intelligence, local execution", degrading rather than stalling
when the global half is lost) is occupied three times over, in three different decades and three
different layers: Impala (embedded in each daemon, decisions local, shared state via statestore,
explicitly low-overhead and imprecise under load), Doorman (global server tree vends time-bounded
leases, go/no-go decided in-process, falls back to a configured or dynamically computed safe
capacity when the server is unreachable), and SIGCOMM'07 Distributed Rate Limiting (peer-to-peer
limiters, per-packet decision is a local token bucket or locally computed drop probability, only
periodic estimates gossiped). This is the single most thoroughly disproven claim in the set.

**Evidence and corrections.** Impala: "The admission control system is decentralized, embedded in each Impala daemon and
communicating through the statestore mechanism" — verified verbatim in both the 3.x and the
CURRENT doc build, so not a legacy artefact. Doorman: verified in source, not just prose —
`go/ratelimiter/ratelimiter.go` implements `Wait(ctx)` in-process against a capacity channel; the
only RPC is the periodic refresh. DRL: the `FPS-HANDLE-PACKET` pseudocode has three lines and no
remote call; the paper's own related-work section frames itself as "a continuous form of
distributed admission control". Two precision corrections for the write-up: Impala's statestored
is a centralized pub/sub daemon, NOT peer-to-peer gossip; and Impala's "global" half only
propagates aggregate counters against statically configured pool limits, so it refutes the
architectural split without occupying any lease mechanism. Cloudera Runtime ships an optional
centralized Admission Daemon, which qualifies universality for those deployments but is itself
further prior art on both sides of the boundary.

Sources:
- <https://impala.apache.org/docs/build3x/html/topics/impala_admission.html>
- <https://impala.apache.org/docs/build/html/topics/impala_admission.html>
- <https://github.com/youtube/doorman/blob/master/doc/design.md>
- <https://github.com/youtube/doorman/blob/master/go/ratelimiter/ratelimiter.go>
- <https://www.microsoft.com/en-us/research/wp-content/uploads/2007/01/fp076-raghavan.pdf>

### 3. Claim 2 (delegated capacity leases) - PARTIALLY REFUTED; only the log substrate survives

*Confidence: high · verification vote: 3-0 (Doorman occupies leases); 3-0 (Doorman does not occupy no-cluster); 3-0 (Kueue borrowing); 2-1 (DBOS shape)*

PARTIALLY REFUTED — Claim 2 decomposes into four elements, and three of them are separately
occupied; only the fourth (a durable log as the coordination substrate) is unoccupied by anything
found. Doorman occupies renewable time-bounded capacity leases delegated to an embedded client
library and spent locally with no per-call permit server, but coordinates via a dedicated
hierarchical server tree with etcd/ZooKeeper master election and a central config store. DRL
occupies no-cluster, no-central-permit-server distributed capacity, but coordinates by UDP gossip.
Kueue occupies divisible borrowable capacity across named pools (cohorts,
borrowingLimit/lendingLimit), but arbitrates centrally. DBOS occupies the embedded-library-not-
cluster packaging shape, but with Postgres. Nothing found delegates divisible capacity leases over
a Kafka-like log to embedded library instances.

**Evidence and corrections.** Doorman design.md verbatim: leases carry `expiry_time` and `refresh_interval` (typical refresh
8-16s, lease ~5 min), and "It is the client's responsibility not to overshoot the limit L" —
delegation is real and the enforcement is in-process. But: "The rate limiting servers are
distributed in a tree", "Each server loads the global resource configuration from a central
configuration server", plus etcd-based master election; master state is in-memory, no log. Kueue
verbatim: "ClusterQueues that belong to the same cohort can borrow unused quota from each other",
with `borrowingLimit`/`lendingLimit` as live v1beta1 API fields — but reconciled by a leader-
elected `kueue-controller-manager` over API objects, so "borrowable, cap-bounded capacity across
named quota pools" is the accurate phrasing, not "delegated". MultiKueue delegates *work*, not
divisible capacity, and does not refute this. DBOS: "There's no separate orchestration server and
no infrastructure required besides Postgres" — but its `concurrency`/`limiter` are enforced by
every worker hitting the system DB per dequeue (FOR UPDATE SKIP LOCKED), i.e. a central permit
store on the hot path in DB form; only static `worker_concurrency` is local. Net: the log-as-
substrate combination is the residual, and it is a combination claim, not a mechanism claim.

Sources:
- <https://github.com/youtube/doorman/blob/master/doc/design.md>
- <https://github.com/youtube/doorman/blob/master/README.md>
- <https://www.microsoft.com/en-us/research/wp-content/uploads/2007/01/fp076-raghavan.pdf>
- <https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/>
- <https://kueue.sigs.k8s.io/docs/reference/kueue.v1beta1/>
- <https://docs.dbos.dev/architecture>

### 4. The conservation-law safety bias - WEAKENED, NOT REFUTED

*Confidence: high · verification vote: 3-0 (Doorman modes); 2-1 (DRL tradeoff quantification)*

WEAKENED, NOT REFUTED — The conservation-law safety bias ("failure wastes capacity but never
violates the contract") is a stricter position within an already-explored design space, not a new
category. Doorman documents three client fallback modes on lease expiry — pessimistic (assume
zero), optimistic (assume the full request, explicitly risking overload), and safe (fall back to a
configured or server-computed safe capacity) — making the bias a per-client configuration choice
with an explicitly contract-violating option, and states "The system provides no protection
against misbehaving clients." DRL's shipped designs bias the OTHER way: the paper concedes a fully
disconnected system "could over-subscribe the global limit by a factor of N", and the
faithful/naive design (GTB) is shown to fail outright under staleness.

**Evidence and corrections.** Doorman verbatim: "In optimistic mode it can behave as if it got all of the capacity it
requested", and safe_capacity "defines how the server would like clients to behave ... However
since the system is cooperative, clients are not obligated to follow the configured safe
capacity". DRL is quantified empirical prior art for the degrade-not-stall tradeoff: <3% control
overhead, scaling to 490 limiters, 0.47% inter-limiter control loss. TWO CORRECTIONS THAT MUST
TRAVEL WITH THESE NUMBERS: (a) venue conflation — the <3% and 490-limiter figures are from a
7-machine local emulated testbed; only the 0.47% loss figure is PlanetLab (10 nodes). Do not
present "490 limiters on PlanetLab" as one experiment. (b) Do not call GTB "strictly conservative"
— the paper describes it as emulating a centralized token bucket faithfully; "conservative" in the
paper attaches to a different, merely proposed and unevaluated 1/N mitigation. The paper
explicitly defers adversarial/Byzantine analysis to future work, which is exactly where a
conservation-law contract would differentiate.

Sources:
- <https://github.com/youtube/doorman/blob/master/doc/design.md>
- <https://www.microsoft.com/en-us/research/wp-content/uploads/2007/01/fp076-raghavan.pdf>

### 5. Claim 4 (Prescience) - NO DIRECT HIT FOUND, and the corpus is thin here

*Confidence: medium · verification vote: 3-0 (DRL rejects predeclaration); three adjacent claims refuted*

NO DIRECT HIT FOUND — Claim 4 (Prescience: an inverted index over a committed backlog keyed by
producer-declared execution requirements, supporting demand/capacity horizons and admission debt)
was not occupied by anything in the searched corpus, and one whole line of work explicitly designs
away from it. DRL states outright that "resource providers would not require services to specify
the resource demands of each distributed component a priori", and infers demand at runtime from
arrival rates or inferred TCP flow counts instead; a full-text search of the paper for
predict/lookahead/forecast returns zero matches. Every admission system found here decides from
either reactive overload signals or the requirements of the single item at the head of the queue —
none index a backlog.

**Evidence and corrections.** This is a negative finding and must be reported with its limits. The DRL rejection is verified 3-0
against the primary paper, but its scope is narrow: what DRL declines is *services declaring per-
site aggregate demands to a provider for pricing/provisioning* — a different actor, granularity
and purpose from *producers declaring per-record execution envelopes to a scheduler*. Crucially,
PRE-DECLARED REQUIREMENTS THEMSELVES ARE HEAVILY OCCUPIED elsewhere and this finding must not be
read as "nobody predeclares": the verifier named Slurm GRES/licenses, AWS Batch consumable
resources, hierarchical admission control with rate-limit trees (US 7,813,276), and
Kubernetes/Kueue resource requests. The unoccupied element is specifically the INDEX OVER THE
COMMITTED BACKLOG plus horizon/feasibility reasoning, not requirement declaration. Three attempted
counter-hits were themselves refuted in verification (Kueue as quantitative feasibility test 0-3;
Impala planner-estimated memory as weak claim-4 form 1-2; CockroachDB as purely reactive 0-3), so
the corpus is thinner here than elsewhere — treat as absence of evidence within the searched set.

Sources:
- <https://www.microsoft.com/en-us/research/wp-content/uploads/2007/01/fp076-raghavan.pdf>
- <https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/>
- <https://impala.apache.org/docs/build3x/html/topics/impala_admission.html>
- <https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md>

### 6. Claim 5 (the composite) - NO DIRECT HIT FOUND, at medium confidence only

*Confidence: medium · verification vote: synthesized*

NO DIRECT HIT FOUND on the composite (claim 5). No system in the searched corpus combines durable
intent in a log, predeclared per-record requirements, backlog lookahead, key-ordered causal
dispatch, ownership of the dispatch boundary, per-function capacity arbitration, bottleneck
attribution, and decision lineage. The composite's individual pieces are each occupied, but by
systems at different layers with incompatible topologies — a Kubernetes controller, a database's
internal scheduler, a query engine, a rate-limit server tree, a packet limiter, and two durable-
execution runtimes.

**Evidence and corrections.** Assembled from the finding set rather than from any single verified claim, which is why confidence
is medium rather than high. The composite's defensibility rests on integration and on the two
residuals identified above (log-as-coordination-substrate; backlog indexed by declared
requirement), NOT on any individual mechanism — every mechanism examined turned out to be
occupied. Note also that search angle (d) — Flink adaptive scheduler and autoscaler, Kafka
Streams, Beam/Dataflow autoscaling, Pulsar Functions, KEDA, Kafka Share Groups KIP-932 — produced
no surviving verified claims at all, and angle (e) produced none on lookahead/predictive
scheduling research or commercial capacity governance. The composite gap is therefore unproven in
exactly the region (stream-processing elasticity, ordering-vs-concurrency decoupling) closest to
the system's own substrate.

Sources:
- <https://kueue.sigs.k8s.io/docs/concepts/>
- <https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md>
- <https://impala.apache.org/docs/build3x/html/topics/impala_admission.html>
- <https://github.com/youtube/doorman/blob/master/doc/design.md>
- <https://docs.restate.dev/services/flow-control>
- <https://docs.dbos.dev/architecture>

### 7. No-rewrite, nothing-in-the-hot-path - DIFFERENTIATOR CONFIRMED

*Confidence: high · verification vote: 3-0 (x3 Restate claims); 2-1 (DBOS), one DBOS rewrite claim refuted 1-2*

DIFFERENTIATOR CONFIRMED — the durable-execution runtimes (Restate, DBOS) require the application
to be rewritten into their workflow/step model and keep a server or database in the hot path for
every dispatch decision, which is the axis on which "sits under the existing dispatch boundary, no
rewrite" genuinely differs. Restate's flow control is configured through a centralised cluster-
wide rule book (`restate rules` CLI), enforced server-side by vqueues on partition processors,
with the rule book persisted in cluster metadata — rules pushed down, not credits delegated out.
DBOS is genuinely an embedded library with no orchestration server, but its queue concurrency is
enforced by every worker hitting the shared system database on each dequeue.

**Evidence and corrections.** Restate: "Concurrency limits are defined in a cluster-wide rule book"; enforcement via vqueues +
scheduler + scoped ingress, observable in `sys_vqueues`, `sys_scheduler`, `sys_user_limits`;
`applied_rule_book_version` versioned per partition. Restate's own docs draw the same in-handler-
vs-pre-execution distinction the claim relies on: the rate-limiting guide says "Restate doesn't
have built-in rate limiting functionality" and gives a user-code `await limiter.wait()` token-
bucket pattern that requires a server round-trip per decision — a per-call permit server,
reinforcing rather than refuting. DBOS: verified embedded-library packaging, but per-dequeue FOR
UPDATE SKIP LOCKED against the system DB is a central permit store on the hot path; only static
`worker_concurrency` is local. Restate additionally frames concurrency limits as protecting "a
downstream service, database, or third-party API" and scopes are literally named strings — the
same PROBLEM framing as "named shared resources own capacity" — but the mechanism is an operator-
set, runtime-mutable, non-adaptive concurrency bound, not renewable divisible credits. ROADMAP
RISK: Restate names throttling/rate limits, invocation priorities and finite backlog limits as
planned follow-ups; if those ship, re-check this finding, though enforcement is cluster-side by
design.

Sources:
- <https://docs.restate.dev/services/flow-control>
- <https://github.com/restatedev/restate/blob/main/release-notes/v1.7.0.md>
- <https://docs.restate.dev/guides/rate-limiting>
- <https://docs.dbos.dev/architecture>
- <https://docs.dbos.dev/python/reference/queues>
- <https://www.dbos.dev/blog/making-postgres-queues-scale>

### 8. Kueue is the primary comparator for claim 1 - SCOPE CORRECTION

*Confidence: high · verification vote: 3-0 (x2)*

SCOPE CORRECTION — Kueue is the nearest structural analogue to the admission half of the design
and should be treated as the primary comparator for claim 1, but it sits at a coarser granularity
and a different substrate: its unit is a Workload (Job/JobSet/RayJob/Pod group) submitted to the
API server, admission is all-or-nothing, it requires a Kubernetes cluster and a leader-elected
controller in the loop, it reads no external log, and it has no notion of records already
committed elsewhere.

**Evidence and corrections.** "Admission is the process of allowing a Workload to start (Pods to be created)"; "Queueing is the
state of a Workload since the time it is created until Kueue admits it on a ClusterQueue"; "Kueue
automatically manages the Job's suspension via webhook and decides when it's the best time to
start the Job", with documented event sequence Job suspended -> workload admitted -> Job resumed,
mechanically verifiable via the Kubernetes Job `spec.suspend` API. Actively developed
(v1beta1/v1beta2, Hierarchical Cohorts added since). One verified claim about Kueue was REFUTED
0-3 — that admission is decided by a quantitative feasibility test against declared resource
requests in a way matching producer-declared execution envelopes — so do not lean on Kueue as a
claim-4 comparator.

Sources:
- <https://kueue.sigs.k8s.io/docs/concepts/>
- <https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/>
- <https://kueue.sigs.k8s.io/docs/tasks/run/jobs/>
- <https://kueue.sigs.k8s.io/docs/concepts/multikueue/>

## Claims that FAILED verification

Recorded because a killed claim is a result, and because several of these would otherwise be
repeated by the next person who looks. A vote of `0-3` means all three verifiers refuted it.

1. **vote 0-3** - Admission is decided by a quantitative feasibility test against declared resource requests, so
resource requirements must be predeclared before dispatch — the same shape as Hasten's producer-
declared execution envelopes, though only for the item at the head of the queue, with no index
over a backlog and no lookahead/horizon reasoning described.

   Source: <https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/>

2. **vote 0-3** - CockroachDB's admission control is explicitly node-level, not cluster-global: it is scoped to per-
node bottleneck resources (CPU and store write IO) on the reasoning that aggregate provisioning
can be adequate while individual stateful nodes develop hotspots. It therefore does NOT implement
Hasten's claim 3 split (globally coordinated capacity, locally decided dispatch) — there is no
global coordination half at all.

   Source: <https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md>

3. **vote 0-3** - CockroachDB uses two local permit primitives — slots (occupied while KV work executes, released on
completion) and tokens (consumed, for SQL-KV and SQL-SQL) — but both are granted from node-local
capacity; nothing in the design delegates divisible capacity leases from a named shared resource
to remote or embedded instances, so it does not satisfy Hasten's claim 2 (delegated capacity
leases over a log).

   Source: <https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md>

4. **vote 0-3** - Admission decisions come from REACTIVE overload signals measured after the fact — the normalized
runnable goroutine count for CPU, and level-0 sub-level/file count thresholds for LSM health —
with no use of per-item predeclared resource requirements and no lookahead over a pending backlog.
This refutes any prior-art claim on Hasten's claim 4 (requirement-aware lookahead admission).

   Source: <https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md>

5. **vote 1-2** - Impala admits work using *predicted* resource requirements computed before execution (memory
estimates derived from table statistics), not observed usage - a requirement-aware admission
decision, which is the weaker per-query form of claim 4's requirement-declared lookahead. Notably
the estimate is derived by the planner, not declared by the producer of the work, and it covers
only the query being admitted, not a backlog horizon.

   Source: <https://impala.apache.org/docs/build3x/html/topics/impala_admission.html>

6. **vote 0-3** - Distributed Rate Limiting (DRL) enforces a single global rate limit across N independent
enforcement points with NO central permit server and NO coordination cluster: limiters are peer-
to-peer, functionally identical, and exchange only periodic demand estimates via a UDP
gossip/epidemic protocol. This directly occupies Hasten's claim 2 (delegated capacity, no cluster,
no per-call permit server) and claim 3 (global intelligence, local execution) at the network
layer, 19 years earlier.

   Source: <https://www.microsoft.com/en-us/research/wp-content/uploads/2007/01/fp076-raghavan.pdf>

7. **vote 0-3** - Gubernator is a daemon cluster, not an embedded library: rate limit ownership is assigned per key
by consistent hashing across peers, so every limit has a single owning peer process that must be
reached to decide. This occupies the distributed rate limiter without a central service space but
NOT Hasten's claim 2 (embedded, not cluster) — it removes the single central permit server by
sharding it, not by delegating capacity into the application process.

   Source: <https://github.com/mailgun/gubernator/blob/master/docs/architecture.md>

8. **vote 0-3** - Gubernator sits in the per-call hot path — decisions are synchronous RPCs to the owning peer,
mitigated only by micro-batching, not by delegating divisible capacity that is then spent locally.
This is the decides-per-call side of the line Hasten draws, and confirms no existing lease/credit
delegation to embedded instances here.

   Source: <https://github.com/mailgun/gubernator/blob/master/docs/architecture.md>

9. **vote 1-2** - Gubernator's GLOBAL behavior is the closest adjacent construct to Hasten's claim 3 (global
intelligence, local execution): the receiving peer answers immediately from local state while hits
are asynchronously batched to the owner, which then propagates status back to peers. But what is
propagated is replicated *counters*, not delegated divisible capacity leases with a conservation-
law contract — the local half has no independently held allowance, only a stale copy of a global
count.

   Source: <https://github.com/mailgun/gubernator/blob/master/docs/architecture.md>

10. **vote 1-2** - DBOS requires the application to be rewritten into its workflow model: work must be expressed as
annotated workflows and steps to gain durability and queueing. This refutes any claim that DBOS is
a drop-in execution layer over existing consumers, and it is the axis on which Hasten's 'no
rewrite, sits under the dispatch boundary' claim differs.

   Source: <https://docs.dbos.dev/architecture>

## Caveats, verbatim from the sweep

SEARCH COVERAGE IS UNEVEN, AND THE GAPS SIT WHERE IT MATTERS MOST. Search angles (a) admission
control and (b) distributed rate limiting are well covered by primary sources. Angle (c) durable
execution is covered only for Restate and DBOS — Temporal, Cadence, Conductor, Inngest and Azure
Durable Functions produced no surviving verified claims. Angle (d) stream-processing elasticity
produced NOTHING at all: no verified claim touches Flink's reactive/adaptive scheduler or
autoscaler, Kafka Streams, Beam/Dataflow, Pulsar Functions, KEDA, or Kafka Share Groups (KIP-932).
Angle (e) produced no research-literature or commercial capacity-governance hits. Since (d) is the
closest adjacent domain to a Kafka-resident runtime, treating claims 4 and 5 as "gaps" is
premature; they are currently "not found", which is a much weaker statement.

REFUTED CLAIMS THIN OUT SOME SYSTEMS' POSITIONS. Four CockroachDB claims and three Gubernator
claims failed verification (0-3 or 1-2). The surviving CockroachDB evidence supports only the
admission-as-pre-execution-gate definition; do NOT rely on the register for CockroachDB's node-
local scoping, its slots/tokens model, or its reactive-signal design, and do not rely on it for
Gubernator's architecture at all. Gubernator therefore needs re-checking independently if it
matters to the write-up.

TWO CITATION DEFECTS MUST BE FIXED BEFORE ANYTHING IS PUBLISHED. (1) The DRL "instantaneous
decisions about a packet's fate" sentence describes the CENTRALIZED reference token bucket, not
FPS — re-cite the local-hot-path point on the `FPS-HANDLE-PACKET` pseudocode and the peer-to-
peer/independent-operation passage instead. (2) The DRL numbers span two venues: <3% overhead and
490 limiters are a 7-machine local emulated testbed; only 0.47% control loss is PlanetLab. Also
drop "strictly conservative" as a description of GTB — that is not the paper's word for it.

WORDING PRECISION. Kueue's cohort borrowing is centrally arbitrated per scheduling cycle, so
"borrowable, cap-bounded capacity across named quota pools" is accurate and "delegated" is not.
Impala's statestored is a centralized pub/sub daemon, not peer-to-peer gossip, and its "global
intelligence" is aggregate counters against statically configured pool limits — no adaptive
allocation, no lease division. Restate's rule book is distributed to partition processors, so
"centralised" means cluster-owned and cluster-enforced, not single-node.

TIME SENSITIVITY. Restate flow control is v1.7, experimental, opt-in
(`experimental_enable_vqueues`), APIs documented as subject to change, and its roadmap explicitly
names throttling/rate limits, invocation priorities and finite backlog limits — the last of which
edges toward claim 4 and should be re-checked each release. Kueue is actively developed
(Hierarchical Cohorts is new). Doorman is archived read-only since 2024-11-29 and self-described
alpha; this does not weaken it as prior art (prior art does not expire) but means it is an
occupied claim, not a live competitor. Cloudera ships an optional centralized Impala Admission
Daemon, so "embedded, decentralized" is the Apache default rather than universal.

SOURCE QUALITY IS GENERALLY GOOD. Every load-bearing finding rests on primary material — in-repo
design docs, source code, ASF and kubernetes-sigs reference documentation, vendor release notes,
and a peer-reviewed SIGCOMM paper verified by local pdftotext extraction. One finding (the
composite, claim 5) is synthesized across findings rather than directly verified, and is marked
medium accordingly. No finding rests on marketing material, benchmarks, or forum posts.

## Open questions the sweep could not close

- Does anything in the stream-processing world already decouple ordering from concurrency and
schedule against a committed backlog? Kafka Share Groups (KIP-932), Flink's adaptive scheduler and
autoscaler, Beam/Dataflow autoscaling and KEDA were all in the brief and produced zero verified
claims. This is the nearest-neighbour domain and the largest hole in the evidence — the composite
gap cannot be asserted until it is searched.
- Has anyone transported capacity leases or quota over a durable log rather than a dedicated
service? The residual of claim 2 is a substrate choice, and the corpus checked four alternatives
(server tree, UDP gossip, Kubernetes API server, Postgres) without finding a log-based one. Worth
checking Kafka quota/throttling internals, service-mesh RLS implementations with log-backed state,
and any Raft/log-backed token-broker designs before calling it unoccupied.
- Is there academic prior art on backlog-aware or requirement-aware lookahead scheduling — resource-
requirement-indexed queues, horizon/feasibility admission, admission debt? Requirement declaration
itself is heavily occupied (Slurm GRES, AWS Batch consumable resources, Kueue resource requests,
US 7,813,276), so the novelty rests entirely on indexing the backlog by those requirements, and no
scheduling-theory literature was searched.
- Does the conservation-law contract (failure wastes capacity but never violates it) have a formal
precedent? DRL explicitly defers adversarial/Byzantine analysis to future work and Doorman
explicitly disclaims enforcement, so both leave the strict-safety corner open — but
escrow/reservation protocols and distributed-counter literature were not searched and may already
occupy it.
- What is Gubernator's actual architecture and hot-path model? All three claims about it failed
verification, so the register carries no reliable position on the closest 'distributed rate
limiter without a central service' comparator.

## Every source consulted

- <{'url': 'https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/', 'quality': 'primary', 'angle': 'Admission control as a pre-execution scheduling state', 'claimCount': 5}>
- <{'url': 'https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md', 'quality': 'primary', 'angle': 'Admission control as a pre-execution scheduling state', 'claimCount': 5}>
- <{'url': 'https://impala.apache.org/docs/build3x/html/topics/impala_admission.html', 'quality': 'primary', 'angle': 'Admission control as a pre-execution scheduling state', 'claimCount': 5}>
- <{'url': 'https://github.com/youtube/doorman/blob/master/doc/design.md', 'quality': 'primary', 'angle': 'Delegated capacity leases / token brokers without a central permit service', 'claimCount': 5}>
- <{'url': 'https://engineering.grab.com/quotas-service', 'quality': 'blog', 'angle': 'Delegated capacity leases / token brokers without a central permit service', 'claimCount': 5}>
- <{'url': 'https://www.microsoft.com/en-us/research/wp-content/uploads/2007/01/fp076-raghavan.pdf', 'quality': 'primary', 'angle': 'Delegated capacity leases / token brokers without a central permit service', 'claimCount': 5}>
- <{'url': 'https://github.com/mailgun/gubernator/blob/master/docs/architecture.md', 'quality': 'primary', 'angle': 'Delegated capacity leases / token brokers without a central permit service', 'claimCount': 5}>
- <{'url': 'https://docs.restate.dev/services/flow-control', 'quality': 'primary', 'angle': 'Durable execution runtimes: do they schedule, and do they demand a rewrite', 'claimCount': 5}>
- <{'url': 'https://docs.dbos.dev/architecture', 'quality': 'primary', 'angle': 'Durable execution runtimes: do they schedule, and do they demand a rewrite', 'claimCount': 5}>
- <{'url': 'https://temporal.io/blog/rate-limit-downstream-apis', 'quality': 'blog', 'angle': 'Durable execution runtimes: do they schedule, and do they demand a rewrite', 'claimCount': 5}>
- <{'url': 'https://journal.resonatehq.io/p/from-where-do-deterministic-constraints', 'quality': 'blog', 'angle': 'Durable execution runtimes: do they schedule, and do they demand a rewrite', 'claimCount': 5}>
- <{'url': 'https://www.restate.dev/blog/distributed-restate-a-first-look', 'quality': 'blog', 'angle': 'Durable execution runtimes: do they schedule, and do they demand a rewrite', 'claimCount': 5}>
- <{'url': 'https://docs.dbos.dev/python/reference/queues', 'quality': 'primary', 'angle': 'Durable execution runtimes: do they schedule, and do they demand a rewrite', 'claimCount': 5}>
- <{'url': 'https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka', 'quality': 'primary', 'angle': 'Stream elasticity and ordering decoupled from partition count', 'claimCount': 5}>
- <{'url': 'https://github.com/apache/pulsar/wiki/PIP-34:-Add-new-subscribe-type-Key_shared', 'quality': 'primary', 'angle': 'Stream elasticity and ordering decoupled from partition count', 'claimCount': 5}>
- <{'url': 'https://cwiki.apache.org/confluence/display/FLINK/FLIP-271%3A+Autoscaling', 'quality': 'primary', 'angle': 'Stream elasticity and ordering decoupled from partition count', 'claimCount': 5}>
- <{'url': 'https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/', 'quality': 'primary', 'angle': 'Stream elasticity and ordering decoupled from partition count', 'claimCount': 5}>
- <{'url': 'https://cloud.google.com/dataflow/docs/guides/tune-horizontal-autoscaling', 'quality': 'primary', 'angle': 'Stream elasticity and ordering decoupled from partition count', 'claimCount': 5}>
- <{'url': 'https://keda.sh/docs/2.20/scalers/apache-kafka-go/', 'quality': 'primary', 'angle': 'Stream elasticity and ordering decoupled from partition count', 'claimCount': 5}>
- <{'url': 'https://dl.acm.org/doi/10.1145/2901318.2901355', 'quality': 'primary', 'angle': 'Lookahead / requirement-aware scheduling over a committed backlog', 'claimCount': 5}>
- <{'url': 'https://www.usenix.org/system/files/conference/osdi16/osdi16-jyothi.pdf', 'quality': 'primary', 'angle': 'Lookahead / requirement-aware scheduling over a committed backlog', 'claimCount': 5}>
- <{'url': 'https://arxiv.org/abs/2205.02895', 'quality': 'primary', 'angle': 'Lookahead / requirement-aware scheduling over a committed backlog', 'claimCount': 5}>

## Run shape

Five angles, adversarial 3-vote verification. The run's own counters: `angles`=5, `sourcesFetched`=22, `claimsExtracted`=110, `claimsVerified`=25, `confirmed`=15, `killed`=10, `unverified`=0, `afterSynthesis`=8, `urlDupes`=0, `budgetDropped`=8, `agentCalls`=104.
