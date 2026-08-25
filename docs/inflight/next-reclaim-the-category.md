# Goal: own the parallel-consumption category outright

<!-- inflight-type: register -->

**Stated by the owner, 2026-08-21.** Not a plan - a standing goal that other plans should be measured
against. It exists because a competitor appearing made the ambition explicit for the first time, and
an ambition nobody wrote down is one nobody can aim at.

## The goal, in the owner's terms

Reclaim market dominance in parallel Kafka consumption, and make Parallel Consumer so available and
so easy that considering an alternative stops making sense:

- **In every ecosystem.** Not JVM-only. The language proxy (astubbs#242) is the mechanism.
- **Easy.** Easy to adopt, easy to configure, easy to get right by default.
- **Adaptable.** Ordering modes, retries, batching, integrations - the workload shapes the library,
  not the reverse.
- **Full of make-life-easy features**, because that is what accumulates into an unassailable
  position.
- **Solving the problem at the deepest level**, so a user genuinely stops thinking about parallel
  consumption - where every other system still requires them to.

**And the ambition, stated plainly:** be the answer people reach for without shopping around - so
complete, so easy to adopt and so obviously well-kept that evaluating an alternative stops feeling
like time well spent. Won by building, never by attacking anyone else's work.

## What this changes about how work is chosen

Three consequences, and they cut against some current sequencing:

1. **Breadth and accessibility outrank engine speed.** This session measured the engine gap at 1.78x
   with an instant handler and **1.04x at 100ms** - a realistic per-record latency. Meanwhile a single
   configuration setting swung the same build by 1.9x. **Engine microseconds are the smallest term in
   the goal**, and should be funded accordingly.
2. **Credibility is a first-class workstream, not documentation debt.** See below.
3. **The differentiator is funded, and the two records disagree about it.** Adaptive concurrency is
   the one capability no competitor has and the one that most directly delivers "you stop thinking
   about it". `STRATEGY.md` carries it as a named track with priority raised 2026-08-18;
   `roadmap.yaml` still lists it under backlog. It is being built - only the timing is open. See
   the correction at the foot of this note and [`next-auto-scaling.md`](next-auto-scaling.md).

## The credibility problem, stated plainly

**Owner, 2026-08-21:** *"the credibility thing is a pain that hurts because Parallel Consumer really
has been hurt by its unreliability. That's why v6 is going to be such an important release. Need to
reclaim that trust."*

This is the honest centre of the whole analysis, and it is more important than any feature gap. A
library in this position is not chosen on feature count - it is chosen on whether an engineer
believes it will hold up at 3am. Three things follow:

- **v6 is a trust release before it is a feature release.** `roadmap.yaml`'s `known-defects-cleared`
  entry - *"every known critical defect resolved, each with a guard that would catch its return"* - is
  the load-bearing item. The guard clause is the part that rebuilds trust, because it is what
  converts "we fixed it" into "it cannot come back".
- **Evidence practices are the mechanism, not marketing.** Formal verification, a per-message
  overhead figure, asserted performance budgets, a published chaos matrix. A three-month-old
  competitor currently has better *published evidence* than a library with years in production. See
  [`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md)
  and [`next-performance-regression-testing.md`](next-performance-regression-testing.md).
- **Documentation is part of reliability, not adjacent to it.** *"Having all the docs in the README is
  kinda shit."* A 1,500-line README is a trust signal in the wrong direction. The docs site
  ([`parked-docs-site.md`](parked-docs-site.md), astubbs#208) and the promotional site
  (dropped 2026-08-22 - see git history for `next-landing-page.md`) are not cosmetic - **an evaluator who
  cannot find out what the library does concludes nobody is looking after it.**

## Competitive advantages currently unstated - including one nobody has mentioned

The recurring finding of this whole analysis is that PC's advantages are real and invisible. Beyond
the offset map, retries, ordering modes, produce-and-commit and multi-topic already recorded
elsewhere:

**Backwards compatibility, which had not been counted at all.** Two dimensions:

- **JVM reach.** PC targets **Java 8 bytecode**. The competitor's JVM build requires **JDK 21+**. A
  large amount of production Kafka runs on Java 8 and 11, and for those users the comparison is not
  close - it is unavailable. This is a *feature* of PC's build discipline that has always been
  treated as a constraint.
- **Kafka client and broker reach.** PC takes the user's own `Consumer` and `Producer`, so it
  inherits whatever client version they already run and works against whatever brokers that client
  supports. **Researched 2026-08-21 and the contrast is sharper than expected:**

  | | PC | llingr |
  |---|---|---|
  | JVM floor | **Java 8 bytecode** | **JDK 21+**, no older path mentioned anywhere |
  | Kafka client | **whatever the user supplies** | pinned per adapter - franz-go 1.21.5, or confluent-kafka-go 2.14.1 |
  | Broker floor | whatever the user's client supports | **not stated anywhere.** franz-go's README claims Kafka 0.8.0+, but both adapters **reject `read_uncommitted`**, which requires transactions - a hard **0.11+** floor that contradicts their own documentation |
  | Go floor (for the Go engine) | n/a | Go 1.24+, and **1.25** for the franz adapter |
  | Compatibility policy | - | **none published** - no version matrix, no support window, no stability statement beyond one line: *"interface changes are rare and deliberate"* |
  | Version | 0.6.0.0 heading for 1.0 | **everything pre-1.0** - engine v0.12.2, JVM build v0.1.0 |

  Two things follow. **The JDK 21 floor is the sharper edge** - a great deal of production Kafka runs
  on Java 8 and 11, and for those users there is nothing to evaluate. And **"no published
  compatibility policy" is itself a differentiator we could claim** if we publish one: a support
  window and a version matrix are cheap to write and are exactly what a cautious adopter looks for.

  **Positioned per the standing rule:** *"Runs on Java 8 and up, against the Kafka client and broker
  you already have."*

**Half of this now has a feature record and half does not:**

- **Java reach: done.** `docs/features/java-compatibility.yaml` existed already; its summary now leads
  with the claim - *"Runs on Java 8 and up... an application on an older JVM can adopt Parallel
  Consumer without upgrading its runtime first"* - rather than with the mechanism of choosing an
  artifact.
- **Client and broker reach: no feature record exists.** The property is that PC takes the user's own
  `Consumer` and `Producer`, so it inherits their client version and whatever brokers that client
  supports, and adds no client dependency of its own. **That needs authoring**, and it should state
  the boundary honestly too: PC requires the consumer to be configured with auto-commit disabled and
  is documented against particular client behaviours, so "any client version" is not unlimited.
  Deliberately not authored here against a schema this session has not read properly - a wrong
  feature record is worse than a missing one.

Positioned per the standing rule: *"Runs on Java 8 and up, with the Kafka client you already have"* -
never as what a competitor cannot do.

## Related

- [`market-analysis-llingr.md`](market-analysis-llingr.md) - the analysis this came from.
- The dropped landing-page note (git history) and
  [`parked-docs-site.md`](parked-docs-site.md) - the accessibility half.
- [`branch-language-proxy.md`](branch-language-proxy.md) - the every-ecosystem half.
- [`next-auto-scaling.md`](next-auto-scaling.md) - the differentiator that most directly serves
  "stop thinking about it".
- `roadmap.yaml` - where these have to become entries a reader can watch finish, rather than
  intentions.

## What this analysis does to `STRATEGY.md`

`STRATEGY.md` is a **claims** document that nothing tests, and its own rule is that work falsifying a
claim must update it. This analysis produced measurements that bear on three of its tracks. **None of
the claims is falsified; two now have evidence they lacked, and one needs a qualifier.**

**Performance track** - *"The client-side bet only pays if the client is fast - a sub-broker that adds
latency has no reason to exist."*

That claim is now measurable and partly uncomfortable. PC's per-record cost at concurrency 1 is
**118µs**, against 13.2µs for a competing engine and 8.9µs for a bare Go Kafka client. The claim is
not falsified - at any realistic per-record latency the engine gap is a few percent, and PC's
throughput rises monotonically with concurrency - but *"the client is fast"* is currently an assertion
with a number behind it that does not flatter it. **Suggested qualifier**: state that the bet is about
*throughput per instance under real workloads*, not per-record overhead, and note that the overhead
figure is bounded jointly with the Java client until a control arm separates them.

**Self-tuning track** - *"runtime-discovered scaling is the capability nothing else in the ecosystem
offers."*

**Now verified rather than assumed**, which is a strengthening. The closest competitor explicitly
disclaims autoscaling (*"The engine has no opinion on Kubernetes timing, autoscaling..."*), its
primary dial is a manual `ConcurrentKeys`, and its own documentation tells users to size that against
their database connection pool - which is precisely the runtime property this track argues cannot be
known at configuration time. Worth citing the evidence rather than leaving it as a claim.

**Other runtimes track** - *"Parallel Consumer is not current with Kafka today"* is recorded there as
a qualifier on the architectural claim.

Two things to add on the same axis, from
[`next-llingr-questions-and-answers.md`](next-llingr-questions-and-answers.md): PC's **Java 8
bytecode floor** is a reach advantage that the strategy does not currently claim at all, and the
absence of a **published compatibility policy** anywhere in this space means publishing one is cheap
differentiation.

**Recommendation: three small edits, not a rewrite** - a qualifier on the performance claim, evidence
attached to the self-tuning claim, and compatibility reach added as an advantage. Per this repo's
convention that is `pr-strategy-doc-merge-triggers.md`'s business, and this note is the trigger.

## Correction: adaptive concurrency is being built

**Owner, 2026-08-21:** it *is* getting built - only the *when* is undecided. Earlier framing in these
notes ("sits in the backlog marked undecided", "the roadmap does not fund the differentiator")
understated it, and the roadmap entry has since been updated to record the two 2020 prototypes.

`STRATEGY.md` already carries it as a **named track with priority raised 2026-08-18**, which is a
stronger statement of intent than `roadmap.yaml`'s backlog placement suggests. **The two documents
disagree**, and the strategy is the one that reflects the decision. Worth reconciling: if it is a
funded track, it should be a roadmap entry a reader can watch finish rather than a backlog line.
