# Sound bites

Compressed product intent: one-line formulations that each carry a design decision, kept here so
docs, talks, PR bodies and agent prompts reuse the same line instead of re-deriving a weaker one.
Like `CONCEPTS.md` but for sentences rather than vocabulary - a bite earns its place by conveying
a lot in very little, and gets retired when the decision behind it changes. Provenance: seeded
from the 2026-08 strategy conversations (the breakdown lives in `docs/inflight/`, the preserved
handoff documents under `docs/ideation/`). Product and subsystem names are working names; the
bites travel with whatever the names become.

## The thesis

- Ownership and execution are independent.
- The broker schedules partitions; PC schedules keys.
- Kafka provides the log. Hasten provides the runtime.
- Parallel Consumer discovered that Kafka ownership and execution do not have to be the same
  thing. Hasten follows that observation to its logical conclusion.
- Physical log position is provenance. Semantic position determines execution.
- The ordering domain is the scheduling granularity that actually matters.
- Stop using your data architecture as your thread pool.
- We tried to add global rate limiting to Parallel Consumer and discovered the correct solution
  was a distributed execution scheduler.

## The admission model

- Waiting is a scheduling state, not an execution state.
- Never dispatch work already known unable to make useful progress.
- Among admissible work, dispatch what best advances the system.
- Ordering says whether work *may* execute; adaptive concurrency says whether it *should*;
  resource contracts say whether it is *allowed*. Same admission point.
- Know early. Commit late. Execute immediately.
- Don't backpressure execution after saturation. Shape admission before saturation.
- A record existing does not mean a function should be running.
- Many distributed-systems mechanisms are different ways of explaining why outstanding work is
  not currently admissible.
- The failure queue is the original log. A failed record never leaves its causal position.
- Don't DLQ on failure. DLQ only when you deliberately abandon the original execution position.
- A timer is work whose eligibility condition is time.
- No unexplained waiting.

## Resources and coordination

- One new abstraction: shared execution resources. A rather ridiculous number of features fall
  out of it.
- Hasten doesn't synchronize every operation. It synchronizes ownership of capacity, then spends
  capacity locally.
- Globally coordinated, locally consumed.
- Failure wastes capacity; it never violates the constraint.
- Knowledge is global. Authority is sharded. Execution is local.
- The scheduler is sharded by the work itself. Scheduler failover follows work failover. Your
  applications are the scheduler.
- Consumer groups coordinate ownership of work; resource groups coordinate ownership of capacity.
- Distributed backpressure across applications that don't even know one another.
- If Python gets a Stripe 429, Java and Go can back off too.
- Kafka quotas govern Kafka resources; this governs application execution resources.
- Fence the resource, not the applications. Maintenance is a temporary resource constraint.
- Don't acquire resources in your function. Declare them on your function.
- Tell Hasten what your work needs. Don't write code that waits for it.

## Knowledge and foresight

- Kafka lets you read the log by physical position. Prescience indexes the log by execution
  meaning.
- Prescience does not require caching Kafka's data. It requires caching Kafka work's decision
  surface.
- The scheduling metadata is the Spice.
- Memory buys future visibility.
- Prescience tells us what work exists. Demand Horizon projects what it will require. Capacity
  Horizon tells us what will be available. Admission feasibility tells us what can happen when.
  Admission debt tells us where demand is outrunning the ability to proceed.
- Lineage asks what caused this. Why Wait asks what prevents this. Prescience asks what should
  happen next.
- Most infrastructure observes resources; Hasten observes opportunity.
- The scheduler's decision state is the observability.
- A trace tells you what executed. Decision lineage tells you what could have executed, what did,
  why, and what it caused.
- The engine did not infer that concurrency 300 was worse. It tried 300.
- Hasten should understand what work needs, not what the work means.

## Scaling and economics

- Scale the binding constraint, not the visible symptom.
- Scale-out is the consequence of failing to satisfy profitable internal demand.
- Bottleneck-directed autoscaling.
- Configure outcomes, not concurrency.
- It knows when the bottleneck is infrastructure - and when it's the keys.
- Do not scale machines because Kafka happened to hash badly.
- Every team pays for the same uncertainty separately; a global view reserves it once and lends
  it out.
- Hasten continuously moves infrastructure spend toward the current global constraint.
- Cloud cost tools know what infrastructure costs. Hasten knows what additional infrastructure
  would actually accomplish.
- Choose partitions for Kafka. Let PC choose parallelism for your application.
- "How many partitions will you need in three years?" Wrong question.

## Product and adoption

- Programming model: yours. Execution model: Hasten.
- Kafka Streams is the programming model. Hasten is the execution model.
- Keep your code. Replace the runtime underneath it. (A design pressure, not an absolute
  promise.)
- You do you. Just change the import.
- Change your import. We'll discover everything we safely can. Tell us only what we cannot know.
  Give us control only when you're ready.
- One implementation of intelligence. Many implementations of ergonomics.
- Build one primitive x expose through N language bindings = N product surfaces without N
  distributed implementations.
- Internal machinery becomes customer features whenever exposing it is cheaper than building a
  separate subsystem.
- Compatibility is a distribution strategy.
- Don't replace the specialist system. Remove the generic distributed execution machinery it
  shouldn't have needed to build.
- As Kafka gains primitives, Hasten gets smaller, not less useful.
- A globally coordinated runtime with no runtime cluster. The optimizer is distributed through
  the applications it optimizes.
- Locally: a library. Globally: the company's execution operating system.
- The real competitor is manual concurrency management.
- Nile controls supply. Hasten controls demand.
- No feature demo. One company.
- No subsystem gets completed for its own sake.
- Do not ask first how much code a feature requires. Ask how much new conceptual complexity it
  introduces.
- Istio meshes network connections. Hasten meshes execution.

## The name

- Hasten - Why wait?
- Something was going to happen. Hasten helps it happen sooner when there is no good reason to
  wait.
- Find the things that genuinely have to wait. And everything else? Hasten.
