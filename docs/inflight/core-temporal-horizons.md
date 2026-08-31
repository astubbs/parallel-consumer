# Demand Horizon x Capacity Horizon: feasibility, debt, and the death of queue depth

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - the temporal half of the admission model; no preemption, by decision -->

From the 2026-08-30 exchange (model root:
[`core-admission-scheduling-model.md`](core-admission-scheduling-model.md)).

Prescience mostly meant foresight over *work*. Optimal scheduling needs both halves: the **Demand
Horizon** (what committed and scheduled work will require, when - including demand projected
through topologies and from cron registrations,
[`core-scheduled-intent.md`](core-scheduled-intent.md)) and the **Capacity Horizon** (what
resources will be available, when: token replenishment, completions, maintenance windows ending,
reserved capacity arriving - so resource contracts should describe *availability curves*, not
scalar limits). The Golden Path is the best match between the two.

What falls out:

- **Fit-to-gap scheduling, without preemption.** Preemption is rejected outright - dispatched
  work keeps its ownership. Instead: GPU frees in 40ms, DB is free now - do not hold DB idle for
  40ms; admit DB-only work whose predicted duration fits the gap, and avoid the 2-second DB job
  that would idle the GPU later. Work becomes `resource vector x expected duration x eligibility
  window`; non-preemptive reservation planning over a short temporal horizon.
- **Eligibility time is not validity deadline** (2026-08-31, via JMS expiry): "do not run
  before X" and "no longer worth running after Y" are different predicates with different
  terminal outcomes - expiry is a policy completion (EXPIRED), not a failure
  ([`core-work-identity-model.md`](core-work-identity-model.md) owns the disposition vocabulary).
- **Feasibility, not just eligibility.** Deadline 12:00, needs 5s of R, R fenced until 12:03:
  the work is not "waiting" - it is **INFEASIBLE now**, and policy can act immediately
  (escalate, compensate, relax, reroute) instead of discovering the SLO miss after the fact.
  Distributed systems are terrible at telling "not yet" from "never"; this model can. Its
  constructive twin: **admission promises** ("admissible in ~4.2s").
- **Admission debt.** Not queue depth: `incoming demand - sustainable admission capacity`, per
  resource/tenant/causal path, best expressed in time ("this resource accrues 15 seconds of
  execution debt per minute"). Distinguishes transient burst / finite backlog with a clear-time /
  structurally growing debt / backlog that cannot progress at all - the states queue-depth
  autoscaling conflates.
- **Counterfactual capacity value.** "What work would I admit right now if I had more X?" - the
  marginal utility of infrastructure computed from actual committed work, which is the firm
  foundation under the FinOps claims in
  [`core-fleet-capacity-coordination.md`](core-fleet-capacity-coordination.md). Its cousin:
  **counterfactual load testing** - "if admission opened fully, what binds first?" answered from
  the real future work population, before executing it.
- **Aging becomes fundamental.** Destroying FIFO as execution policy destroys FIFO's accidental
  fairness, so starvation protection must be explicit and low in the scheduler: waiting itself
  accrues scheduling claim, with priority overriding deliberately rather than starvation emerging
  silently.
- **The dashboard stops leading with lag.** 12.4m outstanding is not a red number when 11.8m are
  intentionally waiting: lead with "progressing normally" and the breakdown (future-eligible /
  capacity-constrained / **unexpectedly stalled** / infeasible) - the
  [`web-control-plane.md`](web-control-plane.md) true-lag instrument, completed. A record
  existing does not mean a function should be running.
