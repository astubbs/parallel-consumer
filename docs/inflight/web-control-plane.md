# The dashboard's third layer: Observe, Explain, Act - and four cheap instruments first

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - Observe/Explain extend astubbs#268; Act changes its security posture and waits on engine surfaces -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (first review's breakdown:
[`core-engine-thesis.md`](core-engine-thesis.md)). Extends the embedded dashboard (astubbs#268,
[`web-gui-observability-ideas.md`](web-gui-observability-ideas.md)); the model it surfaces is
[`core-execution-opportunity-model.md`](core-execution-opportunity-model.md).

## The product shape

Three layers: **Observe** (what is happening), **Explain** (why the engine believes it is
happening), **Act** (what to do about it) - and the second layer is what makes the third safe to
trust. Every action sits beside the evidence that justifies it: the blocked-frontier panel
(committed / frontier / highest-completed, the blocking record's key, attempt count and age, the
count of later records already done) carries `DLQ record / retry now / pause key`; the
per-function panel (current concurrency, discovered useful range, target vs observed p99) carries
the SLO and contract editors; the scale recommendation shows its own regime checklist before
`request scale-out`. No separate analytics brain - the UI surfaces the controller's internal
model, which is what keeps Explain honest.

**Every manual intervention expires.** An override is a bound plus a duration plus a reason
("cap at 40, 30 minutes, downstream incident"), and control returns to the adaptive system when it
lapses - applies equally to concurrency caps, scale-out holds, contract caps, SLO relaxations and
priority boosts. This kills the classic failure where an emergency override silently becomes
permanent configuration, and the reason field gives the incident review its timeline for free.

## Four instruments buildable early, because the engine already knows the answer

1. **The concurrency-gap explainer** - a recurring support question in disguise: configured 1,000,
   observing 137. Decompose the gap by gate (ordering -611, admitted work -142, downstream -80,
   CPU -30 -> limiting factor: KEY PARALLELISM), and under the adaptive controller show the probe
   that proves the current target is the optimum ("184 -> 220: throughput +0.8%, p99 +19%").
   This is the opportunity model's ladder read as arithmetic.
2. **Hot-key detector** - PC maintains the per-key ordering structures, so hot ordering domains
   need no inference: queued depth, oldest age, processing rate, contribution to total residence -
   and the headline form, *"47% of your backlog is behind 0.003% of keys"*.
3. **Retry impact, not retry count** - "182 retries" sounds harmless; "14,821 records held behind
   retrying ordering domains, frontier delayed 47s" does not. Rank retries by what they block.
4. **"What is PC doing right now"** - one execution-state breakdown where every owned record is in
   exactly one population (completed-awaiting-frontier / buffered / ordering-blocked / executing /
   eligible-awaiting-capacity / retrying), click any population to drill in. A `top` for Kafka
   execution - and the reconciliation property is real, not aspirational: the conservation
   accounting (astubbs#336) already enforces it.

## Two consequences to keep visible

- **Act changes astubbs#268's security posture, materially.** That PR's stated posture is
  *read-only, loopback, Host-allowlist, no authentication* - "there are no write paths" is its
  security argument. The moment a DLQ button exists, that argument is gone: authn/authz become a
  prerequisite of the Act layer, not a hardening item. Observe and Explain can ship on the current
  posture; Act cannot.
- **Each button is an engine API before it is a button.** `DLQ record` needs the DLQ to exist
  (astubbs#149 is requirements-stage, astubbs#8 the held draft); `pause key` is a per-ordering-
  domain pause no current surface offers; expiring overrides need the options system to accept
  runtime-scoped, self-reverting values. The control plane is also the admin API's product
  requirements list.
