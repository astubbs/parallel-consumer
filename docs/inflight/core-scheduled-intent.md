# Scheduled intent: cron is a producer of obligations, and records are only one source

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - "offensively cheap given the machinery underneath", but sequenced behind the admission/resource work it rides on -->

From the follow-up Codex conversation, 2026-08-30 (model root:
[`core-admission-scheduling-model.md`](core-admission-scheduling-model.md)).

## Two distinct time primitives, kept distinct

- **`notBefore = T` on existing work** - the obligation exists; execution is time-ineligible. An
  admission predicate, nothing more (retry backoff is this). If Kafka's delayed-delivery work
  (KIP-1277) ships a broker-side time index, use it as the storage/delivery primitive rather
  than recreating it - time stays an eligibility predicate here either way.
- **`fireAt = T` on a schedule** - the obligation does not exist yet; create it at T. Cron is this.

## The implementation the conversation called stupid-simple

A compacted schedule topic (`scheduleId -> cron expression, target, timezone, enabled, nextFire,
generation`) materialised as a table - **plus the piece the GitHub Codex review, 2026-08-31 caught missing: a
table keyed by scheduleId has no access path for "all rows with nextFire <= now", so each owner
also maintains a partition-local due-time index (a heap or time-ordered store) over its
schedules, rebuilt from the table on failover; that index, its recovery and clock handling ARE
the scheduler, not a free consequence of EOS.** The owner of a due schedule then transactionally
produces the invocation and advances `nextFire`. Everything after that is
ordinary engine machinery - admission, resources, QoS, retries, Why Wait. What falls out free:

- **Distributed singleton cron** - keyed ownership already gives one active processor per
  schedule; EOS gives atomic advance-plus-produce. No leader election, no Quartz cluster, no
  Redis lock.
- **Missed firings become policy, not failure**: catch up / coalesce / skip, decided per
  schedule. Each invocation carries `(scheduleId, logicalFireTime)` - deterministic identity for
  dedup.
- **Better-than-cron semantics with zero cron changes**: "hourly, unless Salesforce is
  unavailable" is just the invocation waiting at admission; "must finish by 06:00" is a deadline;
  "not during maintenance" is a fence. Cron decides when an obligation comes into existence,
  never when execution starts.
- **Certain future demand**: a registered 02:00 reconciliation needing 100 DB slots is Demand
  Horizon content *at registration time* - the cleanest possible Prescience input, visible before
  a single invocation record exists.

## The API is function-first; the topic is an implementation detail

`run reconcileInvoices() every hour` - no topic, serializer or consumer vocabulary in the
programming model. Internally a firing becomes a tiny internal work item so placement, failover
and distribution ride the normal execution machinery rather than the schedule owner RPCing a
chosen process.

## The conceptual correction this surfaced

A scheduled function's callback is not "processing a Kafka record" - it is engine-managed
execution that may do anything. Which generalises the kernel: **Kafka records are one source of
durable obligations, not the definition of them.** Timer events, RPC calls, actor messages
([`core-actor-revival.md`](core-actor-revival.md)) and recovery requirements all produce
obligations into the same admission scheduler. Resist over-generalising this in code ("generic
work abstraction" is architecture astronautics - the conversation's own words); the MVP is
concrete cron. But the model is why the facade/queue surfaces keep falling out cheaply.

Demo tie: the parcels progression ([`docs-executable-progression.md`](docs-executable-progression.md))
gets cron + singleton + resource admission + Why Wait + missed-firing recovery in one stage (a
nightly depot reconciliation).
