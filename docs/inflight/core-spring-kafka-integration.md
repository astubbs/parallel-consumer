# Spring Kafka integration, reconsidered: PC disappearing is the feature

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs a product decision -->

From the Codex strategy review of 2026-08-22/23 (breakdown in
[`core-engine-thesis.md`](core-engine-thesis.md)).

**The history:** users repeatedly asked for Spring integration during the owner's consulting years,
and it was brushed off - PC already had a perfectly good API, and wrapping it in annotations so
someone avoids calling it directly looked like busywork.

**The reconsideration:** the purpose is not wrapping PC's API. `@KafkaListener` users are bound to
partition-level execution without knowing it - processing 100 independent customer IDs should not
require 100 partitions or a hand-built concurrency layer. The conceptual target:

```java
@KafkaListener(topics = "orders", ordering = KEY)
```

with PC as the invisible execution engine underneath. The user never imports anything called
ParallelConsumer. **Infrastructure often wins by disappearing** - the most successful version of
this project may be executions where the developer never learns its name.

This passes the integration filter in [`core-engine-thesis.md`](core-engine-thesis.md) cleanly:
Spring Kafka is a place where Kafka's ownership model is being used as an execution model, and
SmallRye Reactive Messaging independently discovered the same gap and built per-key concurrency
for it - which is evidence of demand, not just theory. (SmallRye's feature is
`max-concurrency`-style key-ordered processing; verify the current shape before citing it in
anything public.)

**Open questions, none investigated:**

- Container-factory seam vs message-listener seam - where does PC actually plug into
  `spring-kafka` without forking it? Spring's `ConcurrentMessageListenerContainer` owns the
  consumer the same way PC does; the two will fight over it.
- Whether the `ordering = KEY` ergonomics need a Spring Boot starter, an annotation attribute
  (needs Spring's cooperation), or container-factory configuration (needs nobody's).
- Where offset commit responsibility lands when Spring's ack modes meet PC's frontier.

**Relationship to ranking:** not on
[`process-candidate-ranking.md`](process-candidate-ranking.md) today. If the facade/adoption-ladder
direction ([`core-alternate-api-facades.md`](core-alternate-api-facades.md)) is taken up, this is
the same move aimed at the largest Java Kafka developer population there is.
