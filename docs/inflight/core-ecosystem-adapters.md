# Layer-2 ecosystem adapters: replace each ecosystem's Kafka execution layer, not its framework

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - a survey-and-classify campaign before any build; agent-shaped research -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)). Generalises
[`core-spring-kafka-integration.md`](core-spring-kafka-integration.md) from one framework to a
strategy, and extends the FRONT ENDS row of the engine-thesis taxonomy.

## The two-layer model

```
Layer 1 - native bindings (astubbs#242, #293 stack)   proves the engine works everywhere
Layer 2 - ecosystem adapters                          how adoption actually happens
          Spring Kafka -> PC        MassTransit (.NET) -> PC     Watermill (Go) -> PC
          rust-rdkafka stream -> PC  <Python survey> -> PC        <Nest-style TS> -> PC
```

A .NET shop rewriting MassTransit consumers into PC consumers is friction; the same shop changing
its transport wiring so `MassTransit -> PC -> Kafka` and inheriting key parallelism, adaptive
concurrency, effective lag and the dashboard is not. Same pattern as Streams-on-PC and the
KafkaConsumer facade ([`core-alternate-api-facades.md`](core-alternate-api-facades.md)): **keep
the programming model people already chose; replace the execution limitations they did not realise
they were choosing with it.** The distribution effect is as valuable as the engineering: the
adapter is what lets "Watermill Kafka concurrency" searches resolve to "install the adapter"
instead of "abandon your framework".

## The hunt heuristic and the classification

Look for ecosystems that already separated *what happens when a message arrives* from *the
machinery that consumes it* - then classify the seam:

- **A - trivial**: pluggable consumer/subscriber interface exists; adapter slots in.
- **B - modest**: needs a custom transport/backend implementation.
- **C - invasive**: application API coupled to Kafka client internals; ignore initially.

The campaign: per language, survey the top ~3 Kafka/messaging frameworks by real adoption,
classify their seams, prototype only A-class. Survey-first is deliberate - especially Python and
TypeScript, where the conversation explicitly declined to guess a winner. The resulting matrix
(language x native binding x adapter x difficulty) is candidate STRATEGY.md material once at
least one non-JVM cell is proven. This is agent-shaped work: independent per-ecosystem research
fanning out, exactly the working model the polyglot demos used (astubbs#331).

## Three design questions every adapter must answer, learned from the Spring note

The Spring open questions generalise; make them the standard checklist so twelve adapters do not
rediscover them:

1. **Who owns dispatch?** The seam must hand over *execution*, not merely consumption - if the
   framework still fans handlers out on its own pool, PC's key-ordering guarantee dies at the
   handoff (Spring's `ConcurrentMessageListenerContainer` fight, generalised).
2. **Who owns retries and errors?** MassTransit and Watermill carry their own retry/DLQ/middleware
   pipelines; PC retrying underneath a framework that also retries produces double retries and
   unaccountable behaviour. Each adapter names one owner per concern, explicitly.
3. **Who owns the commit?** Framework ack modes meet PC's offset frontier; the adapter decides
   where acknowledgement semantics land and documents what changed.

**Maintenance bound:** every adapter is a foreign API that drifts. The mitigation already exists -
plug each adapter into the shared conformance matrix
([`release-certified-execution-semantics.md`](release-certified-execution-semantics.md)) so drift
surfaces as a red cell instead of a user bug report.
