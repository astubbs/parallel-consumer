/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The sandbox: run any Parallel Consumer definition with no broker, no Docker and no test environment, against
 * records it generates itself.
 *
 * <h2>How a definition starts in the sandbox</h2>
 * A definition is written once. What changes between a broker and the sandbox is the argument to {@code start},
 * and nothing else (R33):
 *
 * <pre>{@code
 * ParallelConsumerDefinition pc = ParallelConsumer.connect(props);
 * pc.json("orders", Order.class)
 *         .process(ctx -> { inventory.reserve(ctx.value()); return Outcome.succeeded(); });
 * pc.json("dispatches", Dispatch.class)
 *         .process(ctx -> { routing.plan(ctx.value()); return Outcome.succeeded(); });
 *
 * try (ConsumerHandle handle = pc.start()) { ... }                     // a broker
 * try (ConsumerHandle handle = pc.start(sandbox)) { ... }              // no broker
 * }</pre>
 *
 * {@link bz.stub.parallelconsumer.sandbox.Sandbox} is an implementation of the fluent API's one runtime seam,
 * {@link bz.stub.parallelconsumer.fluent.ClientRuntime} (KTD9). It answers the two questions that seam asks - which
 * consumer, which producer - with a subclass of the mock consumer that already ships in the core artefact and
 * Kafka's own {@code MockProducer}, and then does two things a plain client factory could not:
 *
 * <ol>
 *   <li><b>Assigns the partitions after the engine has subscribed.</b> A mock consumer assigns nothing on
 *       subscribe, so somebody has to; and it has to happen once a rebalance listener exists to be told. That is
 *       what {@code ClientRuntime#started} is for.</li>
 *   <li><b>Closes the instance when the generator reaches its bound</b>, draining first, so the final state of a
 *       bounded run is readable after the close rather than being whatever the middle of it looked like.</li>
 * </ol>
 *
 * <h2>The generator</h2>
 * {@link bz.stub.parallelconsumer.sandbox.RandomObjects} fills each route's declared value type with realistic
 * random data - Instancio for the object graph, Datafaker for leaf values chosen by field <em>name</em>, so an
 * {@code email} field holds an email address and a {@code totalAmount} holds money - and Avro's own
 * {@code RandomData} for a specific record, whose schema knows more about its fields than its Java types do. A
 * Protobuf message type is refused naming the type: this version has no filler for one. A seed makes a run
 * reproducible, and records are addressed by index rather than by sequence, so one record can be reproduced
 * without replaying the run.
 *
 * <p>Each generated object is encoded with its own route's serialiser before it reaches the mock consumer, because
 * the engine under the facade reads raw bytes (KTD2). That is why a route declared with a hand-written
 * deserialiser and no serialiser beside it is refused at start, naming its topic: there would be nothing to
 * encode with, and a record the route cannot read back is worse than a refusal.
 *
 * <h2>The classic API</h2>
 * {@link bz.stub.parallelconsumer.sandbox.ClassicSandbox} hands the same clients and generator to an options
 * builder, so an existing classic-API application or example runs broker-free too (AE26). Core never depends on
 * this module; this module depends on core.
 *
 * <h2>The broker-free test kit</h2>
 * A test drives the same sandbox with its own records - {@code sandbox.consumer().publish(...)} - and asserts on
 * what the instance did with them, which is the same seam the generator uses. The generator is a convenience on
 * top, not a requirement.
 *
 * <p><b>Incubating.</b> Every public type here carries
 * {@link org.apache.kafka.common.annotation.InterfaceStability.Unstable}, like the fluent package it serves.
 */
package bz.stub.parallelconsumer.sandbox;
