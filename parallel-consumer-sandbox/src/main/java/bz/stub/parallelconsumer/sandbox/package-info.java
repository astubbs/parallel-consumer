/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The sandbox: run any Parallel Consumer definition with no broker, no Docker and no test environment, against
 * records you publish or records it makes up.
 *
 * <h2>The two shapes, and which one to reach for</h2>
 * <b>Publish, settle, assert</b> - for a test that knows its own data, which is most of them:
 *
 * <pre>{@code
 * Sandbox sandbox = Sandbox.builder().handPublished().build();
 * try (ParallelConsumerInstance instance = pc.start(sandbox)) {
 *     sandbox.pipe("orders", "cust-1", new Order("o-1"));
 *     sandbox.awaitSettled();
 *     assertThat(inventory.reserved()).containsExactly("o-1");
 * }
 * }</pre>
 *
 * {@link bz.stub.parallelconsumer.sandbox.Sandbox#pipe(java.lang.String, java.lang.Object, java.lang.Object)}
 * encodes with the route's own serialiser and publishes from the caller's thread;
 * {@link bz.stub.parallelconsumer.sandbox.Sandbox#awaitSettled()} blocks until every record published so far is
 * accounted for - completed, or parked - and refuses if the run ended before it got there. The classic API has
 * the same pair on {@link bz.stub.parallelconsumer.sandbox.ClassicSandbox}.
 *
 * <p><b>The driver</b> - for a soak or a demo, where the point is volume rather than particular records: it
 * publishes on a thread of its own at a declared rate until a {@link bz.stub.parallelconsumer.sandbox.Bound} is
 * reached, then settles and closes the instance. It is what a sandbox does unless
 * {@code Sandbox.builder().handPublished()} says otherwise.
 *
 * <p><b>Why a settle exists at all</b>, when the broker-free drivers of the stream-processing libraries users
 * compare us with need none: those engines are single-threaded, so a piped record is processed on the caller's
 * thread and an assertion on the next line is already safe. This one is the real engine - polled on one thread,
 * dispatched on a worker, committed on the control thread - so a publish that returned would say nothing about
 * whether the function had run.
 *
 * <p>The close differs between the two for the same reason: a close cannot be run from inside the engine it
 * closes (KTD6), so the driven path hands it to the driver's own thread, and the caller-published path has the
 * caller's thread, which was never inside the engine.
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
 * try (ParallelConsumerInstance instance = pc.start()) { ... }                     // a broker
 * try (ParallelConsumerInstance instance = pc.start(sandbox)) { ... }              // no broker
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
 *   <li><b>Closes the instance when the driver reaches its bound</b> - after waiting for every published
 *       record to be accounted for, either by a commit or by being parked, because draining is not the same as
 *       finishing ({@link bz.stub.parallelconsumer.sandbox.SandboxConsumer#awaitEveryPublishedRecordCommitted()})
 *       - so the final state of a bounded run is readable after the close rather than being whatever the middle of
 *       it looked like. A definition that parks by design is an ordinary bounded run here.</li>
 * </ol>
 *
 * <h2>What a record contains is not this module's business</h2>
 * The driver is fed by a function from a record's index to its value, one per topic, and a driven sandbox with a
 * topic nobody has supplied one for is refused at start naming it. <b>This module therefore has no third-party
 * dependencies at all</b>: it is core, plus the mock clients core already ships. Realistic fake objects - an
 * {@code email} field that holds an email address, an {@code Instant} inside a plausible window, an Avro record
 * filled from its own schema - are a separate concern, and a separate artefact supplies them as exactly such a
 * function.
 *
 * <p>Addressed by <em>index</em> rather than called in sequence, so the record at a given index is the same
 * whatever order the topics were served in and whatever the pacing did - which is what makes a sandbox failure
 * reproducible without replaying the run up to it.
 *
 * <p>Each value is encoded with its own route's serialiser before it reaches the mock consumer, because the engine
 * under the facade reads raw bytes (KTD2). That is why a route declared with a hand-written deserialiser and no
 * serialiser beside it is refused at start, naming its topic: there would be nothing to encode with, and a record
 * the route cannot read back is worse than a refusal.
 *
 * <h2>The classic API</h2>
 * {@link bz.stub.parallelconsumer.sandbox.ClassicSandbox} hands the same clients and driver to an options
 * builder, so an existing classic-API application or example runs broker-free too (AE26). Core never depends on
 * this module; this module depends on core.
 *
 * <p><b>Incubating.</b> Every public type here carries
 * {@link org.apache.kafka.common.annotation.InterfaceStability.Unstable}, like the fluent package it serves.
 */
package bz.stub.parallelconsumer.sandbox;
