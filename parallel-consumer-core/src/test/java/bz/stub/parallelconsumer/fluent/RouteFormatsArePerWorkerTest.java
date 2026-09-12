package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * A route's deserialiser is used by every worker that decodes one of its records, and Kafka only contracts a
 * {@link Deserializer} to be safe on the one thread a {@code KafkaConsumer} polls on - so a stateful deserialiser,
 * perfectly correct under the client it was written for, could race or throw here (found by the cross-model review on
 * astubbs/parallel-consumer#502). A format built from a supplier gives each worker thread its own instance
 * (owner-directed, 2026-09-12: "then the hand in must be a supplier").
 *
 * <h2>Why this is deterministic rather than a race</h2>
 * The deserialiser under test is not merely unsafe when raced, it <b>refuses</b> any thread but the first to use it,
 * which is the strictest reading of the contract Kafka gives. So sharing one instance across workers fails every
 * time rather than occasionally, and the test needs no repetition to be trustworthy. Two records are forced to be in
 * flight at once through a barrier in the processing function, so "more than one worker decoded" is guaranteed
 * rather than hoped for - and asserted, so the test cannot pass by only ever using one thread.
 */
@Timeout(120)
class RouteFormatsArePerWorkerTest extends AbstractFluentEngineTest {

    /**
     * Every instance the supplier has made, so the test can assert they were all closed.
     */
    private final List<ThreadConfinedDeserializer> made = new CopyOnWriteArrayList<>();

    /**
     * Two records in flight at once, which is what makes a second worker decode. Its timeout is what turns the
     * shared-instance case into a failure instead of a hang: the partner never arrives, because its decode threw.
     */
    private final CyclicBarrier bothInFlight = new CyclicBarrier(2);

    private ThreadConfinedDeserializer newDeserializer() {
        ThreadConfinedDeserializer created = new ThreadConfinedDeserializer();
        made.add(created);
        return created;
    }

    /**
     * The claim itself: with a per-worker supplier, a deserialiser that tolerates exactly one thread decodes every
     * record - and more than one thread did the decoding, so the isolation is what made it work.
     */
    @Test
    void aThreadConfinedDeserialiserWorksWhenEachWorkerHasItsOwn() {
        var pc = ParallelConsumer.connect(props())
                // Unordered with distinct keys, so the two records cannot be serialised behind one shard.
                .withDefaultOrdering(ProcessingOrder.UNORDERED)
                .withDefaultConcurrency(8);
        pc.topic(TOPIC)
                .consumed(Consumed.perWorker(Serdes.String()::deserializer, this::newDeserializer))
                .process(context -> {
                    bothInFlight.await(defaultTimeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 2);
        runtime.publish(TOPIC, 0, 0, "key-0", "the first order");
        runtime.publish(TOPIC, 1, 0, "key-1", "the second order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.succeededCount()).isEqualTo(2));

        Set<String> decodingThreads = decodingThreads();
        assertWithMessage("the test proves nothing unless more than one worker decoded: %s", decodingThreads)
                .that(decodingThreads.size()).isAtLeast(2);
        assertWithMessage("no instance was used from a thread other than its own")
                .that(rejections()).isEqualTo(0);
    }

    /**
     * Every instance the supplier made is closed when the instance shuts down - one per worker thread, not one in
     * total, which is the half of this change that a leak would hide.
     */
    @Test
    void everyPerWorkerInstanceIsClosedWhenTheInstanceShutsDown() {
        var pc = ParallelConsumer.connect(props())
                .withClosePath(ClosePath.DONT_DRAIN_FIRST)
                .withDefaultOrdering(ProcessingOrder.UNORDERED)
                .withDefaultConcurrency(8);
        pc.topic(TOPIC)
                .consumed(Consumed.perWorker(Serdes.String()::deserializer, this::newDeserializer))
                .process(context -> {
                    bothInFlight.await(defaultTimeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                    return Outcome.succeeded();
                });

        ParallelConsumerInstance started = runtime.startAndAssign(pc, 2);
        runtime.publish(TOPIC, 0, 0, "key-0", "the first order");
        runtime.publish(TOPIC, 1, 0, "key-1", "the second order");
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(pc.dispatcher().succeededCount()).isEqualTo(2));
        assertWithMessage("more than one instance exists to close").that(made.size()).isAtLeast(2);

        started.close();
        handle = null;

        for (ThreadConfinedDeserializer each : made) {
            assertWithMessage("every instance the supplier made was closed: %s", each)
                    .that(each.closed.get()).isAtLeast(1);
        }
    }

    /**
     * A format helper whose serialiser is not on the classpath passes null deliberately - {@code Formats} documents
     * the serialiser as optional - and that must stay an ABSENT half rather than becoming a supplier of null. A
     * format that claimed it could write and then failed with a NullPointerException at the first produced record
     * would have replaced {@link Produced}'s refusal with a crash.
     */
    @Test
    void anAbsentHalfStaysAbsentRatherThanBecomingASupplierOfNull() {
        Format<String> readOnly = Format.named(new ThreadConfinedDeserializer(), null, "read-only", String.class);

        assertThat(readOnly.hasDeserializer()).isTrue();
        assertWithMessage("a null serialiser is an absent half, not one that throws when asked")
                .that(readOnly.hasSerializer()).isFalse();
        assertThat(readOnly.serializer()).isNull();
    }

    /**
     * Wrapping a per-worker format to classify its decode failures must keep it per worker. Wrapping a single
     * instance would collapse it into a shared one and put back the very race the supplier removes - silently, since
     * the wrapper itself is stateless and looks innocent.
     */
    @Test
    void classifyingDecodeFailuresKeepsAPerWorkerFormatPerWorker() {
        Format<String> perWorker = Format.readingPerWorker(this::newDeserializer);
        Format<String> classified = Formats.classifyDecodeFailures(perWorker, Decode::transientFailure);

        int madeBeforeDecoding = made.size();
        decodeOnANewThread(classified, "first");
        decodeOnANewThread(classified, "second");

        assertWithMessage("each decoding thread got its own inner deserialiser through the classifying wrapper")
                .that(made.size() - madeBeforeDecoding).isAtLeast(2);
        assertWithMessage("and none of them was used from a thread other than its own")
                .that(rejections()).isEqualTo(0);
    }

    /**
     * Decodes one value on a thread of its own and waits for it, so each call is a distinct worker as far as the
     * format is concerned.
     */
    private void decodeOnANewThread(Format<String> format, String value) {
        AtomicReference<RuntimeException> failed = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                String ignoredDecoded = format.deserializer()
                        .deserialize(TOPIC, value.getBytes(StandardCharsets.UTF_8));
            } catch (RuntimeException thrown) {
                failed.set(thrown);
            }
        }, "decoder-" + value);
        worker.start();
        try {
            worker.join(defaultTimeout.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting for " + worker.getName(), interrupted);
        }
        if (failed.get() != null) {
            throw failed.get();
        }
    }

    private Set<String> decodingThreads() {
        Set<String> threads = ConcurrentHashMap.newKeySet();
        for (ThreadConfinedDeserializer each : made) {
            threads.addAll(each.threadsThatUsedIt);
        }
        return Collections.unmodifiableSet(threads);
    }

    private int rejections() {
        int total = 0;
        for (ThreadConfinedDeserializer each : made) {
            total += each.rejections.get();
        }
        return total;
    }

    /**
     * A deserialiser that tolerates exactly one thread: the first to use it claims it, and any other is refused.
     * <p>
     * It is the strictest honest reading of Kafka's contract - safe on the poll thread, nothing promised beyond it -
     * and being a refusal rather than a corruption is what makes this suite deterministic. A real one would more
     * likely corrupt a value or throw from inside its own state; either way the cure is the same.
     */
    private static final class ThreadConfinedDeserializer implements Deserializer<String> {

        private final Set<String> threadsThatUsedIt = ConcurrentHashMap.newKeySet();

        private final AtomicInteger rejections = new AtomicInteger();

        private final AtomicInteger closed = new AtomicInteger();

        /**
         * Plain, not volatile, and deliberately so: this object's whole claim is that it is not safe to share, and a
         * fence here would be this test pretending otherwise.
         */
        private Thread owner;

        @Override
        public String deserialize(String topic, byte[] data) {
            Thread current = Thread.currentThread();
            if (owner == null) {
                owner = current;
            } else if (owner != current) {
                rejections.incrementAndGet();
                throw new SerializationException("this deserializer belongs to " + owner.getName()
                        + " and was used from " + current.getName());
            }
            threadsThatUsedIt.add(current.getName());
            return data == null ? null : new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }

        @Override
        public String toString() {
            return "ThreadConfinedDeserializer(owner=" + (owner == null ? "none" : owner.getName())
                    + ", closed=" + closed.get() + ")";
        }
    }
}
