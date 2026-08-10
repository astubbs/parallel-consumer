package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Wake-on-work's shutdown proof (astubbs#255, item 3).
 * <p>
 * <b>Why this test exists at all.</b> The design note for wake-on-work names one trap explicitly: do not reach
 * for {@code KafkaConsumer#wakeup()} as the wake signal, because Kafka Streams' vocabulary already gives
 * {@code wakeup()} the meaning <em>shutdown</em>, and a wake delivered while the thread is not polling arms
 * the <em>next</em> poll instead - so a stray completion signal could swallow a shutdown one. That is a
 * failure that shows up once in a thousand shutdowns and never reproduces on demand. This module took the
 * other road and built its own condition, which is precisely why the shutdown path is the one that has to be
 * demonstrated rather than argued: the hazard being avoided is a shutdown-path hazard.
 * <p>
 * <b>The precondition is asserted, not assumed.</b> A close that happens to land while the StreamThread is
 * somewhere else would pass this test while proving nothing. So the topology is held with records genuinely
 * in flight, and {@link PcDispatchCounters#getSplitPollWaits()} - incremented only where the thread parks on
 * our own condition - is required to be non-zero <em>before</em> the close is attempted.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streams.PcWorkSignal
 */
@Slf4j
// PcDispatchSwitch and PcDispatchCounters are process-wide: a concurrently running class would flip the arm
// this test measured and write the counter it reads.
@Isolated
class WakeOnWorkShutdownTest extends BrokerStreamsIntegrationTest {

    private static final int POOL_SIZE = 4;

    /**
     * Long enough that the records are unambiguously still in the chain when the close is called, and short
     * enough that the teardown's own drain can finish inside {@link #CLOSE_TIMEOUT}.
     * <p>
     * That second half is not a detail. <b>Closing this module's task drains in-flight work on purpose</b> -
     * the patched {@code suspend()} pumps to quiescence before the topology is torn down around it, so a
     * worker still inside the chain would otherwise be forwarding into a record collector that is about to
     * close. A first version of this test used a 30s record against a 20s close budget and both arms failed
     * identically, which is the designed drain doing its job rather than a shutdown defect. The control arm
     * is what said so.
     */
    private static final Duration RECORD_COST = Duration.ofSeconds(3);

    private static final int RECORDS = 2;

    /**
     * Generous against the real cost of a broker-backed close plus the drain above, so that a close which
     * merely ran out of patience is distinguishable from one that completed.
     */
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(60);

    @BeforeEach
    void resetCounters() {
        PcDispatchCounters.reset();
    }

    @AfterEach
    void restoreDefaultDispatch() {
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * The proof: close a topology while workers are inside the chain and the StreamThread is parked on the
     * wake-on-work condition, and it still shuts down cleanly and promptly.
     */
    @Test
    void closingWhileParkedOnTheWakeConditionShutsDownCleanly() throws InterruptedException {
        assertShutsDownCleanlyMidDispatch("wake-shutdown-on", true);
    }

    /**
     * The control arm. The same close, with wake-on-work off, so the StreamThread is blocked in
     * {@code Consumer#poll()} the way stock is.
     * <p>
     * Both arms are expected to pass - that is the point. The claim being defended is that owning the poll
     * wait does not <em>change</em> the shutdown outcome, and a single green arm cannot say that: it would
     * leave open whether this topology shuts down cleanly for reasons unrelated to the mechanism.
     */
    @Test
    void theSameCloseWorksWithWakeOnWorkOff() throws InterruptedException {
        assertShutsDownCleanlyMidDispatch("wake-shutdown-off", false);
    }

    private void assertShutsDownCleanlyMidDispatch(final String name, final boolean wakeOnWork)
            throws InterruptedException {
        PcDispatchSwitch.enable(POOL_SIZE);
        PcDispatchSwitch.setWakeOnWork(wakeOnWork);
        assertThat(PcDispatchSwitch.isWakeOnWorkEnabled())
                .as("%s arm must run with wake-on-work %s", name, wakeOnWork ? "ON" : "OFF")
                .isEqualTo(wakeOnWork);

        String inputTopic = setupTopic(name + "-in");
        String outputTopic = setupTopic(name + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger interrupted = new AtomicInteger();

        produce(inputTopic);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        stream.mapValues((key, value) -> {
            entered.countDown();
            try {
                // Still in the chain when the close lands - which is the whole scenario. A block, not a spin,
                // so the close is not competing with this thread for a core.
                Thread.sleep(RECORD_COST.toMillis());
            } catch (InterruptedException e) {
                // Expected: the close interrupts the pool. Counted rather than swallowed silently, so the
                // teardown path this test exercises is visible in the run's output.
                Thread.currentThread().interrupt();
                interrupted.incrementAndGet();
            }
            return value;
        }).to(outputTopic);

        Properties props = baseStreamsProps(name + "-" + System.nanoTime());
        // One StreamThread, matching every other arm in this module: the only concurrency is the one under test.
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        KafkaStreams streams = startAndAwaitRunning(builder, props);
        boolean closedCleanly;
        long closeMillis;
        try {
            assertThat(entered.await(120, TimeUnit.SECONDS))
                    .as("a record must actually be inside the processor chain, or the close under test is not "
                            + "happening mid-dispatch and this test proves nothing")
                    .isTrue();

            if (wakeOnWork) {
                // THE PRECONDITION. Without this the close might land while the thread is anywhere at all, and
                // a green result would say nothing about the wait this change introduced.
                await().atMost(Duration.ofSeconds(60))
                        .until(() -> PcDispatchCounters.getSplitPollWaits() > 0);
                log.info("=== {} - StreamThread has parked on the wake condition {} time(s) before close",
                        name, PcDispatchCounters.getSplitPollWaits());
            }
        } finally {
            long startNanos = System.nanoTime();
            closedCleanly = streams.close(CLOSE_TIMEOUT);
            closeMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        }

        log.info("=== {} - close returned {} in {}ms (wake-on-work {}), {} worker(s) interrupted",
                name, closedCleanly, closeMillis, wakeOnWork ? "ON" : "OFF", interrupted.get());

        assertThat(closedCleanly)
                .as("%s: closing mid-dispatch must succeed within %s. A false here is the shutdown-path "
                        + "hazard this design exists to avoid, arriving anyway.", name, CLOSE_TIMEOUT)
                .isTrue();
        assertThat(streams.state())
                .as("%s: and the client must actually reach NOT_RUNNING rather than merely reporting success",
                        name)
                .isEqualTo(KafkaStreams.State.NOT_RUNNING);
        assertThat(closeMillis)
                .as("%s: the close must COMPLETE rather than run out the clock. Returning true only at the "
                        + "%s mark would be a timeout wearing a success's clothes - and a StreamThread that "
                        + "sat out its poll budget on our own condition is exactly how that would happen.",
                        name, CLOSE_TIMEOUT)
                .isLessThan(CLOSE_TIMEOUT.toMillis() / 2);

        if (wakeOnWork) {
            assertThat(PcDispatchCounters.getSplitPollWaits())
                    .as("%s: and the mechanism must genuinely have been in play - a zero here means the "
                            + "StreamThread never parked on our condition and this arm tested stock", name)
                    .isPositive();
        } else {
            assertThat(PcDispatchCounters.getSplitPollWaits())
                    .as("%s: the control arm must be genuinely inert. A non-zero here means the kill switch "
                            + "did not reach the run and both arms measured the same thing.", name)
                    .isZero();
        }
    }

    private void produce(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }
}
