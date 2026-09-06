package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.state.PartitionState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Registering the counter for an encoding must happen exactly once, however two encoders interleave.
 * <p>
 * {@code OffsetMapCodecManager.encodingCounters} is a lazily populated cache whose miss handler <em>registers a
 * meter</em>. Written as a {@code get}-then-{@code put} on a plain {@code HashMap} - which is what it was until
 * this test arrived - the check and the act are two steps, so two encoders inside that window both miss, both
 * register, and one of the two {@code put}s is discarded. The field's javadoc carries what that costs; this test
 * is only interested in the atomicity property, which is the thing a fix can pin.
 * <p>
 * <b>The interleaving is driven, not waited for - and that is deliberate.</b> Nothing in production reaches this
 * field from two threads today: {@code tryCommitOffsetsOnRevoke} declines {@code commitLock} rather than blocking
 * on it, and {@code ConsumerOffsetCommitter.commit} sends a non-owner caller through the request queue instead of
 * encoding on its thread, so one encoder runs at a time per instance in every commit mode. A test that started
 * two threads and hoped would therefore be green on the broken code as often as not. This one opens the window by
 * hand, through the one seam the miss handler crosses - {@code PCMetrics.getCounterFromMetricDef} - so the result
 * is the same on every run and on every machine.
 * <p>
 * <b>What it establishes, and what it does not.</b> It establishes that the check-then-act is not atomic, which
 * is a property of this class. It does <em>not</em> establish that production interleaves here; see the field's
 * javadoc for why it does not, and for what would change that. The value of pinning it now is that
 * confluentinc#233 (split encode from decode) and confluentinc#200 (parallelise encoding) would both make it
 * reachable, and no gate in this repo reasons about which thread arrives at a field.
 * <p>
 * <b>Verdict against the pre-fix tree</b>: {@code expected 1, but was 2} - both encoders registered the counter
 * for the chosen encoding. Against the fix, the second encoder blocks inside {@code computeIfAbsent} until the
 * first has published its entry, then reads it, so the rendezvous below times out by design and the count is 1.
 * <p>
 * <b>The count alone is not enough</b>, and the test asserts one more fact for that reason. On the pre-fix tree a
 * second encoder that reaches the lookup only <em>after</em> the window has closed finds the first encoder's
 * entry, registers nothing, and the count is 1 - a green run against the broken code. So the seam also records
 * where the second encoder was when the window closed: inside the counter lookup (blocked beneath it on the fix,
 * or registering on the pre-fix tree), or not there yet. Only the first is a valid run; the second is the harness
 * failing to open the window it needs, and it fails as that rather than passing as a verdict.
 *
 * @author Antony Stubbs
 */
@Slf4j
// READ side of the codec's static state. The encode path reads OffsetMapCodecManager.forcedCodec and
// OffsetSimultaneousEncoder.compressionForced, which OffsetEncodingTests and OffsetEncodingBackPressureUnitTest
// write under these locks - and the root pom runs methods in parallel unless -Pci says otherwise, so without
// declaring them here a writer could force a codec under this test mid-encode. Same shape as
// WorkManagerOffsetMapCodecManagerTest; READ mode excludes only the writers.
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
@ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
class EncodingCounterRegistrationIsAtomicTest {

    private static final TopicPartition TP = new TopicPartition("encoding-counter-atomicity", 0);

    /**
     * The commit would resume from here, and the payload is encoded relative to it.
     */
    private static final long BASE_OFFSET = 0L;

    /**
     * There has to be an incomplete offset below the highest succeeded one or PC encodes no payload at all and
     * the encode path never reaches the counter. Offset 1 stays incomplete under a succeeded high-water mark of
     * 5, which is the smallest shape that produces one.
     */
    private static final long INCOMPLETE_OFFSET = 1L;

    private static final long HIGHEST_SUCCEEDED = 5L;

    /**
     * How long the first encoder holds the registration window open waiting for the second to arrive in it.
     * <p>
     * It is a rendezvous bound, not an assertion deadline - the assertion below is exact either way. On the
     * pre-fix tree the second encoder arrives in milliseconds and nothing waits; on the fixed tree it is blocked
     * by design and can never arrive, so this is what releases the first encoder and lets the test finish. Long
     * enough that a loaded machine cannot make a broken tree look fixed.
     */
    private static final long WINDOW_HOLD_SECONDS = 5;

    /**
     * Ceiling on every wait the test body makes: the rendezvous that confirms the first encoder reached the
     * window, and each {@link Thread#join(long)}. An {@code int} because that is what
     * {@code LatchTestUtils.awaitLatch} takes; it widens for the join.
     */
    private static final int THREAD_JOIN_SECONDS = 60;

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @SneakyThrows
    void twoEncodersInterleavingRegisterTheEncodingCounterOnlyOnce() {
        var metrics = new WindowHoldingMetrics();
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder().build()) {
            @Override
            public PCMetrics pcMetrics() {
                return metrics;
            }
        };

        var state = stateWithSomethingToEncode(module);
        var codecManager = new OffsetMapCodecManager<String, String>(module);

        var firstFailure = new AtomicReference<Throwable>();
        var encodesCompleted = new AtomicInteger();

        var firstEncoder = encoderThread("first-encoder", codecManager, state, firstFailure, encodesCompleted);
        firstEncoder.start();

        awaitLatch(metrics.firstEncoderInsideWindow, THREAD_JOIN_SECONDS);

        var secondEncoder = encoderThread("second-encoder", codecManager, state, firstFailure, encodesCompleted);
        metrics.secondEncoder.set(secondEncoder);
        secondEncoder.start();

        firstEncoder.join(TimeUnit.SECONDS.toMillis(THREAD_JOIN_SECONDS));
        secondEncoder.join(TimeUnit.SECONDS.toMillis(THREAD_JOIN_SECONDS));

        if (firstFailure.get() != null) {
            throw firstFailure.get();
        }

        assertThat(encodesCompleted.get())
                .isEqualTo(2);
        // the harness precondition, before the verdict: a second encoder that was not yet at the lookup when the
        // window closed would find the first's entry on ANY tree, so a count of 1 from that run proves nothing
        assertWithMessage("the second encoder must be inside the counter lookup when the registration window "
                + "closes - registering (pre-fix) or blocked beneath it (fixed); a late arrival is a harness "
                + "failure, not a pass")
                .that(metrics.secondEncoderInsideLookupWhenWindowClosed)
                .isTrue();
        assertThat(metrics.encodingUsageRegistrations.get())
                .isEqualTo(1);
    }

    /**
     * A state whose commit needs a payload: offsets up to {@link #HIGHEST_SUCCEEDED} are succeeded except
     * {@link #INCOMPLETE_OFFSET}, so {@code encodeOffsetsCompressed} has a gap to encode.
     */
    private static PartitionState<String, String> stateWithSomethingToEncode(PCModuleTestEnv module) {
        SortedSet<Long> incompletes = new TreeSet<>();
        incompletes.add(INCOMPLETE_OFFSET);
        return new PartitionState<>(0L, module, TP, HighestOffsetAndIncompletes.of(HIGHEST_SUCCEEDED, incompletes));
    }

    private static Thread encoderThread(String name,
                                        OffsetMapCodecManager<String, String> codecManager,
                                        PartitionState<String, String> state,
                                        AtomicReference<Throwable> firstFailure,
                                        AtomicInteger encodesCompleted) {
        var thread = new Thread(() -> {
            try {
                byte[] encoded = codecManager.encodeOffsetsCompressed(BASE_OFFSET, state);
                assertThat(encoded.length).isGreaterThan(0);
                encodesCompleted.incrementAndGet();
            } catch (Throwable t) {
                firstFailure.compareAndSet(null, t);
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }

    /**
     * The seam. Counts registrations of {@link PCMetricsDef#OFFSETS_ENCODING_USAGE}, and holds the first one open
     * so that the second encoder is inside the cache-miss window while the first is still in it.
     * <p>
     * Every other metric definition passes straight through - the state and the codec manager both register
     * meters during construction, and parking those would deadlock the setup rather than test anything.
     */
    private static final class WindowHoldingMetrics extends PCMetrics {

        private final AtomicInteger encodingUsageRegistrations = new AtomicInteger();

        /**
         * Counted down once the first encoder is parked inside the window, so the test starts the second encoder
         * at the only moment that matters rather than racing it.
         */
        private final CountDownLatch firstEncoderInsideWindow = new CountDownLatch(1);

        /**
         * Counted down by a second encoder that reaches registration at all - which only a non-atomic
         * check-then-act allows.
         */
        private final CountDownLatch secondEncoderReachedRegistration = new CountDownLatch(1);

        /**
         * The second encoder's thread, set by the test before it starts, so the seam can ask where that thread is
         * at the one moment it matters - while the first encoder still holds the window, before it publishes.
         */
        private final AtomicReference<Thread> secondEncoder = new AtomicReference<>();

        /**
         * Where the second encoder was when the window closed. {@code true} if it was inside
         * {@code getCounterMeterForEncoding} - registering, on the pre-fix tree, or blocked beneath it on the fix.
         * {@code false} means it had not reached the lookup yet, and the run establishes nothing: see the class
         * javadoc. Written by the first encoder's thread, read by the test thread after {@code join}.
         */
        private volatile boolean secondEncoderInsideLookupWhenWindowClosed;

        private WindowHoldingMetrics() {
            super(new SimpleMeterRegistry(),
                    UniLists.of(Tag.of("test", "encoding-counter-atomicity")),
                    "encoding-counter-atomicity");
        }

        @Override
        @SneakyThrows
        public Counter getCounterFromMetricDef(PCMetricsDef metricDef, Tag... additionalTags) {
            if (metricDef != PCMetricsDef.OFFSETS_ENCODING_USAGE) {
                return super.getCounterFromMetricDef(metricDef, additionalTags);
            }
            if (encodingUsageRegistrations.incrementAndGet() == 1) {
                firstEncoderInsideWindow.countDown();
                boolean secondArrived =
                        secondEncoderReachedRegistration.await(WINDOW_HOLD_SECONDS, TimeUnit.SECONDS);
                // sampled here, still inside the window: once this method returns the first encoder publishes
                // its entry and the second is free to finish, so the test thread could only ever see it gone
                secondEncoderInsideLookupWhenWindowClosed = secondArrived || secondEncoderIsInsideTheLookup();
                log.info("First encoder releasing the registration window; a second encoder reached "
                        + "registration during it: {}; a second encoder was inside the lookup as it closed: {}",
                        secondArrived, secondEncoderInsideLookupWhenWindowClosed);
            } else {
                secondEncoderReachedRegistration.countDown();
            }
            return super.getCounterFromMetricDef(metricDef, additionalTags);
        }

        /**
         * Whether the second encoder's stack currently passes through the codec manager's counter lookup. On the
         * fix that thread is blocked inside {@code computeIfAbsent} beneath it; before the fix it would already
         * have counted the latch down and never be asked. A thread not yet started, still encoding, or already
         * finished is not inside it.
         */
        private boolean secondEncoderIsInsideTheLookup() {
            Thread thread = secondEncoder.get();
            if (thread == null) {
                return false;
            }
            return Arrays.stream(thread.getStackTrace())
                    .anyMatch(frame -> frame.getClassName().equals(OffsetMapCodecManager.class.getName())
                            && frame.getMethodName().equals("getCounterMeterForEncoding"));
        }
    }
}
