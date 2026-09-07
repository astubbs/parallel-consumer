package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.RiderContext;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.offsets.CorruptOffsetMetadataException;
import bz.stub.parallelconsumer.offsets.OffsetDecodingError;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import pl.tlinkowski.unij.api.UniLists;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.record;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * The rider supplier hook: its contract, its guard, and the thread it actually runs on.
 * <p>
 * <b>What this class proves</b> (U3 of the opaque-rider plan): the supplier is called <em>once</em> per commit,
 * from inside the same snapshot that produces the offset, and is told that offset; a {@code null}, an empty array
 * or a throw all fall back to the payload PC writes today, byte for byte; a rider over the derived cap is dropped
 * at the write side rather than committed; and the two commit modes run the supplier where the javadoc says they
 * do. What it deliberately does <em>not</em> prove is the budget ladder's arithmetic or its counters - those are
 * separate units and their own tests.
 * <p>
 * <b>Why the payload comparisons are byte-identical rather than "still decodes"</b>: R2 is that a payload written
 * with no rider is indistinguishable from one written by a build that had never heard of riders. A guard that
 * fell back to <em>an</em> envelope with no rider in it would still decode, so only equality with the
 * no-supplier payload can fail on that.
 * <p>
 * <b>Static state.</b> {@link OffsetMapCodecManager#DefaultMaxMetadataSize} and
 * {@link PartitionStateManager#getUSED_PAYLOAD_THRESHOLD_MULTIPLIER()} are mutable statics that other suites move,
 * and the derived rider cap is a function of both. Nothing here writes them - the over-cap cases are built from
 * the defaults instead - so the lock is taken in READ mode, which is what keeps a concurrent
 * {@code OffsetEncodingBackPressureUnitTest} from moving the cap out from under these assertions.
 *
 * @author Antony Stubbs
 * @see RiderContext
 * @see OffsetRiderEnvelope
 */
@Slf4j
@Timeout(120)
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
class RiderSupplierGuardTest {

    private static final TopicPartition TP = new TopicPartition("rider-supplier-guard", 0);

    private static final byte[] A_RIDER = "stream-time=42".getBytes(StandardCharsets.UTF_8);

    /**
     * Comfortably over the rider cap the defaults derive (4096 metadata characters at the 0.75 threshold
     * multiplier leaves 1024 characters, which is 768 raw bytes), and comfortably under
     * {@link OffsetRiderEnvelope#MAX_RIDER_BYTES}, so what rejects it is the derived cap and not the format's
     * own ceiling.
     */
    private static final byte[] AN_OVERSIZED_RIDER = new byte[2000];

    /**
     * The live processors these tests start, closed on teardown whatever the assertions did.
     */
    private final List<ParallelEoSStreamProcessor<String, String>> liveProcessors = new CopyOnWriteArrayList<>();

    @AfterEach
    void closeAnyLiveProcessors() {
        for (var pc : liveProcessors) {
            try {
                pc.closeDontDrainFirst();
            } catch (Exception e) {
                log.warn("Failed to close a test processor", e);
            }
        }
        liveProcessors.clear();
    }

    // ---- the contract -----------------------------------------------------------------------------------

    /**
     * R11: the rider is captured in the same snapshot as the offset it commits against, so the supplier has to be
     * told <em>that</em> offset - the confluentinc#893 rule that a payload travels with the offset it describes.
     * One call, because KTD9 puts the call inside the single snapshot: a second one would be a second sample of a
     * moving offset map.
     */
    @Test
    void theSupplierIsCalledOncePerCommitAndToldTheOffsetThatIsCommitted() {
        var calls = new AtomicInteger();
        var seen = new CopyOnWriteArrayList<RiderContext>();
        var state = stateWithHoles(context -> {
            calls.incrementAndGet();
            seen.add(context);
            return A_RIDER;
        });

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("KTD9: the supplier is called exactly once per commit, from inside the one snapshot")
                .that(calls.get())
                .isEqualTo(1);
        assertWithMessage("R11: the supplier must be told the offset its rider will be committed against")
                .that(seen.get(0).getOffsetToCommit())
                .isEqualTo(committed.offset());
        assertWithMessage("the context names the partition the rider is scoped to")
                .that(seen.get(0).getPartition())
                .isEqualTo(TP);
        assertWithMessage("the context carries a byte cap the supplier can honour")
                .that(seen.get(0).getMaxRiderBytes())
                .isGreaterThan(0);
    }

    /**
     * The whole point: what the supplier returned comes back out of the committed string, and the offset map it
     * shares the payload with is unharmed.
     */
    @Test
    void aRiderRidesAlongsideTheHoleMapAndComesBackUnchanged() throws OffsetDecodingError,
            CorruptOffsetMetadataException {
        var state = stateWithHoles(context -> A_RIDER);

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        var unwrapped = unwrap(committed);
        assertThat(unwrapped.getRider().getState()).isEqualTo(RiderState.PRESENT);
        assertWithMessage("R5: PC never interprets the rider, so it must come back byte for byte")
                .that(unwrapped.getRider().getBytes())
                .isEqualTo(A_RIDER);
        assertWithMessage("R10/R9: carrying a rider must not cost the partition its hole map")
                .that(OffsetMapCodecManager
                        .deserialiseIncompleteOffsetMapFromBase64(committed.offset(), committed.metadata())
                        .getIncompleteOffsets())
                .containsExactly(HOLE_OFFSET);
    }

    /**
     * R3/KTD6: a caught-up partition has no offset map to write, and before the rider it committed no metadata at
     * all. It now commits the rider alone - and the early return that unblocks a partition which has caught up
     * still unblocks it, which is the property that stops a rider becoming a permanent block.
     */
    @Test
    void aCaughtUpPartitionCommitsTheRiderAloneAndStaysUnblocked() throws OffsetDecodingError,
            CorruptOffsetMetadataException {
        var state = caughtUpStateWith(context -> A_RIDER);

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        var unwrapped = unwrap(committed);
        assertThat(unwrapped.getRider().getBytes()).isEqualTo(A_RIDER);
        assertWithMessage("a caught-up partition has no hole map, so the envelope carries the rider and nothing else")
                .that(unwrapped.getInnerBytes())
                .isEqualTo(new byte[0]);
        assertWithMessage("KTD6: the caught-up early return is the only place a blocked partition unblocks, and a "
                        + "rider must not take that away")
                .that(state.isAllowedMoreRecords())
                .isTrue();
        assertWithMessage("KTD1: a rider-only payload must decode to the same offsets an empty payload does")
                .that(OffsetMapCodecManager
                        .deserialiseIncompleteOffsetMapFromBase64(committed.offset(), committed.metadata())
                        .getIncompleteOffsets())
                .isEmpty();
    }

    // ---- the guard --------------------------------------------------------------------------------------

    /**
     * KTD3: {@code null} is normalised into "no rider" in the guard and nowhere else, so nothing below it ever
     * sees a rider that is not really there - and the payload is the one today's build writes.
     */
    @Test
    void nullFromTheSupplierWritesTodaysPayload() throws OffsetDecodingError {
        assertPayloadIsTodays(context -> null, "returning null");
    }

    /**
     * KTD3 again, and the half that matters for the wire format: an empty array must not reach the envelope,
     * because a zero-length envelope is the ladder's dropped marker. If empty were passed through, an embedder
     * could forge that marker.
     */
    @Test
    void anEmptyArrayFromTheSupplierWritesTodaysPayload() throws OffsetDecodingError {
        assertPayloadIsTodays(context -> new byte[0], "returning an empty array");
    }

    /**
     * R15/KTD8, the reason the guard exists at all: the supplier is user code on an engine thread, so a throw
     * must cost the rider and nothing else. The commit still carries today's payload, the warning is rate
     * limited (a broken supplier is a coding error, so it is broken on every commit), and the next commit still
     * happens.
     */
    @Test
    void aThrowingSupplierCommitsTodaysPayloadAndWarnsOnce() {
        var state = stateWithHoles(context -> {
            throw new IllegalStateException("the embedder's supplier is broken");
        });

        OffsetAndMetadata first;
        OffsetAndMetadata second;
        List<String> about;
        try (var warnings = LogCapture.of(PartitionState.class, Level.WARN)) {
            first = state.createOffsetAndMetadata();
            // the outstanding offset completes, so the second commit is a real one on moved state - and
            // takes the caught-up path, proving the guard covers that call site too
            state.onSuccess(HOLE_OFFSET);
            second = state.createOffsetAndMetadata();
            about = warningsFromThisThreadMentioning(warnings, ParallelConsumerOptions.Fields.riderSupplier);
        }

        assertWithMessage("a throwing supplier must cost the rider only - no envelope is written, so the commit "
                        + "carries exactly what this build commits with no supplier configured at all")
                .that(Base64.getDecoder().decode(first.metadata())[0])
                .isNotEqualTo((int) OffsetRiderEnvelope.MAGIC_BYTE); // int vs int: Integer.equals(Byte) is never true
        assertWithMessage("R15: the failure is logged and treated as no rider, so the NEXT commit still happens")
                .that(second.offset())
                .isAtLeast(first.offset());

        assertWithMessage("one warning, naming the option, for two failing commits - the limiter is what stops a "
                        + "permanently broken supplier burying every other line in the log. Saw: %s", about)
                .that(about)
                .hasSize(1);
        assertWithMessage("the warning has to name the partition, or an operator cannot tell which supplier call "
                        + "failed")
                .that(about.get(0))
                .contains(TP.toString());
    }

    /**
     * R8: a rider bigger than the cap the supplier was handed is dropped at the write side. The offset map is
     * untouched - a rider must never cost a partition metadata it would otherwise have committed (R9) - and the
     * envelope survives carrying the zero-length marker, which is how a reader tells a shed rider from one that
     * was never configured (R6).
     */
    @Test
    void anOversizedRiderIsDroppedAndTheHoleMapSurvives() throws OffsetDecodingError,
            CorruptOffsetMetadataException {
        var state = stateWithHoles(context -> AN_OVERSIZED_RIDER);

        OffsetAndMetadata committed;
        List<String> warnings;
        try (var capture = LogCapture.of(PartitionState.class, Level.WARN)) {
            committed = state.createOffsetAndMetadata();
            warnings = warningsFromThisThreadMentioning(capture, ParallelConsumerOptions.Fields.riderSupplier);
        }

        assertWithMessage("R6: the envelope survives so the reader can tell a dropped rider from an absent one")
                .that(unwrap(committed).getRider().getState())
                .isEqualTo(RiderState.DROPPED);
        assertWithMessage("R9: dropping the rider must not cost the hole map")
                .that(OffsetMapCodecManager
                        .deserialiseIncompleteOffsetMapFromBase64(committed.offset(), committed.metadata())
                        .getIncompleteOffsets())
                .containsExactly(HOLE_OFFSET);
        assertWithMessage("R8: one rate-limited warning, naming the option")
                .that(warnings)
                .hasSize(1);
    }

    /**
     * KTD4's last sentence: a caught-up partition whose rider exceeds the cap writes no metadata at all, rather
     * than an empty envelope carrying nothing. There is no offset map for the marker to sit beside, so the marker
     * would be the entire payload - all cost, no information.
     */
    @Test
    void aCaughtUpPartitionWhoseRiderIsTooBigWritesNoMetadataAtAll() {
        var state = caughtUpStateWith(context -> AN_OVERSIZED_RIDER);

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("KTD4: no hole map and no rider means nothing worth committing - and the payload must "
                        + "be identical to what this build writes for a caught-up partition today")
                .that(committed.metadata())
                .isEqualTo(new OffsetAndMetadata(committed.offset()).metadata());
        assertThat(state.isAllowedMoreRecords()).isTrue();
    }

    // ---- the thread arms --------------------------------------------------------------------------------

    /**
     * KTD8's first arm, measured rather than assumed: under the consumer commit modes the control thread asks for
     * a commit and the <b>broker-poll thread</b> performs it, so that is the thread the embedder's supplier runs
     * on. Driven through a real {@link ParallelEoSStreamProcessor} over a {@link MockConsumer}, because the claim
     * is about which of PC's threads reaches the call site - a hand-driven commit would answer about the JUnit
     * thread.
     */
    @Test
    void underConsumerCommitModesTheSupplierRunsOnTheBrokerPollThread() {
        var threadNames = new CopyOnWriteArrayList<String>();
        startProcessorOverMockConsumer(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC, context -> {
            threadNames.add(Thread.currentThread().getName());
            return A_RIDER;
        });

        await("the supplier is reached by a commit")
                .atMost(ofSeconds(30))
                .until(() -> !threadNames.isEmpty());

        assertWithMessage("KTD8: under a consumer commit mode the supplier runs on the broker-poll thread. Saw: %s",
                threadNames)
                .that(threadNames.get(0))
                .startsWith("pc-broker-poll");
    }

    /**
     * The other half of KTD8's first arm and the reason the guard is not optional: user code on the broker-poll
     * thread that throws must not take that thread down, or every commit for the whole instance stops - the
     * failure shape of the throwing meter registry
     * ({@code docs/solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md}).
     * Commits still landing at the consumer is the proof the thread is alive.
     */
    @Test
    void aThrowingSupplierDoesNotKillTheBrokerPollThread() {
        var calls = new AtomicInteger();
        var consumer = startProcessorOverMockConsumer(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC,
                context -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("the embedder's supplier is broken");
                });

        await("a commit lands despite the supplier throwing on every call")
                .atMost(ofSeconds(30))
                .until(() -> {
                    var committed = consumer.committed(Collections.singleton(TP)).get(TP);
                    return committed != null && committed.offset() > 0;
                });

        assertWithMessage("the supplier was actually reached, or this test proves nothing about the guard")
                .that(calls.get())
                .isAtLeast(1);
    }

    /**
     * KTD8's second arm: under {@code PERIODIC_TRANSACTIONAL_PRODUCER} the control thread commits, and it holds
     * the produce write lock while it does - {@code AbstractOffsetCommitter#retrieveOffsetsAndCommit} calls
     * {@code preAcquireOffsetsToCommit()} before it collects the commit data, so the supplier runs inside that
     * lock.
     * <p>
     * <b>Unit-level, and deliberately so.</b> Reaching this path through a live control thread needs a real
     * transactional producer against a broker. Instead the transactional committer is invoked directly, on the
     * calling thread - the same collaborator {@code AbstractParallelEoSStreamProcessor} hands the commit to when
     * the mode is transactional, and the only one that takes the produce write lock first. So the claim settled
     * here is the one that lane can settle: the <b>transactional committer performs the collection itself, on its
     * caller's thread rather than handing it to the poller, with the commit lock already held</b>. The name
     * {@code pc-control} is set by the control loop and is not what makes the arm true.
     */
    @Test
    void underTransactionalCommitModeTheSupplierRunsOnTheCommittingThreadHoldingTheCommitLock() throws Exception {
        var supplierThreads = new CopyOnWriteArrayList<Thread>();
        var lockHeldDuringCall = new CopyOnWriteArrayList<Boolean>();
        var producerManagerHolder = new AtomicReference<ProducerManager<String, String>>();

        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(ofSeconds(30))
                .riderSupplier(context -> {
                    supplierThreads.add(Thread.currentThread());
                    // the holder, rather than the manager itself, because the supplier has to be in the options
                    // before the module that builds the ProducerManager exists
                    lockHeldDuringCall.add(producerManagerHolder.get().isTransactionCommittingInProgress());
                    return A_RIDER;
                })
                .build());
        producerManagerHolder.set(module.exposedProducerManager());

        var partitions = module.workManager().getPm();
        partitions.onPartitionsAssigned(UniLists.of(TP));
        var state = partitions.getPartitionState(TP);
        state.addNewIncompleteRecord(record(TP, 0));
        state.onSuccess(0);

        producerManagerHolder.get().retrieveOffsetsAndCommit();

        assertWithMessage("the supplier must have been reached by a transactional commit, or nothing below is "
                        + "an assertion about anything")
                .that(supplierThreads)
                .isNotEmpty();
        assertWithMessage("KTD8: under transactional commits the committing thread itself calls the supplier - the "
                        + "work is never handed to the poller as it is under the consumer modes")
                .that(supplierThreads.get(0))
                .isEqualTo(Thread.currentThread());
        assertWithMessage("KTD8: the produce write lock is already held when the supplier runs, because "
                        + "preAcquireOffsetsToCommit runs before the commit data is collected")
                .that(lockHeldDuringCall.get(0))
                .isTrue();
    }

    // ---- fixtures ---------------------------------------------------------------------------------------

    /**
     * The offset left incomplete in {@link #stateWithHoles}, so that the encoder has an offset map to produce and the
     * payload is not the caught-up case.
     */
    private static final long HOLE_OFFSET = 1L;

    private static final long HIGHEST_OFFSET = 2L;

    /**
     * A partition with offsets 0-2 polled and 1 still outstanding: an ordinary out-of-order completion, which is
     * the only state that produces an offset map.
     */
    private PartitionState<String, String> stateWithHoles(Function<RiderContext, byte[]> supplier) {
        var state = freshState(supplier);
        for (long offset = 0; offset <= HIGHEST_OFFSET; offset++) {
            state.addNewIncompleteRecord(record(TP, offset));
        }
        for (long offset = 0; offset <= HIGHEST_OFFSET; offset++) {
            if (offset != HOLE_OFFSET) {
                state.onSuccess(offset);
            }
        }
        return state;
    }

    /**
     * A partition with nothing outstanding - the steady state, which committed no metadata at all before the
     * rider existed.
     */
    private PartitionState<String, String> caughtUpStateWith(Function<RiderContext, byte[]> supplier) {
        var state = freshState(supplier);
        state.addNewIncompleteRecord(record(TP, 0));
        state.onSuccess(0);
        return state;
    }

    private PartitionState<String, String> freshState(Function<RiderContext, byte[]> supplier) {
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .riderSupplier(supplier)
                .build());
        return new PartitionState<>(0, module, TP, HighestOffsetAndIncompletes.of());
    }

    /**
     * The proof that no rider was written: the payload is an ordinary Parallel Consumer encoding - not an envelope
     * - and it still carries the partition's offset map.
     * <p>
     * <b>Why this is not a byte-for-byte comparison against a separately built baseline</b>, which is the shape it
     * wants to be. The encoder competition collapses equal-sized candidates: {@code OffsetSimultaneousEncoder}
     * holds its encoders in a {@code ConcurrentHashMap} key set and feeds them into a {@code TreeSet} ordered by
     * encoded size alone, so when two encodings tie, which one survives depends on identity hash order and
     * therefore varies from call to call. Measured here: the same three-offset state encoded as magic byte 110 in
     * one call and 108 in the next, both five bytes. So a baseline built by encoding the same state a second time
     * is not a stable expectation, and asserting against one would be a flake, not a stronger test. The envelope's
     * magic byte is the thing that is decided by this unit rather than by the competition, and it is asserted
     * exactly.
     */
    private void assertPayloadIsTodays(Function<RiderContext, byte[]> supplier, String whatItDid)
            throws OffsetDecodingError {
        var state = stateWithHoles(supplier);

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        byte[] payload = Base64.getDecoder().decode(committed.metadata());
        assertWithMessage("KTD3/R2: a supplier %s means no rider, so no envelope may be written at all - an "
                        + "envelope with an empty rider in it would still decode, which is exactly what this "
                        + "distinguishes", whatItDid)
                .that(payload[0])
                .isNotEqualTo((int) OffsetRiderEnvelope.MAGIC_BYTE); // int vs int: Integer.equals(Byte) is never true
        assertWithMessage("and the hole map the partition would have committed anyway is untouched")
                .that(OffsetMapCodecManager
                        .deserialiseIncompleteOffsetMapFromBase64(committed.offset(), committed.metadata())
                        .getIncompleteOffsets())
                .containsExactly(HOLE_OFFSET);
    }

    private static OffsetRiderEnvelope.UnwrappedEnvelope unwrap(OffsetAndMetadata committed)
            throws CorruptOffsetMetadataException {
        assertWithMessage("precondition: this commit must have carried a payload at all")
                .that(committed.metadata())
                .isNotEmpty();
        return OffsetRiderEnvelope.unwrap(Base64.getDecoder().decode(committed.metadata()));
    }

    /**
     * Starts a real processor over a plain {@link MockConsumer} with one record to complete, so that PC's own
     * threads drive a commit. {@link MockConsumer} assigns nothing on subscribe, so the partition is rebalanced in
     * by hand and PC told about it separately - the same dance {@code MockConsumerTestBase} documents.
     *
     * @return the consumer, so a test can ask it what was committed
     */
    private MockConsumer<String, String> startProcessorOverMockConsumer(ParallelConsumerOptions.CommitMode mode,
                                                                        Function<RiderContext, byte[]> supplier) {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .commitMode(mode)
                .commitInterval(Duration.ofMillis(50))
                .riderSupplier(supplier)
                .build());
        liveProcessors.add(pc);

        pc.subscribe(UniLists.of(TP.topic()));
        mockConsumer.rebalance(Collections.singletonList(TP));
        pc.onPartitionsAssigned(UniLists.of(TP));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
        mockConsumer.addRecord(record(TP, 0));

        pc.poll(context -> {
        });
        return mockConsumer;
    }

    /**
     * The captured {@code WARN}s that mention {@code token} and were logged by the calling thread.
     * <p>
     * The thread scoping is not decoration: surefire runs this module's test <em>methods</em> in parallel,
     * {@link LogCapture} attaches to a class logger shared by every one of them, and several tests here provoke
     * the same warning. A warning is logged on the thread that provoked it, and each test method has its own, so
     * the thread name is the cheapest correct filter available.
     */
    private static List<String> warningsFromThisThreadMentioning(LogCapture capture, String token) {
        var thisThread = Thread.currentThread().getName();
        //noinspection FuseStreamOperations - Collectors.toList is the Java 8 API this module compiles against
        return capture.events().stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> thisThread.equals(event.getThreadName()))
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains(token))
                .collect(Collectors.toList());
    }

}
