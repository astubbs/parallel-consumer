package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope;
import bz.stub.parallelconsumer.offsets.UnknownOffsetMetadataMagicException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.util.Base64;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static bz.stub.parallelconsumer.offsets.OffsetCodecTestUtils.magicByteOfAnEncodingThatDoesNotExistYet;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * The forward-compatibility contract of the opaque rider, proved against a real broker rather than argued: a payload
 * carrying <b>both</b> a hole map and a rider, read by a Parallel Consumer that does not know the envelope's magic
 * byte, degrades exactly the way astubbs#207's {@link InvalidOffsetMetadataHandlingPolicy} designed - {@code IGNORE}
 * warns and replays from the committed offset, {@code FAIL} refuses the assignment.
 *
 * <h2>Why an old reader is simulated rather than run</h2>
 * There is no released artifact that predates the envelope to run here, and no second classloader to run one in. What
 * an old reader <em>sees</em>, though, is fully determined by one byte: a leading magic byte its {@code OffsetEncoding}
 * does not claim. So a plain {@link KafkaConsumer} rewrites that byte in the committed metadata and commits it back,
 * and this build - whose {@code magicByteOfAnEncodingThatDoesNotExistYet()} names a byte no encoding claims - reads the
 * result through the same unknown-magic path an old reader would. The hazard for a <em>released</em> PC is worse than
 * this (0.5.2.6 to 0.5.3.3 throw a bare {@code RuntimeException} upstream of any policy, so the failure is a crash-loop
 * on durable state), and that half rests on reading 0.5.3.3's source, not on a test. The PR body says so.
 *
 * <h2>The payload this transplants, and why it is the crash shape</h2>
 * The interesting payload is a hole map and a rider in the <b>same</b> string. That is not what an orderly, drained
 * shutdown leaves: a drained instance has nothing incomplete, so it commits a rider-only envelope, which proves the
 * envelope and nothing about the hole map.
 * <p>
 * So two records here are fed to a user function that always throws for them ({@link #NEVER_COMPLETE}). They are
 * incomplete for as long as the instance lives, every other record completes around them, and the periodic committer
 * therefore writes holes-plus-rider <em>while the instance is still running</em> - byte for byte the payload a crash
 * leaves behind. {@link #crashPayload} is captured from the broker at that moment, and everything below is built from
 * that capture. The first instance is then closed, purely so the group empties: a genuinely abandoned instance keeps
 * heartbeating, the group never becomes empty, and the Kafka coordinator rejects an offset commit from a non-member of
 * a live group - which is exactly what the plain-{@link KafkaConsumer} rewrite below is. Its close-time commit is
 * overwritten by the rewrite and never read.
 *
 * @author Antony Stubbs
 * @see OffsetRiderEnvelope
 * @see ForeignOffsetMetadataOnAssignmentTest the same degradation at the rebalance frame, without a broker
 */
@Slf4j
// Per-method timeout, following the rest of this package: without one, a thread blocked on a broker call becomes a
// job that runs to the CI-level timeout with no failing test to point at. Well above this test's own budget - the
// commit capture, two takeovers and their group joins.
@Timeout(600)
class OffsetRiderUpgradeDowngradeTest extends BrokerIntegrationTest<String, String> {

    /**
     * A fixed eight bytes, because this test is about the slot rather than its contents: PC never interprets a rider,
     * so anything that survives round trip byte for byte proves as much as a structured blob would.
     */
    static final byte[] RIDER = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF, 1, 2, 3, 4};

    static final int RECORD_COUNT = 20;

    /**
     * The two offsets whose user function always throws. The lower one becomes the committed offset (nothing below it
     * is outstanding); the higher one is the hole ABOVE it, which is the part a bare committed offset cannot express
     * and the hole map must carry.
     */
    static final long BLOCKING_OFFSET = 3L;

    static final long HOLE_ABOVE_THE_BASE = 7L;

    static final Set<Long> NEVER_COMPLETE = UniSets.of(BLOCKING_OFFSET, HOLE_ABOVE_THE_BASE);

    String groupId;

    TopicPartition tp;

    /**
     * What the broker held while the first instance was still running with holes in flight - the payload a crash
     * leaves. Captured once, and every arm below is built from this one capture.
     */
    OffsetAndMetadata crashPayload;

    /**
     * The same commit with the leading magic byte rewritten: what an old reader sees.
     */
    String downgradedMetadata;

    /**
     * Offsets the first instance genuinely completed, so the no-loss claim at the end can be about every record
     * produced rather than only the ones the replacement saw.
     */
    final SortedSet<Long> completedByTheCrashedInstance = new ConcurrentSkipListSet<>();

    /**
     * Runs the first instance until the broker holds a commit carrying holes and a rider together, captures it, and
     * rewrites its leading magic byte.
     */
    @SneakyThrows
    private void writeAndCaptureTheCrashPayload() {
        setupTopic(OffsetRiderUpgradeDowngradeTest.class.getSimpleName());
        tp = new TopicPartition(getTopic(), partitionNumber);
        // the topic name already carries this run's nonce, so the group id inherits it rather than
        // drawing a second one - a group shared between two runs would read the other run's commit
        groupId = "rider-upgrade-downgrade-" + getTopic();

        getKcu().produceMessages(getTopic(), RECORD_COUNT);

        var consumer = getKcu().<String, String>createNewConsumer(groupId);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(UNORDERED)
                .maxConcurrency(4)
                .commitInterval(ofMillis(200))
                // clone: the supplier's contract is that PC does not interpret what it returns, and handing out the
                // same array every call would let a defect in PC's own copying pass unnoticed here
                .riderSupplier(riderContext -> RIDER.clone())
                .build();
        var crashing = new ParallelEoSStreamProcessor<String, String>(options);
        register(crashing);
        crashing.subscribe(UniSets.of(getTopic()));
        crashing.poll(pollContext -> {
            long offset = pollContext.offset();
            if (NEVER_COMPLETE.contains(offset)) {
                throw new FakeRuntimeException("offset " + offset + " never completes, so the commit carries holes");
            }
            // a retried batch or an overlap around the poll can redeliver an already-completed offset to this same
            // instance; the set's dedup absorbs the repeat, so a false return here is expected, not a bug
            boolean ignoredWasNew = completedByTheCrashedInstance.add(offset);
        });

        await().alias("a commit carrying both a hole map and a rider")
                .atMost(ofSeconds(120))
                .failFast("the writing instance died", crashing::isClosedOrFailed)
                .untilAsserted(() -> assertHolesAndARiderAreCommitted(readCommitted()));

        crashPayload = readCommitted();
        assertHolesAndARiderAreCommitted(crashPayload);

        byte[] raw = Base64.getDecoder().decode(crashPayload.metadata());
        assertWithMessage("the captured payload must really be an envelope - otherwise the byte rewritten below is not the "
                        + "envelope's, and this test proves nothing about the rider")
                .that(raw[0])
                .isEqualTo(OffsetRiderEnvelope.MAGIC_BYTE);
        raw[0] = magicByteOfAnEncodingThatDoesNotExistYet();
        downgradedMetadata = Base64.getEncoder().encodeToString(raw);

        // Only to empty the consumer group - see the class javadoc. The commit this makes is overwritten below.
        crashing.close();
    }

    /**
     * The two claims that make this payload the interesting one: a rider came back whole, and it came back alongside
     * a hole the committed offset alone cannot express.
     */
    private void assertHolesAndARiderAreCommitted(OffsetAndMetadata committed) throws Exception {
        assertWithMessage("nothing committed for %s yet", tp).that(committed).isNotNull();
        assertWithMessage("the commit must stop at the lowest offset that never completes")
                .that(committed.offset())
                .isEqualTo(BLOCKING_OFFSET);

        var rider = OffsetMapCodecManager.decodeRider(committed.offset(), committed.metadata(), IGNORE);
        assertThat(rider.getState()).isEqualTo(OffsetRiderEnvelope.RiderState.PRESENT);
        assertWithMessage("the rider must survive the round trip through the broker byte for byte")
                .that(rider.getBytes())
                .isEqualTo(RIDER);

        var offsets = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(committed.offset(),
                committed.metadata(), IGNORE);
        assertWithMessage("holes AND a rider in the same payload - a drained shutdown would leave a rider-only envelope")
                .that(offsets.getIncompleteOffsets())
                .contains(HOLE_ABOVE_THE_BASE);
    }

    /**
     * Puts the downgraded payload back into the group with a plain {@link KafkaConsumer}, which is how an old reader
     * is simulated on one classpath. The group is empty at this point, which is what lets a non-member commit.
     */
    private void commitTheDowngradedPayload() {
        try (KafkaConsumer<String, String> rewriter = getKcu().createNewConsumer(groupId)) {
            rewriter.commitSync(UniMaps.of(tp, new OffsetAndMetadata(crashPayload.offset(), downgradedMetadata)));
        }
    }

    @SneakyThrows
    private OffsetAndMetadata readCommitted() {
        return getKcu().getAdmin()
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get()
                .get(tp);
    }

    private ParallelEoSStreamProcessor<String, String> takeOverTheGroup(InvalidOffsetMetadataHandlingPolicy policy) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(getKcu().<String, String>createNewConsumer(groupId))
                .ordering(UNORDERED)
                .maxConcurrency(4)
                .commitInterval(ofMillis(200))
                .invalidOffsetMetadataPolicy(policy)
                .build();
        var takingOver = new ParallelEoSStreamProcessor<String, String>(options);
        register(takingOver);
        takingOver.subscribe(UniSets.of(getTopic()));
        return takingOver;
    }

    /**
     * AE6's {@code IGNORE} half, and the reason the default is {@code IGNORE}: an unreadable payload costs replayed
     * records, never lost ones. The replacement resumes at the committed offset and every record produced is processed
     * at least once across the two instances.
     */
    @Test
    void anOldReaderUnderIgnoreReplaysFromTheCommittedOffsetAndLosesNothing() {
        writeAndCaptureTheCrashPayload();
        commitTheDowngradedPayload();

        var processedByTheReplacement = new ConcurrentSkipListSet<Long>();
        var replacement = takeOverTheGroup(IGNORE);
        replacement.poll(pollContext -> {
            // IGNORE replays from the committed offset, so the replacement can see an offset more than once; the
            // set's dedup absorbs the repeat and the no-loss assertion below only needs the distinct offsets
            boolean ignoredWasNew = processedByTheReplacement.add(pollContext.offset());
        });

        await().alias("the replacement replays everything from the committed offset up")
                .atMost(ofSeconds(120))
                .failFast("the replacement died - IGNORE must not stop", replacement::isClosedOrFailed)
                .untilAsserted(() -> assertThat(processedByTheReplacement)
                        .containsAtLeastElementsIn(offsetsFrom(crashPayload.offset())));

        var everythingProcessed = new TreeSet<Long>(completedByTheCrashedInstance);
        everythingProcessed.addAll(processedByTheReplacement);
        assertWithMessage("no record may be lost: what the crashed instance completed plus what the replacement replayed "
                        + "has to cover every offset produced")
                .that(everythingProcessed)
                .containsAtLeastElementsIn(offsetsFrom(0));
    }

    /**
     * The other half of the same contract: {@code FAIL} exists so a deployment can refuse to silently replay, so it
     * has to actually stop - and the typed exception has to reach the operator rather than being swallowed by the
     * rebalance callback's recovery, which is astubbs#207's defect one layer down.
     */
    @Test
    void anOldReaderUnderFailRefusesTheAssignment() {
        writeAndCaptureTheCrashPayload();
        commitTheDowngradedPayload();

        var refusing = takeOverTheGroup(FAIL);
        refusing.poll(pollContext -> log.error("FAIL must never reach the user function - offset {}",
                pollContext.offset()));

        await().alias("FAIL stops rather than replaying a payload it cannot read")
                .atMost(ofSeconds(120))
                .until(refusing::isClosedOrFailed);

        assertWithMessage("the operator has to be told which encoding was unreadable, not just that PC stopped")
                .that(describeCauseChain(refusing.getFailureCause()))
                .contains(UnknownOffsetMetadataMagicException.class.getName());
    }

    /**
     * Every offset the test produced, from {@code fromInclusive} up. Built rather than hard coded so the no-loss
     * assertion moves with {@link #RECORD_COUNT}.
     */
    private static SortedSet<Long> offsetsFrom(long fromInclusive) {
        var offsets = new TreeSet<Long>();
        for (long offset = fromInclusive; offset < RECORD_COUNT; offset++) {
            // the loop offset strictly increases, so this can never be false; naming it keeps that invariant
            // visible instead of leaving a discarded boolean that reads the same whether it was checked or not
            boolean ignoredWasNew = offsets.add(offset);
        }
        return offsets;
    }

    /**
     * The whole cause chain's class names, because the typed exception arrives several wrappers deep: Kafka wraps a
     * throwing rebalance listener, and PC's supervisor wraps the poller's failure again. Asserting on the chain rather
     * than on the outermost type is the difference between pinning the contract and pinning today's wrapping.
     */
    private static String describeCauseChain(Throwable failure) {
        var rendered = new StringBuilder();
        for (Throwable link = failure; link != null && link != link.getCause(); link = link.getCause()) {
            rendered.append(link.getClass().getName()).append(": ").append(link.getMessage()).append('\n');
        }
        return rendered.toString();
    }
}
