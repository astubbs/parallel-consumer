package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ManagedTruth.assertTruth;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;

@Slf4j
public class OffsetEncodingTests extends ParallelEoSStreamProcessorTestBase {

    PCModuleTestEnv module = new PCModuleTestEnv();

    /**
     * The {@link OffsetEncoding}s that cannot represent the scenario built by
     * {@link #ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential} - a highest succeeded offset of
     * 72,770 with a long unbroken run of incompletes underneath it - and so fall back instead of round tripping. Two
     * distinct mechanisms put codecs in this list:
     * <ul>
     *     <li><b>v1 short overflow</b> - {@link OffsetEncoding#BitSet}, {@link OffsetEncoding#BitSetCompressed},
     *     {@link OffsetEncoding#RunLength} and {@link OffsetEncoding#RunLengthCompressed} encode their length (bitset
     *     bit count / run length) as a {@code short}, so they cannot express anything past {@link Short#MAX_VALUE}
     *     (32,767). The encoders detect the overflow while encoding and drop themselves from the candidate set: the
     *     bitset limit is the {@code Short.MAX_VALUE} guard in {@link BitSetEncoder}'s {@code initV1}, and the
     *     run-length limit is {@code MathUtils.toShortExact} throwing {@code RunLengthV1EncodingNotSupported} out of
     *     {@link RunLengthEncoder#serialise()}. Note it is neither {@link BitSetEncoder#MAX_LENGTH_ENCODABLE}, which
     *     is {@code Integer.MAX_VALUE} and is consulted only on the v2 path, nor {@link OffsetRunLength}, which is
     *     the decode side and carries no overflow detection at all - raising either would leave these codecs
     *     overflowing at 32,767 exactly as before.</li>
     *     <li><b>metadata size</b> - {@link OffsetEncoding#BitSetV2} encodes the length as an int, so it does not
     *     overflow, but uncompressed it needs one bit per offset in the range and the resulting payload exceeds
     *     {@link OffsetMapCodecManager#DefaultMaxMetadataSize} (4096 bytes), so it is rejected when the payload is
     *     written.</li>
     * </ul>
     * In both cases the forced codec produces nothing, no offset map is committed, and the reloaded partition state
     * falls back to what the bare committed offset alone can tell it.
     * <p>
     * This is the single source of truth for that set - {@link #isWorkingCodec(OffsetEncoding)} and the degraded
     * branches of the test both read it, rather than each restating the list.
     */
    private static final List<OffsetEncoding> CODECS_THAT_DEGRADE = UniLists.of(
            BitSet, BitSetCompressed, RunLength, RunLengthCompressed, // v1 short overflow
            BitSetV2 // too large for the metadata payload uncompressed
    );

    @Test
    void runLengthDeserialise() {
        var sb = ByteBuffer.allocate(3);
        sb.put((byte) 0); // magic byte placeholder, can ignore
        sb.putShort((short) 1);
        byte[] array = new byte[2];
        sb.rewind();
        sb.get(array);
        ByteBuffer wrap = ByteBuffer.wrap(array);
        byte b = wrap.get(); // simulate reading magic byte
        ByteBuffer slice = wrap.slice();
        List<Integer> integers = OffsetRunLength.runLengthDeserialise(slice);
        assertThat(integers).isEmpty();
    }

    /**
     * Triggers Short shortfall in BitSet encoder and tests encodable range of RunLength encoding - system should
     * gracefully drop runlength if it has Short overflows (too hard to measure every runlength of incoming records
     * before accepting?)
     * <p>
     * https://github.com/confluentinc/parallel-consumer/issues/37 Support BitSet encoding lengths longer than
     * Short.MAX_VALUE confluentinc#37
     * <p>
     * https://github.com/confluentinc/parallel-consumer/issues/35 RuntimeException when running with very high options
     * in 0.2.0.0 (Bitset too long to encode) confluentinc#35
     * <p>
     */
    @SneakyThrows
    @ParameterizedTest
    @ValueSource(longs = {
            10_000L,
            100_000L,
            100_000_0L,
//            100_000_000L, // very~ slow
    })
    @ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
    void largeIncompleteOffsetValues(long nextExpectedOffset) {
        long lowWaterMark = 123L;
        var incompletes = new TreeSet<>(UniSets.of(lowWaterMark, 2345L, 8765L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, nextExpectedOffset, incompletes);
        OffsetSimultaneousEncoder.compressionForced = true;

        //
        encoder.invoke();
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(smallestBytes);
        OffsetMapCodecManager.HighestOffsetAndIncompletes decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getIncompleteOffsets()).containsExactlyInAnyOrderElementsOf(incompletes);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetPair bitsetUnwrap = EncodedOffsetPair.unwrap(encoder.packEncoding(new EncodedOffsetPair(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                OffsetMapCodecManager.HighestOffsetAndIncompletes decodedBitsets = bitsetUnwrap.getDecodedIncompletes(lowWaterMark);
                assertThat(decodedBitsets.getIncompleteOffsets())
                        .as(encodingToUse.toString())
                        .containsExactlyInAnyOrderElementsOf(incompletes);
            } else {
                log.info("Encoding not performed: " + encodingToUse);
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

    /**
     * Verifying that encoding / decoding returns correct highest seen offset when nextExpectedOffset is below the
     * baseOffsetToCommit
     */
    @SneakyThrows
    @Test
    @ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
    void verifyEncodingWithNextExpectedBelowWatermark() {
        long baseOffsetToCommit = 123L;
        long highestSucceededOffset = 122L;
        var incompletes = new TreeSet<>(UniSets.of(2345L, 8765L)); // no incompletes below low watermark or next expected

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(baseOffsetToCommit, highestSucceededOffset, incompletes);
        OffsetSimultaneousEncoder.compressionForced = true;

        //
        encoder.invoke();
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(smallestBytes);
        OffsetMapCodecManager.HighestOffsetAndIncompletes decodedIncompletes = unwrap.getDecodedIncompletes(baseOffsetToCommit);
        assertThat(decodedIncompletes.getIncompleteOffsets()).isEmpty();
        assertThat(decodedIncompletes.getHighestSeenOffset().isPresent()).isTrue();
        assertThat(decodedIncompletes.getHighestSeenOffset().get()).isEqualTo(highestSucceededOffset);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetPair bitsetUnwrap = EncodedOffsetPair.unwrap(encoder.packEncoding(new EncodedOffsetPair(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                OffsetMapCodecManager.HighestOffsetAndIncompletes decodedBitsets = bitsetUnwrap.getDecodedIncompletes(baseOffsetToCommit);
                assertThat(decodedBitsets.getIncompleteOffsets()).isEmpty();
                assertThat(decodedBitsets.getHighestSeenOffset().isPresent()).isTrue();
                assertThat(decodedBitsets.getHighestSeenOffset().get()).isEqualTo(highestSucceededOffset);
            } else {
                log.info("Encoding not performed: " + encodingToUse);
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

    /**
     * Test for offset encoding when there is a very large range of offsets, and where the offsets aren't sequential.
     * <p>
     * There's no guarantee that offsets are always sequential. The most obvious case is with a compacted topic - there
     * will always be offsets missing.
     *
     * @see #ensureEncodingGracefullyWorksWhenOffsetsArentSequentialTwo
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource(OffsetEncoding.class)
    // needed due to static accessors in parallel tests
    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = READ)
    // depends on OffsetMapCodecManager#DefaultMaxMetadataSize
    @ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
    void ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential(OffsetEncoding encoding) {
        assumeThat(encoding)
                .as("Codec skipped, not applicable") // byte array not currently used
                .isNotIn(ByteArray, ByteArrayCompressed, KafkaStreams, KafkaStreamsV2);

        // todo don't use static public accessors to change things - makes parallel testing harder and is smelly
        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);
        OffsetSimultaneousEncoder.compressionForced = true;

        var records = new ArrayList<ConsumerRecord<String, String>>();
        final int FIRST_SUCCEEDED_OFFSET = 0;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, FIRST_SUCCEEDED_OFFSET, "akey", "avalue")); // will complete
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 1, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 4, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 5, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 69, "akey", "avalue")); // will complete
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 100, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 1_000, "akey", "avalue"));
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 20_000, "akey", "avalue")); // near upper limit of Short.MAX_VALUE
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 25_000, "akey", "avalue")); // will complete, near upper limit of Short.MAX_VALUE
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 30_000, "akey", "avalue")); // near upper limit of Short.MAX_VALUE

        // Extremely large tests for v2 encoders
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 40_000, "akey", "avalue")); // higher than Short.MAX_VALUE
        int avoidOffByOne = 2;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 40_000 + Short.MAX_VALUE + avoidOffByOne, "akey", "avalue")); // runlength higher than Short.MAX_VALUE
        int highestSucceeded = 40_000 + Short.MAX_VALUE + avoidOffByOne + 1;
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, highestSucceeded, "akey", "avalue")); // will complete to force whole encoding


        var incompleteRecords = new ArrayList<>(records);
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == FIRST_SUCCEEDED_OFFSET).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == 69).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == 25_000).findFirst().get());
        incompleteRecords.remove(incompleteRecords.stream().filter(x -> x.offset() == highestSucceeded).findFirst().get());

        List<Long> expected = incompleteRecords.stream().map(ConsumerRecord::offset)
                .sorted()
                .collect(Collectors.toList());

        //
        ktu.send(consumerSpy, records);

        //
        ParallelConsumerOptions<String, String> options = parallelConsumer.getWm().getOptions();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        TopicPartition tp = new TopicPartition(INPUT_TOPIC, 0);
        recordsMap.put(tp, new ArrayList<>(records));
        ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(recordsMap);

        // write offsets
        final ParallelConsumerOptions<String, String> newOptions = options.toBuilder().consumer(consumerSpy).build();
        final long FIRST_COMMITTED_OFFSET = 1L;
        {
            final PCModule<String, String> moduleTwo = new PCModule<>(newOptions);
            WorkManager<String, String> wmm = moduleTwo.workManager();
            wmm.onPartitionsAssigned(UniSets.of(new TopicPartition(INPUT_TOPIC, 0)));
            wmm.registerWork(new EpochAndRecordsMap<>(testRecords, wmm.getPm()));

            List<WorkContainer<String, String>> work = wmm.getWorkIfAvailable();
            assertThat(work).hasSameSizeAs(records);

            KafkaTestUtils.completeWork(wmm, work, FIRST_SUCCEEDED_OFFSET);

            KafkaTestUtils.completeWork(wmm, work, 69);

            KafkaTestUtils.completeWork(wmm, work, 25_000);

            KafkaTestUtils.completeWork(wmm, work, highestSucceeded);


            // make the commit
            var completedEligibleOffsets = wmm.collectCommitDataForDirtyPartitions();
            assertThat(completedEligibleOffsets.get(tp).offset()).isEqualTo(FIRST_COMMITTED_OFFSET);
            consumerSpy.commitSync(completedEligibleOffsets);

            {
                // check for graceful fall back to the smallest available encoder
                OffsetMapCodecManager<String, String> om = new OffsetMapCodecManager<>(module);
                OffsetMapCodecManager.forcedCodec = Optional.empty(); // turn off forced
                var state = wmm.getPm().getPartitionState(tp);
                String bestPayload = om.makeOffsetMetadataPayload(FIRST_COMMITTED_OFFSET, state);
                assertThat(bestPayload).isNotEmpty();
            }
        }

        // check
        {
            var committed = consumerSpy.committed(UniSets.of(tp)).get(tp);
            assertThat(committed.offset()).isEqualTo(FIRST_COMMITTED_OFFSET);

            if (isWorkingCodec(encoding)) {
                assertThat(committed.metadata()).isNotBlank();
            } else {
                // Degraded: the forced codec produced no encoding at all, so there is no offset map to attach -
                // the commit carries the bare offset and an empty metadata string. See CODECS_THAT_DEGRADE.
                assertThat(committed.metadata())
                        .as("a degraded codec commits the bare offset with no offset map")
                        .isEmpty();
            }
        }

        // simulate a rebalance or some sort of reset, by instantiating a new WM with the state from the last

        // read offsets
        {
            final PCModule<String, String> moduleThree = new PCModule<>(options);
            var newWm = moduleThree.workManager();
            newWm.onPartitionsAssigned(UniSets.of(tp));

            //
            var pm = newWm.getPm();
            var partitionState = pm.getPartitionState(tp);

            if (isWorkingCodec(encoding)) {
                // check state reloaded ok from consumer
                assertTruth(partitionState).getOffsetHighestSucceeded().isEqualTo(highestSucceeded);
            } else {
                // Degraded: with no offset map in the metadata there is nothing to reload, so the fresh state knows
                // only what the bare committed offset implies, and has not yet seen any record.
                assertDegradedReloadedState(partitionState, FIRST_COMMITTED_OFFSET, FIRST_COMMITTED_OFFSET - 1);
            }

            //
            ConsumerRecords<String, String> testRecordsWithBaseCommittedRecordRemoved = new ConsumerRecords<>(UniMaps.of(tp,
                    testRecords.records(tp)
                            .stream()
                            .filter(x ->
                                    x.offset() >= FIRST_COMMITTED_OFFSET)
                            .collect(Collectors.toList())));
            EpochAndRecordsMap<String, String> epochAndRecordsMap = new EpochAndRecordsMap<>(testRecordsWithBaseCommittedRecordRemoved, newWm.getPm());
            newWm.registerWork(epochAndRecordsMap);

            // Asserted once: nothing between here and the work retrieval below mutates partitionState, so a second
            // and third copy of these same checks would re-verify state that cannot have changed.
            if (isWorkingCodec(encoding)) {
                // check state reloaded ok from consumer
                assertTruth(partitionState).getOffsetHighestSequentialSucceeded().isEqualTo(FIRST_SUCCEEDED_OFFSET);

                assertTruth(partitionState).getOffsetHighestSucceeded().isEqualTo(highestSucceeded);

                long offsetHighestSeen = partitionState.getOffsetHighestSeen();
                assertThat(offsetHighestSeen).isEqualTo(highestSucceeded);

                var incompletes = partitionState.getIncompleteOffsetsBelowHighestSucceeded();
                Truth.assertThat(incompletes).containsExactlyElementsIn(expected);
            } else {
                // Degraded: re-polling the records lifts the highest *seen* offset back to the true high water mark,
                // but which of them had succeeded lived only in the offset map that was never committed, so the
                // highest *succeeded* offset stays where the bare committed offset left it.
                assertDegradedReloadedState(partitionState, FIRST_COMMITTED_OFFSET, highestSucceeded);
            }

            // check record is marked as incomplete
            var anIncompleteRecord = records.get(3);
            assertThat(partitionState.isRecordPreviouslyCompleted(anIncompleteRecord)).isFalse();


            var workRetrieved = newWm.getWorkIfAvailable();
            var workRetrievedOffsets = workRetrieved.stream().map(WorkContainer::offset).collect(Collectors.toList());
            assertTruth(workRetrieved).isNotEmpty();

            if (isWorkingCodec(encoding)) {
                Truth.assertWithMessage("Contains only incomplete records")
                        .that(workRetrievedOffsets)
                        .containsExactlyElementsIn(expected)
                        .inOrder();
            } else {
                // Degraded: no offset map survived, so the records that had already succeeded above the committed
                // offset (69, 25,000 and the highest succeeded) come back as work. Nothing is lost - work is repeated.
                var everyRecordFromTheCommittedOffsetUp = records.stream()
                        .map(ConsumerRecord::offset)
                        .filter(offset -> offset >= FIRST_COMMITTED_OFFSET)
                        .collect(Collectors.toList());
                // Pinning the exact ordered contents already determines everything else about this list - a further
                // doesNotContainSequence(expected) could not fail unless the assertion above had already failed.
                Truth.assertWithMessage("Degraded codec redelivers every record from the committed offset up, including the ones that had succeeded")
                        .that(workRetrievedOffsets)
                        .containsExactlyElementsIn(everyRecordFromTheCommittedOffsetUp)
                        .inOrder();
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

    /**
     * Whether the given {@link OffsetEncoding} can fully represent this test's scenario, i.e. is not one of
     * {@link #CODECS_THAT_DEGRADE}. This is a plain predicate, not a JUnit assumption - both branches are asserted.
     */
    private boolean isWorkingCodec(OffsetEncoding encoding) {
        return !CODECS_THAT_DEGRADE.contains(encoding);
    }

    /**
     * The state one of {@link #CODECS_THAT_DEGRADE} leaves behind after a rebalance: because no offset map was
     * committed, everything above the committed offset was forgotten. The highest succeeded (and so also the highest
     * *sequentially* succeeded) offset can only be one below the committed offset, and no incompletes are known. The
     * highest *seen* offset is the one thing that recovers, and only once the records have been polled again.
     *
     * @param committedOffset      the bare offset that was committed - all the reloaded state has to go on
     * @param expectedHighestSeen  {@code committedOffset - 1} before the records are re-registered, the true high water
     *                             mark afterwards
     */
    private void assertDegradedReloadedState(PartitionState<String, String> partitionState,
                                             long committedOffset,
                                             long expectedHighestSeen) {
        long lastKnownSucceeded = committedOffset - 1;
        assertTruth(partitionState).getOffsetHighestSucceeded().isEqualTo(lastKnownSucceeded);
        assertTruth(partitionState).getOffsetHighestSequentialSucceeded().isEqualTo(lastKnownSucceeded);
        assertThat(partitionState.getOffsetHighestSeen())
                .as("highest seen offset")
                .isEqualTo(expectedHighestSeen);
        Truth.assertWithMessage("no incompletes are known - the offset map that recorded them was never committed")
                .that(partitionState.getIncompleteOffsetsBelowHighestSucceeded())
                .isEmpty();
    }

    /**
     * This version of non sequential test just test the encoder directly, and is only half the story, as at the
     * encoding stage they don't know which offsets have never been seen, and assume simply working with continuous
     * ranges.
     * <p>
     * See more info in the class javadoc of {@link BitsetEncoder}.
     *
     * @see BitsetEncoder
     * @see #ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential
     */
    @SneakyThrows
    @Test
    @ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
    void ensureEncodingGracefullyWorksWhenOffsetsArentSequentialTwo() {
        long nextExpectedOffset = 101;
        long lowWaterMark = 0;
        var incompletes = new TreeSet<>(UniSets.of(1L, 4L, 5L, 100L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, nextExpectedOffset, incompletes);
        OffsetSimultaneousEncoder.compressionForced = true;

        //
        encoder.invoke();
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(smallestBytes);
        OffsetMapCodecManager.HighestOffsetAndIncompletes decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getIncompleteOffsets()).containsExactlyInAnyOrderElementsOf(incompletes);

        if (nextExpectedOffset - lowWaterMark > BitSetEncoder.MAX_LENGTH_ENCODABLE)
            assertThat(encodingMap.keySet()).as("Gracefully ignores that BitSet can't be supported").doesNotContain(OffsetEncoding.BitSet);
        else
            assertThat(encodingMap.keySet()).contains(OffsetEncoding.BitSet);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetPair bitsetUnwrap = EncodedOffsetPair.unwrap(encoder.packEncoding(new EncodedOffsetPair(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                OffsetMapCodecManager.HighestOffsetAndIncompletes decodedBitsets = bitsetUnwrap.getDecodedIncompletes(lowWaterMark);
                assertThat(decodedBitsets.getIncompleteOffsets())
                        .as(encodingToUse.toString())
                        .containsExactlyInAnyOrderElementsOf(incompletes);
            } else {
                log.info("Encoding not performed: " + encodingToUse);
            }
        }

        OffsetSimultaneousEncoder.compressionForced = false;
    }

}
