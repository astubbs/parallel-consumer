package io.confluent.parallelconsumer;

import io.confluent.csid.utils.KafkaTestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.OffsetEncoding.ByteArray;
import static io.confluent.parallelconsumer.OffsetEncoding.ByteArrayCompressed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class ContinuousEncodingTests extends ParallelEoSStreamProcessorTestBase {

    @SneakyThrows
    @ParameterizedTest
    @ValueSource(longs = {
            10_000L,
            100_000L,
            100_000_000L, // slow
    })
    void largeIncompleteOffsetValues(long nextExpectedOffset) {
        var incompletes = new HashSet<Long>();
        long lowWaterMark = 123L;
        incompletes.addAll(UniSets.of(lowWaterMark, 2345L, 8765L));

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(lowWaterMark, nextExpectedOffset, incompletes);
        encoder.compressionForced = true;

        //
        encoder.invoke();
        Map<OffsetEncoding, byte[]> encodingMap = encoder.getEncodingMap();

        //
        byte[] smallestBytes = encoder.packSmallest();
        EncodedOffsetPair unwrap = EncodedOffsetPair.unwrap(smallestBytes);
        ParallelConsumer.Tuple<Long, Set<Long>> decodedIncompletes = unwrap.getDecodedIncompletes(lowWaterMark);
        assertThat(decodedIncompletes.getRight()).containsExactlyInAnyOrderElementsOf(incompletes);

        //
        for (OffsetEncoding encodingToUse : OffsetEncoding.values()) {
            log.info("Testing {}", encodingToUse);
            byte[] bitsetBytes = encodingMap.get(encodingToUse);
            if (bitsetBytes != null) {
                EncodedOffsetPair bitsetUnwrap = EncodedOffsetPair.unwrap(encoder.packEncoding(new EncodedOffsetPair(encodingToUse, ByteBuffer.wrap(bitsetBytes))));
                ParallelConsumer.Tuple<Long, Set<Long>> decodedBitsets = bitsetUnwrap.getDecodedIncompletes(lowWaterMark);
                assertThat(decodedBitsets.getRight())
                        .as(encodingToUse.toString())
                        .containsExactlyInAnyOrderElementsOf(incompletes);
            } else {
                log.info("Encoding not performed: " + encodingToUse);
            }
        }
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(OffsetEncoding.class)
    void ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential(OffsetEncoding encoding) throws BrokenBarrierException, InterruptedException {
        assumeThat("Codec skipped, not applicable", encoding,
                not(in(of(ByteArray, ByteArrayCompressed)))); // byte array not currently used

        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);
        OffsetSimultaneousEncoder.compressionForced = true;

        var records = new ArrayList<ConsumerRecord<String, String>>();
        records.add(new ConsumerRecord<>(INPUT_TOPIC, 0, 0, "akey", "avalue")); // will complete
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

        var firstSucceededRecordRemoved = new ArrayList<>(records);
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 0).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 69).findFirst().get());
        firstSucceededRecordRemoved.remove(firstSucceededRecordRemoved.stream().filter(x -> x.offset() == 25_000).findFirst().get());

        //
        ktu.send(consumerSpy, records);

        //
        ParallelConsumerOptions options = parallelConsumer.getWm().getOptions();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        TopicPartition tp = new TopicPartition(INPUT_TOPIC, 0);
        recordsMap.put(tp, records);
        ConsumerRecords<String, String> crs = new ConsumerRecords<>(recordsMap);

        // write offsets
        Map<TopicPartition, OffsetAndMetadata> completedEligibleOffsetsAndRemove;
        {
            WorkManager<String, String> wm = new WorkManager<>(options, consumerSpy);
            wm.registerWork(crs);

            List<WorkContainer<String, String>> work = wm.maybeGetWork();

            KafkaTestUtils.completeWork(wm, work, 0);

            // test
            assertThat(wm.partitionMaximumRecrodRangeOrMaxMessagesCountOrSomething.get(tp)).isFalse();

            KafkaTestUtils.completeWork(wm, work, 69);

            // test
            assertThat(wm.partitionMaximumRecrodRangeOrMaxMessagesCountOrSomething.get(tp)).isFalse();


            KafkaTestUtils.completeWork(wm, work, 25_000);

            // test
            assertThat(wm.partitionMaximumRecrodRangeOrMaxMessagesCountOrSomething.get(tp)).isFalse();

            completedEligibleOffsetsAndRemove = wm.findCompletedEligibleOffsetsAndRemove();

            // check for graceful fall back to the smallest available encoder
            OffsetMapCodecManager<String, String> om = new OffsetMapCodecManager<>(wm, consumerSpy);
            Set<Long> collect = firstSucceededRecordRemoved.stream().map(x -> x.offset()).collect(Collectors.toSet());
            OffsetMapCodecManager.forcedCodec = Optional.empty(); // turn off forced
            String bestPayload = om.makeOffsetMetadataPayload(1, tp, collect);
            assertThat(bestPayload).isNotEmpty();
        }
        consumerSpy.commitSync(completedEligibleOffsetsAndRemove);

        // read offsets
        {
            WorkManager<String, String> newWm = new WorkManager<>(options, consumerSpy);
            newWm.onPartitionsAssigned(UniSets.of(tp));
            newWm.registerWork(crs);
            List<WorkContainer<String, String>> workContainers = newWm.maybeGetWork();
            switch (encoding) {
                case BitSet, BitSetCompressed, // BitSetV1 both get a short overflow due to the length being too long
                        BitSetV2, // BitSetv2 uncompressed is too large to fit in metadata payload
                        RunLength, RunLengthCompressed // RunLength V1 max runlength is Short.MAX_VALUE
                        -> {
                    assertThatThrownBy(() ->
                            assertThat(workContainers).extracting(WorkContainer::getCr)
                                    .containsExactlyElementsOf(firstSucceededRecordRemoved))
                            .hasMessageContaining("but some elements were not expected")
                            .hasMessageContaining("offset = 25000");
                }
                default -> {
                    assertThat(workContainers).extracting(WorkContainer::getCr).containsExactlyElementsOf(firstSucceededRecordRemoved);
                }
            }
        }
    }


}
