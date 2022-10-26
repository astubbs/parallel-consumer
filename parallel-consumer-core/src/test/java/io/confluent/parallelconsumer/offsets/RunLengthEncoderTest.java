package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import javax.lang.model.type.TypeKind;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.JavaUtils.toTreeSet;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static io.confluent.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;
import static javax.lang.model.type.TypeKind.INT;
import static javax.lang.model.type.TypeKind.SHORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;

class RunLengthEncoderTest {

    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential
     */
    @SneakyThrows
    @Test
    void noGaps() {
        var incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        List<Integer> runs = UniLists.of(1, 3, 1, 1, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, 0L, incompletes);

        {
            long base = 0;
            RunLengthEncoder rl = new RunLengthEncoder(base, offsetSimultaneousEncoder, v2);

            addStandardData(rl);

//            rl.addTail();

            // before serialisation
            {
                assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

                List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets();

                assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
            }
        }
    }

    private static void addStandardData(RunLengthEncoder rl) {
        long base = 0;
        long highest = 9;

        rl.encodeIncompleteOffset(base, 0, highest); // 1
        rl.encodeCompleteOffset(base, 1, highest); // 3
        rl.encodeCompleteOffset(base, 2, highest);
        rl.encodeCompleteOffset(base, 3, highest);
        rl.encodeIncompleteOffset(base, 4, highest); // 1
        rl.encodeCompleteOffset(base, 5, highest); // 1
        rl.encodeIncompleteOffset(base, 6, highest); // 3
        rl.encodeIncompleteOffset(base, 7, highest);
        rl.encodeIncompleteOffset(base, 8, highest);
        rl.encodeCompleteOffset(base, 9, highest); // 1
        rl.encodeIncompleteOffset(base, 10, highest); // 1
    }


    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential
     */
    @SneakyThrows
    @Test
    void noGapsSerialisation() {
        var incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(toTreeSet()); // lol - DRY!
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet()); // lol - DRY!
        List<Integer> runs = UniLists.of(1, 3, 1, 1, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, 0L, incompletes);

        {
            long base = 0;

            RunLengthEncoder rl = new RunLengthEncoder(base, offsetSimultaneousEncoder, v2);

            addStandardData(rl);

            // after serialisation
            {
                byte[] raw = rl.serialise();

                byte[] wrapped = offsetSimultaneousEncoder.packEncoding(new EncodedOffsetPair(OffsetEncoding.RunLengthV2, ByteBuffer.wrap(raw)));

                HighestOffsetAndIncompletes result = OffsetMapCodecManager.decodeCompressedOffsets(0, wrapped);

                assertThat(result.getHighestSeenOffset()).contains(10L);

                assertThat(result.getIncompleteOffsets()).containsExactlyElementsOf(incompletes);
            }
        }
    }

    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential.
     */
    @SneakyThrows
    @Test
    void gapsInOffsetsWork() {
        var incompletes = UniSets.of(0, 6, 10).stream().map(x -> (long) x).collect(toTreeSet());

        // NB: gaps between completed offsets get encoded as succeeded offsets. This doesn't matter because they don't exist and we'll neve see them.
        Set<Long> completes = UniSets.of(1, 2, 3, 4, 5, 9).stream().map(x -> (long) x).collect(Collectors.toSet());
        List<Integer> runs = UniLists.of(1, 5, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, 0L, incompletes);

        {

            long base = 0;
            long highest = 9;

            RunLengthEncoder rl = new RunLengthEncoder(base, offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(base, 0, highest);
            rl.encodeCompleteOffset(base, 1, highest);
            // gap completes at 2
            rl.encodeCompleteOffset(base, 3, highest);
            rl.encodeCompleteOffset(base, 4, highest);
            rl.encodeCompleteOffset(base, 5, highest);
            rl.encodeIncompleteOffset(base, 6, highest);
            // gap incompletes at 7
            rl.encodeIncompleteOffset(base, 8, highest);
            rl.encodeCompleteOffset(base, 9, highest);
            rl.encodeIncompleteOffset(base, 10, highest);

//            rl.addTail();

            assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

            List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets();

            assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
        }
    }


    /**
     * Check RLv2 errors on integer overflow. Integer version of this test is very slow (1.5 minutes).
     */
    @SneakyThrows
    @ParameterizedTest()
    @EnumSource(OffsetEncoding.Version.class)
    void vTwoIntegerOverflow(OffsetEncoding.Version versionToTest) {
        final long integerMaxOverflowOffset = 100;
        final long overflowedValue = Integer.MAX_VALUE + integerMaxOverflowOffset;

        var incompletes = UniSets.of(0L, 4L, 6L, 7L, 8L, 10L, overflowedValue).stream().collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        final long highestSucceededOffset = overflowedValue - 1;
        OffsetSimultaneousEncoder offsetSimultaneousEncoder
                = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, highestSucceededOffset, incompletes);

        {
            final OffsetEncoding.Version versionsToTest = v2;
            testRunLength(overflowedValue, offsetSimultaneousEncoder, versionToTest, highestSucceededOffset);
        }
    }

    private static void testRunLength(long overflowedValue,
                                      OffsetSimultaneousEncoder offsetSimultaneousEncoder,
                                      OffsetEncoding.Version versionsToTest,
                                      long currentHighestCompleteOffset) throws EncodingNotSupportedException {
        RunLengthEncoder rl = new RunLengthEncoder(0L, offsetSimultaneousEncoder, versionsToTest);

        addStandardData(rl);


        // inject overflow offset
        var errorAssertion = Assertions.assertThatThrownBy(() -> {
            for (var relativeOffset : Range.range(11, overflowedValue)) {
                rl.encodeCompleteOffset(0l, relativeOffset, currentHighestCompleteOffset);
            }
        });

        switch (versionsToTest) {
            case v1 -> {
                errorAssertion.isInstanceOf(RunLengthV1EncodingNotSupported.class);
                errorAssertion.hasMessageContainingAll("too big", "Short");
            }
            case v2 -> {
                errorAssertion.isInstanceOf(RunLengthV2EncodingNotSupported.class);
                errorAssertion.hasMessageContainingAll("too big", "Integer");
            }
        }
    }

    /**
     * Test simultaneous encoder with run-length overflow errors fail gracefully.
     * <p>
     * Integer version of this test is very slow (1.5 minutes).
     */
    @ParameterizedTest
    @EnumSource(names = {"SHORT", "INT"}, mode = INCLUDE)
    void testSimultaneousWithOverflowErrors(TypeKind primitiveSize) {
        Assumptions.assumeTrue(primitiveSize == SHORT || primitiveSize == INT);

        final long integerMaxOverflowOffset = 100;
        final int maxValue = switch (primitiveSize) {
            case SHORT -> Short.MAX_VALUE;
            case INT -> Integer.MAX_VALUE;
            default -> throw new IllegalStateException("Unexpected value: " + primitiveSize);
        };

        final long overflowedValue = maxValue + integerMaxOverflowOffset;

        var incompletes = UniSets.of(0L, 4L, 6L, 7L, 8L, 10L, overflowedValue).stream().collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        OffsetSimultaneousEncoder offsetSimultaneousEncoder
                = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, overflowedValue - 1, incompletes);

        offsetSimultaneousEncoder.oldIinvoke();

        final Map<OffsetEncoding, byte[]> encodingMap = offsetSimultaneousEncoder.getEncodingMap();

        //
        switch (primitiveSize) {
            case SHORT -> Truth.assertThat(encodingMap).hasSize(2);
            case INT -> Truth.assertThat(encodingMap).hasSize(0);
            default -> throw new IllegalStateException("Unexpected value: " + primitiveSize);
        }
        ;
    }

}
