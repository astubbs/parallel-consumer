package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.PartitionState;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.utils.Range.range;

@Slf4j
@UtilityClass
public class OffsetCodecTestUtils {

    /**
     * x is complete
     * <p>
     * o is incomplete
     */
    static String incompletesToBitmapString(long finalOffsetForPartition, long highestSeen, Set<Long> incompletes) {
        var runLengthString = new StringBuilder();
        Long lowWaterMark = finalOffsetForPartition;
        long end = highestSeen - lowWaterMark;
        for (final var relativeOffset : range(end)) {
            long offset = lowWaterMark + relativeOffset;
            if (incompletes.contains(offset)) {
                runLengthString.append("o");
            } else {
                runLengthString.append("x");
            }
        }
        return runLengthString.toString();
    }

    static String incompletesToBitmapString(long finalOffsetForPartition, PartitionState<?, ?> state) {
        return incompletesToBitmapString(finalOffsetForPartition,
                state.getOffsetHighestSeen(), state.getIncompleteOffsetsBelowHighestSucceeded());
    }

    /**
     * x is complete
     * <p>
     * o is incomplete
     */
    static TreeSet<Long> bitmapStringToIncomplete(final long baseOffset, final String inputBitmapString) {
        var incompleteOffsets = new TreeSet<Long>();

        final long longLength = inputBitmapString.length();
        range(longLength).forEach(index -> {
            var bit = inputBitmapString.charAt(Math.toIntExact(index));
            if (bit == 'o') {
                incompleteOffsets.add(baseOffset + index);
            } else if (bit == 'x') {
                log.trace("Dropping completed offset");
            } else {
                throw new IllegalArgumentException("Invalid encoding - unexpected char: " + bit);
            }
        });

        return incompleteOffsets;
    }

    /**
     * A magic byte that no {@link OffsetEncoding} in this build claims - i.e. what the metadata of a commit written by
     * a FUTURE version of Parallel Consumer looks like to this one.
     * <p>
     * Derived from the enum rather than hard coded, so that adding an encoding cannot silently turn a
     * forward-compatibility test into a test of that new encoding.
     */
    static byte magicByteOfAnEncodingThatDoesNotExistYet() {
        Set<Byte> claimed = Arrays.stream(OffsetEncoding.values())
                .map(OffsetEncoding::getMagicByte)
                .collect(Collectors.toSet());
        for (int candidate = Byte.MIN_VALUE; candidate <= Byte.MAX_VALUE; candidate++) {
            if (!claimed.contains((byte) candidate)) {
                return (byte) candidate;
            }
        }
        throw new IllegalStateException("Every possible magic byte is claimed by an encoding - the wire format is full");
    }

    /**
     * A module whose consumer already has {@code metadata} committed against {@code tp}, as a previous owner of the
     * consumer group would have left it behind - the setup every offset-metadata regression test needs, and the one
     * two of them had a copy of each.
     * <p>
     * The consumer is owned here rather than handed back: neither caller does anything with it after construction,
     * and a test that needs to reach it can read {@code module.options().getConsumer()}.
     *
     * @param committedOffset the offset the metadata is committed against - what the payload is decoded relative to
     */
    static PCModuleTestEnv moduleWithCommittedMetadata(TopicPartition tp, long committedOffset, String metadata) {
        return moduleWithCommittedMetadata(tp, committedOffset, metadata, UnaryOperator.identity());
    }

    /**
     * The same, with {@link ParallelConsumerOptions#getInvalidOffsetMetadataPolicy()} set explicitly. Leaving it unset
     * is not the same test: the default is what a reporter actually runs, so a regression has to be pinned under it.
     */
    static PCModuleTestEnv moduleWithCommittedMetadata(TopicPartition tp,
                                                       long committedOffset,
                                                       String metadata,
                                                       ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy policy) {
        return moduleWithCommittedMetadata(tp, committedOffset, metadata,
                builder -> builder.invalidOffsetMetadataPolicy(policy));
    }

    /**
     * @param configure anything else the test needs on the options builder
     */
    static PCModuleTestEnv moduleWithCommittedMetadata(TopicPartition tp,
                                                       long committedOffset,
                                                       String metadata,
                                                       UnaryOperator<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> configure) {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(tp));
        mockConsumer.commitSync(UniMaps.of(tp, new OffsetAndMetadata(committedOffset, metadata)));

        var options = configure.apply(ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)).build();
        return new PCModuleTestEnv(options);
    }

}
