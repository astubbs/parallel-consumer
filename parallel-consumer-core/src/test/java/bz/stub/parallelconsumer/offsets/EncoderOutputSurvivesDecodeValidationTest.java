package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The other half of {@link CorruptOffsetMetadataTest}: everything the encoders actually produce must survive the
 * decode-time validation those tests exercise.
 * <p>
 * <b>This is the expensive direction to get wrong.</b> A missed corrupt payload fabricates an offset map; a wrongly
 * <em>rejected</em> one discards a real map and replays work that had completed. The second is the failure this class
 * exists to prevent, and a static argument that the checks are safe is not the same as a test that fails when they
 * stop being.
 * <p>
 * The sizes are chosen where the arithmetic is most likely to be wrong rather than at random: {@code ceil(bits/8)}
 * boundaries (1, 7, 8, 9), and bitsets whose trailing bits are all unset - the case where
 * {@link java.util.BitSet#toByteArray()} truncates trailing zero bytes and could, if the encoder wrote only that
 * array, produce a body shorter than the length field claims.
 *
 * @author Antony Stubbs
 * @see OffsetBitSet#deserialiseBitSetWrapToIncompletes
 * @see OffsetRunLength#runLengthDecodeToIncompletes
 */
@Slf4j
class EncoderOutputSurvivesDecodeValidationTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    static final long BASE_OFFSET = 0L;

    /**
     * Encodes {@code length} offsets from {@link #BASE_OFFSET} where only {@code incompleteIndexes} are incomplete,
     * then hands the bytes back through the production decode path exactly as a rebalance would.
     */
    private static void roundTrip(OffsetEncoding encoding, int length, SortedSet<Long> incompletes) throws Exception {
        long highestSucceeded = BASE_OFFSET + length - 1;
        var simultaneous = new OffsetSimultaneousEncoder(BASE_OFFSET, highestSucceeded, incompletes);
        simultaneous.invoke();

        byte[] body = simultaneous.getEncodingMap().get(encoding);
        assertThat(body)
                .as("this build must be able to produce %s at length %s - otherwise the round trip proves nothing",
                        encoding, length)
                .isNotNull();

        ByteBuffer withMagic = ByteBuffer.allocate(1 + body.length);
        withMagic.put(encoding.magicByte);
        withMagic.put(body);

        assertThatCode(() -> {
            var decoded = EncodedOffsetPair.decodeToIncompletes(withMagic.array(), BASE_OFFSET,
                    InvalidOffsetMetadataHandlingPolicy.FAIL, TP);
            assertThat(decoded.getIncompleteOffsets())
                    .as("a payload this build wrote must decode back to the offsets it encoded")
                    .isEqualTo(incompletes);
        })
                .as("FAIL must not reject %s output at length %s - a false rejection discards a real offset map "
                        + "and replays completed work", encoding, length)
                .doesNotThrowAnyException();
    }

    /**
     * Every offset incomplete: the bitset's bytes are all set, so nothing is truncated and the body is at its widest.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 8, 9, 15, 16, 17, 100})
    void bitSetOutputWithEveryOffsetIncompleteDecodes(int length) throws Exception {
        var incompletes = new TreeSet<Long>();
        for (long i = 0; i < length; i++) {
            incompletes.add(BASE_OFFSET + i);
        }
        roundTrip(OffsetEncoding.BitSetV2, length, incompletes);
    }

    /**
     * Only the FIRST offset incomplete, so every trailing bit is unset - the shape that makes
     * {@link java.util.BitSet#toByteArray()} return fewer bytes than the declared length implies.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 8, 9, 15, 16, 17, 100})
    void bitSetOutputWithOnlyTrailingZeroBytesDecodes(int length) throws Exception {
        var incompletes = new TreeSet<Long>();
        incompletes.add(BASE_OFFSET);
        roundTrip(OffsetEncoding.BitSetV2, length, incompletes);
    }

    /**
     * Run-length output across shapes that stress the whole-number-of-entries and non-empty checks: a single run, an
     * alternating pattern, and a long trailing run of completed offsets.
     */
    @ParameterizedTest
    @CsvSource({
            "1,  1",
            "2,  1",
            "8,  1",
            "9,  2",
            "16, 3",
            "17, 5",
            "64, 7",
    })
    void runLengthOutputDecodes(int length, int everyNthIncomplete) throws Exception {
        var incompletes = new TreeSet<Long>();
        for (long i = 0; i < length; i += everyNthIncomplete) {
            incompletes.add(BASE_OFFSET + i);
        }
        roundTrip(OffsetEncoding.RunLengthV2, length, incompletes);
    }
}
