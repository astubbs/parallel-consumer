package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Methods for encoding and decoding the run-lengths.
 *
 * @author Antony Stubbs
 */
@Slf4j
@UtilityClass
public class OffsetRunLength {

    /**
     * @return run length encoding, always starting with an 'o' count
     */
    static List<Integer> runLengthEncode(final String in) {
        final AtomicInteger length = new AtomicInteger();
        final AtomicBoolean previous = new AtomicBoolean(false);
        final List<Integer> encoding = new ArrayList<>();
        in.chars().forEachOrdered(bit -> {
            final boolean current = switch (bit) {
                case 'o' -> false;
                case 'x' -> true;
                default -> throw new IllegalArgumentException(bit + " in " + in);
            };
            if (previous.get() == current) {
                length.getAndIncrement();
            } else {
                previous.set(current);
                encoding.add(length.get());
                length.set(1);
            }
        });
        encoding.add(length.get()); // add tail
        return encoding;
    }

    /**
     * @see #runLengthEncode
     */
    static String runLengthDecodeToString(final List<Integer> in) {
        final StringBuilder sb = new StringBuilder(in.size());
        boolean current = false;
        for (final Integer i : in) {
            for (int x = 0; x < i; x++) {
                if (current) {
                    sb.append('x');
                } else {
                    sb.append('o');
                }
            }
            current = !current; // toggle
        }
        return sb.toString();
    }


    /**
     * @see #runLengthEncode
     */
    static HighestOffsetAndIncompletes runLengthDecodeToIncompletes(OffsetEncoding encoding, final long baseOffset, final ByteBuffer in)
            throws CorruptOffsetMetadataException {
        in.rewind();
        // asShortBuffer()/asIntBuffer() silently DROP a trailing partial element, so a body of the wrong width decodes
        // as a shorter, plausible-looking run list instead of failing: 3 bytes read as one short fabricated five
        // incomplete offsets from a payload no encoder here could have produced. A whole number of elements is the
        // one structural claim the buffer can settle on its own.
        int elementBytes = switch (encoding.version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
        if (in.remaining() % elementBytes != 0) {
            throw new CorruptOffsetMetadataException(msg(
                    "{} run-length body is {} byte(s), which is not a whole number of {}-byte entries",
                    encoding.description(), in.remaining(), elementBytes));
        }
        final ShortBuffer v1ShortBuffer = in.asShortBuffer();
        final IntBuffer v2IntegerBuffer = in.asIntBuffer();

        final var incompletes = new TreeSet<Long>();

        /*
        Set highestSeenOffset to baseOffset -1 initially - in case the metadata doesn't actually contain any data and
        highestSeenOffset would remain at 0 otherwise.
        That may cause warning / state truncation.
        Issue confluentinc#546 - https://github.com/confluentinc/parallel-consumer/issues/546
         */
        //TODO: look at offset encoding logic - maybe in those cases we should not create metadata at all?
        long highestSeenOffset = (baseOffset > 0) ? (baseOffset - 1) : 0L;

        Supplier<Boolean> hasRemainingTest = () -> {
            return switch (encoding.version) {
                case v1 -> v1ShortBuffer.hasRemaining();
                case v2 -> v2IntegerBuffer.hasRemaining();
            };
        };
        if (log.isTraceEnabled()) {
            // print out all run lengths
            var runlengths = new ArrayList<Number>();
            try {
                while (hasRemainingTest.get()) {
                    Number runLength = switch (encoding.version) {
                        case v1 -> v1ShortBuffer.get();
                        case v2 -> v2IntegerBuffer.get();
                    };
                    runlengths.add(runLength);
                }
            } catch (BufferUnderflowException u) {
                log.error("Error decoding offsets", u);
            }
            log.debug("Unrolled runlengths: {}", runlengths);
            v1ShortBuffer.rewind();
            v2IntegerBuffer.rewind();
        }

        // decodes incompletes
        boolean currentRunLengthIsComplete = false;
        long currentOffset = baseOffset;
        while (hasRemainingTest.get()) {
            try {
                Number runLength = switch (encoding.version) {
                    case v1 -> v1ShortBuffer.get();
                    case v2 -> v2IntegerBuffer.get();
                };

                // A run length is a count, so it is never negative in anything the encoder produced. Unchecked, a
                // negative run walks currentOffset BACKWARDS and yields a highest-seen offset below the committed one,
                // silently, with no error anywhere downstream to tell it from a real map.
                if (runLength.longValue() < 0) {
                    throw new CorruptOffsetMetadataException(msg("negative run length ({}) at offset {}",
                            runLength, currentOffset));
                }
                if (currentRunLengthIsComplete) {
                    log.trace("Ignoring {} completed offset(s) (offset:{})", runLength, currentOffset);
                    currentOffset += runLength.longValue();
                    highestSeenOffset = currentOffset - 1;
                } else {
                    log.trace("Adding {} incomplete offset(s) (starting with offset:{})", runLength, currentOffset);
                    for (int relativeOffset = 0; relativeOffset < runLength.longValue(); relativeOffset++) {
                        incompletes.add(currentOffset);
                        highestSeenOffset = currentOffset;
                        currentOffset++;
                    }
                }
                log.trace("Highest seen: {}", highestSeenOffset);
            } catch (BufferUnderflowException u) {
                log.error("Error decoding offsets", u);
                throw u;
            }
            currentRunLengthIsComplete = !currentRunLengthIsComplete; // toggle
        }
        return HighestOffsetAndIncompletes.of(highestSeenOffset, incompletes);
    }

    static List<Integer> runLengthDeserialise(final ByteBuffer in) {
        // view as short buffer
        in.rewind();
        final ShortBuffer shortBuffer = in.asShortBuffer();

        //
        final List<Integer> results = new ArrayList<>(shortBuffer.capacity());
        while (shortBuffer.hasRemaining()) {
            results.add((int) shortBuffer.get());
        }
        return results;
    }

}
