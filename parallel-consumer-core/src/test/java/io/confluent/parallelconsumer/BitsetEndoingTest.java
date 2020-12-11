package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.OffsetEncoding.Version.v2;
import static org.assertj.core.api.Assertions.assertThat;

public class BitsetEndoingTest {
    @SneakyThrows
    @Test
    void basic() {
        Set<Long> incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(Collectors.toSet()); // lol - DRY!
        Set<Long> completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(Collectors.toSet()); // lol - DRY!
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(-1, 0L, incompletes);
        BitsetEncoder bs = new BitsetEncoder(incompletes.size(), offsetSimultaneousEncoder, v2);

        bs.encodeIncompleteOffset(0); // 1
        bs.encodeCompletedOffset(1); // 3
        bs.encodeCompletedOffset(2);
        bs.encodeCompletedOffset(3);
        bs.encodeIncompleteOffset(4); // 1
        bs.encodeCompletedOffset(5); // 1
        bs.encodeIncompleteOffset(6); // 3
        bs.encodeIncompleteOffset(7);
        bs.encodeIncompleteOffset(8);
        bs.encodeCompletedOffset(9); // 1
        bs.encodeIncompleteOffset(10); // 1

        // before serialisation
        {
            assertThat(bs.getBitSet().stream().toArray()).containsExactly(1, 2, 3, 5, 9);
        }

        // after serialisation
        {
            byte[] raw = bs.serialise();

            byte[] wrapped = offsetSimultaneousEncoder.packEncoding(new EncodedOffsetPair(OffsetEncoding.RunLengthV2, ByteBuffer.wrap(raw)));

            OffsetMapCodecManager.HighestOffsetAndIncompletes result = OffsetMapCodecManager.decodeCompressedOffsets(0, wrapped);

            assertThat(result.getHighestSeenOffset()).isEqualTo(10);

            assertThat(result.getIncompleteOffsets()).containsExactlyElementsOf(incompletes);
        }
    }
}
