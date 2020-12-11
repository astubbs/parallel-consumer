package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.OffsetEncoding.Version.v2;
import static org.assertj.core.api.Assertions.assertThat;

public class RunLengthEncoderTest {

    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential
     */
    @SneakyThrows
    @Test
    void gapsInOffsetsWork() {
        Set<Long> incompletes = UniSets.of(0, 6, 10).stream().map(x -> (long) x).collect(Collectors.toSet()); // lol - DRY!
        Set<Long> completes = UniSets.of(1, 3, 4, 5, 9).stream().map(x -> (long) x).collect(Collectors.toSet()); // lol - DRY!
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(-1, 0L, incompletes);

        {
            RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(0);
            rl.encodeCompletedOffset(1);
            // gap completes at 2
            rl.encodeCompletedOffset(3);
            rl.encodeCompletedOffset(4);
            rl.encodeCompletedOffset(5);
            rl.encodeIncompleteOffset(6);
            // gap incompletes at 7
            rl.encodeIncompleteOffset(8);
            rl.encodeCompletedOffset(9);
            rl.encodeIncompleteOffset(10);

            List<Long> longs = rl.calculateSucceededActualOffsets(0);
            
            assertThat(longs).containsExactlyElementsOf(completes);


            byte[] raw = rl.serialise();

            byte[] wrapped = offsetSimultaneousEncoder.packEncoding(new EncodedOffsetPair(OffsetEncoding.RunLengthV2, ByteBuffer.wrap(raw)));

            HighestOffsetAndIncompletes result = OffsetMapCodecManager.decodeCompressedOffsets(0, wrapped);

            assertThat(result.getHighestSeenOffset()).isEqualTo(10);

            assertThat(result.getIncompleteOffsets()).containsExactlyElementsOf(incompletes);
        }
    }
}
