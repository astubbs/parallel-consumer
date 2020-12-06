package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;

@Slf4j
public class OffsetSimultaneousEncoderTest {
    @SneakyThrows
    @Test
    void general() {
        long highest = 0L;
        int base = 0;

        OffsetSimultaneousEncoder o = new OffsetSimultaneousEncoder(base, highest);

        //
        o.encodeCompletedOffset(base, highest, highest);
        highest++;
        o.encodeCompletedOffset(base, highest, highest);

        highest++;
        o.encodeCompletedOffset(base, highest, highest);

        //
        HashSet<OffsetEncoderBase> encoders = o.encoders;
        log.debug(encoders.toString());
        o.serializeAllEncoders();
        Object smallestCodec = o.getSmallestCodec();
        byte[] bytes = o.packSmallest();
        Assertions.assertThat(encoders);
    }
}
