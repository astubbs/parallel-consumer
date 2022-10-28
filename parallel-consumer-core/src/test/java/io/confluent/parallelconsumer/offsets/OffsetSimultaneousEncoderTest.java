package io.confluent.parallelconsumer.offsets;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

@Slf4j
class OffsetSimultaneousEncoderTest {

    /**
     * todo docs
     */
    @SneakyThrows
    @Test
    void general() {
        long highest = 0L;
        int base = 0;

        OffsetSimultaneousEncoder o = new OffsetSimultaneousEncoder(base, highest);

        //
        o.encodeCompleteOffset(base, highest, highest);
        highest++;
        o.encodeCompleteOffset(base, highest, highest);

        highest++;
        o.encodeCompleteOffset(base, highest, highest);

        //
        Set<OffsetEncoder> encoders = o.getActiveEncoders();
        log.debug(encoders.toString());
        o.serializeAllEncoders();
        Object smallestCodec = o.getSmallestCodec();
        byte[] bytes = o.packSmallest();
        Assertions.assertThat(encoders);
    }

}
