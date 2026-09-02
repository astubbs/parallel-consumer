package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

/**
 * R7: configuration renders through an allow-list, and everything outside it is a value nobody gets to see.
 */
class ProducerConfigRedactionTest {

    @Test
    void allowListedKeysRenderTheirValueAndEveryOtherKeyIsRedacted() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "pc-3-app-abc");
        config.put("sasl.jaas.config", "password=\"s3cret\"");
        config.put("some.custom.interceptor.token", "t0ken");

        String rendered = ProducerConfigRedaction.render(config);

        assertThat(rendered).contains("bootstrap.servers=broker:9092");
        assertThat(rendered).contains("transactional.id=pc-3-app-abc");
        assertThat(rendered).contains("sasl.jaas.config=<redacted>");
        assertThat(rendered).contains("some.custom.interceptor.token=<redacted>");
        assertThat(rendered).doesNotContain("s3cret");
        assertThat(rendered).doesNotContain("t0ken");
    }

    @Test
    void anEmptyMapRendersAsEmptyBraces() {
        assertThat(ProducerConfigRedaction.render(UniMaps.of())).isEqualTo("{}");
    }

    @Test
    void aMapOfOnlyUnknownKeysRendersKeysAndNoValues() {
        String rendered = ProducerConfigRedaction.render(UniMaps.of("x.secret", "value-one", "y.secret", "value-two"));

        assertThat(rendered).contains("x.secret=<redacted>");
        assertThat(rendered).contains("y.secret=<redacted>");
        assertThat(rendered).doesNotContain("value-");
    }

    @Test
    void aNullMapRendersAsAbsent() {
        assertThat(ProducerConfigRedaction.render(null)).isEqualTo("null");
    }

    @Test
    void theAllowListHoldsOnlyKeysThatCarryNoCredential() {
        for (String key : ProducerConfigRedaction.ALLOW_LISTED_KEYS) {
            assertThat(key).doesNotContain("password");
            assertThat(key).doesNotContain("jaas");
            assertThat(key).doesNotContain("secret");
            assertThat(key).doesNotContain("token");
            assertThat(key).doesNotContain("user.info");
        }
    }
}
