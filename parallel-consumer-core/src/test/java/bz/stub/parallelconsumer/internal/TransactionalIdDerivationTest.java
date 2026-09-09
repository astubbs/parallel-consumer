package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * R4, R5, R6: the derived {@code transactional.id}, its documented prefix, and why the prefix set is prefix-free.
 */
class TransactionalIdDerivationTest {

    private static final UUID UUID_A = UUID.fromString("00000000-0000-0000-0000-00000000000a");
    private static final UUID UUID_B = UUID.fromString("00000000-0000-0000-0000-00000000000b");

    /**
     * Covers AE4.
     */
    @Test
    void thePrefixIsTheDocumentedLengthPrefixedGroupIdAndTheIdStartsWithIt() {
        assertThat(TransactionalIdDerivation.prefixFor("app")).isEqualTo("pc-3-app-");
        assertThat(TransactionalIdDerivation.derive("app", UUID_A)).isEqualTo("pc-3-app-" + UUID_A);
        assertThat(TransactionalIdDerivation.derive("app", UUID_A)).startsWith(TransactionalIdDerivation.prefixFor("app"));
    }

    /**
     * Covers AE4: the pairs a plain {@code prefix-groupId-} scheme gets wrong, plus the same-length and the
     * length-digits-share-a-prefix cases.
     */
    @ParameterizedTest
    @CsvSource({
            "app, app-x",
            "a, abcdefghij",
            "app, apq",
            "app-, app-x",
            "1, 10",
            "x-1, x-10",
    })
    void noGroupsPrefixIsAStringPrefixOfAnothers(String groupA, String groupB) {
        String prefixA = TransactionalIdDerivation.prefixFor(groupA);
        String prefixB = TransactionalIdDerivation.prefixFor(groupB);

        assertWithMessage("%s must not prefix %s", prefixA, prefixB).that(prefixB.startsWith(prefixA)).isFalse();
        assertWithMessage("%s must not prefix %s", prefixB, prefixA).that(prefixA.startsWith(prefixB)).isFalse();
    }

    @Test
    void twoInstancesOfTheSameGroupNeverShareAnId() {
        assertThat(TransactionalIdDerivation.derive("app", UUID_A))
                .isNotEqualTo(TransactionalIdDerivation.derive("app", UUID_B));
    }

    /**
     * Covers AE4: the caller's value does not take effect, and one WARN names both.
     */
    @Test
    void aCallerSetIdIsReplacedInTransactionalModeAndOneWarnNamesBothValues() {
        Map<String, Object> callers = UniMaps.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-own-id-tx");

        var warns = new java.util.ArrayList<ILoggingEvent>();
        Map<String, Object> resolved = captureWarns(warns, "my-own-id-tx", () ->
                TransactionalIdDerivation.resolve(callers, true, "app", UUID_A));

        assertThat(resolved.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo("pc-3-app-" + UUID_A);
        assertThat(resolved.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("broker:9092");
        assertThat(callers.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo("my-own-id-tx");
        assertThat(warns).hasSize(1);
        assertThat(warns.get(0).getFormattedMessage()).contains("my-own-id-tx");
        assertThat(warns.get(0).getFormattedMessage()).contains("pc-3-app-" + UUID_A);
    }

    @Test
    void noCallerSetIdMeansNoWarn() {
        var warns = new java.util.ArrayList<ILoggingEvent>();
        Map<String, Object> resolved = captureWarns(warns, "quiet-group", () ->
                TransactionalIdDerivation.resolve(UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092"), true, "quiet-group", UUID_A));

        assertThat(resolved.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo("pc-11-quiet-group-" + UUID_A);
        assertThat(warns).isEmpty();
    }

    @Test
    void inANonTransactionalModeNoIdIsDerivedAndACallerSetOneIsRemovedWithAWarn() {
        Map<String, Object> callers = UniMaps.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-own-id-nontx");

        var warns = new java.util.ArrayList<ILoggingEvent>();
        Map<String, Object> resolved = captureWarns(warns, "my-own-id-nontx", () ->
                TransactionalIdDerivation.resolve(callers, false, "app", UUID_A));

        assertThat(resolved).doesNotContainKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        assertThat(warns).hasSize(1);
        assertThat(warns.get(0).getFormattedMessage()).contains("my-own-id-nontx");
    }

    /**
     * Captures the WARNs this test's own call emitted. Test methods run concurrently here and share the logger, so the
     * capture is filtered by a marker only this test's input carries - without it a sibling's WARN lands in the list.
     */
    private static <T> T captureWarns(List<ILoggingEvent> sink, String marker, java.util.function.Supplier<T> action) {
        var logger = (Logger) LoggerFactory.getLogger(TransactionalIdDerivation.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            return action.get();
        } finally {
            logger.detachAppender(appender);
            sink.addAll(appender.list.stream()
                    .filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
                    .filter(event -> event.getFormattedMessage().contains(marker))
                    .collect(Collectors.toList()));
        }
    }
}
