package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.processor.StateStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The refusal logic on its own - no topology, no {@code StreamTask}, no broker.
 * <p>
 * Two things are being separated here on purpose. This class proves that the <em>classification and the
 * message</em> are right; {@link ProcessorApiBackstopTest} proves that {@code StreamTask}'s constructor
 * actually calls it. A single end-to-end test would conflate "we do not detect a session store" with "we
 * never got asked", and those have different fixes.
 * <p>
 * The stores come from {@link RefusedStoreFixtures}, and they are real Kafka stores rather than stubs - see
 * that class for why that matters to the {@code instanceof} chain being exercised here.
 *
 * @author Antony Stubbs
 */
// The switch is process-wide static state - there is no seam through KafkaStreams to inject a collaborator -
// and this module inherits concurrent JUnit execution from core's junit-platform.properties. Left concurrent,
// these methods would toggle each other's switch and each other's verdicts.
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
class PcSupportedEnvelopeTest {

    private static final String TASK_ID = "0_0";

    private static final boolean AT_LEAST_ONCE = false;
    private static final boolean EXACTLY_ONCE = true;

    @AfterEach
    void restoreSwitch() {
        // Hand the JVM back at the artifact's default rather than parking it wherever this class left it.
        PcDispatchSwitch.resetToDefault();
    }

    // ---------------------------------------------------------------------------------------------------
    // The seam guard. Without this, layers 2 and 3 would break Apache Kafka's own suite, which this module
    // runs unmodified with the seam OFF as its behaviour-preservation evidence.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void seamOffRefusesNothingEvenWhenEverythingUnsupportedIsPresent() {
        PcDispatchSwitch.disable();

        assertThatCode(() -> PcSupportedEnvelope.checkTask(TASK_ID, allUnsupportedStores(), EXACTLY_ONCE))
                .as("a seam-off run must be behaviourally identical to stock Kafka Streams, whose own tests "
                        + "build exactly these constructs")
                .doesNotThrowAnyException();
    }

    @Test
    void seamOffRefusesNothingAtTheDslCallEither() {
        PcDispatchSwitch.disable();

        for (final PcUnsupportedConstruct construct : PcUnsupportedConstruct.values()) {
            assertThatCode(construct::refuse)
                    .as("%s must be a no-op with the seam off", construct)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void seamOnRefusesEveryConstructAndNamesItInTheMessage() {
        PcDispatchSwitch.enable(2);

        for (final PcUnsupportedConstruct construct : PcUnsupportedConstruct.values()) {
            assertThatThrownBy(construct::refuse)
                    .as("%s must refuse with the seam on", construct)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(construct.getDisplayName())
                    .hasMessageContaining(construct.getReason())
                    .hasMessageContaining("astubbs#255")
                    // The property is the whole escape hatch. A refusal that does not carry it is a dead end
                    // for whoever hits it.
                    .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Classification: what the backstop sees in a topology.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void aWindowStoreIsClassifiedAsWindowing() {
        assertThat(PcSupportedEnvelope.findUnsupported(Collections.singletonList(windowStore()), AT_LEAST_ONCE))
                .containsExactly(PcUnsupportedConstruct.WINDOW_STORE);
    }

    @Test
    void aSessionStoreIsClassifiedAsSessionWindowing() {
        assertThat(PcSupportedEnvelope.findUnsupported(Collections.singletonList(sessionStore()), AT_LEAST_ONCE))
                .containsExactly(PcUnsupportedConstruct.SESSION_STORE);
    }

    @Test
    void aSuppressionBufferIsClassifiedAsSuppression() {
        assertThat(PcSupportedEnvelope.findUnsupported(Collections.singletonList(suppressionBuffer()), AT_LEAST_ONCE))
                .containsExactly(PcUnsupportedConstruct.SUPPRESSION_BUFFER);
    }

    @Test
    void aVersionedKeyValueStoreIsRefusedEvenThoughItIsNotAWindowStore() {
        // The one that gets missed. VersionedKeyValueStore extends StateStore directly, so it satisfies none of
        // the window/session/buffer checks and looks like an ordinary key-value store - yet RocksDBVersionedStore
        // silently DROPS puts older than its non-volatile observedStreamTime. Reachable with no refused DSL call
        // anywhere: Materialized.as(Stores.persistentVersionedKeyValueStore(...)) is enough.
        assertThat(PcSupportedEnvelope.findUnsupported(
                Collections.singletonList(versionedKeyValueStore()), AT_LEAST_ONCE))
                .containsExactly(PcUnsupportedConstruct.VERSIONED_KEY_VALUE_STORE);
    }

    @Test
    void exactlyOnceIsFoundFromConfigurationWithNoStoresAtAll() {
        // EOS is not a topology shape, which is why it cannot be inferred from the store list.
        assertThat(PcSupportedEnvelope.findUnsupported(Collections.<StateStore>emptyList(), EXACTLY_ONCE))
                .containsExactly(PcUnsupportedConstruct.EXACTLY_ONCE);
    }

    @Test
    void everyUnsupportedConstructInOneTopologyIsReportedTogether() {
        // Someone who removes their windowed aggregation only to be refused again for the session store has
        // been made to pay twice for one diagnosis.
        assertThat(PcSupportedEnvelope.findUnsupported(allUnsupportedStores(), EXACTLY_ONCE))
                .containsExactlyInAnyOrder(
                        PcUnsupportedConstruct.EXACTLY_ONCE,
                        PcUnsupportedConstruct.WINDOW_STORE,
                        PcUnsupportedConstruct.SESSION_STORE,
                        PcUnsupportedConstruct.SUPPRESSION_BUFFER,
                        PcUnsupportedConstruct.VERSIONED_KEY_VALUE_STORE);
    }

    @Test
    void repeatedStoresOfOneKindAreOneProblemNotThree() {
        assertThat(PcSupportedEnvelope.findUnsupported(
                Arrays.asList(windowStore(), windowStore(), windowStore()), AT_LEAST_ONCE))
                .containsExactly(PcUnsupportedConstruct.WINDOW_STORE);
    }

    // ---------------------------------------------------------------------------------------------------
    // Positive controls. Without these the guard could refuse everything and every test above would still
    // pass.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void aPlainKeyValueStoreStaysSupported() {
        PcDispatchSwitch.enable(2);

        // Non-windowed stateful aggregation is the supported stateful case (KTD3), and it is what
        // PcDrivenStatefulProofTest exercises. Refusing it here would silently delete that proof.
        assertThatCode(() -> PcSupportedEnvelope.checkTask(
                TASK_ID, Collections.singletonList(keyValueStore()), AT_LEAST_ONCE))
                .doesNotThrowAnyException();
    }

    @Test
    void aStatelessAtLeastOnceTaskStaysSupported() {
        PcDispatchSwitch.enable(2);

        assertThatCode(() -> PcSupportedEnvelope.checkTask(
                TASK_ID, Collections.<StateStore>emptyList(), AT_LEAST_ONCE))
                .doesNotThrowAnyException();
    }

    // ---------------------------------------------------------------------------------------------------
    // The message a user actually reads.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void theTaskRefusalNamesTheTaskAndEveryConstruct() {
        PcDispatchSwitch.enable(2);

        assertThatThrownBy(() -> PcSupportedEnvelope.checkTask(TASK_ID, allUnsupportedStores(), EXACTLY_ONCE))
                .isInstanceOf(UnsupportedOperationException.class)
                // Which topology is at fault, for someone running several tasks.
                .hasMessageContaining("Task " + TASK_ID)
                .hasMessageContaining(PcUnsupportedConstruct.WINDOW_STORE.getDisplayName())
                .hasMessageContaining(PcUnsupportedConstruct.SESSION_STORE.getDisplayName())
                .hasMessageContaining(PcUnsupportedConstruct.SUPPRESSION_BUFFER.getDisplayName())
                .hasMessageContaining(PcUnsupportedConstruct.VERSIONED_KEY_VALUE_STORE.getDisplayName())
                .hasMessageContaining(PcUnsupportedConstruct.EXACTLY_ONCE.getDisplayName())
                .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");
    }

    @Test
    void everyConstructCarriesBothANameAndAReason() {
        for (final PcUnsupportedConstruct construct : PcUnsupportedConstruct.values()) {
            assertThat(construct.getDisplayName()).as("%s display name", construct).isNotBlank();
            assertThat(construct.getReason()).as("%s reason", construct).isNotBlank();
        }
    }

    // ---------------------------------------------------------------------------------------------------

    private static List<StateStore> allUnsupportedStores() {
        final List<StateStore> stores = new ArrayList<>();
        stores.add(windowStore());
        stores.add(sessionStore());
        stores.add(suppressionBuffer());
        stores.add(versionedKeyValueStore());
        stores.add(keyValueStore());
        return stores;
    }

    private static StateStore windowStore() {
        return RefusedStoreFixtures.build(RefusedStoreFixtures.windowStoreBuilder());
    }

    private static StateStore sessionStore() {
        return RefusedStoreFixtures.build(RefusedStoreFixtures.sessionStoreBuilder());
    }

    private static StateStore suppressionBuffer() {
        return RefusedStoreFixtures.build(RefusedStoreFixtures.suppressionBufferBuilder());
    }

    private static StateStore versionedKeyValueStore() {
        return RefusedStoreFixtures.build(RefusedStoreFixtures.versionedKeyValueStoreBuilder());
    }

    private static StateStore keyValueStore() {
        return RefusedStoreFixtures.build(RefusedStoreFixtures.keyValueStoreBuilder());
    }
}
