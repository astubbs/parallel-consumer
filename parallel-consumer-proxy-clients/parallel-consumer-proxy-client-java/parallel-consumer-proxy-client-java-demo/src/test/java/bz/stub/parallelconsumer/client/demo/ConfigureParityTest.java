package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ProcessingOrder;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <b>The two gRPC arms must ask the sidecar for the same session, or their difference is not the
 * client library's cost.</b>
 *
 * <h2>This test exists because the defect happened twice</h2>
 *
 * The control arm writes its {@code Configure} by hand, which is the whole point of it - it gets no
 * help from the client library, and that is what it demonstrates. It also means every field the
 * library sets is a field a human has to remember, and twice one was missed:
 *
 * <ul>
 *   <li>{@code ordering} left unset. The field is optional and unspecified means
 *       parallel-consumer-core's default, which is KEY - so that arm ran key-ordered against four
 *       unordered ones while looking identical, and the numbers this branch published were measured
 *       that way.</li>
 *   <li>{@code capabilities} left empty, which negotiates a different session from the one the
 *       library negotiates.</li>
 * </ul>
 *
 * Both were invisible: a wrong session produces a working arm and a plausible throughput figure.
 * Asserting the fields is the only thing that catches the class rather than the instance, so a
 * field added to {@code Configure} later should be added here too.
 *
 * @author Antony Stubbs
 */
class ConfigureParityTest {

    private static final String TOPIC = "parity-topic";

    private static Map<String, String> kafkaProperties() {
        var properties = new LinkedHashMap<String, String>();
        properties.put("bootstrap.servers", "broker:9092");
        properties.put("group.id", "parity-group");
        properties.put("enable.auto.commit", "false");
        return properties;
    }

    private static DemoOptions options() {
        return DemoOptions.parse(new String[]{"--concurrency", "37"}, Collections.emptyMap());
    }

    @Test
    void theHandWrittenRequestAsksForTheSameSubscriptionAsTheLibrary() {
        var raw = ReferenceDemo.rawConfigure(options(), TOPIC, kafkaProperties());
        var library = ReferenceDemo.libraryOptions(options(), TOPIC, kafkaProperties());

        assertThat(raw.getTopicsList()).isEqualTo(library.topics());
    }

    /**
     * The first defect. UNORDERED is not the protocol's default - unspecified means core's default,
     * which is KEY - so this has to be asserted rather than assumed from the enum's ordering.
     */
    @Test
    void theHandWrittenRequestSetsOrderingExplicitlyAndToUnordered() {
        var raw = ReferenceDemo.rawConfigure(options(), TOPIC, kafkaProperties());
        var library = ReferenceDemo.libraryOptions(options(), TOPIC, kafkaProperties());

        assertThat(raw.hasOrdering())
                .withFailMessage("ordering must be SET, not left to the protocol default, which is KEY")
                .isTrue();
        assertThat(raw.getOrdering()).isEqualTo(ProcessingOrder.PROCESSING_ORDER_UNORDERED);
        assertThat(library.ordering())
                .contains(bz.stub.parallelconsumer.client.ProcessingOrder.UNORDERED);
    }

    /** The second defect. An omitted list negotiates the baseline, not the library's session. */
    @Test
    void theHandWrittenRequestDeclaresTheSameCapabilityTheLibraryDoes() {
        var raw = ReferenceDemo.rawConfigure(options(), TOPIC, kafkaProperties());

        assertThat(raw.getCapabilitiesList())
                .withFailMessage("the client library declares this token; an empty list is a "
                        + "different session, not a smaller one")
                .containsExactly(ReferenceDemo.DISPATCH_CAPABILITY);
    }

    @Test
    void bothArmsAskForTheSameConcurrency() {
        var raw = ReferenceDemo.rawConfigure(options(), TOPIC, kafkaProperties());
        var library = ReferenceDemo.libraryOptions(options(), TOPIC, kafkaProperties());

        assertThat(raw.getMaxConcurrency()).isEqualTo(37);
        assertThat(library.maxConcurrency().orElseThrow()).isEqualTo(37);
    }

    /**
     * Including the setting the two transports disagree about. The sidecar forces auto-commit off
     * whatever the map says, so this arm would work without it - but the demo passes it, and the
     * two arms must pass the same map or they are not the same session.
     */
    @Test
    void bothArmsPassTheSameKafkaProperties() {
        var raw = ReferenceDemo.rawConfigure(options(), TOPIC, kafkaProperties());
        var library = ReferenceDemo.libraryOptions(options(), TOPIC, kafkaProperties());

        assertThat(raw.getKafkaPropertiesMap()).isEqualTo(library.kafkaProperties());
        assertThat(raw.getKafkaPropertiesMap()).containsEntry("enable.auto.commit", "false");
    }
}
