package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.concurrent.TimeUnit;

import static bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest.FALLBACK_IMAGE;
import static bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest.imageForClientVersion;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards which broker the whole integration suite actually runs against. The suite is this project's
 * evidence base, so what started matters as much as what passed - and a container is easy to change
 * back by accident, in a one-line import or a copied constant, with every test still green.
 *
 * @author Antony Stubbs
 */
@Slf4j
class BrokerImageTest extends BrokerIntegrationTest<String, String> {

    /**
     * Asserts against the container that is running, not against the source that names it, so that a
     * change which silently failed to take effect is distinguishable from one that worked.
     */
    @Test
    void theBrokerUnderTestIsTheApacheKafkaImage() {
        String running = kafkaContainer.getDockerImageName();
        log.info("Broker under test: {}", running);

        assertThat(running)
                .as("the integration suite must run on the Apache Kafka image, not a vendor distribution")
                .startsWith("apache/kafka:");
    }

    /**
     * The broker has to speak the same Kafka release as the client, or a version-matrix run proves
     * nothing about the pairing it claims to test.
     */
    @Test
    void theBrokerVersionMatchesTheClientOnTheClasspath() {
        String clientVersion = AppInfoParser.getVersion().split("-")[0];

        assertThat(kafkaContainer.getDockerImageName())
                .as("broker image must carry the client's own Kafka version. If this fails after a " +
                        "-Dkafka.version bump, the requested release has no apache/kafka image and the " +
                        "suite silently fell back - the run is testing a mismatched pair, not the one asked for")
                .isEqualTo("apache/kafka:" + clientVersion);
    }

    /**
     * The suite tunes the broker through {@code KAFKA_*} environment variables, and each image has its own
     * entrypoint translating them into broker properties. An image swap can therefore drop that tuning
     * silently: everything still starts, the tests still pass, and the broker is simply running defaults.
     * So this asks the broker what it is actually configured with, rather than trusting the container spec.
     */
    @Test
    void theBrokerEnvironmentTuningReachesTheBrokerConfig() {
        Config brokerConfig = describeTheRunningBroker();

        assertThat(brokerConfig.get("group.initial.rebalance.delay.ms").value())
                .as("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS must still reach the broker - the suite's group " +
                        "formation timing depends on it")
                .isEqualTo("500");
        assertThat(brokerConfig.get("transaction.state.log.num.partitions").value())
                .as("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS must still reach the broker")
                .isEqualTo("1");
        assertThat(brokerConfig.get("transaction.state.log.replication.factor").value())
                .as("a single-node broker cannot replicate the transaction log more than once")
                .isEqualTo("1");
    }

    /**
     * The Apache image is KRaft-only. Recorded because the Confluent image it replaced ran an embedded
     * ZooKeeper by default, so this is the one real behavioural change in the move.
     */
    @Test
    void theBrokerRunsInKRaftMode() {
        assertThat(describeTheRunningBroker().get("process.roles").value())
                .as("KRaft combined mode - no ZooKeeper")
                .contains("broker", "controller");
    }

    /**
     * Describes the one node in the cluster BY ITS OWN ID. The empty-string broker resource describes
     * cluster-wide dynamic defaults instead, where every static config reads back null - which looks
     * exactly like a setting that failed to apply.
     */
    @SneakyThrows
    private Config describeTheRunningBroker() {
        var admin = getKcu().getAdmin();
        String nodeId = String.valueOf(admin.describeCluster().nodes().get(30, TimeUnit.SECONDS)
                .iterator().next().id());
        var resource = new ConfigResource(ConfigResource.Type.BROKER, nodeId);
        return admin.describeConfigs(UniLists.of(resource)).all().get(30, TimeUnit.SECONDS).get(resource);
    }

    /**
     * There is no version translation any more - the tag is the Kafka version. The Confluent images
     * needed CP major = AK major + 4 and a forced {@code .0} patch; both are gone, and this pins that.
     */
    @Test
    void theImageTagIsTheKafkaVersionUntranslated() {
        assertThat(imageForClientVersion("3.9.2")).isEqualTo("apache/kafka:3.9.2");
        assertThat(imageForClientVersion("4.0.0")).isEqualTo("apache/kafka:4.0.0");
    }

    @Test
    void preReleaseClientVersionsResolveToTheirReleaseImage() {
        assertThat(imageForClientVersion("4.0.0-SNAPSHOT")).isEqualTo("apache/kafka:4.0.0");
        assertThat(imageForClientVersion("4.1.0-rc1")).isEqualTo("apache/kafka:4.1.0");
    }

    @Test
    void unparseableClientVersionsFallBackRatherThanFailingToPull() {
        assertThat(imageForClientVersion("unknown")).isEqualTo(FALLBACK_IMAGE);
        assertThat(imageForClientVersion("")).isEqualTo(FALLBACK_IMAGE);
        assertThat(imageForClientVersion(null)).isEqualTo(FALLBACK_IMAGE);
    }

    /**
     * {@code apache/kafka} images start at 3.7.0. An older client asks for a tag that was never
     * published, so the fallback keeps the failure legible instead of an opaque registry error.
     */
    @Test
    void clientVersionsOlderThanTheFirstPublishedImageFallBack() {
        assertThat(imageForClientVersion("3.6.2")).isEqualTo(FALLBACK_IMAGE);
        assertThat(imageForClientVersion("2.8.1")).isEqualTo(FALLBACK_IMAGE);
        assertThat(imageForClientVersion("3.7.0")).isEqualTo("apache/kafka:3.7.0");
    }
}
