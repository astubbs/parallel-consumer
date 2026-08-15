package bz.stub.parallelconsumer.client.harness;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.AsyncRecordProcessor;
import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.client.conformance.SpikeConformanceTest;
import bz.stub.parallelconsumer.client.conformance.SpikeFixture;
import bz.stub.parallelconsumer.client.grpc.GrpcParallelConsumerClient;
import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * The shared spike suite under the gRPC transport - the identical test classes the direct sibling runs, with
 * the wire hop as the only difference (KTD20's control experiment, structural from the first unit). The
 * fixture is the proxy's own {@code ProxyHarness} engine lane, from the proxy test-jar: a real gRPC server on
 * an ephemeral loopback port, the real {@code ConfigureHandler} and {@code ProxyProcessor} behind it, mock
 * Kafka clients underneath - so the whole run stays in the surefire lane while every byte crosses a genuine
 * stream. Seeding is scenario-shaped because that is the engine lane's contract: the harness seeds when the
 * client's {@code Configure} arrives, so the connecting client must name the scenario's topic.
 * <p>
 * <b>It lives in this module rather than beside the transport it tests</b>, and that placement is the whole
 * reason this module exists: the engine dependency it needs is a reactor edge, so holding it in the transport
 * module made every wrapper of that transport build the engine. The reasoning, and the reactor-list
 * measurement that says the hazard is gone, are in this module's pom.
 *
 * @author Antony Stubbs
 */
class GrpcSpikeConformanceTest extends SpikeConformanceTest {

    @Override
    protected SpikeFixture fixture(String topic, List<SpikeFixture.Seed> seeds) {
        var seedRecords = new ArrayList<HarnessScenario.SeedRecord>(seeds.size());
        for (SpikeFixture.Seed seed : seeds) {
            seedRecords.add(new HarnessScenario.SeedRecord(seed.key(), seed.value()));
        }
        return new GrpcFixture(new ProxyHarness(new HarnessScenario(topic, seedRecords)));
    }

    private static final class GrpcFixture implements SpikeFixture {

        private final ProxyHarness harness;
        private final int port;

        private GrpcParallelConsumerClient client;

        private GrpcFixture(ProxyHarness harness) {
            this.harness = harness;
            this.port = harness.startEngine();
        }

        @Override
        public void start(ClientOptions options, RecordProcessor processor) {
            // nothing after it: the harness performs assignment and seeding when the Configure arrives
            build(options).poll(processor);
        }

        @Override
        public void startAsync(ClientOptions options, AsyncRecordProcessor processor) {
            build(options).pollAsync(processor);
        }

        private GrpcParallelConsumerClient build(ClientOptions options) {
            client = GrpcParallelConsumerClient.builder()
                    .port(port)
                    .options(options)
                    .build();
            return client;
        }

        @Override
        public OptionalLong committedOffset() {
            return harness.lastCommittedOffset();
        }

        @Override
        public List<ProducedRecord> produced() {
            var produced = new ArrayList<ProducedRecord>();
            for (var record : harness.engineProducedRecords()) {
                produced.add(ProducedRecord.decodeUtf8(record.topic(), record.key(), record.value()));
            }
            return produced;
        }

        @Override
        public long recordsOutForProcessing() {
            return harness.engineRecordsOutForProcessing();
        }

        @Override
        public void close() {
            // client first, so the engine is not dispatching into a vanished stream during its own teardown;
            // harness.close() then asserts the engine ended without a failure cause
            if (client != null) {
                client.close();
            }
            harness.close();
        }
    }
}
