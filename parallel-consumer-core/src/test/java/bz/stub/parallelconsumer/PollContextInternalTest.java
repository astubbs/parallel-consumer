package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.WorkContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * {@link PollContextInternal#summariseForLog()} exists so that a log line about a batch cannot grow with the batch -
 * see astubbs#170 / confluentinc#640.
 *
 * @author Antony Stubbs
 */
class PollContextInternalTest {

    private static final String SECRET_VALUE = "a-value-that-must-not-reach-the-error-line";

    @Test
    void summarisesTheBatchWithoutRenderingIt() {
        var module = new PCModuleTestEnv();
        List<WorkContainer<String, String>> batch = new ArrayList<>();
        batch.add(workFor(module, "topic-a", 0, 5));
        batch.add(workFor(module, "topic-a", 0, 6));
        batch.add(workFor(module, "topic-b", 1, 9));

        var context = new PollContextInternal<>(batch);

        assertThat(context.summariseForLog()).isEqualTo("3 records across 2 partitions: "
                + "topic-a-0: 2 records, offsets 5-6; "
                + "topic-b-1: 1 record, offset 9");
        assertThat(context.summariseForLog()).doesNotContain(SECRET_VALUE);

        // the unabridged render is what made the line unbounded - it stays available for DEBUG
        assertThat(context.toString()).contains(SECRET_VALUE);
    }

    @Test
    void emptyBatchSummarisesAsNoRecords() {
        assertThat(new PollContextInternal<>(new ArrayList<WorkContainer<String, String>>()).summariseForLog())
                .isEqualTo("0 records");
    }

    private static WorkContainer<String, String> workFor(PCModule<String, String> module, String topic, int partition, long offset) {
        var record = new ConsumerRecord<>(topic, partition, offset, "a-key", SECRET_VALUE);
        return new WorkContainer<>(0, record, module);
    }

}
