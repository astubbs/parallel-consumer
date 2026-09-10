package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The facade's rebalance listener runs before the user's, and the user's cannot undo it (KTD2, R10).
 * <p>
 * The order is what makes the ledger's per-assignment rule hold: a user listener that reads anything about the
 * instance sees a state in which partitions it no longer owns have already stopped counting. The rest of this
 * class is about what a badly behaved user listener must not be able to cost.
 */
class RebalanceListenerChainTest {

    private static final String TOPIC = "orders";

    private static final TopicPartition PARTITION_ZERO = new TopicPartition(TOPIC, 0);

    private static ConsumerRecord<byte[], byte[]> record(int partition, long offset) {
        return new ConsumerRecord<>(TOPIC, partition, offset, new byte[0], new byte[0]);
    }

    /**
     * Records what happened and in which order, so "before" is asserted rather than assumed.
     */
    private static class RecordingListener implements ConsumerRebalanceListener {

        final List<String> calls = new ArrayList<>();

        private final RuntimeException throwOnRevoke;

        RecordingListener(RuntimeException throwOnRevoke) {
            this.throwOnRevoke = throwOnRevoke;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            calls.add("revoked " + partitions);
            if (throwOnRevoke != null) {
                throw throwOnRevoke;
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            calls.add("assigned " + partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            calls.add("lost " + partitions);
        }
    }

    @Test
    void theLedgerIsClearedBeforeTheUsersListenerRuns() {
        var ledger = new AttemptLedger();
        var parked = new ParkedRecords();
        ledger.advance(record(0, 5));
        var users = new RecordingListener(null) {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                super.onPartitionsRevoked(partitions);
                // read from inside the user's listener: by the time it runs, the clear has happened
                calls.add("ledger held " + ledger.attempts(TOPIC, 0, 5));
            }
        };

        new FacadeRebalanceListener(ledger, parked, users)
                .onPartitionsRevoked(Collections.singletonList(PARTITION_ZERO));

        assertThat(users.calls).containsExactly("revoked [orders-0]", "ledger held 0").inOrder();
    }

    /**
     * The user's listener is theirs: a throw from it propagates exactly as it does on the classic API, where the
     * engine wraps it and rethrows. What must not happen is the facade's own clear being skipped because of it -
     * that would leave the count of a partition somebody else now owns.
     */
    @Test
    void aUserListenerThatThrowsStillLeavesTheLedgerCleared() {
        var ledger = new AttemptLedger();
        var parked = new ParkedRecords();
        ledger.advance(record(0, 5));
        var users = new RecordingListener(new FakeRuntimeException("the user's listener is broken"));

        var chain = new FacadeRebalanceListener(ledger, parked, users);
        var thrown = assertThrows(FakeRuntimeException.class,
                () -> chain.onPartitionsRevoked(Collections.singletonList(PARTITION_ZERO)));

        assertThat(thrown).hasMessageThat().contains("the user's listener is broken");
        assertThat(ledger.attempts(TOPIC, 0, 5)).isEqualTo(0);
    }

    /**
     * A partition taken away without a revoke is the case that would otherwise leave a ledger entry behind for
     * ever, since nothing else will ever mention that partition again.
     */
    @Test
    void aLostPartitionIsClearedToo() {
        var ledger = new AttemptLedger();
        var parked = new ParkedRecords();
        ledger.advance(record(0, 5));
        var users = new RecordingListener(null);

        new FacadeRebalanceListener(ledger, parked, users)
                .onPartitionsLost(Collections.singletonList(PARTITION_ZERO));

        assertThat(ledger.attempts(TOPIC, 0, 5)).isEqualTo(0);
        assertThat(users.calls).containsExactly("lost [orders-0]");
    }

    /**
     * The parked set is per assignment for the same reason the ledger is: a parked record belongs to whoever owns
     * its partition now, and a fresh owner is the only party that can resume or export it (R10, R27).
     */
    @Test
    void aRevokedPartitionsParkedRecordsGoWithIt() {
        var ledger = new AttemptLedger();
        var parked = new ParkedRecords();
        parked.onAssigned(Collections.singletonList(PARTITION_ZERO));
        parked.spendCycle(record(0, 5));
        assertThat(parked.park(parkedRecord(0, 5))).isTrue();

        new FacadeRebalanceListener(ledger, parked, new RecordingListener(null))
                .onPartitionsRevoked(Collections.singletonList(PARTITION_ZERO));

        assertThat(parked.count()).isEqualTo(0);
        assertThat(parked.cyclesUsed(record(0, 5))).isEqualTo(0);
        // And the partition is no longer ours, so a worker still finishing on it cannot write a phantom entry.
        assertThat(parked.isOurs(PARTITION_ZERO)).isFalse();
        assertThat(parked.park(parkedRecord(0, 5))).isFalse();
    }

    private static ParkedRecord parkedRecord(int partition, long offset) {
        return new ParkedRecord(record(partition, offset), "key", 3, 1, new FakeRuntimeException("it failed"),
                "it ran out of attempts", Instant.now());
    }

    @Test
    void anAssignedPartitionStartsCountingFromZeroEvenIfARevokeWasMissed() {
        var ledger = new AttemptLedger();
        var parked = new ParkedRecords();
        ledger.advance(record(0, 5));
        var users = new RecordingListener(null);

        new FacadeRebalanceListener(ledger, parked, users)
                .onPartitionsAssigned(Collections.singletonList(PARTITION_ZERO));

        assertThat(ledger.attempts(TOPIC, 0, 5)).isEqualTo(0);
        assertThat(users.calls).containsExactly("assigned [orders-0]");
    }

    @Test
    void aDefinitionWithNoUserListenerRebalancesJustTheSame() {
        var ledger = new AttemptLedger();
        var parked = new ParkedRecords();
        ledger.advance(record(0, 5));

        var chain = new FacadeRebalanceListener(ledger, parked, null);
        chain.onPartitionsRevoked(Collections.singletonList(PARTITION_ZERO));
        chain.onPartitionsAssigned(Collections.singletonList(PARTITION_ZERO));
        chain.onPartitionsLost(Collections.singletonList(PARTITION_ZERO));

        assertThat(ledger.attempts(TOPIC, 0, 5)).isEqualTo(0);
    }
}
