package io.confluent.parallelconsumer;

import lombok.Value;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Queue;

public class OffsetPayloadPerformanceHistory {

    @Value
    static class PayloadHistory {
        long offsetRange;
        int payloadSizeRequired;
    }

    Queue<PayloadHistory> history2;

    Queue<OffsetAndMetadata> history;

    public void onSuccess(OffsetAndMetadata entry) {
        history.add(entry);
    }

    public boolean predictCanStore(int quantity) {
//        long required = quantity * getOffsetsPerByteCurrentPerformance();
        long predictedAvailable = getSpaceLeft() / getOffsetsPerByteCurrentPerformance();
        return quantity < predictedAvailable;
    }

    private int getSpaceLeft(){
        return OffsetMapCodecManager.DefaultMaxMetadataSize - getPreviousPayloadHistory();
    }

    private int getPreviousPayloadHistory() {
        return history2.peek().payloadSizeRequired;
    }

    /**
     * @return (offsets/byte) the current number of messages per byte stored.
     */
    public long getOffsetsPerByteCurrentPerformance() {
        final long[] totalOffsets = {0};
        final int[] totalPayloadSizeRequired = {0};
        history2.forEach(x -> {
            totalOffsets[0] += x.offsetRange;
            totalPayloadSizeRequired[0] += x.payloadSizeRequired;
        });
        var performance = totalOffsets[0] / totalPayloadSizeRequired[0];
        return performance;
    }
}
