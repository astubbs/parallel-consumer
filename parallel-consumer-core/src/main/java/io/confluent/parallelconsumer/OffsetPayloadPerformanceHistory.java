package io.confluent.parallelconsumer;

import lombok.Value;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Queue;

public class OffsetPayloadPerformanceHistory {

    private static final int MAX_SIZE = 5;

    @Value
    static class PayloadHistory {
        long offsetRange;
        int payloadSizeRequired;
    }

    private Queue<PayloadHistory> history2;
    private Queue<PayloadHistory> badHistory;

    private Queue<OffsetAndMetadata> history;

    private int count = 0;

    public void count() {
        count++;
    }

    public void resetCount() {
        count = 0;
    }

    public boolean canFitCountPlusOne() {
        return predictCanStore(count + 1);
    }

    public void onSuccess(OffsetAndMetadata entry) {
        history.add(entry);
        evictMaybe();
    }

    private void evictMaybe() {
        if(history2.size()>MAX_SIZE)
            history2.remove();
    }

    public void onFailure(final OffsetAndMetadata offsetWithExtraMap) {
        badHistory.add(new PayloadHistory(offsetWithExtraMap, );
        evictMaybe();
    }

    public boolean predictCanStore(int quantity) {
//        long required = quantity * getOffsetsPerByteCurrentPerformance();
        long predictedAvailable = getSpaceLeft() / getOffsetsPerByteCurrentPerformance();
        return quantity < predictedAvailable;
    }

    public int getSpaceLeft() {
        return (int) ((OffsetMapCodecManager.DefaultMaxMetadataSize - getPreviousPayloadHistory()) * 0.8);
    }

    private int getPreviousPayloadHistory() {
        return history2.peek().payloadSizeRequired;
    }

    /**
     * @return (offsets / byte) the current number of messages per byte stored.
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
