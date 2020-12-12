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

    private Queue<PayloadHistory> history;


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

    public void onSuccess(OffsetAndMetadata entry, long endOffset) {
        addEntry(entry, endOffset);
    }

    private void addEntry(final OffsetAndMetadata entry, final long endOffset) {
        long startOffset = entry.offset();
        long range = endOffset - startOffset;
        history.add(new PayloadHistory(range, entry.metadata().length()));
        evictMaybe();
    }

    private void evictMaybe() {
        if (history.size() > MAX_SIZE)
            history.remove();
    }

    public void onFailure(final OffsetAndMetadata entry, long endOffset) {
        addEntry(entry, endOffset);
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
        return history.peek().payloadSizeRequired;
    }

    /**
     * @return (offsets / byte) the current number of messages per byte stored.
     */
    public long getOffsetsPerByteCurrentPerformance() {
        final long[] totalOffsets = {0};
        final int[] totalPayloadSizeRequired = {0};
        history.forEach(x -> {
            totalOffsets[0] += x.offsetRange;
            totalPayloadSizeRequired[0] += x.payloadSizeRequired;
        });
        var performance = totalOffsets[0] / totalPayloadSizeRequired[0];
        return performance;
    }
}
