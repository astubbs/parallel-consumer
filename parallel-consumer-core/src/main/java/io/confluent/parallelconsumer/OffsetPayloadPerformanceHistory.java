package io.confluent.parallelconsumer;

import lombok.Value;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

public class OffsetPayloadPerformanceHistory {

    /**
     * Number of history entries to keep to calculate trend against
     */
    private static final int MAX_SIZE = 5;

    /**
     * The cut off multiple point for storage allowed
     */
    public static final double THRESHOLD = 0.5;

    @Value
    static class PayloadHistory {
        long offsetRange;
        int payloadSizeRequired;
    }

    private Queue<PayloadHistory> history = new LinkedList<>();

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

    private void addEntry(final OffsetAndMetadata offsetAndMetadata, final long endOffset) {
        long startOffset = offsetAndMetadata.offset();
        long range = endOffset - startOffset;
        int encodingLengthUsed = offsetAndMetadata.metadata().length();
        PayloadHistory historyEntry = new PayloadHistory(range, encodingLengthUsed);
        history.add(historyEntry);
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
        if (history.isEmpty())
            return true;
//        long required = quantity * getOffsetsPerByteCurrentPerformance();
        long predictedAvailable = getSpaceLeft() * getOffsetsPerByteCurrentPerformance();
        return quantity < predictedAvailable;
    }

    public int getSpaceLeft() {
        Optional<Integer> previousPayloadHistory = getPreviousPayloadHistory();
        int previous = (previousPayloadHistory.isEmpty()) ? 0 : previousPayloadHistory.get();
        int absRemaining = OffsetMapCodecManager.DefaultMaxMetadataSize - previous;
        double remainingOfThreshold = absRemaining * THRESHOLD;
        return (int) remainingOfThreshold;
    }

    private Optional<Integer> getPreviousPayloadHistory() {
        PayloadHistory peek = history.peek();
        return (peek == null) ? Optional.empty() : Optional.of(peek.payloadSizeRequired);
    }

    /**
     * @return (offsets / byte) the current number of messages per byte stored.
     */
    public long getOffsetsPerByteCurrentPerformance() {
        if (history.isEmpty())
            return Long.MAX_VALUE;
        final long[] totalOffsets = {0};
        final int[] totalPayloadSizeRequired = {0};
        history.forEach(x -> {
            totalOffsets[0] += x.offsetRange;
            totalPayloadSizeRequired[0] += x.payloadSizeRequired;
        });
        var performance = Math.ceil(totalOffsets[0] / totalPayloadSizeRequired[0]);
        return (long) performance;
    }
}
