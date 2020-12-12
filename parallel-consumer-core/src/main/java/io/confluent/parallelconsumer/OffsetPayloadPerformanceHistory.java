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
        long numberOfRecords;
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
        return predictCanStoreThisMore(count + 1);
    }

    public boolean canFitNewRange(long startOffset, long highestSucceeded) {
        long length = highestSucceeded - startOffset;
        return predictCanStoreThisMany(Math.toIntExact(length));
    }

    public void onSuccess(OffsetAndMetadata entry, long endOffset) {
        addEntry(entry, endOffset);
    }

    private void addEntry(final OffsetAndMetadata offsetAndMetadata, final long endOffset) {
        long startOffset = offsetAndMetadata.offset();
        long numberOfRecords = endOffset - startOffset; // todo fix
        int encodingLengthUsed = offsetAndMetadata.metadata().length();
        PayloadHistory historyEntry = new PayloadHistory(numberOfRecords, encodingLengthUsed);
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

    public boolean predictCanStoreThisMore(int quantity) {
        if (history.isEmpty())
            return true;
//        long required = quantity * getOffsetsPerByteCurrentPerformance();
        int remainingChars = getSpaceLeftOverInPreviousRun();
        var offsetsPerChar = getOffsetsPerCharCurrentPerformance();
        var predictedAvailable = remainingChars * offsetsPerChar;
        return quantity < predictedAvailable;
    }

    public boolean predictCanStoreThisMany(int quantity) {
        if (history.isEmpty())
            return true;
//        long required = quantity * getOffsetsPerByteCurrentPerformance();
//        int remainingChars = getSpaceLeftOverInPreviousRun();
        var offsetsPerChar = getOffsetsPerCharCurrentPerformance();
        var predictedAvailable = getMax() * offsetsPerChar;
        return quantity < predictedAvailable;
    }

    public int getSpaceLeftOverInPreviousRun() {
        Optional<Integer> previousPayloadSize = getPreviousPayloadSizeHistory();
        int previousSize = (previousPayloadSize.isEmpty()) ? 0 : previousPayloadSize.get();
        int absRemaining = getMax() - previousSize;
        double remainingOfThreshold = absRemaining * THRESHOLD;
        return (int) remainingOfThreshold;
    }

    private int getMax() {
        return OffsetMapCodecManager.DefaultMaxMetadataSize;
    }

    private Optional<Integer> getPreviousPayloadSizeHistory() {
        PayloadHistory peek = history.peek();
        return (peek == null) ? Optional.empty() : Optional.of(peek.payloadSizeRequired);
    }

    /**
     * @return (offsets / byte) the current number of messages per byte stored.
     */
    public double getOffsetsPerCharCurrentPerformance() {
        if (history.isEmpty())
            return Long.MAX_VALUE;
        final long[] totalNumRecords = {0};
        final int[] totalPayloadSizeRequired = {0};
        history.forEach(x -> {
            totalNumRecords[0] += x.numberOfRecords;
            totalPayloadSizeRequired[0] += x.payloadSizeRequired;
        });
//        double offsetsPerChar = Math.ceil((double)totalNumRecords[0] / totalPayloadSizeRequired[0]);
        double offsetsPerChar = (double) totalNumRecords[0] / totalPayloadSizeRequired[0];
        return offsetsPerChar;
    }
}
