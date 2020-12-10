package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.csid.utils.StringUtils.msg;

public class PartitionOffsetEncoding {


    /**
     * Todo: Does this need to be run per message of a a result set? or only once the batch has been finished, once per
     * partition? As we can't change anything mid flight - only for the next round
     */
    private void manageOffsetEncoderSpaceRequirements() {
        int perPartition = getMetadataSpaceAvailablePerPartition();
        double tolerance = 0.7; // 90%

        boolean anyPartitionsAreHalted = false;

        // for each encoded partition so far, check if we're within tolerance of max space
        for (final Map.Entry<TopicPartition, OffsetSimultaneousEncoder> entry : partitionContinuousOffsetEncoders.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetSimultaneousEncoder encoder = entry.getValue();
            int encodedSize = encoder.getEncodedSizeEstimate();

            int allowed = (int) (perPartition * tolerance);

            boolean moreMessagesAreAllowed = allowed > encodedSize;

            boolean previousMessagesAllowedState = partitionMoreRecordsAllowedToProcess.get(tp);
            // update partition with tolerance threshold crossed status
            partitionMoreRecordsAllowedToProcess.put(tp, moreMessagesAreAllowed);
            if (!moreMessagesAreAllowed && previousMessagesAllowedState) {
                anyPartitionsAreHalted = true;
                log.debug(msg("Back-pressure for {} activated, no more messages allowed, best encoder {} needs {} which is more than " +
                                "calculated restricted space of {} (max: {}, tolerance {}%). Messages will be allowed again once messages " +
                                "complete and encoding space required shrinks.",
                        tp, encoder.getSmallestCodec(), encodedSize, allowed, perPartition, tolerance * 100));
            } else if (moreMessagesAreAllowed && !previousMessagesAllowedState) {
                log.trace("Partition is now unblocked, needed {}, allowed {}", encodedSize, allowed);
            } else if (!moreMessagesAreAllowed && !previousMessagesAllowedState) {
                log.trace("Partition {} still blocked for new message processing", tp);
            }

            boolean offsetEncodingAlreadyWontFitAtAll = encodedSize > perPartition;
            if (offsetEncodingAlreadyWontFitAtAll) {
                log.warn("Despite attempts, current offset encoding requirements are now above what will fit. Offset encoding " +
                        "will be dropped for this round, but no more messages for this partition will be attempted until " +
                        "messages complete successfully and the offset encoding space required shrinks again.");
                log.warn(msg("Back-pressure for {} activated, no more messages allowed, best encoder {} needs {} which is more than calculated " +
                                "restricted space of {} (max: {}, tolerance {}%). Messages will be allowed again once messages complete and encoding " +
                                "space required shrinks.",
                        tp, encoder.getSmallestCodec(), encodedSize, allowed, perPartition, tolerance * 100));
            }

        }
        if (anyPartitionsAreHalted) {
            log.debug("Some partitions were halted");
        }
    }

//    private int getTotalOffsetMetaCharacterLength(final Map<TopicPartition, OffsetAndMetadata> perPartitionNextExpectedOffset, int totalOffsetMetaCharacterLength, final LinkedHashSet<Long> incompleteOffsets, final TopicPartition topicPartitionKey) {
//        // offset map building
//        // Get final offset data, build the the offset map, and replace it in our map of offset data to send
//        // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?
//        if (!incompleteOffsets.isEmpty()) {
//            long offsetOfNextExpectedMessage;
//            OffsetAndMetadata finalOffsetOnly = perPartitionNextExpectedOffset.get(topicPartitionKey);
//            if (finalOffsetOnly == null) {
//                // no new low water mark to commit, so use the last one again
//                offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
//            } else {
//                offsetOfNextExpectedMessage = finalOffsetOnly.offset();
//            }
//
//            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumerMgr);
//            try {
//                // TODO change from offsetOfNextExpectedMessage to getting the pre computed one from offsetOfNextExpectedMessage
//                Long highestCompletedOffset = partitionOffsetHighestSucceeded.get(topicPartitionKey);
//                if (highestCompletedOffset == null) {
//                    log.error("What now?");
//                }
//                // encode
//                String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, topicPartitionKey, incompleteOffsets);
//                totalOffsetMetaCharacterLength += offsetMapPayload.length();
//                OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
//                perPartitionNextExpectedOffset.put(topicPartitionKey, offsetWithExtraMap);
//            } catch (EncodingNotSupportedException e) {
//                log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance", e);
////                backoffer.onFailure();
//            }
//        }
//        return totalOffsetMetaCharacterLength;
//    }

    /**
     * Once all the offset maps have been calculated, check if they're too big, and if so, remove all of them.
     * <p>
     * Implication of this is that if the system has to recover from this offset, then it will have to replay all the
     * messages that were otherwise complete.
     * <p>
     * Must be thread safe.
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    private void maybeStripOffsetPayload(Map<TopicPartition, OffsetAndMetadata> offsetsToSend,
                                         int totalOffsetMetaCharacterLength) {
        // TODO: Potential optimisation: if max metadata size is shared across partitions, the limit used could be relative to the number of
        //  partitions assigned to this consumer. In which case, we could derive the limit for the number of downloaded but not committed
        //  offsets, from this max over some estimate. This way we limit the possibility of hitting the hard limit imposed in the protocol, thus
        //  retaining the offset map feature, at the cost of potential performance by hitting a soft maximum in our uncommitted concurrent processing.
        if (totalOffsetMetaCharacterLength > OffsetMapCodecManager.DefaultMaxMetadataSize) {
            log.warn("Offset map data too large (size: {}) to fit in metadata payload - stripping offset map out. " +
                            "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = 4096",
                    totalOffsetMetaCharacterLength);
            // strip all payloads
            // todo iteratively strip the largest payloads until we're under the limit
            int totalSizeEstimates = 0;
            for (var entry : offsetsToSend.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetSimultaneousEncoder offsetSimultaneousEncoder = partitionContinuousOffsetEncoders.get(tp);
                int encodedSizeEstimate = offsetSimultaneousEncoder.getEncodedSizeEstimate();
                log.debug("Estimate for {} {}", tp, encodedSizeEstimate);
                totalSizeEstimates += encodedSizeEstimate;
                OffsetAndMetadata v = entry.getValue();
                OffsetAndMetadata stripped = new OffsetAndMetadata(v.offset()); // meta data gone
                offsetsToSend.replace(tp, stripped);
            }
            log.debug("Total estimate for all partitions {}", totalSizeEstimates);
//            backoffer.onFailure();
        } else if (totalOffsetMetaCharacterLength != 0) {
            log.debug("Offset map small enough to fit in payload: {} (max: {})", totalOffsetMetaCharacterLength, OffsetMapCodecManager.DefaultMaxMetadataSize);
//            backoffer.onSuccess();
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> serialiseEncoders() {
        if (!isDirty()) {
            log.trace("Nothing to commit, work state is clean");
            return UniMaps.of();
        }

        Map<TopicPartition, OffsetAndMetadata> offsetMetadataToCommit = new HashMap<>();
//        int totalPartitionQueueSizeForLogging = 0;
        int totalOffsetMetaCharacterLengthUsed = 0;

        for (final Map.Entry<TopicPartition, OffsetSimultaneousEncoder> tpEncoder : partitionContinuousOffsetEncoders.entrySet()) {
            TopicPartition topicPartitionKey = tpEncoder.getKey();
            OffsetSimultaneousEncoder precomputed = tpEncoder.getValue();
            log.trace("Serialising available encoders for {} using {}", topicPartitionKey, precomputed);
            try {
                precomputed.serializeAllEncoders();

                OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumerMgr);
                String smallestMetadataPacked = om.makeOffsetMetadataPayload(precomputed);

                totalOffsetMetaCharacterLengthUsed += smallestMetadataPacked.length();

                long nextExpectedOffsetFromBroker = precomputed.getBaseOffset();
                OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(nextExpectedOffsetFromBroker, smallestMetadataPacked);
                offsetMetadataToCommit.put(topicPartitionKey, offsetWithExtraMap);
            } catch (EncodingNotSupportedException e) {
                log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance", e);
//                backoffer.onFailure();
            }
        }

        maybeStripOffsetPayload(offsetMetadataToCommit, totalOffsetMetaCharacterLengthUsed);

        log.debug("Scan finished, coalesced to {} offset(s) ({}) to be committed",
                offsetMetadataToCommit.size(), offsetMetadataToCommit);

        return offsetMetadataToCommit;
    }
}
