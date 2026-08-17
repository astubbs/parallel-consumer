package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Health of PC's offset-encoding: how long encoding takes, which codecs are winning, and how close the encoded
 * payload is to the broker's offset-metadata size limit.
 * <p>
 * The payload figures are the ones that matter operationally - running out of metadata space is how PC loses its
 * record of which offsets are incomplete, so a rising {@link #getMetadataSpaceUsedMax()} is a warning worth showing
 * before it becomes an incident.
 * <p>
 * <strong>Absent is not zero.</strong> Boxed fields are {@code null} when the meter was not present; the usage map
 * is empty rather than null, and a codec that has never been used is simply absent from it.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Value
public class EncodingSnapshot {

    /**
     * Number of encoding operations timed by {@code pc.offsets.encoding.time}.
     */
    Long encodingCount;

    /**
     * Total time spent encoding, in milliseconds, from {@code pc.offsets.encoding.time}.
     */
    Double encodingTotalTimeMillis;

    /**
     * Longest single encoding observed in the timer's decaying window, in milliseconds.
     */
    Double encodingMaxTimeMillis;

    /**
     * Use count per codec, from the {@code pc.offsets.encoding.usage} counter family.
     * <p>
     * Keyed by the value of the counter's {@code encoding} tag (e.g. {@code BitSetV2Compressed}, {@code RunLength}).
     * Note the tag key really is {@code encoding} - {@code PCMetricsDef} documents it as {@code codec}, but that
     * metadata is only used for generating the metrics reference, and the emitting call site in
     * {@code OffsetMapCodecManager} tags it {@code encoding}. Unmodifiable, insertion-ordered, never null.
     */
    Map<String, Double> usageByEncoding;

    /**
     * Sample count for {@code pc.metadata.space.used}.
     */
    Long metadataSpaceUsedCount;

    /**
     * Mean ratio of encoded payload size to available offset-metadata space, from {@code pc.metadata.space.used}.
     */
    Double metadataSpaceUsedMean;

    /**
     * Worst observed ratio of payload size to available space. The number that predicts an encoding failure.
     */
    Double metadataSpaceUsedMax;

    /**
     * Sample count for {@code pc.payload.ratio.used}.
     */
    Long payloadRatioUsedCount;

    /**
     * Mean ratio of payload size to number of offsets encoded, from {@code pc.payload.ratio.used}.
     */
    Double payloadRatioUsedMean;

    /**
     * Worst observed ratio of payload size to number of offsets encoded.
     */
    Double payloadRatioUsedMax;

    @Builder(toBuilder = true)
    EncodingSnapshot(Long encodingCount,
                     Double encodingTotalTimeMillis,
                     Double encodingMaxTimeMillis,
                     Map<String, Double> usageByEncoding,
                     Long metadataSpaceUsedCount,
                     Double metadataSpaceUsedMean,
                     Double metadataSpaceUsedMax,
                     Long payloadRatioUsedCount,
                     Double payloadRatioUsedMean,
                     Double payloadRatioUsedMax) {
        this.encodingCount = encodingCount;
        this.encodingTotalTimeMillis = encodingTotalTimeMillis;
        this.encodingMaxTimeMillis = encodingMaxTimeMillis;
        this.usageByEncoding = usageByEncoding == null
                ? Collections.<String, Double>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(usageByEncoding));
        this.metadataSpaceUsedCount = metadataSpaceUsedCount;
        this.metadataSpaceUsedMean = metadataSpaceUsedMean;
        this.metadataSpaceUsedMax = metadataSpaceUsedMax;
        this.payloadRatioUsedCount = payloadRatioUsedCount;
        this.payloadRatioUsedMean = payloadRatioUsedMean;
        this.payloadRatioUsedMax = payloadRatioUsedMax;
    }
}
