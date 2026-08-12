package io.confluent.parallelconsumer.dashboard.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Builds the <em>interpolated view</em> the page's panels render from - the shape {@code app.js} hands to
 * {@code model.js} on every animation frame.
 *
 * <h2>The two forms of an offset, kept apart on purpose</h2>
 * <p>
 * Every partition carries its four offsets twice. {@code offsets} holds them as the exact strings that came off the
 * wire and is what the reader is shown; {@code geometry} holds the same four as approximate {@code double}s and
 * places pixels and nothing else. Above 2<sup>53</sup> the second form is wrong, which is precisely why the first
 * exists - so these fixtures build both, from one source string, exactly as the page does.
 */
final class PageViews {

    /**
     * An offset one above {@code Number.MAX_SAFE_INTEGER}, so a value that goes anywhere near a JavaScript
     * {@code Number} comes back a digit short. Any assertion using this is an assertion that it did not.
     */
    static final String ABOVE_SAFE_INTEGER = "9007199254740993";

    private PageViews() {
    }

    static JsonObject view(JsonObject... partitions) {
        JsonArray array = new JsonArray();
        for (JsonObject partition : partitions) {
            array.add(partition);
        }
        return new JsonObject()
                .put("fraction", 1)
                .put("settled", true)
                .put("empty", false)
                .put("partitions", array);
    }

    /**
     * One partition row. Offsets are passed as strings because that is how they arrive, and passing them as
     * {@code long} here would quietly re-introduce the very conversion these fixtures exist to catch.
     */
    static JsonObject partition(String topic,
                                int partition,
                                String committed,
                                String sequential,
                                String completed,
                                String seen,
                                Long incompleteOffsets) {
        return new JsonObject()
                .put("key", topic + "-" + partition)
                .put("topic", topic)
                .put("partition", partition)
                .put("offsets", new JsonObject()
                        .put("committed", committed)
                        .put("sequential", sequential)
                        .put("completed", completed)
                        .put("seen", seen))
                .put("geometry", new JsonObject()
                        .put("committed", approximate(committed))
                        .put("sequential", approximate(sequential))
                        .put("completed", approximate(completed))
                        .put("seen", approximate(seen)))
                .put("incompleteOffsets", incompleteOffsets)
                .put("processedRecords", 0)
                .put("failedRecords", 0)
                .put("slowRecords", 0)
                .put("assignmentEpoch", 1)
                .put("offsetOrderingConsistent", true);
    }

    /**
     * A partition whose commit has caught up with its processing - the shape a real Parallel Consumer produces once
     * everything assigned to it is done.
     * <p>
     * <strong>The committed offset is {@code highestDone + 1}, not {@code highestDone}.</strong> Kafka's committed
     * offset is the next offset to consume, and {@code PartitionState.getOffsetToCommit()} publishes
     * {@code highestSequentialSucceeded + 1} to the gauge the dashboard samples. A fixture that sets
     * {@code committed == sequential} therefore never exercises the caught-up path at all: it is a partition state no
     * running instance can produce, and it hid an off-by-one in both the Java invariant and the page's model.
     */
    static JsonObject caughtUpPartition(String topic, int partition, String highestDone) {
        String committed = new java.math.BigInteger(highestDone).add(java.math.BigInteger.ONE).toString();
        return partition(topic, partition, committed, highestDone, highestDone, highestDone, 0L);
    }

    /**
     * The same as {@link #partition} but declaring that the four markers were not read in one consistent instant, so
     * their order cannot be relied on.
     */
    static JsonObject inconsistentlySampledPartition(String topic,
                                                     int partition,
                                                     String committed,
                                                     String sequential,
                                                     String completed,
                                                     String seen,
                                                     Long incompleteOffsets) {
        return partition(topic, partition, committed, sequential, completed, seen, incompleteOffsets)
                .put("offsetOrderingConsistent", false);
    }

    /**
     * The numeric form the page uses for pixels. Null in, null out: an absent reading is not a measured zero.
     */
    private static Double approximate(String offset) {
        return offset == null ? null : Double.valueOf(offset);
    }
}
