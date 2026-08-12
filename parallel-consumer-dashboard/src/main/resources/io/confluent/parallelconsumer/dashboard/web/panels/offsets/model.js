/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * The offset view model: one partition's four markers turned into something a renderer can draw, computed ONCE and
 * shared by every registered style so two styles can never disagree about the same partition.
 *
 * THE ARITHMETIC RULE. Every quantity the reader is shown is computed with BigInt from the string offsets the
 * server sent. The numbers in `geometry` exist only to place pixels: above 2^53 they are wrong, and a bar a few
 * hundred pixels wide does not care. Nothing crosses between the two.
 *
 * WHAT THE SPANS MEAN, AND THE WORDING TRAP. The span between the highest sequentially-succeeded offset and the
 * highest succeeded offset is work this instance has ALREADY FINISHED and which is encoded into the commit
 * metadata. It survives a restart and is not replayed. It is the library succeeding: a plain single-threaded
 * consumer would be stopped at the lowest incomplete offset and would have to reprocess that entire span. Calling
 * it uncommitted, pending, at risk or lost is wrong on the facts and inverts the story, so nothing here does.
 */

import {formatBigCount, toBigInt} from '../../ui.js';

/**
 * Builds one model per partition, sorted with the widest won span first - so the partition doing the most
 * impressive work, and equally the one whose head-of-line blocking is most severe, is at the top rather than
 * wherever its partition number happens to put it.
 */
export function buildPartitionModels(view) {
    const partitions = Array.isArray(view.partitions) ? view.partitions : [];
    return partitions.map(buildPartitionModel).sort(byWonSpanDescending);
}

/**
 * The hero number: across every assigned partition, how many records this instance has completed beyond the point a
 * consumer without this machinery would have been stopped at. Exactly highest-succeeded minus
 * highest-sequential-succeeded, summed - the same arithmetic the proposed core meter uses, so the graphic and the
 * meter cannot drift apart.
 */
export function totalWonRecords(models) {
    let total = 0n;
    for (const model of models) {
        if (model.wonCount !== null) {
            total += model.wonCount;
        }
    }
    return total;
}

function buildPartitionModel(partition) {
    // THE COMMITTED OFFSET IS EXCLUSIVE AND THE OTHER THREE ARE NOT. Kafka commits the NEXT offset to consume, so the
    // wire's `committed` reads one ABOVE the highest offset actually done - on a caught-up partition it sits one past
    // sequential, completed and seen. Comparing it raw against the other three makes "caught up" look like an
    // inversion and makes the ready span negative, so it is converted here, once, at the model boundary. Every LABEL
    // below keeps `partition.offsets.committed`, the raw wire value: that is the number the broker holds and the
    // offset a restart resumes from, and it is what the reader should be shown.
    const committedRaw = toBigInt(partition.offsets.committed);
    const committed = committedRaw === null ? null : committedRaw - 1n;
    const sequential = toBigInt(partition.offsets.sequential);
    const completed = toBigInt(partition.offsets.completed);
    const seen = toBigInt(partition.offsets.seen);

    const readySpan = span(committed, sequential);
    const wonSpan = span(sequential, completed);
    const seenSpan = span(completed, seen);

    const incompleteOffsets = numberOrNull(partition.incompleteOffsets);
    const hasIncomplete = incompleteOffsets !== null && Math.round(incompleteOffsets) > 0;
    // the lowest offset that is NOT done, which is where a single-threaded consumer would be stopped. It is one past
    // the highest contiguous success by definition, and it is computed with BigInt like everything else.
    const stopOffset = hasIncomplete && sequential !== null ? sequential + 1n : null;

    // the drawn axis uses the same inclusive committed position the spans are computed from, so the picture and the
    // arithmetic cannot disagree about where the bar starts
    const positions = geometryPositions(inclusiveGeometry(partition.geometry));

    return {
        key: partition.key,
        label: partition.topic + '-' + partition.partition,
        topic: partition.topic,
        partition: partition.partition,

        // exact, for display
        offsets: partition.offsets,
        /**
         * The committed marker as an inclusive offset - the highest offset actually committed, one below the wire
         * value. Only the axis end and the caught-up sentence use it, because those two label the DRAWN axis rather
         * than the broker's number; everything that reports "last committed" prints `offsets.committed`.
         */
        committedInclusiveText: committed === null ? null : committed.toString(),
        stopOffsetText: stopOffset === null ? null : stopOffset.toString(),
        wonCount: wonSpan,
        wonCountText: formatBigCount(wonSpan),
        readyCount: readySpan,
        seenAheadCount: seenSpan,

        incompleteOffsets: incompleteOffsets,
        hasIncomplete: hasIncomplete,
        processedRecords: partition.processedRecords,
        failedRecords: partition.failedRecords,
        slowRecords: partition.slowRecords,
        assignmentEpoch: partition.assignmentEpoch,

        /**
         * False means the four markers were not read in one consistent instant, so their order cannot be trusted.
         * Read from the document rather than recomputed here - the snapshot is the thing that knows.
         */
        offsetOrderingConsistent: partition.offsetOrderingConsistent !== false,

        /**
         * Every known marker sits on the same offset: nothing outstanding at all. This must read as healthy, not as
         * an error - it is a partition that is completely caught up.
         */
        collapsed: allKnownEqual([committed, sequential, completed, seen]),

        /** 0..1 along this partition's own axis. Per partition, so one wide partition cannot squash the rest. */
        positions: positions,
        /** True when the axis has no width at all, so a renderer draws one marker rather than dividing by zero. */
        degenerateAxis: positions.axisWidth <= 0
    };
}

/**
 * The difference between two offsets, in BigInt, or null when either is absent. Negative differences are clamped to
 * zero: a negative span can only come from markers sampled at different instants, and drawing a bar backwards
 * would be a worse answer than drawing nothing.
 */
function span(low, high) {
    if (low === null || high === null) {
        return null;
    }
    const difference = high - low;
    return difference < 0n ? 0n : difference;
}

function allKnownEqual(values) {
    const known = values.filter(value => value !== null);
    if (known.length === 0) {
        return false;
    }
    return known.every(value => value === known[0]);
}

/**
 * The geometry numbers with the committed marker moved to its inclusive position, so the bar is drawn from the same
 * point the exact arithmetic above measures from. Pixels only - see the header for why these never mix with the
 * BigInt values.
 */
function inclusiveGeometry(geometry) {
    const committed = geometry.committed;
    if (committed === null || committed === undefined) {
        return geometry;
    }
    return {
        committed: committed - 1,
        sequential: geometry.sequential,
        completed: geometry.completed,
        seen: geometry.seen
    };
}

/**
 * Turns the four approximate geometry numbers into fractions along this partition's own axis.
 *
 * PER-PARTITION SCALE, DELIBERATELY. A shared axis across partitions would let one partition whose seen offset is
 * millions ahead of its commit squash every informative span on the page into a single pixel. The cost is that two
 * rows are not directly comparable by width, which is why each row prints its own axis ends.
 */
function geometryPositions(geometry) {
    const values = [geometry.committed, geometry.sequential, geometry.completed, geometry.seen]
        .filter(value => value !== null && value !== undefined);
    if (values.length === 0) {
        return {axisLow: null, axisHigh: null, axisWidth: 0, committed: 0, sequential: 0, completed: 0, seen: 0};
    }
    const low = Math.min.apply(null, values);
    const high = Math.max.apply(null, values);
    const width = high - low;
    const at = value => {
        if (value === null || value === undefined || width <= 0) {
            return 0;
        }
        return (value - low) / width;
    };
    return {
        axisLow: low,
        axisHigh: high,
        axisWidth: width,
        committed: at(geometry.committed),
        sequential: at(geometry.sequential),
        completed: at(geometry.completed),
        seen: at(geometry.seen)
    };
}

function numberOrNull(value) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) {
        return null;
    }
    return Number(value);
}

function byWonSpanDescending(left, right) {
    const a = left.wonCount === null ? -1n : left.wonCount;
    const b = right.wonCount === null ? -1n : right.wonCount;
    if (a > b) {
        return -1;
    }
    if (a < b) {
        return 1;
    }
    return left.label.localeCompare(right.label);
}
