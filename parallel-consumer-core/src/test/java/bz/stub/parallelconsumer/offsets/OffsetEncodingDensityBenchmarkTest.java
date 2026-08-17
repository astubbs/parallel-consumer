package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.RepoRoot;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Measures how DENSE the offset-metadata encodings are - the incumbent encoders that ship today, against three
 * candidate wire formats that have been proposed but never measured. This is unit U1 of
 * {@code docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md}, and its committed output
 * ({@code docs/offset-encoding-density-benchmark.md}) is the permanent answer to issue
 * <a href="https://github.com/astubbs/parallel-consumer/issues/192">astubbs#192</a> ("why not RoaringBitmap? why
 * not a denser text encoding than Base64?").
 *
 * <h2>It is a SIZE benchmark, not a timing benchmark</h2>
 * There is no JMH here and there must not be. Encoded size is a deterministic function of the incompletes
 * distribution and the range length, so it can be measured exactly, reproducibly and cheaply - which is what makes
 * the output committable and diffable. Encode-time CPU is explicitly out of scope for this plan (PR astubbs#106 owns it).
 *
 * <h2>Why the candidates are LAYOUT WRITERS and not encoders</h2>
 * The three candidates below ({@link #chunkedBitSetLayout}, {@link #sparseDeltaListLayout},
 * {@link #unsignedRunLengthLayout}) emit real bytes in the shape the plan's HTD sketches, but they are NOT
 * registered {@link OffsetEncoder}s and have no decode path - deliberately (KTD4). Real bytes are the point: they
 * go through the same zstd and outer-string arithmetic as the incumbents, so the numbers are commensurable
 * post-compression, which closed-form arithmetic could never be. Building a full encoder for a candidate that
 * turns out to lose would be exactly the abandoned experimental code the plan forbids.
 *
 * <h2>Determinism is a hard requirement</h2>
 * The report is committed and gated by {@code bin/offset-encoding-density-report.sh --check}, so two runs must
 * produce byte-identical output. That means: seeded {@link Random} only (never {@code Math.random}), no
 * timestamps, no absolute paths, no host details, and {@link Locale#ROOT} on every format call.
 *
 * @author Antony Stubbs
 * @see OffsetSimultaneousEncoder
 */
@Slf4j
// READ side of the codec's static state - see WorkManagerOffsetMapCodecManagerTest's own note. We read
// OffsetMapCodecManager.DefaultMaxMetadataSize only to PIN it (the report itself uses the literal 4096, so a
// concurrent writer cannot change the report's content).
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
// WRITE side: this class sets OffsetSimultaneousEncoder.compressionForced, so unlike the sibling tests it takes
// the lock in the default READ_WRITE mode, excluding both readers and other writers.
@ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK)
class OffsetEncodingDensityBenchmarkTest {

    /**
     * The broker's {@code offset.metadata.max.bytes} default, mirrored by
     * {@link OffsetMapCodecManager#DefaultMaxMetadataSize}.
     * <p>
     * Deliberately a LITERAL and not a read of that field: the field is a mutable static that
     * {@code OffsetEncodingBackPressureTest} and friends overwrite (with 30 / 40) for their own purposes. Reading it
     * here would make the committed report depend on test execution order - a report that changes depending on what
     * else ran is not a report. {@link #capAndThresholdConstantsAreWhatTheReportAssumes()} pins the literal against
     * the real constant so the two cannot drift silently.
     */
    private static final int MAX_METADATA_CHARS = 4096;

    /**
     * The back-pressure engagement point: {@link PartitionStateManager#USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT} of
     * the cap. At or above this many chars, PC starts blocking - so density only buys real headroom here.
     */
    private static final int BACK_PRESSURE_CHARS =
            (int) (MAX_METADATA_CHARS * PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

    /**
     * KTD5's decision rule: a candidate must beat the best incumbent by at least this fraction, post-compression and
     * post-string-encoding, under the production compression rule, on at least one scenario class whose incumbent
     * payload is already at or above {@link #BACK_PRESSURE_CHARS}.
     */
    private static final double SHIP_THRESHOLD_FRACTION = 0.10;

    /**
     * Base seed for the corpus. Mixed with the family and the range size so that every scenario's data depends only
     * on its own identity - not on the order scenarios are generated in, which would make the report depend on the
     * loop structure.
     */
    private static final long CORPUS_SEED = 192_2026L;

    /** Range sizes measured for every scenario family. */
    private static final int[] RANGE_SIZES = {1_000, 10_000, 100_000, 1_000_000};

    private static final Path REPORT_IN_TARGET = Paths.get("target", "offset-encoding-density-benchmark.md");
    private static final String REPORT_IN_DOCS = "docs/offset-encoding-density-benchmark.md";

    /** Set by {@code bin/offset-encoding-density-report.sh --check}; unset in a normal test run. */
    private static final String CHECK_PROPERTY = "density.report.check";

    // ---------------------------------------------------------------------------------------------------------
    // Scenario corpus
    // ---------------------------------------------------------------------------------------------------------

    // The shapes a family can have. Strings rather than an enum ON PURPOSE: the truth-generator-maven-plugin
    // generates a Subject for every enum it finds in these packages, including a nested one, and a nested enum
    // that is not public breaks the generated code (it lands in bz.stub.parallelconsumer, another package). A
    // plain private class is invisible to it.
    private static final String SHAPE_TRAILING_RUN = "trailing-run";
    private static final String SHAPE_UNIFORM = "uniform";
    private static final String SHAPE_CLUSTERED = "clustered";
    private static final String SHAPE_ALTERNATING = "alternating";
    private static final String SHAPE_ALL_INCOMPLETE = "all-incomplete";

    /**
     * One assumed distribution of incomplete offsets. Assumed, not observed - there is no workload telemetry of
     * {@code ratioMetadataSpaceUsedDistributionSummary} available to this project, which is recorded as an
     * assumption in the plan and restated in the report.
     */
    private static final class Family {
        final int index;
        final String label;
        final String shape;
        /** Only meaningful for {@link #SHAPE_UNIFORM}. */
        final double density;

        Family(int index, String label, String shape, double density) {
            this.index = index;
            this.label = label;
            this.shape = shape;
            this.density = density;
        }
    }

    private static final Family[] FAMILIES = {
            // Contiguous completes, then one incomplete run at the tail. The healthy steady state: work in flight.
            new Family(0, "trailing-incomplete-run", SHAPE_TRAILING_RUN, 0),
            new Family(1, "uniform-random 0.1%", SHAPE_UNIFORM, 0.001d),
            new Family(2, "uniform-random 1%", SHAPE_UNIFORM, 0.01d),
            new Family(3, "uniform-random 5%", SHAPE_UNIFORM, 0.05d),
            new Family(4, "uniform-random 20%", SHAPE_UNIFORM, 0.20d),
            // Bursts of 10-100 incompletes at random starts - a few slow keys, or a poison-pill cluster.
            new Family(5, "clustered-bursts", SHAPE_CLUSTERED, 0),
            // Every other offset incomplete: the pathological worst case for run-length.
            new Family(6, "alternating-xo", SHAPE_ALTERNATING, 0),
            // Nothing succeeded yet.
            new Family(7, "all-incomplete", SHAPE_ALL_INCOMPLETE, 0),
    };

    private static final Family FAMILY_ALL_INCOMPLETE = FAMILIES[7];

    private static long seedFor(Family family, int rangeSize) {
        return CORPUS_SEED * 31 + family.index * 1_000_003L + rangeSize;
    }

    /**
     * The corpus, addressed by a family's label - so sibling tests can assert against the same scenarios this report
     * is generated from instead of inventing their own.
     *
     * @param familyLabel one of the {@link #FAMILIES} labels, e.g. {@code "uniform-random 1%"}
     * @see DeltaListEncodingTest#matchesTheBenchmarkLayoutWriterByteForByte()
     * @see OffsetEncoderRegistrationTest#theShippedPayloadIsNeverLargerThanBeforeTheDeltaList()
     */
    static SortedSet<Long> corpusIncompletes(String familyLabel, int rangeSize) {
        for (final Family family : FAMILIES) {
            if (family.label.equals(familyLabel)) {
                return incompletesFor(family, rangeSize);
            }
        }
        throw new IllegalArgumentException("no such scenario family: " + familyLabel);
    }

    /**
     * @return the incomplete offsets for a scenario, as absolute offsets from a base of 0 (the encoders work on
     *         relative offsets, so the base offset cannot change any size measured here)
     */
    private static SortedSet<Long> incompletesFor(Family family, int rangeSize) {
        final Random rnd = new Random(seedFor(family, rangeSize));
        final TreeSet<Long> out = new TreeSet<>();
        switch (family.shape) {
            case SHAPE_TRAILING_RUN -> {
                // 5% of the range, in flight at the tail. Scale-free, so the shape is the same at every range size.
                int tail = Math.max(1, rangeSize / 20);
                for (int i = rangeSize - tail; i < rangeSize; i++) {
                    out.add((long) i);
                }
            }
            case SHAPE_UNIFORM -> {
                int target = Math.max(1, (int) Math.round(rangeSize * family.density));
                while (out.size() < target) {
                    out.add((long) rnd.nextInt(rangeSize));
                }
            }
            case SHAPE_CLUSTERED -> {
                int bursts = Math.max(1, rangeSize / 1_000);
                for (int b = 0; b < bursts; b++) {
                    int length = 10 + rnd.nextInt(91); // 10..100 inclusive
                    int start = rnd.nextInt(rangeSize);
                    for (int i = start; i < Math.min(start + length, rangeSize); i++) {
                        out.add((long) i);
                    }
                }
            }
            case SHAPE_ALTERNATING -> {
                for (int i = 1; i < rangeSize; i += 2) {
                    out.add((long) i);
                }
            }
            case SHAPE_ALL_INCOMPLETE -> {
                for (int i = 0; i < rangeSize; i++) {
                    out.add((long) i);
                }
            }
            default -> throw new IllegalStateException("unknown scenario shape: " + family.shape);
        }
        return out;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Candidate layout writers (KTD4) - real bytes, no decode path, not registered encoders
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Placeholder magic bytes. The real reservations are in KTD7 ({@code 'r'/'z'}, {@code 'd'/'D'},
     * {@code 'u'/'U'}); only the WIDTH of the field matters to a size measurement, so nothing here should be read
     * as claiming a byte.
     */
    private static final byte MAGIC_CHUNKED = 0x01;
    private static final byte MAGIC_DELTA = 0x02;
    private static final byte MAGIC_URLE = 0x03;

    private static final int CHUNK_BITS = 1 << 16;

    private static final byte CONTAINER_ARRAY = 1;
    private static final byte CONTAINER_BITMAP = 2;
    private static final byte CONTAINER_RUN = 3;

    private static final String CANDIDATE_CHUNKED = "chunked-bitset";
    private static final String CANDIDATE_DELTA = "delta-list";
    private static final String CANDIDATE_URLE = "u-run-length";
    private static final String[] CANDIDATES = {CANDIDATE_CHUNKED, CANDIDATE_DELTA, CANDIDATE_URLE};

    /**
     * Roaring-style chunked bitset: {@code [magic:1][rangeLength:int4][chunkCount:short]} then, per NON-EMPTY
     * 2^16-bit chunk of the relative-offset space, {@code [chunkIndex:short][containerType:byte][payload]} where
     * the payload is the smallest of
     * <ul>
     *     <li>array: {@code [cardinality:short][sorted in-chunk positions as shorts]}</li>
     *     <li>bitmap: {@code [byteLength:short][ceil(chunkUsedBits/8) bytes]}</li>
     *     <li>run: {@code [runCount:short][start:short, length-1:short]...}</li>
     * </ul>
     * SET BITS ARE INCOMPLETE OFFSETS - the inverse of {@link BitSetEncoder}, which marks completions. That
     * inversion is the whole point: incompletes are the sparse side, and sparsity is what an array container
     * exploits.
     * <p>
     * Run lengths are stored as {@code length - 1}, as Roaring's own run container does, because a run can span a
     * whole 65,536-bit chunk and 65,536 does not fit in 16 bits. An array container is not offered when its
     * cardinality would exceed 65,535 for the same reason (it could never win at that size anyway - a bitmap
     * container is 8KB).
     * <p>
     * Ties between containers of equal size resolve in declaration order (array, bitmap, run), so the output is
     * deterministic.
     */
    static byte[] chunkedBitSetLayout(long rangeLength, SortedSet<Long> incompletes, long baseOffset) {
        final TreeMap<Integer, List<Integer>> chunks = new TreeMap<>();
        for (final long offset : incompletes) {
            final long relative = offset - baseOffset;
            final int chunkIndex = (int) (relative >>> 16);
            final int inChunk = (int) (relative & 0xFFFF);
            chunks.computeIfAbsent(chunkIndex, key -> new ArrayList<>()).add(inChunk);
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_CHUNKED);
        putInt(out, rangeLength);
        putShort(out, chunks.size());

        for (final Map.Entry<Integer, List<Integer>> chunk : chunks.entrySet()) {
            final int chunkIndex = chunk.getKey();
            final List<Integer> positions = chunk.getValue(); // ascending: incompletes is a SortedSet
            final long chunkStartBit = (long) chunkIndex * CHUNK_BITS;
            final int chunkUsedBits = (int) Math.min(CHUNK_BITS, rangeLength - chunkStartBit);

            final List<int[]> runs = runsOf(positions);

            final int arraySize = positions.size() > 0xFFFF ? Integer.MAX_VALUE : 2 + 2 * positions.size();
            final int bitmapSize = 2 + ceilDiv(chunkUsedBits, 8);
            final int runSize = 2 + 4 * runs.size();

            putShort(out, chunkIndex);
            if (arraySize <= bitmapSize && arraySize <= runSize) {
                out.write(CONTAINER_ARRAY);
                putShort(out, positions.size());
                for (final int position : positions) {
                    putShort(out, position);
                }
            } else if (bitmapSize <= runSize) {
                out.write(CONTAINER_BITMAP);
                final int byteLength = ceilDiv(chunkUsedBits, 8);
                putShort(out, byteLength);
                final byte[] bitmap = new byte[byteLength];
                for (final int position : positions) {
                    bitmap[position >>> 3] |= (byte) (1 << (position & 7));
                }
                out.write(bitmap, 0, bitmap.length);
            } else {
                out.write(CONTAINER_RUN);
                putShort(out, runs.size());
                for (final int[] run : runs) {
                    putShort(out, run[0]);
                    putShort(out, run[1] - 1); // length - 1, so a full-chunk run fits 16 bits
                }
            }
        }
        return out.toByteArray();
    }

    /**
     * Sparse delta-list: {@code [magic:1][rangeLength:int4][count:varint]} then unsigned LEB128 varints.
     * <p>
     * FIRST VALUE IS THE FIRST INCOMPLETE'S RELATIVE OFFSET, then the gap to each next incomplete. Gaps are written
     * as-is rather than as {@code gap - 1}: the saving would be one byte per gap of exactly 128 (and of exactly
     * 16,384, ...), which is noise, and writing what the plan's HTD describes keeps the modelled format and the
     * documented format the same thing.
     */
    static byte[] sparseDeltaListLayout(long rangeLength, SortedSet<Long> incompletes, long baseOffset) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_DELTA);
        putInt(out, rangeLength);
        putVarint(out, incompletes.size());
        long previous = 0;
        boolean first = true;
        for (final long offset : incompletes) {
            final long relative = offset - baseOffset;
            putVarint(out, first ? relative : relative - previous);
            previous = relative;
            first = false;
        }
        return out.toByteArray();
    }

    /**
     * Unsigned run-length: {@code [magic:1][rangeLength:int4]} then a bare array of unsigned-short run lengths,
     * alternating and STARTING WITH AN INCOMPLETE RUN - the same convention as {@link RunLengthEncoder} (whose
     * {@code previousRunLengthState} starts {@code false} = incomplete) and {@link OffsetRunLength}'s decoder. A
     * leading zero-length incomplete run is emitted when offset 0 is complete, exactly as the incumbent does.
     * <p>
     * This is the existing optimisation TODO in {@link OffsetSimultaneousEncoder} made measurable: v1 spends 2 bytes
     * per run but caps a run at {@link Short#MAX_VALUE}; v2 lifts the cap to {@link Integer#MAX_VALUE} but doubles
     * every entry to 4 bytes. Unsigned shorts keep 2 bytes and reach 65,535.
     * <p>
     * A run longer than 65,535 SPLITS as {@code [65535, 0, remainder]} - the zero-length opposite run hands the
     * state straight back, so the same logical run continues. Repeated as needed for very long runs.
     */
    static byte[] unsignedRunLengthLayout(long rangeLength, SortedSet<Long> incompletes, long baseOffset) {
        final List<Long> runs = new ArrayList<>();
        long cursor = 0;
        Long intervalStart = null;
        Long intervalEnd = null;
        // walk the incompletes as maximal contiguous intervals
        for (final long offset : incompletes) {
            final long relative = offset - baseOffset;
            if (intervalStart == null) {
                intervalStart = relative;
                intervalEnd = relative;
            } else if (relative == intervalEnd + 1) {
                intervalEnd = relative;
            } else {
                cursor = appendInterval(runs, cursor, intervalStart, intervalEnd);
                intervalStart = relative;
                intervalEnd = relative;
            }
        }
        if (intervalStart != null) {
            cursor = appendInterval(runs, cursor, intervalStart, intervalEnd);
        }
        if (cursor < rangeLength) {
            if (runs.isEmpty()) {
                runs.add(0L); // zero-length leading incomplete run
            }
            runs.add(rangeLength - cursor);
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_URLE);
        putInt(out, rangeLength);
        for (long run : runs) {
            while (run > 0xFFFF) {
                putShort(out, 0xFFFF);
                putShort(out, 0); // zero-length opposite run - state returns to the run being split
                run -= 0xFFFF;
            }
            putShort(out, (int) run);
        }
        return out.toByteArray();
    }

    /**
     * Appends the complete run before {@code [start, end]} (if any) and then the incomplete run itself, preserving
     * the incomplete-first alternation.
     *
     * @return the new cursor: one past the interval
     */
    private static long appendInterval(List<Long> runs, long cursor, long start, long end) {
        if (start > cursor) {
            if (runs.isEmpty()) {
                runs.add(0L); // zero-length leading incomplete run, so the first real run is a complete one
            }
            runs.add(start - cursor);
        }
        runs.add(end - start + 1);
        return end + 1;
    }

    /** @return maximal contiguous runs over an ascending list of positions, as {@code {start, length}} pairs */
    private static List<int[]> runsOf(List<Integer> ascendingPositions) {
        final List<int[]> runs = new ArrayList<>();
        int start = -1;
        int previous = -2;
        for (final int position : ascendingPositions) {
            if (position != previous + 1) {
                if (start >= 0) {
                    runs.add(new int[]{start, previous - start + 1});
                }
                start = position;
            }
            previous = position;
        }
        if (start >= 0) {
            runs.add(new int[]{start, previous - start + 1});
        }
        return runs;
    }

    private static void putShort(ByteArrayOutputStream out, int value) {
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static void putInt(ByteArrayOutputStream out, long value) {
        out.write((int) ((value >>> 24) & 0xFF));
        out.write((int) ((value >>> 16) & 0xFF));
        out.write((int) ((value >>> 8) & 0xFF));
        out.write((int) (value & 0xFF));
    }

    private static void putVarint(ByteArrayOutputStream out, long value) {
        long remaining = value;
        while ((remaining & ~0x7FL) != 0) {
            out.write((int) ((remaining & 0x7F) | 0x80));
            remaining >>>= 7;
        }
        out.write((int) remaining);
    }

    private static int ceilDiv(int dividend, int divisor) {
        return (dividend + divisor - 1) / divisor;
    }

    /** {@code String.repeat} is Java 11; this module compiles with {@code --release 8} (Jabel). */
    private static String repeat(String text, int times) {
        final StringBuilder out = new StringBuilder(text.length() * times);
        for (int i = 0; i < times; i++) {
            out.append(text);
        }
        return out.toString();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Outer string codec arithmetic (KTD6). No codec is implemented here - U2 owns that.
    // ---------------------------------------------------------------------------------------------------------

    /** {@code java.util.Base64} with padding: 4 chars per 3 bytes, rounded up. */
    static int base64Chars(int payloadBytes) {
        return 4 * ceilDiv(payloadBytes, 3);
    }

    /** One sentinel char, then Z85's 5-chars-per-4-bytes with an {@code n+1} chars partial tail group. */
    static int sentinelZ85Chars(int payloadBytes) {
        final int tail = payloadBytes % 4;
        return 1 + 5 * (payloadBytes / 4) + (tail == 0 ? 0 : tail + 1);
    }

    /**
     * KTD6 with U3's floor: Base64 for every payload below 22 bytes (old-reader compatibility - at that size the odd
     * character sentinel+Z85 would save buys no headroom), and the shorter form - from there always Z85 - from 22
     * bytes up. The floor is restated as a literal 22 rather than read from
     * {@code OffsetSimpleSerialisation.Z85_MIN_PAYLOAD_BYTES}, like the formulas above, so the report models the
     * writer's rule independently of the code implementing it.
     */
    static int stringChars(int payloadBytes) {
        if (payloadBytes < 22) {
            return base64Chars(payloadBytes);
        }
        return Math.min(base64Chars(payloadBytes), sentinelZ85Chars(payloadBytes));
    }

    // ---------------------------------------------------------------------------------------------------------
    // Measurement
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The incumbent encodings, paired plain-with-compressed-twin. {@link OffsetEncoding#ByteArray} is absent
     * because {@link OffsetSimultaneousEncoder#addByteBufferEncoder()} is not called in production - it is always
     * beaten by BitSet.
     */
    private static final OffsetEncoding[][] INCUMBENTS = {
            {OffsetEncoding.BitSet, OffsetEncoding.BitSetCompressed},
            {OffsetEncoding.BitSetV2, OffsetEncoding.BitSetV2Compressed},
            {OffsetEncoding.RunLength, OffsetEncoding.RunLengthCompressed},
            {OffsetEncoding.RunLengthV2, OffsetEncoding.RunLengthV2Compressed},
    };

    private static final String[] INCUMBENT_LABELS = {"BitSet v1", "BitSet v2", "RunLength v1", "RunLength v2"};

    /** One encoding's packed (magic byte included) size, and what it costs as a metadata string. */
    private static final class Choice {
        final String name;
        final int packedBytes;

        Choice(String name, int packedBytes) {
            this.name = name;
            this.packedBytes = packedBytes;
        }

        int base64() {
            return base64Chars(packedBytes);
        }

        int z85() {
            return sentinelZ85Chars(packedBytes);
        }

        int chars() {
            return stringChars(packedBytes);
        }
    }

    /** Everything measured for one (family, range size) scenario. */
    private static final class Row {
        final Family family;
        final int rangeSize;
        final int incompleteCount;

        /** Best of plain/compressed per incumbent pair, packed bytes; -1 when the encoder could not be used. */
        final int[] incumbentForced = new int[INCUMBENTS.length];
        final int[] incumbentProduction = new int[INCUMBENTS.length];

        final int[] candidatePlain = new int[CANDIDATES.length];
        final int[] candidateZstd = new int[CANDIDATES.length];

        /** Whether each incumbent's own plain form is large enough to earn a zstd twin (KTD8, per encoder). */
        final boolean[] incumbentTwinRegistered = new boolean[INCUMBENTS.length];

        /** Whether EVERY registered incumbent got a twin - what the {@code twins} column reports. */
        boolean productionTwinsRegistered;

        Choice winnerForced;
        Choice winnerProduction;
        Choice bestIncumbentProduction;
        final Choice[] bestCandidateProduction = new Choice[CANDIDATES.length];

        Row(Family family, int rangeSize, int incompleteCount) {
            this.family = family;
            this.rangeSize = rangeSize;
            this.incompleteCount = incompleteCount;
        }

        String scenario() {
            return family.label;
        }
    }

    private boolean compressionForcedBefore;

    @BeforeEach
    void forceCompressionSoBothViewsComeFromOneEncodePass() {
        compressionForcedBefore = OffsetSimultaneousEncoder.compressionForced;
        OffsetSimultaneousEncoder.compressionForced = true;
    }

    @AfterEach
    void restoreCompressionForced() {
        OffsetSimultaneousEncoder.compressionForced = compressionForcedBefore;
    }

    /**
     * Measures one scenario.
     * <p>
     * ONE {@code invoke()} PASS SERVES BOTH COMPRESSION VIEWS. {@code invoke()} walks every offset in the range for
     * every encoder, so it is the whole cost of this benchmark; running it twice to see the production rule would
     * double that for nothing. With {@code compressionForced = true} the map holds every plain AND compressed
     * entry, and the production rule is then a pure function of the plain sizes -
     * {@code registerEncodings}' gate is literally {@code encoders.stream().noneMatch(OffsetEncoder::quiteSmall)}
     * over the plain sizes of the encoders that registered. {@link #productionCompressionGateModelMatchesReality()}
     * pins the derivation against a real unforced run.
     */
    @SneakyThrows
    private static Row measure(Family family, int rangeSize) {
        final SortedSet<Long> incompletes = incompletesFor(family, rangeSize);
        final Row row = new Row(family, rangeSize, incompletes.size());

        final OffsetSimultaneousEncoder encoder =
                new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke();
        final Map<OffsetEncoding, byte[]> map = encoder.getEncodingMap();

        row.productionTwinsRegistered = true;
        for (int i = 0; i < INCUMBENTS.length; i++) {
            final byte[] plain = map.get(INCUMBENTS[i][0]);
            final byte[] compressed = map.get(INCUMBENTS[i][1]);
            row.incumbentTwinRegistered[i] = productionTwinWouldRegister(plain);
            row.incumbentForced[i] = bestPacked(plain, compressed);
            row.incumbentProduction[i] = bestPacked(plain, row.incumbentTwinRegistered[i] ? compressed : null);
            if (plain != null && !row.incumbentTwinRegistered[i]) {
                row.productionTwinsRegistered = false;
            }
        }

        final long rangeLength = rangeSize;
        final byte[][] layouts = {
                chunkedBitSetLayout(rangeLength, incompletes, 0L),
                sparseDeltaListLayout(rangeLength, incompletes, 0L),
                unsignedRunLengthLayout(rangeLength, incompletes, 0L),
        };
        for (int c = 0; c < CANDIDATES.length; c++) {
            row.candidatePlain[c] = layouts[c].length;
            row.candidateZstd[c] = OffsetSimpleSerialisation.compressZstd(layouts[c]).length;
        }

        // ---- winners
        final List<Choice> forced = new ArrayList<>();
        final List<Choice> production = new ArrayList<>();
        final List<Choice> incumbentProduction = new ArrayList<>();
        for (int i = 0; i < INCUMBENTS.length; i++) {
            addIncumbentChoices(forced, map, INCUMBENTS[i][0], INCUMBENTS[i][1], true);
            addIncumbentChoices(production, map, INCUMBENTS[i][0], INCUMBENTS[i][1], row.incumbentTwinRegistered[i]);
            addIncumbentChoices(incumbentProduction, map, INCUMBENTS[i][0], INCUMBENTS[i][1],
                    row.incumbentTwinRegistered[i]);
        }
        for (int c = 0; c < CANDIDATES.length; c++) {
            final List<Choice> candidateProduction = new ArrayList<>();
            forced.add(new Choice(CANDIDATES[c], row.candidatePlain[c]));
            forced.add(new Choice(CANDIDATES[c] + "+zstd", row.candidateZstd[c]));
            candidateProduction.add(new Choice(CANDIDATES[c], row.candidatePlain[c]));
            // KTD8: a candidate can only ship together with per-encoder compressed registration, so its own zstd
            // twin exists exactly when its own plain form is not quiteSmall.
            if (row.candidatePlain[c] >= OffsetSimultaneousEncoder.LARGE_ENCODED_SIZE_THRESHOLD_BYTES) {
                candidateProduction.add(new Choice(CANDIDATES[c] + "+zstd", row.candidateZstd[c]));
            }
            production.addAll(candidateProduction);
            row.bestCandidateProduction[c] = smallest(candidateProduction);
        }
        row.winnerForced = smallest(forced);
        row.winnerProduction = smallest(production);
        row.bestIncumbentProduction = smallest(incumbentProduction);
        assertWithMessage("no incumbent encoding registered at all for %s @ %s - there would be nothing to commit, "
                + "and nothing for a candidate to be compared against", family.label, rangeSize)
                .that(row.bestIncumbentProduction).isNotNull();
        return row;
    }

    private static void addIncumbentChoices(List<Choice> into, Map<OffsetEncoding, byte[]> map,
                                            OffsetEncoding plain, OffsetEncoding compressed, boolean twinRegistered) {
        if (map.containsKey(plain)) {
            into.add(new Choice(plain.name(), map.get(plain).length + 1));
        }
        if (twinRegistered && map.containsKey(compressed)) {
            into.add(new Choice(compressed.name(), map.get(compressed).length + 1));
        }
    }

    /**
     * The production compression rule, evaluated from one encoder's own plain size: it gets a zstd twin exactly when
     * that size is at or above {@link OffsetSimultaneousEncoder#LARGE_ENCODED_SIZE_THRESHOLD_BYTES}.
     * <p>
     * PER ENCODER (KTD8). It used to be all-or-nothing - no encoder got a twin if ANY ONE of them was small - which is
     * the inefficiency R11 required fixing before a new candidate could ship, since a small candidate would otherwise
     * have suppressed the incumbents' twins and made winning payloads larger.
     * {@link #productionCompressionGateModelMatchesReality()} pins this derivation against a real unforced run.
     */
    private static boolean productionTwinWouldRegister(byte[] plain) {
        return plain != null && plain.length >= OffsetSimultaneousEncoder.LARGE_ENCODED_SIZE_THRESHOLD_BYTES;
    }

    private static int bestPacked(byte[] plain, byte[] compressed) {
        int best = -1;
        if (plain != null) {
            best = plain.length + 1;
        }
        if (compressed != null && (best < 0 || compressed.length + 1 < best)) {
            best = compressed.length + 1;
        }
        return best;
    }

    /** Smallest by string length, ties resolved by insertion order so the winner name is deterministic. */
    private static Choice smallest(List<Choice> choices) {
        Choice best = null;
        for (final Choice choice : choices) {
            if (best == null || choice.chars() < best.chars()) {
                best = choice;
            }
        }
        return best;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Engagement points
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The search grid for the back-pressure engagement point. Geometric (x1.2) so a binary search over it costs
     * about five encodes per family instead of thirty, and CAPPED - a family whose winner never reaches
     * {@link #BACK_PRESSURE_CHARS} within the cap is reported as such rather than searched to exhaustion, because
     * each probe costs a full per-offset encode pass at that range size.
     */
    private static final int ENGAGEMENT_GRID_MIN = 1_000;
    private static final int ENGAGEMENT_GRID_MAX = 200_000;

    private static int[] engagementGrid() {
        final List<Integer> grid = new ArrayList<>();
        long size = ENGAGEMENT_GRID_MIN;
        while (size <= ENGAGEMENT_GRID_MAX) {
            if (grid.isEmpty() || grid.get(grid.size() - 1) != (int) size) {
                grid.add((int) size);
            }
            size = Math.max(size + 1, Math.round(size * 1.2d));
        }
        final int[] out = new int[grid.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = grid.get(i);
        }
        return out;
    }

    private static final class Engagement {
        final Family family;
        /** Largest searched range size whose winner still fits under the threshold, or -1 / -2 for out of range. */
        int lastFittingRange = -1;
        int lastFittingIncompletes;
        int lastFittingChars;
        String lastFittingWinner = "-";
        int firstBlockingRange = -1;
        int firstBlockingIncompletes;
        int firstBlockingChars;
        String firstBlockingWinner = "-";
        boolean neverBlocksWithinCap;
        boolean blocksBelowGridMin;

        Engagement(Family family) {
            this.family = family;
        }
    }

    /**
     * Finds where this family's winning payload crosses the back-pressure threshold, by binary search over
     * {@link #engagementGrid()} on the production-rule winner's string length.
     * <p>
     * THE SEARCH ASSUMES MONOTONICITY of string length in range size for a fixed family, which holds in shape but
     * not exactly (zstd is not monotone in its input, and the winning encoder can switch). The result is therefore
     * the crossing point to within one grid step, which the report states.
     */
    private static Engagement engagementPointOf(Family family) {
        final int[] grid = engagementGrid();
        final Engagement engagement = new Engagement(family);

        final Row atMin = measure(family, grid[0]);
        if (atMin.winnerProduction.chars() >= BACK_PRESSURE_CHARS) {
            engagement.blocksBelowGridMin = true;
            record(engagement, false, atMin);
            return engagement;
        }
        final Row atMax = measure(family, grid[grid.length - 1]);
        if (atMax.winnerProduction.chars() < BACK_PRESSURE_CHARS) {
            engagement.neverBlocksWithinCap = true;
            record(engagement, true, atMax);
            return engagement;
        }

        int fits = 0;                  // known to fit
        int blocks = grid.length - 1;  // known to block
        Row fitsRow = atMin;
        Row blocksRow = atMax;
        while (blocks - fits > 1) {
            final int middle = (fits + blocks) >>> 1;
            final Row row = measure(family, grid[middle]);
            if (row.winnerProduction.chars() < BACK_PRESSURE_CHARS) {
                fits = middle;
                fitsRow = row;
            } else {
                blocks = middle;
                blocksRow = row;
            }
        }
        record(engagement, true, fitsRow);
        record(engagement, false, blocksRow);
        return engagement;
    }

    private static void record(Engagement engagement, boolean fitting, Row row) {
        if (fitting) {
            engagement.lastFittingRange = row.rangeSize;
            engagement.lastFittingIncompletes = row.incompleteCount;
            engagement.lastFittingChars = row.winnerProduction.chars();
            engagement.lastFittingWinner = row.winnerProduction.name;
        } else {
            engagement.firstBlockingRange = row.rangeSize;
            engagement.firstBlockingIncompletes = row.incompleteCount;
            engagement.firstBlockingChars = row.winnerProduction.chars();
            engagement.firstBlockingWinner = row.winnerProduction.name;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Verdicts (KTD5)
    // ---------------------------------------------------------------------------------------------------------

    private static final class CandidateVerdict {
        final String name;
        boolean ships;
        String reason = "";
        /** The best result the candidate managed on any row whose incumbent is at/above the threshold. */
        Row bestQualifyingRow;
        double bestQualifyingImprovement = Double.NEGATIVE_INFINITY;
        int rowsAtOrAboveThreshold;

        /** The best result on ANY row, threshold or not - context for the prose, not for the decision. */
        Row bestAnyRow;
        double bestAnyImprovement = Double.NEGATIVE_INFINITY;
        /** How many scenarios this candidate would win outright in the production view. */
        int scenariosWon;

        CandidateVerdict(String name) {
            this.name = name;
        }
    }

    private static List<CandidateVerdict> verdicts(List<Row> rows) {
        final List<CandidateVerdict> out = new ArrayList<>();
        for (int c = 0; c < CANDIDATES.length; c++) {
            final CandidateVerdict verdict = new CandidateVerdict(CANDIDATES[c]);
            for (final Row row : rows) {
                final int incumbentChars = row.bestIncumbentProduction.chars();
                final double improvement =
                        (incumbentChars - row.bestCandidateProduction[c].chars()) / (double) incumbentChars;
                if (improvement > verdict.bestAnyImprovement) {
                    verdict.bestAnyImprovement = improvement;
                    verdict.bestAnyRow = row;
                }
                if (row.winnerProduction.name.startsWith(CANDIDATES[c])) {
                    verdict.scenariosWon++;
                }
                if (incumbentChars < BACK_PRESSURE_CHARS) {
                    continue;
                }
                verdict.rowsAtOrAboveThreshold++;
                if (improvement > verdict.bestQualifyingImprovement) {
                    verdict.bestQualifyingImprovement = improvement;
                    verdict.bestQualifyingRow = row;
                }
            }
            out.add(verdict);
        }

        // At most the best candidate ships - and only if it clears the bar.
        CandidateVerdict leader = null;
        for (final CandidateVerdict verdict : out) {
            if (verdict.bestQualifyingImprovement >= SHIP_THRESHOLD_FRACTION
                    && (leader == null || verdict.bestQualifyingImprovement > leader.bestQualifyingImprovement)) {
                leader = verdict;
            }
        }

        for (int c = 0; c < out.size(); c++) {
            final CandidateVerdict verdict = out.get(c);
            if (verdict == leader) {
                verdict.ships = true;
                verdict.reason = String.format(Locale.ROOT,
                        "best incumbent %,d chars -> %,d chars on %s @ range %,d (%s), %.2f%% denser; clears the "
                                + "%.0f%% bar on a payload already at or above the %,d-char back-pressure threshold",
                        verdict.bestQualifyingRow.bestIncumbentProduction.chars(),
                        verdict.bestQualifyingRow.bestCandidateProduction[c].chars(),
                        verdict.bestQualifyingRow.scenario(),
                        verdict.bestQualifyingRow.rangeSize,
                        verdict.bestQualifyingRow.bestCandidateProduction[c].name,
                        verdict.bestQualifyingImprovement * 100,
                        SHIP_THRESHOLD_FRACTION * 100,
                        BACK_PRESSURE_CHARS);
            } else if (verdict.rowsAtOrAboveThreshold == 0) {
                verdict.reason = String.format(Locale.ROOT,
                        "no scenario in the corpus puts the incumbent at or above the %,d-char back-pressure "
                                + "threshold, so KTD5's proximity condition is never met",
                        BACK_PRESSURE_CHARS);
            } else {
                verdict.reason = String.format(Locale.ROOT,
                        "best result across the %d scenario(s) at or above the %,d-char threshold was %s @ range "
                                + "%,d: %,d chars vs the incumbent's %,d (%s) - %+.2f%%, short of the required "
                                + "%.0f%%%s",
                        verdict.rowsAtOrAboveThreshold,
                        BACK_PRESSURE_CHARS,
                        verdict.bestQualifyingRow.scenario(),
                        verdict.bestQualifyingRow.rangeSize,
                        verdict.bestQualifyingRow.bestCandidateProduction[c].chars(),
                        verdict.bestQualifyingRow.bestIncumbentProduction.chars(),
                        verdict.bestQualifyingRow.bestIncumbentProduction.name,
                        verdict.bestQualifyingImprovement * 100,
                        SHIP_THRESHOLD_FRACTION * 100,
                        leader == null ? "" : " (" + leader.name + " ships instead)");
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------------------------------------------
    // The benchmark itself
    // ---------------------------------------------------------------------------------------------------------

    @SneakyThrows
    @Test
    void generateDensityReport() {
        final long startedAt = System.nanoTime();
        final List<Row> rows = new ArrayList<>();
        final int totalScenarios = FAMILIES.length * RANGE_SIZES.length;
        int done = 0;
        for (final Family family : FAMILIES) {
            for (final int rangeSize : RANGE_SIZES) {
                final long scenarioStart = System.nanoTime();
                final Row row = measure(family, rangeSize);
                rows.add(row);
                done++;
                log.info("density [{}/{}] {} @ {} - {} incompletes, winner {} ({} chars) in {}ms",
                        done, totalScenarios, family.label, rangeSize, row.incompleteCount,
                        row.winnerProduction.name, row.winnerProduction.chars(),
                        (System.nanoTime() - scenarioStart) / 1_000_000);
            }
        }

        final List<Engagement> engagements = new ArrayList<>();
        for (final Family family : FAMILIES) {
            final long engagementStart = System.nanoTime();
            engagements.add(engagementPointOf(family));
            log.info("density engagement point for {} found in {}ms",
                    family.label, (System.nanoTime() - engagementStart) / 1_000_000);
        }

        final List<CandidateVerdict> verdicts = verdicts(rows);

        // ---- KTD5 admits at most one shipping candidate
        long shipping = verdicts.stream().filter(verdict -> verdict.ships).count();
        assertWithMessage("KTD5 allows at most one candidate to ship").that(shipping).isAtMost(1L);
        for (final CandidateVerdict verdict : verdicts) {
            if (verdict.ships) {
                assertWithMessage("a shipping candidate must clear the KTD5 improvement bar of %s",
                        SHIP_THRESHOLD_FRACTION)
                        .that(verdict.bestQualifyingImprovement).isAtLeast(SHIP_THRESHOLD_FRACTION);
            }
        }

        final String report = renderReport(rows, engagements, verdicts);

        Files.createDirectories(REPORT_IN_TARGET.getParent());
        Files.write(REPORT_IN_TARGET, report.getBytes(StandardCharsets.UTF_8));
        log.info("Density report written to {} in {}s",
                REPORT_IN_TARGET, (System.nanoTime() - startedAt) / 1_000_000_000);
        for (final CandidateVerdict verdict : verdicts) {
            log.info("VERDICT {}: {} - {}", verdict.name, verdict.ships ? "ship" : "case-against", verdict.reason);
        }

        final Path committed = RepoRoot.find().resolve(REPORT_IN_DOCS);
        final String committedReport = Files.exists(committed)
                ? new String(Files.readAllBytes(committed), StandardCharsets.UTF_8)
                : "";
        if (System.getProperty(CHECK_PROPERTY) != null) {
            // Gate mode - bin/offset-encoding-density-report.sh --check sets the property.
            assertWithMessage("%s is stale - regenerate with bin/offset-encoding-density-report.sh", REPORT_IN_DOCS)
                    .that(report).isEqualTo(committedReport);
        } else if (!report.equals(committedReport)) {
            // A plain `mvn test` must NOT go red on report drift: the numbers move whenever an encoder changes,
            // and a developer running the unit suite is not the person being asked to regenerate a doc. The
            // freshness gate lives in pr-checklist, where it is actionable.
            log.warn("{} differs from this run - regenerate it with bin/offset-encoding-density-report.sh",
                    REPORT_IN_DOCS);
        }
    }

    /**
     * The winner this benchmark computes must be the encoding production would actually commit. Under forced
     * compression the smallest registered encoding is what {@link OffsetSimultaneousEncoder#packSmallest()} packs,
     * so the packed length must match - which also proves the +1 magic byte accounting.
     * <p>
     * Compared by SIZE, not by name: {@code sortedEncodings} is a {@link TreeSet} over a size-only comparator, so
     * two encodings of equal size are one entry and the name is arbitrary between them.
     */
    @SneakyThrows
    @Test
    void winnerMatchesWhatPackSmallestWouldCommit() {
        for (final Family family : FAMILIES) {
            final int rangeSize = 10_000;
            final SortedSet<Long> incompletes = incompletesFor(family, rangeSize);
            final OffsetSimultaneousEncoder encoder =
                    new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke();
            int smallest = Integer.MAX_VALUE;
            for (final Map.Entry<OffsetEncoding, byte[]> entry : encoder.getEncodingMap().entrySet()) {
                smallest = Math.min(smallest, entry.getValue().length + 1);
            }
            assertWithMessage("smallest registered encoding for %s must be what packSmallest packs", family.label)
                    .that(encoder.packSmallest().length).isEqualTo(smallest);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Report rendering
    // ---------------------------------------------------------------------------------------------------------

    private static String renderReport(List<Row> rows, List<Engagement> engagements,
                                       List<CandidateVerdict> verdicts) {
        final StringBuilder out = new StringBuilder();
        out.append("# Offset-encoding density benchmark\n\n");
        out.append("**Generated file - do not edit by hand.** Regenerate with `bin/offset-encoding-density-report.sh`\n");
        out.append("(`bin/offset-encoding-density-report.sh --check` fails if this file is stale).\n\n");

        out.append("## What this is\n\n");
        out.append("PC commits its incomplete-offset map as a **string** in the Kafka offset metadata field, capped at\n");
        out.append(String.format(Locale.ROOT,
                "`OffsetMapCodecManager.DefaultMaxMetadataSize` = %,d chars, with back-pressure engaging at 0.75 of that\n",
                MAX_METADATA_CHARS));
        out.append(String.format(Locale.ROOT,
                "(`PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT`) = **%,d chars**. Past the cap the\n",
                BACK_PRESSURE_CHARS));
        out.append("payload is dropped and the partition blocks, so density near the cap is direct headroom.\n\n");
        out.append("[astubbs#192](https://github.com/astubbs/parallel-consumer/issues/192) (mirror of\n");
        out.append("[confluentinc#903](https://github.com/confluentinc/parallel-consumer/issues/903)) asks why PC uses\n");
        out.append("custom run-length/bitset encoders rather than RoaringBitmap, and Base64 rather than something\n");
        out.append("denser. Nobody had ever measured either alternative. This file is the measurement, and it is\n");
        out.append("generated by `OffsetEncodingDensityBenchmarkTest` so it cannot drift from the code it describes.\n\n");
        out.append("It measures **size only** - there is no JMH and no timing here. Encoded size is a deterministic\n");
        out.append("function of the incompletes distribution and the range length, which is what makes these numbers\n");
        out.append("committable. Encode-time CPU is a separate concern owned elsewhere.\n\n");

        out.append("## Method\n\n");
        out.append("- **Incumbents** are measured through the real `OffsetSimultaneousEncoder.invoke()`, reading\n");
        out.append("  `getEncodingMap()` (not `sortedEncodings` - that `TreeSet` silently drops size-ties).\n");
        out.append("- **Candidates** are minimal layout writers inside the test, not registered encoders: real bytes in\n");
        out.append("  the shape the plan's HTD describes, put through the same `zstd` and the same string arithmetic as\n");
        out.append("  the incumbents. Nothing here has a decode path, and none of it is production code.\n");
        out.append("- **Packed bytes** always include the 1-byte magic prefix, so incumbents and candidates are\n");
        out.append("  compared on the same footing. Every candidate layout begins\n");
        out.append("  `[magic:1][rangeLength:int4]`, because the incompletes alone cannot reconstruct the top of the\n");
        out.append("  range (the highest offset is always complete).\n");
        out.append("- **String length** models the writer's floored rule (KTD6): Base64 `4*ceil(n/3)` for payloads\n");
        out.append("  below 22 bytes, and the shorter of that and sentinel+Z85 `1 + 5*floor(n/4) + (n%4 ? n%4+1 : 0)`\n");
        out.append("  - from 22 bytes up always Z85 - beyond it. Both are reported for the winner. No codec is\n");
        out.append("  implemented here; this is the arithmetic only.\n");
        out.append("- **Two compression views.** *Forced* sets `OffsetSimultaneousEncoder.compressionForced`, so every\n");
        out.append("  encoder gets a zstd twin - it shows what compression is worth in principle. *Production* applies\n");
        out.append(String.format(Locale.ROOT,
                "  the real rule, which is now PER ENCODER (KTD8): an encoding gets a zstd twin exactly when its own\n"
                        + "  plain form is at or over %d bytes. Before KTD8 the gate was all-or-nothing - NO twin registered\n",
                OffsetSimultaneousEncoder.LARGE_ENCODED_SIZE_THRESHOLD_BYTES));
        out.append("  for anything if ANY ONE encoder's plain form was under the threshold - which is what R11 required\n");
        out.append("  fixing before a candidate could ship, and which incumbents and candidates alike are now measured\n");
        out.append("  under. **The verdicts use the production view.**\n");
        out.append("- **`ByteArray` is absent** from the tables: `addByteBufferEncoder()` is not called in production,\n");
        out.append("  as BitSet always beats it.\n");
        out.append("- **Scenario data is seeded** (`java.util.Random`, seed derived from the family and range size\n");
        out.append("  only), so this file is byte-identical across runs and across machines.\n");
        out.append("- **The distributions are assumed, not observed.** No workload telemetry of\n");
        out.append("  `ratioMetadataSpaceUsedDistributionSummary` is available to this project, so the corpus is the\n");
        out.append("  plan's best guess at realistic shapes plus two adversarial ones. Read the verdicts with that in\n");
        out.append("  mind.\n\n");

        out.append("### The scenario corpus\n\n");
        out.append("| Scenario | Incomplete offsets are |\n|---|---|\n");
        out.append("| `trailing-incomplete-run` | one contiguous run covering the last 5% of the range - the healthy steady state, work in flight at the tail |\n");
        out.append("| `uniform-random 0.1% / 1% / 5% / 20%` | that fraction of the range, uniformly at random - scattered slow records |\n");
        out.append("| `clustered-bursts` | bursts of 10-100 consecutive offsets, one burst per 1,000 offsets, at random starts - a few slow keys |\n");
        out.append("| `alternating-xo` | every other offset - the pathological case for run-length |\n");
        out.append("| `all-incomplete` | every offset in the range - nothing has succeeded yet |\n\n");

        out.append("## Back-pressure engagement points\n\n");
        out.append(String.format(Locale.ROOT,
                "Where each scenario family's winning payload crosses the %,d-char back-pressure threshold. This is the\n",
                BACK_PRESSURE_CHARS));
        out.append("number that says how much headroom the current pipeline actually has, and it is what any density\n");
        out.append("change has to be judged against.\n\n");
        out.append(String.format(Locale.ROOT,
                "**Method and its limits:** binary search over a geometric grid of range sizes (x1.2 from %,d to %,d),\n",
                ENGAGEMENT_GRID_MIN, ENGAGEMENT_GRID_MAX));
        out.append("evaluating the production-view winner at each probe. The search assumes string length grows\n");
        out.append("monotonically with range size for a fixed family - true in shape, but not exactly, since zstd is\n");
        out.append("not monotone in its input and the winning encoder can switch. **So the crossing is located to\n");
        out.append("within one grid step, not exactly**, and both sides of it are shown. The grid is capped because\n");
        out.append("each probe costs a full per-offset encode pass; families that never block within the cap say so.\n\n");
        out.append("| Scenario | Last fitting range | incompletes | winner | chars | First blocking range | incompletes | winner | chars |\n");
        out.append("|---|---:|---:|---|---:|---:|---:|---|---:|\n");
        for (final Engagement engagement : engagements) {
            out.append("| `").append(engagement.family.label).append("` | ");
            if (engagement.blocksBelowGridMin) {
                out.append(String.format(Locale.ROOT, "below %,d | - | - | - | ", ENGAGEMENT_GRID_MIN));
                out.append(String.format(Locale.ROOT, "%,d | %,d | `%s` | %,d |\n",
                        engagement.firstBlockingRange, engagement.firstBlockingIncompletes,
                        engagement.firstBlockingWinner, engagement.firstBlockingChars));
            } else if (engagement.neverBlocksWithinCap) {
                out.append(String.format(Locale.ROOT, "%,d | %,d | `%s` | %,d | ",
                        engagement.lastFittingRange, engagement.lastFittingIncompletes,
                        engagement.lastFittingWinner, engagement.lastFittingChars));
                out.append(String.format(Locale.ROOT, "none at or below %,d | - | - | - |\n",
                        engagement.lastFittingRange));
            } else {
                out.append(String.format(Locale.ROOT, "%,d | %,d | `%s` | %,d | ",
                        engagement.lastFittingRange, engagement.lastFittingIncompletes,
                        engagement.lastFittingWinner, engagement.lastFittingChars));
                out.append(String.format(Locale.ROOT, "%,d | %,d | `%s` | %,d |\n",
                        engagement.firstBlockingRange, engagement.firstBlockingIncompletes,
                        engagement.firstBlockingWinner, engagement.firstBlockingChars));
            }
        }
        out.append("\nThe deferred follow-up in the plan - exposing the broker's configurable\n");
        out.append("`offset.metadata.max.bytes` as a PC option - is sized directly by this table: raising the cap\n");
        out.append("multiplies every \"first blocking range\" above, with no wire-format change at all.\n\n");

        out.append("## Sizes - forced compression\n\n");
        out.append("Every encoder gets a zstd twin. Incumbent columns are the better of plain and compressed; candidate\n");
        out.append("columns are plain / zstd. All figures are **packed bytes** (magic byte included).\n\n");
        appendSizeTable(out, rows, true);

        out.append("\n## Sizes - production compression rule\n\n");
        out.append("The rule that actually ships, per the Method note above. **This is the table the verdicts use.**\n");
        out.append("A `!` in the *twins* column marks a scenario where at least one registered encoding was itself too\n");
        out.append("small to earn a zstd twin. Under KTD8's per-encoder rule the other encodings keep theirs; under the\n");
        out.append("all-or-nothing gate this report was first generated with, one such encoding suppressed them all.\n\n");
        appendSizeTable(out, rows, false);

        out.append("\n## What the numbers say\n\n");
        for (int c = 0; c < verdicts.size(); c++) {
            final CandidateVerdict verdict = verdicts.get(c);
            out.append(String.format(Locale.ROOT,
                    "- **`%s`**: best result anywhere in the corpus is **%+.1f%%** against the incumbent, on %s @ range\n"
                            + "  %,d (%,d chars vs %,d)%s. It is the production-view winner on **%d of %d** scenarios.\n",
                    verdict.name,
                    verdict.bestAnyImprovement * 100,
                    verdict.bestAnyRow.scenario(),
                    verdict.bestAnyRow.rangeSize,
                    verdict.bestAnyRow.bestCandidateProduction[c].chars(),
                    verdict.bestAnyRow.bestIncumbentProduction.chars(),
                    verdict.bestAnyRow.productionTwinsRegistered
                            ? ""
                            : " - but that is a `!` row, where at least one incumbent was too small to earn a zstd "
                            + "twin, so read the margin against the forced table too",
                    verdict.scenariosWon,
                    rows.size()));
        }
        out.append("- **zstd on a sparse bitmap is the incumbent's strongest move.** A BitSet of a low-density\n");
        out.append("  incompletes set is a long run of zeros with a few ones, and zstd takes it close to its entropy -\n");
        out.append("  which is why the pre-compression size advantage of a sparse format shrinks so much by the time it\n");
        out.append("  reaches the string. Compare the plain and zstd candidate columns to see how much of each\n");
        out.append("  candidate's apparent advantage survives compression.\n");
        out.append("- **A delta list still beats it on the sparse-to-mid uniform families**, because it encodes the same\n");
        out.append("  information in ~1 byte per incomplete instead of 1 bit per OFFSET, and zstd then works on a much\n");
        out.append("  smaller input. The advantage reverses as density rises: at 20% the gaps are short and numerous, so\n");
        out.append("  one bit per offset is the cheaper representation again.\n");
        out.append("- **The run-length families are already optimal and nothing here improves them.** For a trailing\n");
        out.append("  incomplete run, or an all-incomplete range, `RunLength` commits 4-20 chars - a candidate cannot\n");
        out.append("  beat that, and the 5-byte `[magic:1][rangeLength:int4]` header the candidates carry (against the\n");
        out.append("  incumbents' 1 byte, since their payloads already imply the range) is decisive at that size.\n");
        out.append("- **The all-or-nothing compression gate cost real chars, and no longer exists.** It suppressed every\n");
        out.append("  encoder's zstd twin as soon as one encoding was small; the `alternating-xo` rows were the clearest\n");
        out.append("  case, where a tiny run-length encoding cost the bitsets a compression that takes them from\n");
        out.append("  thousands of bytes to a few dozen. KTD8/R11 replaced it with the per-encoder rule these tables are\n");
        out.append("  now generated under, and that change is independent of any candidate.\n");
        out.append("- **Base64 vs Z85 is a small win, and it is free.** Z85 converges on ~6% shorter, applies to every\n");
        out.append("  encoding at once, and is independent of everything else in this report - compare the last two\n");
        out.append("  columns of either table.\n\n");

        out.append("## Verdicts (KTD5)\n\n");
        out.append(String.format(Locale.ROOT,
                "The rule, restated: a candidate ships only if, post-compression and post-string-encoding under the\n"
                        + "production compression rule, it beats the best incumbent by at least **%.0f%%** on at least one\n"
                        + "scenario class whose incumbent payload is already **at or above %,d chars** - so the win is real\n"
                        + "headroom where density matters, not a percentage on a payload of a few dozen bytes. At most the\n"
                        + "best candidate ships. A measured case-against is a result, not a failure: it is the answer to\n"
                        + "astubbs#192.\n\n",
                SHIP_THRESHOLD_FRACTION * 100, BACK_PRESSURE_CHARS));
        for (final CandidateVerdict verdict : verdicts) {
            out.append("```\n");
            out.append("VERDICT ").append(verdict.name).append(": ")
                    .append(verdict.ships ? "ship" : "case-against").append(" - ")
                    .append(verdict.reason).append("\n");
            out.append("```\n\n");
        }

        out.append("### The scenarios the rule was decided on\n\n");
        out.append("Every row whose incumbent payload is at or above the threshold - so the whole input to the rule\n");
        out.append("above is visible and can be checked against the tables. `cap x` is the incumbent payload as a\n");
        out.append("multiple of the 4,096-char metadata cap.\n\n");
        out.append("| Scenario | Range | incumbent chars | cap x | twins |");
        for (final String candidate : CANDIDATES) {
            out.append(' ').append(candidate).append(" chars | vs incumbent |");
        }
        out.append("\n|---|---:|---:|---:|:-:|");
        out.append(repeat("---:|---:|", CANDIDATES.length));
        out.append("\n");
        int qualifying = 0;
        int qualifyingPastTheCap = 0;
        for (final Row row : rows) {
            final int incumbentChars = row.bestIncumbentProduction.chars();
            if (incumbentChars < BACK_PRESSURE_CHARS) {
                continue;
            }
            qualifying++;
            if (incumbentChars > MAX_METADATA_CHARS) {
                qualifyingPastTheCap++;
            }
            out.append(String.format(Locale.ROOT, "| `%s` | %,d | %,d | %.1f | %s |",
                    row.scenario(), row.rangeSize, incumbentChars,
                    incumbentChars / (double) MAX_METADATA_CHARS,
                    row.productionTwinsRegistered ? "yes" : "!"));
            for (int c = 0; c < CANDIDATES.length; c++) {
                final int candidateChars = row.bestCandidateProduction[c].chars();
                out.append(String.format(Locale.ROOT, " %,d | %+.1f%% |",
                        candidateChars, (incumbentChars - candidateChars) * 100d / incumbentChars));
            }
            out.append("\n");
        }
        out.append("\n**Read this table before acting on a verdict.** Two things about it are load-bearing:\n\n");
        out.append(String.format(Locale.ROOT,
                "1. **%d of the %d qualifying rows are already PAST the %,d-char cap, not merely at the %,d-char\n"
                        + "   back-pressure threshold.** Past the cap PC has dropped the payload and blocked the partition\n"
                        + "   whichever encoding won, so a percentage on such a row is a percentage of an already-blocked\n"
                        + "   commit and buys an operator no headroom on its own. What density is worth in the regime that\n"
                        + "   matters is the *engagement point* table above - the range size at which a family starts to\n"
                        + "   block at all - and a candidate moves that only in proportion to its margin. KTD5's rule as\n"
                        + "   written is applied exactly here; whether a win concentrated this far past the cap satisfies\n"
                        + "   its SPIRIT (\"real headroom at the point where density matters\") is a judgement for whoever\n"
                        + "   decides on U4, and this table is what that decision should be made on.\n",
                qualifyingPastTheCap, qualifying, MAX_METADATA_CHARS, BACK_PRESSURE_CHARS));
        out.append("2. **Incumbents and candidates are now measured under the same compression rule.** Both get a zstd\n");
        out.append("   twin exactly when their own plain form is large enough to be worth compressing (KTD8). The first\n");
        out.append("   generation of this report gated the incumbents with the old all-or-nothing rule and the candidates\n");
        out.append("   per encoder, which flattered the candidates on `!` rows; that asymmetry is gone, and every\n");
        out.append("   qualifying row above was a `yes` row under either rule.\n\n");

        out.append("### Why not the RoaringBitmap library (KTD1)\n\n");
        final CandidateVerdict chunked = verdicts.stream()
                .filter(verdict -> verdict.name.equals(CANDIDATE_CHUNKED))
                .findFirst().orElseThrow(IllegalStateException::new);
        out.append(String.format(Locale.ROOT,
                "The `chunked-bitset` rows above ARE Roaring's container model, measured on this corpus: its best\n"
                        + "qualifying result is **%+.2f%%** against the incumbent encoders, far short of the **%.0f%%**\n"
                        + "ship bar above. The design reasoning behind declining the library lives with the code, in the\n"
                        + "offsets package javadoc\n"
                        + "(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/package-info.java`).\n",
                chunked.bestQualifyingImprovement * 100, SHIP_THRESHOLD_FRACTION * 100));
        return out.toString();
    }

    private static void appendSizeTable(StringBuilder out, List<Row> rows, boolean forced) {
        out.append("| Scenario | Range | Incompletes |");
        for (final String label : INCUMBENT_LABELS) {
            out.append(' ').append(label).append(" |");
        }
        for (final String candidate : CANDIDATES) {
            out.append(' ').append(candidate).append(" |");
        }
        if (!forced) {
            out.append(" twins |");
        }
        out.append(" Winner | bytes | Base64 | Z85 |\n");
        out.append("|---|---:|---:|");
        out.append(repeat("---:|", INCUMBENT_LABELS.length));
        out.append(repeat("---|", CANDIDATES.length));
        if (!forced) {
            out.append(":-:|");
        }
        out.append("---|---:|---:|---:|\n");

        for (final Row row : rows) {
            out.append("| `").append(row.scenario()).append("` | ");
            out.append(String.format(Locale.ROOT, "%,d | %,d |", row.rangeSize, row.incompleteCount));
            final int[] incumbent = forced ? row.incumbentForced : row.incumbentProduction;
            for (final int size : incumbent) {
                out.append(size < 0 ? " n/a |" : String.format(Locale.ROOT, " %,d |", size));
            }
            for (int c = 0; c < CANDIDATES.length; c++) {
                out.append(String.format(Locale.ROOT, " %,d / %,d |", row.candidatePlain[c], row.candidateZstd[c]));
            }
            if (!forced) {
                out.append(row.productionTwinsRegistered ? " yes |" : " ! |");
            }
            final Choice winner = forced ? row.winnerForced : row.winnerProduction;
            out.append(String.format(Locale.ROOT, " `%s` | %,d | %,d | %,d |\n",
                    winner.name, winner.packedBytes, winner.base64(), winner.z85()));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Self-checks on the layout writers and the model (the plan's "layout-writer sanity" scenarios)
    // ---------------------------------------------------------------------------------------------------------

    /**
     * All-incomplete is a single unbroken run per chunk, so every chunk picks a RUN container.
     * <p>
     * NOTE: the plan predicted a BITMAP container here. That prediction was made before run containers were part of
     * the per-chunk minimum, and it is simply beaten - a bitmap costs 8KB per full chunk against a run container's
     * 6 bytes, and Roaring itself would pick the run. The assertion below therefore pins what the model actually
     * does AND states the bitmap cost the plan expected, so the comparison is visible rather than assumed.
     */
    @Test
    void chunkedLayoutOnAllIncompletePicksRunContainersNotBitmaps() {
        final int rangeSize = 200_000; // 3 full chunks + a partial one
        final SortedSet<Long> incompletes = incompletesFor(FAMILY_ALL_INCOMPLETE, rangeSize);
        assertThat(incompletes).hasSize(rangeSize);

        final int chunks = ceilDiv(rangeSize, CHUNK_BITS);
        assertThat(chunks).isEqualTo(4);

        final int header = 1 /* magic */ + 4 /* rangeLength */ + 2 /* chunkCount */;
        final int perChunkRunContainer = 2 /* chunkIndex */ + 1 /* containerType */ + 2 /* runCount */ + 4 /* one run */;
        final byte[] layout = chunkedBitSetLayout(rangeSize, incompletes, 0L);
        assertThat(layout).hasLength(header + chunks * perChunkRunContainer);

        int bitmapModel = header;
        for (int chunk = 0; chunk < chunks; chunk++) {
            final int usedBits = Math.min(CHUNK_BITS, rangeSize - chunk * CHUNK_BITS);
            bitmapModel += 2 + 1 + 2 + ceilDiv(usedBits, 8);
        }
        assertWithMessage("the plan expected bitmap containers here; run containers are %s bytes smaller",
                bitmapModel - layout.length)
                .that(bitmapModel).isGreaterThan(layout.length);
    }

    /** A single isolated incomplete is one array container of cardinality 1 - Roaring's sparse-side advantage. */
    @Test
    void chunkedLayoutOnASingleIsolatedIncompleteIsOneArrayContainer() {
        final TreeSet<Long> incompletes = new TreeSet<>();
        incompletes.add(500L);
        final byte[] layout = chunkedBitSetLayout(1_000, incompletes, 0L);

        final int expected = 1 /* magic */ + 4 /* rangeLength */ + 2 /* chunkCount */
                + 2 /* chunkIndex */ + 1 /* containerType */
                + 2 /* cardinality */ + 2 /* the one entry */;
        assertThat(layout).hasLength(expected);
        assertThat(layout).hasLength(14);
        assertWithMessage("container type byte must be ARRAY").that(layout[9] & 0xFF).isEqualTo(CONTAINER_ARRAY);
    }

    /** k incompletes whose deltas all fit in one varint byte cost exactly one byte each. */
    @Test
    void deltaListOfSmallDeltasIsOneBytePerIncomplete() {
        final TreeSet<Long> incompletes = new TreeSet<>();
        final int count = 20;
        for (int i = 1; i <= count; i++) {
            incompletes.add((long) i * 5); // first value 5, then gaps of 5 - all < 128
        }
        final byte[] layout = sparseDeltaListLayout(1_000, incompletes, 0L);

        final int expected = 1 /* magic */ + 4 /* rangeLength */ + 1 /* varint(20) */ + count;
        assertThat(layout).hasLength(expected);
        assertThat(layout).hasLength(26);
    }

    /** The varint boundary: a delta of 128 takes two bytes, 127 takes one. */
    @Test
    void deltaListVarintCrossesToTwoBytesAt128() {
        final TreeSet<Long> justUnder = new TreeSet<>();
        justUnder.add(0L);
        justUnder.add(127L);
        final TreeSet<Long> justOver = new TreeSet<>();
        justOver.add(0L);
        justOver.add(128L);

        assertThat(sparseDeltaListLayout(1_000, justUnder, 0L)).hasLength(1 + 4 + 1 + 1 + 1);
        assertThat(sparseDeltaListLayout(1_000, justOver, 0L)).hasLength(1 + 4 + 1 + 1 + 2);
    }

    /**
     * The unsigned run-length layout keeps {@link RunLengthEncoder}'s incomplete-first alternation, and splits a run
     * longer than 65,535 as {@code [65535, 0, remainder]}.
     */
    @Test
    void unsignedRunLengthAlternatesIncompleteFirstAndSplitsLongRuns() {
        // offset 0 complete, so a zero-length incomplete run leads - exactly as RunLengthEncoder emits it
        final TreeSet<Long> trailing = new TreeSet<>();
        trailing.add(8L);
        trailing.add(9L);
        final byte[] small = unsignedRunLengthLayout(10, trailing, 0L);
        // runs: [0 incomplete][8 complete][2 incomplete]
        assertThat(small).hasLength(1 + 4 + 3 * 2);
        assertThat(readShort(small, 5)).isEqualTo(0);
        assertThat(readShort(small, 7)).isEqualTo(8);
        assertThat(readShort(small, 9)).isEqualTo(2);

        // one complete run of 131,072 needs splitting: 65535, 0, 65535, 0, 2
        final byte[] split = unsignedRunLengthLayout(131_072, new TreeSet<>(), 0L);
        assertThat(split).hasLength(1 + 4 + 6 * 2);
        assertThat(readShort(split, 5)).isEqualTo(0);      // leading zero-length incomplete run
        assertThat(readShort(split, 7)).isEqualTo(0xFFFF);
        assertThat(readShort(split, 9)).isEqualTo(0);      // zero-length opposite run hands the state back
        assertThat(readShort(split, 11)).isEqualTo(0xFFFF);
        assertThat(readShort(split, 13)).isEqualTo(0);
        assertThat(readShort(split, 15)).isEqualTo(131_072 - 2 * 0xFFFF);
    }

    private static int readShort(byte[] bytes, int at) {
        return ((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF);
    }

    /** KTD6's crossover arithmetic, which the report's Base64/Z85 columns rest on. */
    @Test
    void outerCodecArithmeticMatchesKtd6() {
        assertThat(base64Chars(3)).isEqualTo(4);
        assertThat(sentinelZ85Chars(3)).isEqualTo(5);   // Z85 longer here; below 22 the raw formulas interleave
        assertThat(base64Chars(12)).isEqualTo(16);
        assertThat(sentinelZ85Chars(12)).isEqualTo(16); // tied at 12
        assertThat(base64Chars(21)).isEqualTo(28);
        assertThat(sentinelZ85Chars(21)).isEqualTo(28); // tied at 21
        assertThat(base64Chars(24)).isEqualTo(32);
        assertThat(sentinelZ85Chars(24)).isEqualTo(31); // strictly shorter from the 22-byte floor up
        assertThat(base64Chars(1_024)).isEqualTo(1_368);
        assertThat(sentinelZ85Chars(1_024)).isEqualTo(1_281);
        // ...converging on ~6%
        assertThat(1 - sentinelZ85Chars(1_024) / (double) base64Chars(1_024)).isWithin(0.005d).of(0.0636d);
        // the writer's floored rule: base64 below 22 payload bytes - even at 13, where the raw z85 formula
        // (18 chars vs base64's 20) would win a pure shorter-of - then the min from 22 up
        assertThat(stringChars(3)).isEqualTo(4);
        assertThat(sentinelZ85Chars(13)).isEqualTo(18);
        assertThat(stringChars(13)).isEqualTo(20);
        assertThat(stringChars(22)).isEqualTo(29);
        assertThat(stringChars(24)).isEqualTo(31);
    }

    /**
     * The report derives the production compression rule from the plain sizes of a forced run rather than paying for
     * a second encode pass. That derivation must match what an unforced run really registers.
     */
    @Test
    void productionCompressionGateModelMatchesReality() {
        for (final Family family : FAMILIES) {
            for (final int rangeSize : new int[]{1_000, 10_000}) {
                final SortedSet<Long> incompletes = incompletesFor(family, rangeSize);

                OffsetSimultaneousEncoder.compressionForced = true;
                final Map<OffsetEncoding, byte[]> forced =
                        new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke().getEncodingMap();

                OffsetSimultaneousEncoder.compressionForced = false;
                final Map<OffsetEncoding, byte[]> production =
                        new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke().getEncodingMap();

                // Per encoder since KTD8: each twin is present exactly when its own plain form is not quiteSmall.
                for (final OffsetEncoding[] pair : INCUMBENTS) {
                    assertWithMessage("derived production compression gate for %s, %s @ %s",
                            pair[0], family.label, rangeSize)
                            .that(productionTwinWouldRegister(forced.get(pair[0])))
                            .isEqualTo(production.containsKey(pair[1]));
                }
                // and the plain entries must be identical either way - forcing compression adds twins, nothing else
                for (final OffsetEncoding[] pair : INCUMBENTS) {
                    assertWithMessage("plain %s for %s @ %s", pair[0], family.label, rangeSize)
                            .that(production.containsKey(pair[0])).isEqualTo(forced.containsKey(pair[0]));
                }
            }
        }
    }

    /**
     * The corpus must be a pure function of (family, range size) - otherwise the committed report cannot be
     * byte-identical across runs, and the `--check` gate becomes noise.
     */
    @Test
    void corpusAndLayoutsAreDeterministic() {
        for (final Family family : FAMILIES) {
            final SortedSet<Long> first = incompletesFor(family, 10_000);
            final SortedSet<Long> second = incompletesFor(family, 10_000);
            assertWithMessage("corpus for %s must be reproducible", family.label)
                    .that(first).containsExactlyElementsIn(second).inOrder();

            assertThat(chunkedBitSetLayout(10_000, first, 0L)).isEqualTo(chunkedBitSetLayout(10_000, second, 0L));
            assertThat(sparseDeltaListLayout(10_000, first, 0L)).isEqualTo(sparseDeltaListLayout(10_000, second, 0L));
            assertThat(unsignedRunLengthLayout(10_000, first, 0L))
                    .isEqualTo(unsignedRunLengthLayout(10_000, second, 0L));
        }
    }

    /**
     * The empty-incompletes and zero-length-range edges the plan asks about. Documented, not forced: an empty map
     * short-circuits well before any of this in {@code OffsetMapCodecManager}, so what matters here is only that
     * the layout writers stay well-defined.
     */
    @Test
    void emptyAndZeroLengthEdgesAreWellDefined() {
        final TreeSet<Long> none = new TreeSet<>();

        // empty incompletes over a real range: nothing to chunk, one all-complete run
        assertThat(chunkedBitSetLayout(1_000, none, 0L)).hasLength(1 + 4 + 2);
        assertThat(sparseDeltaListLayout(1_000, none, 0L)).hasLength(1 + 4 + 1);
        assertThat(unsignedRunLengthLayout(1_000, none, 0L)).hasLength(1 + 4 + 2 * 2);

        // zero-length range: headers only, and no runs at all
        assertThat(chunkedBitSetLayout(0, none, 0L)).hasLength(1 + 4 + 2);
        assertThat(sparseDeltaListLayout(0, none, 0L)).hasLength(1 + 4 + 1);
        assertThat(unsignedRunLengthLayout(0, none, 0L)).hasLength(1 + 4);
    }

    /**
     * The report hard-codes the cap and the threshold rather than reading the mutable static, so that a concurrent
     * test writing {@code DefaultMaxMetadataSize} cannot change the committed file. This pins the two together, so
     * a real change to the default is caught here instead of silently invalidating the report.
     */
    @Test
    void capAndThresholdConstantsAreWhatTheReportAssumes() {
        assertThat(OffsetMapCodecManager.DefaultMaxMetadataSize).isEqualTo(MAX_METADATA_CHARS);
        assertThat(PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT).isWithin(0d).of(0.75d);
        assertThat(BACK_PRESSURE_CHARS).isEqualTo(3_072);
    }
}
