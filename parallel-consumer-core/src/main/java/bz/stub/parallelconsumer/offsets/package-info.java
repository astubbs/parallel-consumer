/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Offset-map encoding: how PC records per-message completion state beyond the highest committable offset, inside the
 * commit metadata string of an ordinary Kafka offset commit.
 * <p>
 * Density is the design pressure everywhere in this package. The encoded string is measured against the broker's
 * metadata cap ({@code OffsetMapCodecManager#DefaultMaxMetadataSize}, 4096) and against the back-pressure threshold
 * derived from it - so every character saved is direct headroom before PC must drop the map or stop taking new
 * records. The measured evidence behind each decision below lives in
 * {@code docs/offset-encoding-density-benchmark.md} (regenerate with {@code bin/offset-encoding-density-report.sh});
 * the work was driven by
 * <a href="https://github.com/astubbs/parallel-consumer/issues/192">astubbs#192</a>
 * (<a href="https://github.com/confluentinc/parallel-consumer/issues/903">confluentinc#903</a>).
 *
 * <h2>The competitive-set design</h2>
 * There is no single chosen encoding. {@link bz.stub.parallelconsumer.offsets.OffsetSimultaneousEncoder} runs every
 * registered {@link bz.stub.parallelconsumer.offsets.OffsetEncoder} over the same offset map simultaneously, and the
 * smallest result wins, per commit. Each encoder additionally registers a zstd-compressed twin of itself exactly when
 * its own plain form is large enough to be worth compressing - per encoder, not as an all-or-nothing gate across the
 * set. Which family wins depends on the shape of the incompletes: dense maps suit
 * {@link bz.stub.parallelconsumer.offsets.BitSetEncoder} (one bit per offset in the range), long completed stretches
 * suit {@link bz.stub.parallelconsumer.offsets.RunLengthEncoder}, and sparse maps suit
 * {@link bz.stub.parallelconsumer.offsets.DeltaListEncoder} (roughly one byte per incomplete). Adding a candidate
 * encoding never costs density - it only wins commits it deserves to win.
 *
 * <h2>The wire-format compatibility contract: write-new, read-all, forever</h2>
 * Committed metadata outlives the writer: it is read back after restarts, rebalances and upgrades, by whichever PC
 * version owns the partition next. So the contract is that a new version may <em>write</em> new formats, but every
 * format ever shipped stays <em>readable</em> forever. Formats are dispatched by a leading magic byte
 * ({@link bz.stub.parallelconsumer.offsets.OffsetEncoding#decode}); the full registry, including bytes reserved but
 * deliberately not registered:
 * <table>
 *     <caption>Magic-byte registry</caption>
 *     <tr><th>Encoding</th><th>Plain / zstd magic</th><th>Status</th></tr>
 *     <tr><td>ByteArray</td><td>{@code 'L'} / {@code 'î'}</td><td>shipped, v1</td></tr>
 *     <tr><td>BitSet (16-bit length)</td><td>{@code 'l'} / {@code 'a'}</td><td>shipped, v1</td></tr>
 *     <tr><td>RunLength (16-bit runs)</td><td>{@code 'n'} / {@code 'J'}</td><td>shipped, v1</td></tr>
 *     <tr><td>BitSetV2 (32-bit length)</td><td>{@code 'o'} / {@code 's'}</td><td>shipped, v2</td></tr>
 *     <tr><td>RunLengthV2 (32-bit runs)</td><td>{@code 'e'} / {@code 'p'}</td><td>shipped, v2</td></tr>
 *     <tr><td>Sparse delta list</td><td>{@code 'd'} / {@code 'D'}</td><td>shipped, v1</td></tr>
 *     <tr><td>Chunked bitset (Roaring-style containers)</td><td>{@code 'r'} / {@code 'z'}</td>
 *         <td>RESERVED, unregistered - measured case-against below</td></tr>
 *     <tr><td>Unsigned run-length</td><td>{@code 'u'} / {@code 'U'}</td>
 *         <td>RESERVED, unregistered - measured case-against below</td></tr>
 *     <tr><td>Kafka Streams (foreign, detected not written)</td><td>{@code 1} / {@code 2}</td>
 *         <td>recognised so PC can tell a Streams-owned group's metadata from corruption</td></tr>
 * </table>
 * The reserved pairs exist so a future encoder does not have to re-verify which bytes are free against every byte
 * ever shipped; a test asserts the registry's uniqueness directly. The two reserved candidates were measured and
 * declined: the chunked bitset's best qualifying result was +0.03% against the incumbent, the unsigned run-length's
 * was -0.14% - both far short of the 10%-improvement bar the benchmark report sets for taking on a new wire format.
 * The report carries the full per-scenario tables.
 *
 * <h2>The outer string codec: Base64 versus sentinel-prefixed Z85, per payload</h2>
 * The chosen binary encoding is string-encoded into the metadata field. Historically that was always Base64 (33%
 * expansion); {@link bz.stub.parallelconsumer.offsets.Z85Codec} (25% expansion) now competes with it under a
 * floored shorter-wins rule: below 22 payload bytes
 * ({@code OffsetSimpleSerialisation#Z85_MIN_PAYLOAD_BYTES}) the writer always emits Base64, and from 22 bytes up it
 * emits the {@code '%'}-sentinel-prefixed Z85 form, which is by then always strictly shorter. The arithmetic: Base64
 * costs {@code 4*ceil(n/3)} characters and sentinel+Z85 costs {@code 1 + 5*floor(n/4) + (n%4 == 0 ? 0 : n%4 + 1)}.
 * Below the floor the two interleave - sentinel+Z85 is a character shorter at n = 1, 4, 7, ... and equal or longer
 * elsewhere - but payloads that small are nowhere near the metadata cap, so the floor trades those single
 * characters for keeping every small payload in the form all older readers understand, at zero cost where density
 * matters. From 22 bytes up Z85 wins outright, converging on ~6.25% shorter: it fires exactly where density pays.
 * <p>
 * The sentinel scheme: {@code '%'} is outside the Base64 alphabet, so a leading {@code '%'} unambiguously marks Z85
 * (readers accept both forms forever). But {@code '%'} <em>is</em> in the Z85 alphabet itself - a Z85 string can
 * legitimately start with one. The dispatch is therefore only sound at the outermost layer, against Base64, exactly
 * once: the scheme must never be nested, and an already-encoded metadata string must never be re-encoded through it.
 * <p>
 * Z85 (ZeroMQ RFC 32) was chosen over Ascii85 and RFC 1924, and Base85 generally is hand-rolled rather than imported
 * - {@link bz.stub.parallelconsumer.offsets.Z85Codec}'s javadoc records the alphabet-safety reasoning and the
 * partial-block scheme.
 *
 * <h2>Why not the RoaringBitmap library</h2>
 * The perennial suggestion (the subject of astubbs#192) is to replace the custom encoders with RoaringBitmap. The
 * decision is no, and it is a measured one:
 * <ul>
 *     <li><b>The capability overlap is nearly total.</b> Roaring's run containers duplicate
 *     {@link bz.stub.parallelconsumer.offsets.RunLengthEncoder}; its bitmap containers duplicate
 *     {@link bz.stub.parallelconsumer.offsets.BitSetEncoder}. At PC's payload sizes the only capability the
 *     pre-existing set lacked was the sparse array container - and the delta-list captures that dependency-free,
 *     more densely than Roaring's fixed 2 bytes per entry (unsigned varint deltas are 1 byte per incomplete for
 *     gaps under 128).</li>
 *     <li><b>The container model was benchmarked, not dismissed.</b> The {@code chunked-bitset} candidate in the
 *     benchmark report IS Roaring's model - array, bitmap and run containers over 2<sup>16</sup>-bit chunks,
 *     smallest per chunk. Its best result on any qualifying scenario was +0.03% against the incumbent encoders,
 *     short of the 10% bar. "Would Roaring be denser?" is answered by a number: no.</li>
 *     <li><b>The cost is real.</b> The jar is ~450KB of bitmap algebra in a library that deliberately holds its
 *     runtime dependency count to four, for a use that is encode-once/decode-once over a bounded relative range -
 *     none of the algebra is ever exercised.</li>
 * </ul>
 *
 * <h2>Mixed-version caveat (rolling upgrades)</h2>
 * Upgrade-in-place from any older version works: this version reads every format ever shipped. The one-way window is
 * a <em>rolling</em> upgrade: a not-yet-upgraded instance reading a newer instance's commit (a Z85 string - payloads
 * of 22 bytes and up - or a {@code 'd'}/{@code 'D'} magic byte) cannot decode it, drops the offset map, and resumes
 * from the committed base offset. That is safe but can redeliver work that was in flight beyond the committed
 * offset, which matters to non-idempotent processors. The exposure is bounded to the upgrade window and to the
 * payloads named above.
 *
 * @see bz.stub.parallelconsumer.offsets.OffsetMapCodecManager
 * @see bz.stub.parallelconsumer.offsets.OffsetSimultaneousEncoder
 * @see bz.stub.parallelconsumer.offsets.OffsetEncoding
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/192">astubbs#192 - record the encoding-choice
 *         reasoning (mirror of confluentinc#903)</a>
 */
package bz.stub.parallelconsumer.offsets;
