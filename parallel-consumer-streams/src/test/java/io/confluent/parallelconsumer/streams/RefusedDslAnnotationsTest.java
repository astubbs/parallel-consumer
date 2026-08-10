package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Layer 1: the refused DSL methods carry {@code @Deprecated} and {@code @DoNotCall}, so a call site is a
 * compile <b>error</b> under ErrorProne and a warning without it.
 * <p>
 * <b>And a javadoc {@code @deprecated} tag, which is the half a human actually reads.</b> An IDE renders
 * {@code @Deprecated} as a strikethrough and shows that tag as the reason. {@code @DoNotCall}'s message is not
 * a substitute: only ErrorProne reads it, and an ErrorProne build already fails hard - so without the tag, the
 * explanation reaches exactly the audience that needs it least, and everyone else sees {@code stream.join(...)}
 * struck through and reasonably concludes Apache Kafka deprecated {@code join}. It did not; this module refuses
 * it, and only while the dispatch seam is on.
 * <p>
 * <b>Why the two halves are asserted differently.</b> {@code java.lang.Deprecated} is {@code RUNTIME}-
 * retained, so reflection can enumerate every overload and check each one - which makes that half
 * exhaustive by construction, and it is the half that catches a missed overload after a Kafka upgrade adds
 * one. {@code @DoNotCall} is {@code CLASS}-retained: it is in the class file but invisible to reflection.
 * So it is asserted per class, by looking for its descriptor in the compiled constant pool. Weaker, and
 * stated here rather than quietly skipped.
 * <p>
 * <b>Why the compile error itself is not asserted.</b> Nothing in this repository compiles under ErrorProne,
 * so there is no build in which a call site could be observed failing. The annotation being present on the
 * symbol the compiler resolves is what this test can prove, and it is the whole of layer 1's mechanism.
 * <p>
 * The annotations go on the <b>interfaces</b> rather than the impls because a call site resolves to the
 * symbol its receiver's static type declares - {@code stream.join(...)} against a {@code KStream} variable
 * resolves to {@code KStream#join}, and an annotation on {@code KStreamImpl} would never be consulted.
 *
 * @author Antony Stubbs
 * @see PcUnsupportedConstruct
 */
class RefusedDslAnnotationsTest {

    private static final String DO_NOT_CALL_DESCRIPTOR = "Lcom/google/errorprone/annotations/DoNotCall;";

    /**
     * The refused method names, per interface. Deliberately name-based and coarse: every overload of these
     * names on these types is refused, so a Kafka upgrade that adds a twenty-ninth {@code join} overload
     * fails this test instead of shipping an unguarded hole.
     */
    private static final List<String> KSTREAM_REFUSED = Arrays.asList("join", "leftJoin", "outerJoin");
    private static final List<String> KTABLE_REFUSED = Arrays.asList("join", "leftJoin", "outerJoin", "suppress");
    private static final List<String> WINDOWED_BY = Arrays.asList("windowedBy");

    private static final int KSTREAM_OVERLOADS = 28;
    private static final int KTABLE_OVERLOADS = 25;
    private static final int KGROUPED_STREAM_OVERLOADS = 3;
    private static final int COGROUPED_KSTREAM_OVERLOADS = 3;

    private static final int TOTAL_REFUSED_OVERLOADS =
            KSTREAM_OVERLOADS + KTABLE_OVERLOADS + KGROUPED_STREAM_OVERLOADS + COGROUPED_KSTREAM_OVERLOADS;

    /**
     * The four {@code KTable} foreign-key overloads taking a {@link org.apache.kafka.streams.kstream.Named} that
     * Kafka had already deprecated itself. The patch gives them {@code @DoNotCall} but cannot give them a second
     * {@code @Deprecated}, and cannot give them a second javadoc {@code @deprecated} either - two of those in one
     * block is malformed - so its refusal is appended to Kafka's existing tag instead.
     */
    private static final int ALREADY_DEPRECATED_BY_KAFKA = 4;

    private static final int DEPRECATED_ADDED_BY_THE_PATCH =
            TOTAL_REFUSED_OVERLOADS - ALREADY_DEPRECATED_BY_KAFKA;

    @Test
    void everyRefusedKStreamMethodIsDeprecated() {
        assertEveryOverloadDeprecated(KStream.class, KSTREAM_REFUSED, KSTREAM_OVERLOADS);
    }

    @Test
    void everyRefusedKTableMethodIsDeprecated() {
        assertEveryOverloadDeprecated(KTable.class, KTABLE_REFUSED, KTABLE_OVERLOADS);
    }

    @Test
    void everyKGroupedStreamWindowedByIsDeprecated() {
        assertEveryOverloadDeprecated(KGroupedStream.class, WINDOWED_BY, KGROUPED_STREAM_OVERLOADS);
    }

    @Test
    void everyCogroupedKStreamWindowedByIsDeprecated() {
        assertEveryOverloadDeprecated(CogroupedKStream.class, WINDOWED_BY, COGROUPED_KSTREAM_OVERLOADS);
    }

    @Test
    void everyRefusedInterfaceCarriesDoNotCall() throws IOException {
        for (final Class<?> refused : Arrays.asList(
                KStream.class, KTable.class, KGroupedStream.class, CogroupedKStream.class)) {
            assertThat(classFileOf(refused))
                    .as("%s must reference %s, or a call site is only a warning rather than a compile error",
                            refused.getSimpleName(), DO_NOT_CALL_DESCRIPTOR)
                    .contains(DO_NOT_CALL_DESCRIPTOR);
        }
    }

    /**
     * The per-method half of layer 1, which the class-file scan above cannot give: the constant pool holds one
     * UTF8 entry for the descriptor however many methods use it, so that assertion would pass with 1 of 59
     * annotated. Counting the annotations in the tracked patch is exhaustive, and it is the only check that
     * covers the four {@code KTable} foreign-key overloads Kafka had <em>already</em> deprecated itself - for
     * those, {@link #everyRefusedKTableMethodIsDeprecated()} passes whether or not the patch touched them.
     */
    @Test
    void thePatchAnnotatesEveryRefusedOverloadIndividually() throws IOException {
        final List<String> patchLines = Files.readAllLines(
                Paths.get("src/main/patch/pc-streams.patch"), StandardCharsets.UTF_8);

        int doNotCallAdded = 0;
        for (final String line : patchLines) {
            // An added line in a unified diff is "+" then the source line, so the "+" has to come off before
            // trimming - trim() alone leaves it in front of the indentation and matches nothing.
            if (line.startsWith("+") && line.substring(1).trim().startsWith("@DoNotCall(")) {
                doNotCallAdded++;
            }
        }

        assertThat(doNotCallAdded)
                .as("the patch must add one @DoNotCall per refused overload: 28 on KStream, 25 on KTable, 3 on "
                        + "KGroupedStream, 3 on CogroupedKStream. A lower number means an overload is reachable "
                        + "without a compile error; a higher one means something is annotated that should not be.")
                .isEqualTo(TOTAL_REFUSED_OVERLOADS);
    }

    /**
     * The strikethrough has to come with its reason attached, or it misinforms - see this class's header. Two
     * assertions, because neither covers the whole surface on its own.
     * <p>
     * The <b>structural</b> one walks the patch's post-image and requires that every {@code @Deprecated} the patch
     * adds sits behind a javadoc block carrying a {@code @deprecated} tag. It says nothing about wording, so
     * rephrasing a tag does not fail it, while forgetting one on a Kafka overload added by a future upgrade does.
     * It cannot see the four Kafka had already deprecated, because for those the patch adds no {@code @Deprecated}.
     * <p>
     * The <b>count</b> one closes that gap from the other end: each tag names this module once, as the thing doing
     * the refusing, so one such line per refused overload means all 59 carry an explanation - including the four
     * where it was appended to Kafka's own text. It is deliberately coupled to that one phrase; a reword has to
     * come here and say so, which is the cheapest available guard on the sentence the reader is going to see.
     */
    @Test
    void everyRefusedOverloadExplainsTheRefusalInJavadoc() throws IOException {
        final List<String> patchLines = Files.readAllLines(
                Paths.get("src/main/patch/pc-streams.patch"), StandardCharsets.UTF_8);

        // The post-image of one hunk: the lines a reader of the patched file would see, and whether the patch
        // added each. Reset per hunk, because line adjacency does not survive a hunk boundary.
        final List<String> postImage = new ArrayList<>();
        final List<Boolean> addedByPatch = new ArrayList<>();
        int deprecatedAdded = 0;
        int explainedByJavadoc = 0;
        int namesThisModule = 0;

        for (final String line : patchLines) {
            if (line.startsWith("@@") || line.startsWith("--- ") || line.startsWith("+++ ")
                    || line.startsWith("diff ")) {
                explainedByJavadoc += countExplained(postImage, addedByPatch);
                deprecatedAdded += countAddedDeprecated(postImage, addedByPatch);
                postImage.clear();
                addedByPatch.clear();
            } else if (line.startsWith("+")) {
                postImage.add(line.substring(1));
                addedByPatch.add(true);
                if (line.contains("{@code parallel-consumer-streams}")) {
                    namesThisModule++;
                }
            } else if (line.startsWith(" ")) {
                // A context line: unchanged, but present in the patched file and so part of the post-image.
                postImage.add(line.substring(1));
                addedByPatch.add(false);
            }
            // A "-" line is pre-image only and is deliberately not part of what the reader ends up with.
        }
        explainedByJavadoc += countExplained(postImage, addedByPatch);
        deprecatedAdded += countAddedDeprecated(postImage, addedByPatch);

        assertThat(deprecatedAdded)
                .as("the patch must add one @Deprecated per refused overload Kafka had not already deprecated. "
                        + "If this moved, the two halves of layer 1 have drifted apart")
                .isEqualTo(DEPRECATED_ADDED_BY_THE_PATCH);

        assertThat(explainedByJavadoc)
                .as("every @Deprecated the patch adds must sit behind a javadoc @deprecated tag. Without it an IDE "
                        + "strikes the call through with no reason given, and the reader concludes Apache Kafka "
                        + "deprecated the method - which is false, and is the defect this assertion exists to stop")
                .isEqualTo(deprecatedAdded);

        assertThat(namesThisModule)
                .as("each refused overload's javadoc must name parallel-consumer-streams as the thing refusing it, "
                        + "once: 55 new tags plus 4 appended to tags Kafka already had. A lower number means an "
                        + "overload is struck through without saying who struck it")
                .isEqualTo(TOTAL_REFUSED_OVERLOADS);
    }

    private static int countAddedDeprecated(final List<String> postImage, final List<Boolean> addedByPatch) {
        int count = 0;
        for (int i = 0; i < postImage.size(); i++) {
            if (addedByPatch.get(i) && postImage.get(i).trim().equals("@Deprecated")) {
                count++;
            }
        }
        return count;
    }

    /**
     * How many of the {@code @Deprecated} annotations this hunk adds are preceded by a javadoc block that carries a
     * {@code @deprecated} tag. The tag can be a context line rather than an added one - that is the appended-to-
     * Kafka's-own case - so the post-image is what gets walked, not the additions alone.
     */
    private static int countExplained(final List<String> postImage, final List<Boolean> addedByPatch) {
        int explained = 0;
        for (int i = 0; i < postImage.size(); i++) {
            if (!addedByPatch.get(i) || !postImage.get(i).trim().equals("@Deprecated")) {
                continue;
            }

            // Other annotations may sit between the javadoc and this one - @DoNotCall does, and so does
            // @SuppressWarnings on a couple of Kafka's own methods.
            int close = i - 1;
            while (close >= 0
                    && (postImage.get(close).trim().startsWith("@") || postImage.get(close).trim().isEmpty())) {
                close--;
            }
            if (close < 0 || !postImage.get(close).trim().endsWith("*/")) {
                continue;
            }

            for (int k = close - 1; k >= 0; k--) {
                final String text = postImage.get(k).trim();
                if (text.startsWith("/**")) {
                    break;
                }
                if (text.startsWith("* @deprecated")) {
                    explained++;
                    break;
                }
            }
        }
        return explained;
    }

    @Test
    void supportedMethodsOnTheSameInterfacesAreNotDeprecated() {
        // The control. Without it, an indiscriminate pass that deprecated the entire interface would satisfy
        // every assertion above - and would have refused the operators this module exists to support.
        assertNotDeprecated(KStream.class, "mapValues");
        assertNotDeprecated(KStream.class, "filter");
        assertNotDeprecated(KStream.class, "groupByKey");
        assertNotDeprecated(KGroupedStream.class, "count");
        assertNotDeprecated(KGroupedStream.class, "reduce");
        assertNotDeprecated(KTable.class, "toStream");
    }

    private static void assertEveryOverloadDeprecated(final Class<?> type,
                                                      final List<String> refusedNames,
                                                      final int expectedCount) {
        final List<Method> refused = new ArrayList<>();
        for (final Method method : type.getMethods()) {
            if (refusedNames.contains(method.getName())) {
                refused.add(method);
            }
        }

        // A count assertion, so that a rename upstream fails loudly rather than making the loop below vacuous.
        assertThat(refused)
                .as("%s: refused overloads found. If Kafka changed this surface, the patch needs revisiting "
                        + "before this number is edited", type.getSimpleName())
                .hasSize(expectedCount);

        for (final Method method : refused) {
            assertThat(method.isAnnotationPresent(Deprecated.class))
                    .as("%s#%s%s must be @Deprecated", type.getSimpleName(), method.getName(),
                            Arrays.toString(method.getParameterTypes()))
                    .isTrue();
        }
    }

    private static void assertNotDeprecated(final Class<?> type, final String methodName) {
        final List<Method> found = new ArrayList<>();
        for (final Method method : type.getMethods()) {
            if (method.getName().equals(methodName)) {
                found.add(method);
            }
        }

        // Without this, a typo - or Kafka renaming the method - turns the control silently into a no-op, and
        // the control is the only thing stopping "deprecate the whole interface" from passing every test here.
        assertThat(found)
                .as("%s#%s does not exist, so this control proves nothing", type.getSimpleName(), methodName)
                .isNotEmpty();

        for (final Method method : found) {
            assertThat(method.isAnnotationPresent(Deprecated.class))
                    .as("%s#%s is a supported operator and must not be refused", type.getSimpleName(), methodName)
                    .isFalse();
        }
    }

    /**
     * The compiled class file as text. Crude, and enough: the descriptor is a UTF8 constant-pool entry, and it
     * can only be there if something in the file references the annotation.
     */
    private static String classFileOf(final Class<?> type) throws IOException {
        final String resource = type.getName().replace('.', '/') + ".class";
        try (InputStream in = type.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in).as("%s must be loadable as a resource", resource).isNotNull();
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            // ISO_8859_1 keeps every byte addressable rather than collapsing invalid UTF-8
            // sequences, and the descriptor is pure ASCII, so contains() over it is reliable.
            return new String(out.toByteArray(), StandardCharsets.ISO_8859_1);
        }
    }
}
