package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * R3's compile-time half: a route that has not declared produced types cannot return a produced record, and in the
 * Java binding that is a <b>compile error</b>, not a runtime refusal.
 * <p>
 * Nothing an ordinary test can assert would show that, because code which does not compile cannot be in the test
 * source at all. So this compiles two snippets with {@code javac}, and the control arm is what makes the result mean
 * anything: the same statement with {@code produced(...)} added must compile. Without that arm a mistyped snippet, a
 * missing import or a broken classpath would fail to compile too and read as a pass - which is exactly what happened
 * while this test was being written, and is why the compiler runs out of process (see {@link #javac()}).
 */
class ProducingIsACompileErrorWithoutProducedTypesTest {

    private static final Pattern PATH_SEPARATOR = Pattern.compile(Pattern.quote(File.pathSeparator));

    private static final String PRELUDE =
            "package probe;\n"
                    + "import bz.stub.parallelconsumer.fluent.Outcome;\n"
                    + "import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;\n"
                    + "import bz.stub.parallelconsumer.fluent.Produced;\n"
                    + "import org.apache.kafka.clients.producer.ProducerRecord;\n"
                    + "import org.apache.kafka.common.serialization.Serdes;\n"
                    + "class Probe {\n"
                    + "    static void go(ParallelConsumerDefinition pc) {\n";

    private static final String EPILOGUE = "    }\n}\n";

    private static final String PRODUCING_STATEMENT =
            "        pc.string(\"orders\").process(ctx ->\n"
                    + "            Outcome.produce(new ProducerRecord<>(\"order-events\", \"k\", \"v\")));\n";

    private static final String PRODUCING_STATEMENT_WITH_TYPES_DECLARED =
            "        pc.string(\"orders\")\n"
                    + "            .produced(Produced.with(Serdes.String(), Serdes.String()))\n"
                    + "            .process(ctx ->\n"
                    + "                Outcome.produce(new ProducerRecord<>(\"order-events\", \"k\", \"v\")));\n";

    @Test
    void producingFromARouteThatDeclaredNoProducedTypesDoesNotCompile() {
        String output = compile(PRELUDE + PRODUCING_STATEMENT + EPILOGUE);

        assertThat(output).contains("error");
        // The mismatch is on the outcome's produced type pair, which is Void until produced(...) re-types the route.
        assertThat(output).contains("Void");
    }

    /**
     * The control arm. If this fails, the arm above proves nothing about the type rule - it would only prove that
     * the snippet does not compile, which a typo achieves just as well.
     */
    @Test
    void theSameStatementCompilesOnceTheRouteDeclaresWhatItProduces() {
        assertThat(compile(PRELUDE + PRODUCING_STATEMENT_WITH_TYPES_DECLARED + EPILOGUE).trim()).isEmpty();
    }

    /**
     * @return everything javac said, empty when the snippet compiled cleanly
     */
    private String compile(String source) {
        Path javac = javac();
        try {
            Path work = Files.createTempDirectory("pc-fluent-type-rule");
            Path file = work.resolve("Probe.java");
            Files.write(file, source.getBytes(StandardCharsets.UTF_8));

            List<String> command = new ArrayList<>(Arrays.asList(
                    javac.toString(),
                    "-classpath", classpathWithoutJabel(),
                    "-proc:none",
                    "-d", work.toString(),
                    file.toString()));
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output;
            try (InputStream stream = process.getInputStream()) {
                output = new String(readAll(stream), StandardCharsets.UTF_8);
            }
            int exit = process.waitFor();
            // Guards the false negative this test hit while it was being written: a compiler that fails for its own
            // reasons, saying nothing about the source, is indistinguishable from a snippet that failed to
            // type-check.
            assertThat(exit == 0).isEqualTo(output.trim().isEmpty());
            return output;
        } catch (IOException e) {
            throw new IllegalStateException("could not run javac over the snippet", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while compiling the snippet", e);
        }
    }

    private static byte[] readAll(InputStream stream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int read;
        while ((read = stream.read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        return buffer.toByteArray();
    }

    /**
     * The compiler runs <b>out of process</b>, which is not the obvious choice and is worth the paragraph.
     * <p>
     * In-process, {@code ToolProvider.getSystemJavaCompiler()} instantiates every
     * {@code com.sun.source.util.Plugin} on the classpath through {@link java.util.ServiceLoader}, and this
     * project's test classpath carries Jabel, whose plugin retransforms javac's own classes through an agent as it
     * initialises. Inside a running surefire JVM that retransformation is refused - "class redefinition failed:
     * attempted to delete a method" - and the compile task then returns false having emitted <b>no diagnostics at
     * all</b>, which reads exactly like a snippet that failed to type-check. Neither {@code -proc:none} nor an empty
     * {@code -processorpath} avoids it: javac falls back to its own class loader when looking for plugins. A
     * separate process gets a clean javac.
     */
    private Path javac() {
        Path candidate = Paths.get(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win") ? "javac.exe" : "javac");
        assumeTrue(Files.isExecutable(candidate), "no javac beside this runtime (a JRE rather than a JDK)");
        return candidate;
    }

    /**
     * Jabel is a compile-time back-porting plugin and these snippets are Java 8-compatible source, so it has nothing
     * to do here; leaving it off keeps the snippet's compilation as plain as possible.
     */
    private String classpathWithoutJabel() {
        List<String> entries = new ArrayList<>();
        for (String entry : PATH_SEPARATOR.split(System.getProperty("java.class.path"), -1)) {
            if (!entry.contains("jabel")) {
                entries.add(entry);
            }
        }
        return String.join(File.pathSeparator, entries);
    }
}
