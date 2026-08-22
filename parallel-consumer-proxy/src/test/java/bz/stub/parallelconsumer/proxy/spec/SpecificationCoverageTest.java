package bz.stub.parallelconsumer.proxy.spec;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The specification-to-schema diff, enforced empty: every message, field, enum and enum value in the frozen
 * {@code proxy.proto} must appear <b>in the section of {@code docs/protocol-specification.md} that documents
 * its owner</b>, written the way that document writes a schema name - in backticks. The freeze's premise is
 * that ten client authors work from the specification alone, so a schema element the specification does not
 * name is a hole an author falls through, and it must fail the build rather than wait for language six to
 * find it. The names are read from the generated file descriptor, so a schema addition that forgets its
 * documentation goes red here with no list to maintain.
 * <p>
 * <b>Why both anchors, rather than "does the document contain this word".</b> The first version of this test
 * searched the whole specification for the bare name as a substring, which the prose answers by accident: an
 * undocumented future field called {@code deadline}, {@code reason} or {@code window} passed, because all
 * three words are already somewhere in the document ("per-call deadline negotiation", "optional
 * {@code reason} rides the redelivery", "the window expires"). A guard that passes on undocumented fields is
 * worse than none, because the freeze is now relying on it. The backtick anchor removes the prose
 * collisions; the section anchor removes the rest, because a field is only documented where its message is -
 * a {@code reason} on {@code Heartbeat} is not documented by {@code Report.Failure}'s row. Both are checked
 * by {@link #anUndocumentedFieldWhoseNameCollidesWithTheProseIsCaught}, the negative control without which
 * this test's green is unfalsifiable.
 *
 * @author Antony Stubbs
 */
class SpecificationCoverageTest {

    private static final Path SPECIFICATION =
            Path.of(System.getProperty("basedir", ".")).resolve("docs/protocol-specification.md");

    /**
     * What starts a documentation section. Two forms, because the reference uses both: a markdown heading
     * ({@code ### `Configure`}), and a bold label opening a block inside one ({@code **`ClientMessage`** -
     * everything the client can say}), which is how the envelopes and the enums are written.
     */
    private static final Pattern SECTION_ANCHOR = Pattern.compile("^(#{2,4} |\\*\\*`\\w+`\\*\\*)");

    @Test
    void everyMessageFieldEnumAndServiceInTheFrozenSchemaAppearsInTheSpecification() throws IOException {
        var specification = Specification.read(SPECIFICATION);
        FileDescriptor schema = Configure.getDescriptor().getFile();

        var missing = collectMissing(schema, specification);

        assertWithMessage("frozen schema elements missing from docs/protocol-specification.md - the "
                + "specification is the contract ten authors implement from, so document each of these "
                + "(and its meaning) in its own message's section, written in backticks the way the "
                + "reference tables write a field; do not delete it from the schema:\n%s",
                String.join("\n", missing))
                .that(missing).isEmpty();
    }

    /**
     * The negative control: a fabricated field whose name the prose already contains, in a message whose
     * section never mentions it. The unanchored predicate this test used to carry reports it as documented -
     * the check's whole failure mode, and the reason a green run above means something.
     */
    @Test
    void anUndocumentedFieldWhoseNameCollidesWithTheProseIsCaught() throws IOException {
        var specification = Specification.read(SPECIFICATION);

        for (String prosePresent : List.of("deadline", "reason", "window")) {
            assertWithMessage("the control is only a control while '%s' really is somewhere in the prose - "
                            + "if the document lost the word, pick another collision", prosePresent)
                    .that(specification.text()).contains(prosePresent);

            // Heartbeat is the smallest message with a section of its own, and it documents no field at all
            assertWithMessage("a fabricated Heartbeat.%s passed as documented", prosePresent)
                    .that(specification.documents("Heartbeat", prosePresent)).isFalse();
        }

        // and the check still says yes to the real thing, so the control is not passing by breaking everything
        assertThat(specification.documents("Report.Failure", "reason")).isTrue();
        assertThat(specification.documents("Configure", "reconnect_window")).isTrue();
    }

    private static List<String> collectMissing(FileDescriptor schema, Specification specification) {
        var missing = new ArrayList<String>();
        for (Descriptor message : schema.getMessageTypes()) {
            collectMissing(message, specification, missing);
        }
        for (EnumDescriptor enumType : schema.getEnumTypes()) {
            collectMissing(enumType, specification, missing);
        }
        for (var service : schema.getServices()) {
            // the service and its one method are named in the prose rather than in a reference table, so
            // they carry the backtick anchor only - there is no owning message section to look inside
            requireBackticked(specification, service.getName(), "service " + service.getName(), missing);
            for (var method : service.getMethods()) {
                requireBackticked(specification, method.getName(),
                        "rpc " + service.getName() + "/" + method.getName(), missing);
            }
        }
        return missing;
    }

    private static void collectMissing(Descriptor message, Specification specification, List<String> missing) {
        if (message.getOptions().getMapEntry()) {
            return; // synthetic map-entry types (e.g. KafkaPropertiesEntry) are spelled as map<...> fields
        }
        if (message.getContainingType() == null) {
            requireSection(specification, message.getName(), "message " + message.getFullName(), missing);
        } else {
            // a nested message is documented inside its parent's section, as Success is inside Report's
            requireDocumented(specification, pathOf(message.getContainingType()), message.getName(),
                    "message " + message.getFullName(), missing);
        }
        for (FieldDescriptor field : message.getFields()) {
            requireDocumented(specification, pathOf(message), field.getName(),
                    "field " + message.getFullName() + "." + field.getName(), missing);
        }
        for (Descriptor nested : message.getNestedTypes()) {
            collectMissing(nested, specification, missing);
        }
        for (EnumDescriptor nested : message.getEnumTypes()) {
            collectMissing(nested, specification, missing);
        }
    }

    private static void collectMissing(EnumDescriptor enumType, Specification specification,
                                       List<String> missing) {
        requireSection(specification, enumType.getName(), "enum " + enumType.getFullName(), missing);
        for (EnumValueDescriptor value : enumType.getValues()) {
            requireDocumented(specification, enumType.getName(), value.getName(),
                    "enum value " + enumType.getFullName() + "." + value.getName(), missing);
        }
    }

    /** A message's chain of simple names, package stripped: {@code Report}, {@code Report.Success}. */
    private static String pathOf(Descriptor message) {
        String qualifier = message.getFile().getPackage();
        return qualifier.isEmpty()
                ? message.getFullName()
                : message.getFullName().substring(qualifier.length() + 1);
    }

    private static void requireSection(Specification specification, String name, String description,
                                       List<String> missing) {
        if (!specification.hasSectionFor(name)) {
            missing.add(description + " - no section of the reference is headed `" + name + "`");
        }
    }

    private static void requireDocumented(Specification specification, String owner, String name,
                                          String description, List<String> missing) {
        if (!specification.documents(owner, name)) {
            missing.add(description + " - `" + name + "` does not appear in the section documenting `"
                    + owner + "`");
        }
    }

    private static void requireBackticked(Specification specification, String name, String description,
                                          List<String> missing) {
        if (!specification.mentionsInACodeSpan(name)) {
            missing.add(description + " - " + name + " is named in no code span of the document");
        }
    }

    /**
     * The specification, split into the sections its own headings and bold labels define, so a name can be
     * looked for where it would have to be documented rather than anywhere at all.
     */
    private static final class Specification {

        private final String text;

        /** Section anchor line to that section's body, in document order. */
        private final Map<String, String> sections;

        private Specification(String text, Map<String, String> sections) {
            this.text = text;
            this.sections = sections;
        }

        static Specification read(Path path) throws IOException {
            String text = Files.readString(path);
            var lines = text.split("\n", -1);
            var sections = new LinkedHashMap<String, String>();
            String anchor = null;
            var body = new StringBuilder();
            for (String line : lines) {
                if (SECTION_ANCHOR.matcher(line).find()) {
                    if (anchor != null) {
                        sections.merge(anchor, body.toString(), (first, second) -> first + "\n" + second);
                    }
                    anchor = line;
                    body.setLength(0);
                }
                body.append(line).append('\n');
            }
            if (anchor != null) {
                sections.merge(anchor, body.toString(), (first, second) -> first + "\n" + second);
            }
            return new Specification(text, sections);
        }

        String text() {
            return text;
        }

        /**
         * Whether the document names this inside a code span - the service and its method, which the wire
         * contract writes fully qualified in one span
         * ({@code `parallelconsumer.proxy.v1.ProxyService/Session`}) rather than as a bare backticked word.
         * Still an anchor: the prose around it does not count, only what the document marked as code.
         */
        boolean mentionsInACodeSpan(String name) {
            var span = Pattern.compile("`[^`]*\\b" + Pattern.quote(name) + "\\b[^`]*`");
            return span.matcher(text).find();
        }

        /** Whether some section is headed by this name - what a message or enum needs of its own. */
        boolean hasSectionFor(String name) {
            return !bodiesDocumenting(name).isEmpty();
        }

        /**
         * Whether {@code name} is documented in the section belonging to {@code ownerPath} - backticked, and
         * inside that section rather than anywhere in the document.
         * <p>
         * {@code ownerPath} is the owner's chain of simple names ({@code Report.Success}), walked from the
         * innermost outwards: a nested message rarely has a section of its own, and the specification
         * documents it inside its parent's. The qualified spelling counts as well as the bare one, because
         * that is how the reference writes a nested type's row (`Report.Success`).
         */
        boolean documents(String ownerPath, String name) {
            var owners = ownerPath.split("\\.");
            String innermost = owners[owners.length - 1];
            for (int i = owners.length - 1; i >= 0; i--) {
                var bodies = bodiesDocumenting(owners[i]);
                if (bodies.isEmpty()) {
                    continue;
                }
                return bodies.stream().anyMatch(body -> body.contains(backticked(name))
                        || body.contains(backticked(innermost + "." + name)));
            }
            return false;
        }

        private List<String> bodiesDocumenting(String name) {
            var bodies = new ArrayList<String>();
            sections.forEach((anchor, body) -> {
                if (anchor.contains(backticked(name))) {
                    bodies.add(body);
                }
            });
            return bodies;
        }

        private static String backticked(String name) {
            return '`' + name + '`';
        }
    }
}
