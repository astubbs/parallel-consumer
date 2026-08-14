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
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The specification-to-schema diff, enforced empty: every message, field, enum and enum value in the frozen
 * {@code proxy.proto} must appear by name in {@code docs/protocol-specification.md}. The freeze's premise is
 * that ten client authors work from the specification alone - so a schema element the specification does not
 * name is a hole an author falls through, and it must fail the build rather than wait for language six to
 * find it. The names are read from the generated file descriptor, so a schema addition that forgets its
 * documentation goes red here with no list to maintain.
 */
class SpecificationCoverageTest {

    private static final Path SPECIFICATION =
            Path.of(System.getProperty("basedir", ".")).resolve("docs/protocol-specification.md");

    @Test
    void everyMessageFieldEnumAndServiceInTheFrozenSchemaAppearsInTheSpecification() throws IOException {
        String specification = Files.readString(SPECIFICATION);
        FileDescriptor schema = Configure.getDescriptor().getFile();

        var missing = new ArrayList<String>();
        for (Descriptor message : schema.getMessageTypes()) {
            collectMissing(message, specification, missing);
        }
        for (EnumDescriptor enumType : schema.getEnumTypes()) {
            collectMissing(enumType, specification, missing);
        }
        for (var service : schema.getServices()) {
            requireNamed(specification, service.getName(), "service " + service.getName(), missing);
            for (var method : service.getMethods()) {
                requireNamed(specification, method.getName(),
                        "rpc " + service.getName() + "/" + method.getName(), missing);
            }
        }

        assertWithMessage("frozen schema elements missing from docs/protocol-specification.md - the "
                + "specification is the contract ten authors implement from, so document each of these "
                + "(and its meaning), do not delete it from the schema:\n%s", String.join("\n", missing))
                .that(missing).isEmpty();
    }

    private static void collectMissing(Descriptor message, String specification, List<String> missing) {
        if (message.getOptions().getMapEntry()) {
            return; // synthetic map-entry types (e.g. KafkaPropertiesEntry) are spelled as map<...> fields
        }
        requireNamed(specification, message.getName(), "message " + message.getFullName(), missing);
        for (FieldDescriptor field : message.getFields()) {
            requireNamed(specification, field.getName(),
                    "field " + message.getFullName() + "." + field.getName(), missing);
        }
        for (Descriptor nested : message.getNestedTypes()) {
            collectMissing(nested, specification, missing);
        }
        for (EnumDescriptor nested : message.getEnumTypes()) {
            collectMissing(nested, specification, missing);
        }
    }

    private static void collectMissing(EnumDescriptor enumType, String specification, List<String> missing) {
        requireNamed(specification, enumType.getName(), "enum " + enumType.getFullName(), missing);
        for (EnumValueDescriptor value : enumType.getValues()) {
            requireNamed(specification, value.getName(),
                    "enum value " + enumType.getFullName() + "." + value.getName(), missing);
        }
    }

    private static void requireNamed(String specification, String name, String description,
                                     List<String> missing) {
        if (!specification.contains(name)) {
            missing.add(description);
        }
    }
}
