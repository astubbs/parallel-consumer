package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * The four lines that let this JVM host the engine while the client library still spawns a sidecar.
 * <p>
 * <b>Why the engine has to be in THIS process.</b> The suite's assertions are about engine state - the
 * committed offset, the produced records - and the only way to read that state without inventing a
 * results protocol is to hold the {@code ProxyHarness} in the asserting JVM. So the harness binds its
 * gRPC server here, on an ephemeral loopback port, and the runner has to reach that port.
 * <p>
 * <b>Why a shim rather than a {@code --port} flag.</b> The client libraries' one entry point spawns a
 * sidecar and reads {@code port: <n>} from its stdout, holding its stdin as the parent-death lifeline -
 * that is the specified lifecycle, not an implementation detail. Adding a connect-to-an-existing-port
 * option to the API would be a real surface decision binding ten languages, taken here for a test's
 * convenience; the client-authoring guide and the protocol specification own that call. A script that
 * announces a port and then holds its stdin needs no such decision, and it exercises the library's real
 * spawn-and-reap path rather than routing around it.
 * <p>
 * <b>Pure builtins, one process.</b> {@code printf} and {@code read} are shell builtins, so the script is
 * a single process that itself holds the lifeline - no {@code cat} grandchild that would outlive its
 * parent's reap. {@code read} returns non-zero at EOF, which ends the loop and the script.
 *
 * @author Antony Stubbs
 */
public final class SidecarShim {

    /**
     * Writes an executable shim announcing {@code port}, and returns its absolute path - absolute because
     * the client libraries refuse a relative or PATH-resolved sidecar, a rule about which binary receives
     * the user's Kafka credentials that this suite has no business making an exception to.
     */
    public static Path write(Path directory, String name, int port) {
        try {
            Files.createDirectories(directory);
            var script = directory.resolve(name + "-sidecar.sh").toAbsolutePath();
            Files.writeString(script, """
                    #!/bin/sh
                    # Written by the conformance suite (bz.stub.parallelconsumer.conformance.SidecarShim).
                    # The engine is in the suite's own JVM; this announces where, then holds stdin as the
                    # parent-death lifeline the client library reaps by closing.
                    printf 'port: %d\\n'
                    while read -r _ignored; do :; done
                    exit 0
                    """.formatted(port));
            Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwxr-x---"));
            return script;
        } catch (IOException e) {
            throw new UncheckedIOException("writing the sidecar shim for port " + port, e);
        }
    }

    private SidecarShim() {
    }
}
