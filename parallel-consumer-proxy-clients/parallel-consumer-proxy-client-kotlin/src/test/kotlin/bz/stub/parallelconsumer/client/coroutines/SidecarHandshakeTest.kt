// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import com.google.common.truth.Truth.assertWithMessage
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.net.InetAddress
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions

/**
 * **The Kotlin client's handshake, against the real sidecar rather than a stand-in.**
 *
 * The sidecar spawned is `parallel-consumer-proxy`'s `NoEngineMain` - the production lifecycle with
 * the engine supplier swapped - launched as an ordinary
 * child process - so this exercises the whole lifecycle contract the specification describes and not
 * an in-process shortcut: launch directly, hold the stdin pipe, find the port line, connect to
 * loopback, put `Configure` on the wire, and turn what came back into a Kotlin failure.
 *
 * **What it can prove on this branch, and what it deliberately cannot.** The sidecar here hosts no
 * Parallel Consumer engine and answers every session `UNIMPLEMENTED`, so there is no dispatch to
 * observe and none is faked. The dispatch scenarios - one record end to end, the in-flight ceiling,
 * the redelivery history - belong to the shared conformance suite and are deferred until an engine
 * exists to run them against.
 *
 * **It runs only in the harness lane** (`-Dpc.foreignClients`, which this module's CI row passes),
 * because the classpath it needs is what a permanent Maven edge to the sidecar module would cost -
 * see the `kotlin-e2e-harness` profile. When the classpath file is missing it FAILS and names
 * the command; a test that quietly does not run is not a passing test.
 *
 * **The status code is the assertion, not merely "it failed".** A refusal from the authority
 * allowlist is `PERMISSION_DENIED` and one from the admission slot is `RESOURCE_EXHAUSTED`, both
 * raised by interceptors *before* the service method runs. Only `UNIMPLEMENTED` can have come from
 * the service itself, so the code is what separates "the connection was turned away" from "the
 * handshake was delivered and answered".
 */
class SidecarHandshakeTest {

    @Test
    fun `the handshake reaches the session service and its refusal reaches the caller`() = runBlocking {
        val refused = assertThrows<Throwable> {
            ParallelConsumerClient.open(
                // kafka_properties is empty deliberately: this sidecar reads none, and real
                // credentials never belong in a test
                options = ClientOptions(topics = listOf("handshake-topic")),
                sidecar = engineLessSidecar(),
            )
        }

        val status = statusOf(refused)
        assertWithMessage(
            "UNIMPLEMENTED is the only code the session SERVICE raises - the allowlist answers " +
                "PERMISSION_DENIED and the admission slot RESOURCE_EXHAUSTED, both before the " +
                "service method runs, so this code is what proves the handshake was delivered",
        ).that(status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
        assertWithMessage("the refusal must name what is missing, or a client author debugs their own code")
            .that(status.description).contains(NO_ENGINE_DESCRIPTION)
    }

    /**
     * The control arm, and it is permanent rather than a one-off demonstration: pointed at a port
     * nothing is listening on, the same client fails in a way that is not the refusal above. Without
     * it, the test that matters could be passing on any failure at all - which is the shape of an
     * assertion that cannot fail for the reason it names.
     */
    @Test
    fun `a sidecar that is not listening fails differently from one that refuses`() = runBlocking {
        val failed = assertThrows<Throwable> {
            ParallelConsumerClient.open(
                options = ClientOptions(topics = listOf("handshake-topic")),
                sidecar = SidecarCommand(writeAnnouncer(reserveThenReleaseAPort()), emptyList()),
            )
        }

        assertWithMessage("nothing answered, so nothing can have refused: $failed")
            .that(statusOrNull(failed)?.code).isNotEqualTo(Status.Code.UNIMPLEMENTED)
    }

    /**
     * The sidecar command. `Main` is a classpath invocation, so "the sidecar binary" here is the JVM
     * launcher and the classpath is an argument - everything awkward about that lives in this one
     * function.
     *
     * NO ARGUMENTS beyond the classpath: the sidecar takes none and refuses to start when given one,
     * because everything is configured connect-time over the protocol.
     */
    private fun engineLessSidecar(): SidecarCommand {
        val classpathFile = Path.of("target", "sidecar-classpath.txt").toAbsolutePath()
        check(Files.isRegularFile(classpathFile)) {
            "$classpathFile is missing - it is written by the kotlin-e2e-harness profile: run " +
                "`./mvnw --batch-mode test -pl :parallel-consumer-proxy-client-kotlin -Dpc.foreignClients`"
        }
        val classpath = Files.readString(classpathFile).trim()
        check(classpath.isNotEmpty()) { "$classpathFile is empty" }

        // PATH lookup would be wrong in the library and is unnecessary here: this is the JVM the
        // test itself is running on
        val java = Path.of(System.getProperty("java.home"), "bin", "java")
        return SidecarCommand(java, listOf("-cp", classpath, SIDECAR_MAIN))
    }

    /** A loopback port the OS has just handed out and nothing is listening on. */
    private fun reserveThenReleaseAPort(): Int =
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }

    /**
     * A sidecar that announces a port and then holds its stdin, which is the spawning contract's
     * whole client-visible surface - so the control arm drives the library's REAL connect path at a
     * dead port rather than the different path a child that printed nothing would take. `printf` and
     * `read` are shell builtins, so it is one process holding its own lifeline.
     */
    private fun writeAnnouncer(port: Int): Path {
        val script = Files.createTempDirectory("pc-kotlin-announcer").resolve("announcer.sh")
        Files.writeString(
            script,
            """
            #!/bin/sh
            printf 'port: $port\n'
            while read -r _ignored; do :; done
            exit 0
            """.trimIndent() + "\n",
        )
        Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"))
        return script
    }

    /**
     * The gRPC status behind a failure. Asserted to be present rather than tolerated as absent: a
     * non-gRPC failure here would mean the transport never reached the wire, which is a different
     * finding from either outcome these tests distinguish and must not be reported as one of them.
     */
    private fun statusOf(thrown: Throwable): Status =
        statusOrNull(thrown) ?: throw AssertionError("no gRPC status in the failure chain, so the " +
            "transport never reached the wire", thrown)

    private fun statusOrNull(thrown: Throwable): Status? {
        var cause: Throwable? = thrown
        while (cause != null) {
            if (cause is StatusRuntimeException) return cause.status
            cause = cause.cause
        }
        return null
    }

    private companion object {
        const val SIDECAR_MAIN = "bz.stub.parallelconsumer.proxy.NoEngineMain"

        /** What the sidecar's refusal must name, so a client author does not debug their own code. */
        const val NO_ENGINE_DESCRIPTION = "hosts no Parallel Consumer engine"
    }
}
