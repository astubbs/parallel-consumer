// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.time.Duration

private val log = LoggerFactory.getLogger("bz.stub.parallelconsumer.client.coroutines.Sidecar")

/** How many stderr lines are kept for diagnostics when a spawn goes wrong. */
private const val STDERR_TAIL_LINES = 30

/**
 * Where the sidecar binary is and how to invoke it - supplied by the application, always.
 *
 * The library never resolves the binary through `PATH`, a relative lookup, or any directory an
 * attacker could influence: this process hands that binary the user's Kafka credentials, so which
 * binary runs is security-relevant. An absolute path is therefore a precondition, not a preference.
 *
 * [arguments] are not configuration. Configuration is code and travels in `Configure`; these are
 * for the harness's own flags, and nothing credential-bearing may appear among them.
 */
public class SidecarCommand(
    public val executable: Path,
    public val arguments: List<String> = emptyList(),
) {
    init {
        require(executable.isAbsolute) {
            "the sidecar must be named by an absolute path, got '$executable' - this process hands it your " +
                "Kafka credentials, so it is never resolved through PATH or a relative lookup"
        }
    }

    override fun toString(): String = "SidecarCommand($executable, ${arguments.size} argument(s))"
}

/**
 * A running sidecar process and the loopback port it reported.
 *
 * Three lifecycle rules from the specification are implemented here and nowhere else: the process
 * is launched **directly, never through a shell** (a shell wrapper holds the stdin write end and
 * defeats the proxy's parent-death signal, leaking a JVM that still holds group membership); its
 * stdin pipe is held open and never written to, because EOF on it is what tells the proxy its
 * parent is gone; and closing that pipe is how the process is reaped.
 */
internal class Sidecar private constructor(
    private val process: Process,
    val port: Int,
    private val stderrTail: ArrayDeque<String>,
) {

    /** The last few stderr lines, for an error message that names its own cause. */
    fun diagnostics(): String = synchronized(stderrTail) { stderrTail.joinToString(separator = "\n") }

    /**
     * Closes the lifecycle pipe and waits for the process to go. EOF on stdin is the parent-death
     * signal, so this is the reap: no kill, no signal, nothing the proxy has to interpret.
     */
    fun reap(grace: Duration) {
        try {
            process.outputStream.close()
        } catch (ignored: IOException) {
            log.debug("The sidecar's stdin was already closed", ignored)
        }
        if (!process.waitFor(grace.inWholeMilliseconds, TimeUnit.MILLISECONDS)) {
            log.warn("The sidecar did not exit within {} of its stdin closing; forcing it", grace)
            process.destroyForcibly()
        }
    }

    companion object {

        /** `port: <n>` - the line the proxy prints when it has bound. */
        private val PORT_LINE = Regex("""^port:\s*(\d+)$""")

        /**
         * Launches the sidecar and waits for its port line.
         *
         * The line is *scanned for* rather than read as the first line: the specification says
         * first, and the shipped sidecar honours that, but the test-mode harness logs before it -
         * a harness limitation every language's test absorbs. Scanning satisfies both, and this
         * client never depends on the difference.
         */
        suspend fun spawn(command: SidecarCommand, budget: Duration): Sidecar {
            val process = ProcessBuilder(listOf(command.executable.toString()) + command.arguments)
                // no shell, and stdin stays an open pipe we never write to: its EOF is the
                // parent-death signal the proxy watches
                .redirectErrorStream(false)
                .start()

            val tail = ArrayDeque<String>()
            drainToTail(process.errorStream.bufferedReader(), tail, "pc-sidecar-stderr")
            val stdout = Channel<String>(Channel.UNLIMITED)
            drainToChannel(process.inputStream.bufferedReader(), stdout, "pc-sidecar-stdout")

            val port = withTimeoutOrNull(budget) { awaitPortLine(stdout) }
            if (port == null) {
                process.destroyForcibly()
                error(
                    "the sidecar did not report 'port: <n>' within $budget. Its last output was:\n" +
                        tail.joinToString(separator = "\n")
                )
            }
            log.info("Sidecar listening on loopback port {}", port)
            return Sidecar(process, port, tail)
        }

        private suspend fun awaitPortLine(stdout: Channel<String>): Int {
            for (line in stdout) {
                val match = PORT_LINE.find(line.trim())
                if (match != null) {
                    return match.groupValues[1].toInt()
                }
                log.debug("sidecar: {}", line)
            }
            error("the sidecar's stdout ended before it reported a port - the process died")
        }

        /**
         * Both drains run for the process's whole life, not just until the port arrives: a pipe
         * nobody reads fills up and blocks the writer, which would hang the sidecar mid-session
         * rather than at a moment anyone would connect to this cause.
         */
        private fun drainToChannel(reader: java.io.BufferedReader, sink: Channel<String>, name: String) {
            thread(isDaemon = true, name = name) {
                reader.use { it.forEachLine { line -> sink.trySend(line) } }
                sink.close()
            }
        }

        private fun drainToTail(reader: java.io.BufferedReader, tail: ArrayDeque<String>, name: String) {
            thread(isDaemon = true, name = name) {
                reader.use {
                    it.forEachLine { line ->
                        synchronized(tail) {
                            tail.addLast(line)
                            if (tail.size > STDERR_TAIL_LINES) {
                                tail.removeFirst()
                            }
                        }
                    }
                }
            }
        }
    }
}
