// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines.demo

/**
 * The demo's dials, and the interface every per-language demo mirrors.
 *
 * The surface - the seven flags, their defaults, their `PC_DEMO_*` environment variables and the
 * precedence between them - is the shared contract in `parallel-consumer-proxy/demo/README.md`.
 * Nothing here is a Kotlin choice: flags beat the environment beats the defaults, because a
 * container passes configuration by environment and a person at a terminal passes flags, and each
 * has to be able to override the other.
 *
 * **R39 does not govern a demo.** R39 constrains how configuration reaches the *proxy*; a demo is
 * an application, so `--records` is not a breach of it. Said here because without it the flags read
 * as a violation of the plan's own rule.
 */
internal class DemoOptions private constructor(
    val records: Int,
    val delayMs: Int,
    val maxConcurrency: Int,
    val partitions: Int,
    val replayFactor: Int,
    /** An address the caller supplied; `null` means "start a broker". */
    val bootstrap: String?,
    /** A topic the caller supplied; `null` means the demo names its own. */
    val topic: String?,
) {

    /** The records the big replay consumes in total, the small replay's own included. */
    val bigReplayRecords: Int get() = records * maxOf(1, replayFactor)

    /** Whether the big replay is worth running; a factor of 1 or less skips it. */
    val bigReplayWanted: Boolean get() = replayFactor > 1

    /**
     * The effective configuration, printed before the run.
     *
     * A number without its settings is not reproducible, so this is part of the contract rather
     * than a debugging aid. **The bootstrap address is deliberately absent**: own-cluster mode puts
     * a user's real broker there, and nothing in this demo logs or echoes it.
     */
    override fun toString(): String =
        "records = $records" +
            "\n  delayMs = $delayMs" +
            "\n  maxConcurrency = $maxConcurrency" +
            "\n  partitions = $partitions" +
            "\n  replayFactor = $replayFactor"

    companion object {

        /** The prefix on every environment variable this demo reads, so a reader greps one string. */
        const val ENV_PREFIX: String = "PC_DEMO_"

        /**
         * Whether the caller asked for the usage text rather than a run.
         *
         * Handled here and not only in `run.sh`, because the script is not the only way in:
         * `docker compose run demo --help` reaches this main directly, and answering that with
         * "unknown option: --help" would be a poor first impression.
         */
        fun isHelpRequested(args: Array<String>): Boolean = args.any { it == "-h" || it == "--help" }

        /**
         * Parses the command line, falling back to the environment and then to the defaults.
         *
         * @param args the process arguments, which may legitimately be empty - that is the
         *   double-click case, and it must work
         * @param env the environment to read, passed in rather than read from [System] so this is
         *   testable without mutating the process environment
         * @throws IllegalArgumentException on an unknown flag, a missing value, or a value that is
         *   not a number in range - a demo that silently ignores a misspelled flag reports numbers
         *   for settings nobody asked for
         */
        @Suppress("CyclomaticComplexMethod") // one branch per flag; splitting it would only hide the table
        fun parse(args: Array<String>, env: Map<String, String>): DemoOptions {
            var records = envInt(env, "RECORDS", DEFAULT_RECORDS, atLeast = 1)
            var delayMs = envInt(env, "DELAY_MS", DEFAULT_DELAY_MS, atLeast = 0)
            var maxConcurrency = envInt(env, "CONCURRENCY", DEFAULT_CONCURRENCY, atLeast = 1)
            var partitions = envInt(env, "PARTITIONS", DEFAULT_PARTITIONS, atLeast = 1)
            // 0 and 1 both mean "skip the big replay", so this one is allowed to be zero
            var replayFactor = envInt(env, "REPLAY_FACTOR", DEFAULT_REPLAY_FACTOR, atLeast = 0)
            var bootstrap = envText(env, "BOOTSTRAP")
            var topic = envText(env, "TOPIC")

            var i = 0
            while (i < args.size) {
                val flag = args[i]
                when (flag) {
                    "--records" -> records = number(flag, value(args, ++i, flag), atLeast = 1)
                    "--delay-ms" -> delayMs = number(flag, value(args, ++i, flag), atLeast = 0)
                    "--concurrency" -> maxConcurrency = number(flag, value(args, ++i, flag), atLeast = 1)
                    "--partitions" -> partitions = number(flag, value(args, ++i, flag), atLeast = 1)
                    "--replay-factor" -> replayFactor = number(flag, value(args, ++i, flag), atLeast = 0)
                    "--bootstrap" -> bootstrap = value(args, ++i, flag)
                    "--topic" -> topic = value(args, ++i, flag)
                    else -> throw IllegalArgumentException("unknown option: $flag")
                }
                i++
            }

            // Checked as a Long rather than trusted as an Int later: records * replayFactor
            // overflows silently, and a wrapped value turns the big replay into a tiny one that
            // still prints a confident throughput figure.
            val bigReplay = records.toLong() * maxOf(1, replayFactor)
            require(bigReplay <= Int.MAX_VALUE) {
                "--records times --replay-factor is $bigReplay, which is more records than the demo can " +
                    "count; lower one of them"
            }
            return DemoOptions(records, delayMs, maxConcurrency, partitions, replayFactor, bootstrap, topic)
        }

        private const val DEFAULT_RECORDS = 2_000
        private const val DEFAULT_DELAY_MS = 2
        private const val DEFAULT_CONCURRENCY = 100
        private const val DEFAULT_PARTITIONS = 10
        private const val DEFAULT_REPLAY_FACTOR = 20

        private fun value(args: Array<String>, index: Int, flag: String): String {
            require(index < args.size) { "$flag needs a value" }
            return args[index]
        }

        private fun envText(env: Map<String, String>, suffix: String): String? =
            env[ENV_PREFIX + suffix]?.trim()?.takeIf { it.isNotEmpty() }

        private fun envInt(env: Map<String, String>, suffix: String, fallback: Int, atLeast: Int): Int =
            envText(env, suffix)?.let { number(ENV_PREFIX + suffix, it, atLeast) } ?: fallback

        private fun number(name: String, raw: String, atLeast: Int): Int {
            val parsed = raw.trim().toIntOrNull()
                ?: throw IllegalArgumentException("$name needs a whole number, got '$raw'")
            require(parsed >= atLeast) { "$name must be at least $atLeast, got $parsed" }
            return parsed
        }
    }
}
