package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.BURST;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.CONVERGENCE_DEADLINE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.QUANTUM;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RATE_PER_SECOND;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RATE_TOLERANCE_PERCENT;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.REBALANCE_ALLOWANCE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.SESSION_TIMEOUT;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.WINDOW;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.WINDOW_QUANTA;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.overshootBound;

/**
 * The churn ladder's published curve (the partition-share plan's KTD11, R12): one markdown record, dated,
 * written by the RUN from what it observed - never by hand - into the module's build directory beside the
 * failsafe report, so the workflow uploads the two together and a maintainer commits the download under
 * {@code docs/test-hardening/} with the workflow run URL filled into the placeholder line.
 * <p>
 * Every number in the table is the run's output. The header names the commit under test (read from
 * {@code git rev-parse HEAD} at run time, or says why it could not be), the machine (a calibration is a claim
 * about one machine), the envelope the rungs were judged against, and the rule that turns the observations
 * into the bound a user reads - so the record stands alone, the way a dated audit does.
 *
 * @author Antony Stubbs
 * @see NavigatorProofEnvelope
 */
@Slf4j
public final class NavigatorLadderRecord {

    /** The directory under the module's {@code target/} the record is written to; the workflow globs it. */
    public static final String DIRECTORY = "navigator-ladder";

    /** The record's file name prefix; the date follows, then {@code .md}. */
    public static final String FILE_PREFIX = "navigator-overshoot-ladder-";

    /** The line a maintainer replaces with the workflow run URL when committing the download. */
    public static final String WORKFLOW_RUN_PLACEHOLDER = "Workflow run: <filled in at download>";

    /** One transition inside a rung: a join or a kill, and what it moved. */
    @Value
    public static class MoveObservation {
        /** {@code join} or {@code kill}. */
        String kind;
        /** Partitions whose holder changed across the transition. */
        int partitionsMoved;
        /** Of those, the ones that moved between two holders whose injected clock offsets differ (KD5). */
        int skewedPartitionsMoved;
        /** The summed per-quantum share of the skewed partitions, in credits. */
        double skewedShare;
        /** The skew term this move contributes: the skewed share rounded up to a whole credit, at least one. */
        long term;
        /** From the transition's broker-time anchor until the group was observed stable again (reported). */
        Duration anchorToStable;
        /** Expected firings over the convergence deadline minus observed - the undershoot the transition cost. */
        double undershoot;
    }

    /**
     * Everything one rung observed, filled in as the rung proceeds so a failed rung still records the half it
     * reached: what is known at the start is fixed at construction, what the rung measures arrives through the
     * recording methods, and what a failed rung never reached stays absent rather than zero.
     */
    @Getter
    public static final class RungObservation {
        private final int rungNumber;
        private final String label;
        private final int members;
        private final String assignor;
        private final String offsetsMillis;
        /** The largest aligned-window count, or -1 while unmeasured. */
        private long maxWindowCount = -1;
        private Optional<Instant> maxWindowStart = Optional.empty();
        private int windowsScanned;
        private double strictBound;
        private long skewTerm;
        private double bound;
        private final List<MoveObservation> moves = new ArrayList<>();
        private Optional<Duration> killMemberGone = Optional.empty();
        private Optional<NavigatorProofEnvelope.FleetIdentity> fleetIdentity = Optional.empty();
        private Duration wallTime = Duration.ZERO;
        private String verdict = "did not complete";

        public RungObservation(int rungNumber, String label, int members, String assignor, String offsetsMillis) {
            this.rungNumber = rungNumber;
            this.label = label;
            this.members = members;
            this.assignor = assignor;
            this.offsetsMillis = offsetsMillis;
        }

        public void recordWindow(long maxWindowCount, Instant maxWindowStart, int windowsScanned) {
            this.maxWindowCount = maxWindowCount;
            this.maxWindowStart = Optional.of(maxWindowStart);
            this.windowsScanned = windowsScanned;
        }

        public void recordBound(double strictBound, long skewTerm) {
            this.strictBound = strictBound;
            this.skewTerm = skewTerm;
            this.bound = strictBound + skewTerm;
        }

        public void addMove(MoveObservation move) {
            moves.add(move);
        }

        public void recordKillMemberGone(Duration killMemberGone) {
            this.killMemberGone = Optional.of(killMemberGone);
        }

        public void recordFleetIdentity(NavigatorProofEnvelope.FleetIdentity fleetIdentity) {
            this.fleetIdentity = Optional.of(fleetIdentity);
        }

        public void recordWallTime(Duration wallTime) {
            this.wallTime = wallTime;
        }

        public void verdict(String verdict) {
            this.verdict = verdict;
        }

        private Optional<MoveObservation> move(String kind) {
            for (MoveObservation move : moves) {
                if (move.getKind().equals(kind)) {
                    return Optional.of(move);
                }
            }
            return Optional.empty();
        }
    }

    private final Path basedir;
    private final int plannedRungs;
    private final List<RungObservation> rungs = new ArrayList<>();
    private final Instant startedAt = Instant.now();

    public NavigatorLadderRecord(Path basedir, int plannedRungs) {
        this.basedir = basedir;
        this.plannedRungs = plannedRungs;
    }

    public void add(RungObservation rung) {
        rungs.add(rung);
    }

    /** The rungs recorded so far, in ladder order. */
    public List<RungObservation> rungs() {
        return new ArrayList<>(rungs);
    }

    /** Writes the record under {@code target/navigator-ladder/} and returns its path. */
    public Path write() {
        Path directory = basedir.resolve("target").resolve(DIRECTORY);
        Path file = directory.resolve(FILE_PREFIX + LocalDate.now(ZoneOffset.UTC) + ".md");
        try {
            Files.createDirectories(directory);
            Files.write(file, render().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("could not write the navigator ladder record to " + file, e);
        }
        log.info("navigator ladder record written to {}", file);
        return file;
    }

    /** The record as markdown. */
    public String render() {
        StringBuilder text = new StringBuilder();
        line(text, "# Navigator overshoot ladder - " + LocalDate.now(ZoneOffset.UTC));
        line(text, "");
        line(text, "Commit under test: " + commitUnderTest(basedir));
        line(text, WORKFLOW_RUN_PLACEHOLDER);
        line(text, "Machine: " + machineDescription());
        line(text, "Run started: " + startedAt + ", record written: " + Instant.now());
        line(text, "");
        line(text, "Written by `NavigatorChurnLadderIT` from its own observations. Every number below is the run's "
                + "output; the only hand step is the workflow run URL above (the plan's U6, KTD11).");
        line(text, "");
        line(text, "## Envelope (NavigatorProofEnvelope)");
        line(text, "");
        line(text, "- Contract: " + fmt(RATE_PER_SECOND) + " credits/s, quantum " + QUANTUM.toMillis() + " ms, burst "
                + BURST);
        line(text, "- Window: " + WINDOW_QUANTA + " quanta (" + WINDOW.getSeconds() + " s), quantum-aligned on the "
                + "broker's clock, slid one quantum at a time across each rung's span");
        line(text, "- Rate tolerance: " + RATE_TOLERANCE_PERCENT + "% (informational here - the ladder gates on the "
                + "bound, not on rates)");
        line(text, "- Session timeout " + SESSION_TIMEOUT.toMillis() + " ms, rebalance allowance "
                + REBALANCE_ALLOWANCE.toMillis() + " ms, convergence deadline " + CONVERGENCE_DEADLINE.toMillis()
                + " ms");
        line(text, "");
        line(text, "## The rule (KTD11, and the settled defect rule)");
        line(text, "");
        line(text, "- Strict bound, every rung: rate x window + burst + one quantum's credits = "
                + fmt(overshootBound(WINDOW)) + " firings in any aligned window. On a zero-offset rung any crossing "
                + "is a defect; no re-derivation is permitted.");
        line(text, "- Skew term, offset rungs only: for each transition, the summed per-quantum share of the "
                + "partitions that moved between two holders whose clocks disagree about the quantum index, "
                + "rounded up to a whole credit and to at least one; summed over the rung's transitions. It may be "
                + "re-derived only by a derivation written into this record before the re-run that tests it.");
        line(text, "- Undershoot per transition: expected firings over the convergence deadline (rate x deadline) "
                + "minus observed; reported, never gated (KTD13).");
        line(text, "- Fleet identity (R10, AE7): minted + overdraft of the children that stopped gracefully, "
                + "against their summed EXACT per-index entitlement (what each index's read mints, sampled per "
                + "index on the child's own clock, with a closing sample after the processor stops) plus one "
                + "credit per tagged child for a sampler pass starved past a whole index. The killed child has "
                + "no record, by definition.");
        line(text, "");
        line(text, "## Rungs");
        line(text, "");
        line(text, rungCountLine());
        line(text, "");
        line(text, "| rung | N | assignor | offsets (ms) | max window | strict bound | skew term | bound | join: moved "
                + "(skewed, share) | kill: moved (skewed, share) | join to stable | kill: gone / stable | join "
                + "undershoot | kill undershoot | fleet minted+overdraft / ceiling | wall | verdict |");
        line(text, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
        for (RungObservation rung : rungs) {
            line(text, row(rung));
        }
        line(text, "");
        line(text, "Columns: `max window` is the largest aggregate firing count over any quantum-aligned window of "
                + WINDOW_QUANTA + " quanta inside the rung's span (from the barrier that opened the rung to one "
                + "quantum past the kill's convergence deadline); `moved` counts partitions whose holder changed; "
                + "`skewed` those that moved between holders with different injected offsets, with their summed "
                + "per-quantum share in credits; latencies are wall-clock from the transition to the admin client "
                + "reporting the group stable (reported, never gated).");
        return text.toString();
    }

    private String rungCountLine() {
        int largestN = 0;
        int inside = 0;
        for (RungObservation rung : rungs) {
            largestN = Math.max(largestN, rung.getMembers());
            if (rung.getVerdict().startsWith("inside")) {
                inside++;
            }
        }
        String why = rungs.size() == plannedRungs
                ? "every planned rung ran on this machine - the cap is the plan's largest N, not the machine's"
                : "the ladder stopped short - read the last rung's verdict and the failsafe report for why";
        return "Rung count reached: " + rungs.size() + " of " + plannedRungs + " planned, largest N " + largestN
                + ", " + inside + " inside their bound (" + why + ").";
    }

    private static String row(RungObservation rung) {
        Optional<MoveObservation> join = rung.move("join");
        Optional<MoveObservation> kill = rung.move("kill");
        return "| " + rung.getRungNumber()
                + " | " + rung.getMembers()
                + " | " + rung.getAssignor()
                + " | " + rung.getOffsetsMillis()
                + " | " + (rung.getMaxWindowCount() < 0 ? "-" : rung.getMaxWindowCount() + rung.getMaxWindowStart()
                        .map(start -> " (from " + start + ", " + rung.getWindowsScanned() + " windows)").orElse(""))
                + " | " + fmt(rung.getStrictBound())
                + " | " + rung.getSkewTerm()
                + " | " + fmt(rung.getBound())
                + " | " + join.map(NavigatorLadderRecord::moved).orElse("-")
                + " | " + kill.map(NavigatorLadderRecord::moved).orElse("-")
                + " | " + join.map(move -> millis(move.getAnchorToStable())).orElse("-")
                + " | " + rung.getKillMemberGone().map(NavigatorLadderRecord::millis).orElse("-") + " / "
                        + kill.map(move -> millis(move.getAnchorToStable())).orElse("-")
                + " | " + join.map(move -> fmt(move.getUndershoot())).orElse("-")
                + " | " + kill.map(move -> fmt(move.getUndershoot())).orElse("-")
                + " | " + rung.getFleetIdentity().map(identity -> (identity.getMinted() + identity.getOverdraft())
                        + " / " + identity.getCeiling() + " (shares " + fmt(identity.getSharesSummed()) + ", "
                        + identity.getTaggedChildren() + " tagged)").orElse("-")
                + " | " + millis(rung.getWallTime())
                + " | " + rung.getVerdict()
                + " |";
    }

    private static String moved(MoveObservation move) {
        return move.getPartitionsMoved() + " (" + move.getSkewedPartitionsMoved() + ", " + fmt(move.getSkewedShare())
                + ")";
    }

    private static String millis(Duration duration) {
        return duration.toMillis() + " ms";
    }

    private static String fmt(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static void line(StringBuilder text, String line) {
        text.append(line).append('\n');
    }

    // ------------------------------------------------------------------
    // Provenance
    // ------------------------------------------------------------------

    /** {@code git rev-parse HEAD} in the module directory, or a line saying why it could not be read. */
    static String commitUnderTest(Path basedir) {
        try {
            Process git = new ProcessBuilder("git", "rev-parse", "HEAD").directory(basedir.toFile())
                    .redirectErrorStream(true).start();
            String output = new String(readAll(git), StandardCharsets.UTF_8).trim();
            if (!git.waitFor(10, TimeUnit.SECONDS)) {
                git.destroyForcibly();
                return "unavailable (git rev-parse did not return within 10s)";
            }
            return git.exitValue() == 0 ? output : "unavailable (git rev-parse exited " + git.exitValue() + ": "
                    + output + ")";
        } catch (IOException e) {
            return "unavailable (" + e + ")";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "unavailable (interrupted reading git rev-parse)";
        }
    }

    private static byte[] readAll(Process process) throws IOException {
        try (java.io.InputStream in = process.getInputStream();
             java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }

    /** The machine a calibration is a claim about: OS, architecture, processors visible to the JVM, JDK. */
    static String machineDescription() {
        return System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                + System.getProperty("os.arch") + ", " + Runtime.getRuntime().availableProcessors()
                + " processors visible to the parent JVM, " + System.getProperty("java.vendor") + " "
                + System.getProperty("java.version") + ", kafka-clients "
                + org.apache.kafka.common.utils.AppInfoParser.getVersion();
    }
}
