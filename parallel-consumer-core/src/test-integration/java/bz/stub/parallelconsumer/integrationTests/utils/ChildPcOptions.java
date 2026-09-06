package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.navigator.ResourceContract;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The argument contract between {@link ChildPcProcess} (the launcher, in the failsafe JVM) and
 * {@link ChildPcMain} (the entry point, in the child JVM): one value type, serialised by {@link #toArgs()} and
 * parsed back by {@link #parse(String[])}, so the two sides cannot drift apart - a flag the launcher writes and
 * the child does not read, or vice versa, is a parse failure here rather than a silently ignored option.
 * <p>
 * Wire form: {@code --key=value} pairs; lists are comma-separated; a contract is
 * {@code name:ratePerSecond:quantumSeconds:burst}. Unknown keys and missing required keys are refused loudly.
 * <p>
 * The child runs ONE {@code ParallelEoSStreamProcessor} under the default {@code PARTITION_SHARE} allocation
 * strategy - the strategy this harness exists to prove across JVMs; {@code IN_PROCESS} has no meaning across a
 * process boundary, so the contract does not offer it. {@link #assignor} is the consumer group's
 * partition-assignment protocol (the "both protocols" of the churn ladder), not the allocation strategy.
 *
 * @author Antony Stubbs
 * @see ChildPcMain
 * @see ChildPcProcess
 */
@Value
@Builder(toBuilder = true)
public class ChildPcOptions {

    /** The broker's floor for {@code session.timeout.ms} ({@code group.min.session.timeout.ms} default). */
    public static final int BROKER_MIN_SESSION_TIMEOUT_MS = 6_000;

    /** Heartbeat to pair with the floor session timeout - a third of it, the client's recommended ratio. */
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 2_000;

    /** The consumer group's partition-assignment protocol: eager range, or cooperative sticky. */
    public enum Assignor {
        RANGE("org.apache.kafka.clients.consumer.RangeAssignor"),
        COOPERATIVE_STICKY("org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        private final String className;

        Assignor(String className) {
            this.className = className;
        }

        /** The value for {@code partition.assignment.strategy}. */
        public String className() {
            return className;
        }
    }

    // --- required ---

    String bootstrapServers;
    String groupId;
    /** Names the child everywhere: the consumer's {@code client.id}, the pc instance tag, the output record key. */
    String instanceId;
    @Singular
    List<String> inputTopics;
    /** One record per dispatch lands here; created by the parent with log-append time (KTD8). */
    String outputTopic;
    /** The end-of-run conservation ledger record lands here (the fleet-conservation decision). */
    String ledgerTopic;

    // --- navigator ---

    @Singular
    List<String> resourceTags;
    @Singular
    List<ResourceContract> contracts;

    // --- consumer ---

    @Builder.Default
    Assignor assignor = Assignor.RANGE;
    @Builder.Default
    int sessionTimeoutMs = BROKER_MIN_SESSION_TIMEOUT_MS;
    @Builder.Default
    int heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

    // --- clock (KTD9) ---

    /** Added to the child's module clock - the skew injection; zero means the real clock. */
    @Builder.Default
    long clockOffsetMillis = 0;

    // --- lifetime ---

    /** Self-terminate after this many seconds (emitting the ledger); zero means run until told to stop. */
    @Builder.Default
    int runSeconds = 0;

    // --- harness self-test knobs (never set by a lane) ---

    /** Throw from {@code main} before subscribing - proves an early exit is reported as such. */
    @Builder.Default
    boolean failBeforeSubscribe = false;
    /** Print this many lines to stdout before the first poll - proves the pipe pumps never stall or drop. */
    @Builder.Default
    int spamStdoutLines = 0;

    private static final String CONTRACT_SEPARATOR = ":";

    /** The wire form {@link ChildPcMain} parses back. */
    public List<String> toArgs() {
        List<String> args = new ArrayList<>();
        args.add(flag("bootstrap-servers", bootstrapServers));
        args.add(flag("group-id", groupId));
        args.add(flag("instance-id", instanceId));
        args.add(flag("input-topics", String.join(",", inputTopics)));
        args.add(flag("output-topic", outputTopic));
        args.add(flag("ledger-topic", ledgerTopic));
        args.add(flag("resource-tags", String.join(",", resourceTags)));
        List<String> contractForms = new ArrayList<>();
        for (ResourceContract contract : contracts) {
            contractForms.add(contract.getName() + CONTRACT_SEPARATOR + contract.getRatePerSecond()
                    + CONTRACT_SEPARATOR + contract.getQuantum().getSeconds() + CONTRACT_SEPARATOR
                    + contract.getBurst());
        }
        args.add(flag("contracts", String.join(",", contractForms)));
        args.add(flag("assignor", assignor.name()));
        args.add(flag("session-timeout-ms", Integer.toString(sessionTimeoutMs)));
        args.add(flag("heartbeat-interval-ms", Integer.toString(heartbeatIntervalMs)));
        args.add(flag("clock-offset-millis", Long.toString(clockOffsetMillis)));
        args.add(flag("run-seconds", Integer.toString(runSeconds)));
        args.add(flag("fail-before-subscribe", Boolean.toString(failBeforeSubscribe)));
        args.add(flag("spam-stdout-lines", Integer.toString(spamStdoutLines)));
        return args;
    }

    private static String flag(String key, String value) {
        return "--" + key + "=" + value;
    }

    /** The inverse of {@link #toArgs()}; every key is required so a drifted launcher fails here, not later. */
    public static ChildPcOptions parse(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--") || !arg.contains("=")) {
                throw new IllegalArgumentException("ChildPcOptions: expected --key=value, got '" + arg + "'");
            }
            int eq = arg.indexOf('=');
            values.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        ChildPcOptionsBuilder builder = ChildPcOptions.builder()
                .bootstrapServers(required(values, "bootstrap-servers"))
                .groupId(required(values, "group-id"))
                .instanceId(required(values, "instance-id"))
                .inputTopics(list(required(values, "input-topics")))
                .outputTopic(required(values, "output-topic"))
                .ledgerTopic(required(values, "ledger-topic"))
                .resourceTags(list(required(values, "resource-tags")))
                .assignor(Assignor.valueOf(required(values, "assignor").toUpperCase(Locale.ROOT)))
                .sessionTimeoutMs(Integer.parseInt(required(values, "session-timeout-ms")))
                .heartbeatIntervalMs(Integer.parseInt(required(values, "heartbeat-interval-ms")))
                .clockOffsetMillis(Long.parseLong(required(values, "clock-offset-millis")))
                .runSeconds(Integer.parseInt(required(values, "run-seconds")))
                .failBeforeSubscribe(Boolean.parseBoolean(required(values, "fail-before-subscribe")))
                .spamStdoutLines(Integer.parseInt(required(values, "spam-stdout-lines")));
        for (String form : list(required(values, "contracts"))) {
            String[] parts = form.split(CONTRACT_SEPARATOR, -1);
            if (parts.length != 4) {
                throw new IllegalArgumentException("ChildPcOptions: a contract is name:rate:quantumSeconds:burst, "
                        + "got '" + form + "'");
            }
            builder.contract(new ResourceContract(parts[0], Double.parseDouble(parts[1]),
                    Integer.parseInt(parts[3]), Duration.ofSeconds(Long.parseLong(parts[2]))));
        }
        values.keySet().removeAll(KNOWN_KEYS);
        if (!values.isEmpty()) {
            throw new IllegalArgumentException("ChildPcOptions: unknown keys " + values.keySet()
                    + " - the launcher and the child have drifted apart");
        }
        if (builder.build().inputTopics.isEmpty()) {
            throw new IllegalArgumentException("ChildPcOptions: at least one input topic is required");
        }
        return builder.build();
    }

    private static final List<String> KNOWN_KEYS = Arrays.asList("bootstrap-servers", "group-id", "instance-id",
            "input-topics", "output-topic", "ledger-topic", "resource-tags", "contracts", "assignor",
            "session-timeout-ms", "heartbeat-interval-ms", "clock-offset-millis", "run-seconds",
            "fail-before-subscribe", "spam-stdout-lines");

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException("ChildPcOptions: missing --" + key);
        }
        return value;
    }

    private static List<String> list(String commaSeparated) {
        List<String> items = new ArrayList<>();
        for (String item : commaSeparated.split(",", -1)) {
            if (!item.isEmpty()) {
                items.add(item);
            }
        }
        return items;
    }

    /** The contract registered under {@code name}, for the child's own quantum arithmetic. */
    public ResourceContract contractNamed(String name) {
        for (ResourceContract contract : contracts) {
            if (contract.getName().equals(name)) {
                return contract;
            }
        }
        throw new IllegalArgumentException("no contract named '" + name + "' among " + contracts);
    }
}
