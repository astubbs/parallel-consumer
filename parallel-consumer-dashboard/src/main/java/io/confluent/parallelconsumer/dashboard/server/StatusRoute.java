package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.json.SnapshotJson;
import io.confluent.parallelconsumer.dashboard.snapshot.PcSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import lombok.Value;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The self-diagnostic page at {@code /status} (plan R41): why the dashboard is showing you nothing.
 *
 * <h2>What it is for</h2>
 * <p>
 * Every way this module fails quietly produces the same symptom - a page of blanks - and the causes are not
 * distinguishable from the page itself. No {@code MeterRegistry} was passed. The registry was passed to the dashboard
 * but not to Parallel Consumer. The loop-end callback was never registered, or the control loop has stopped running.
 * Parallel Consumer's meters are not bound yet. The static assets are not on the classpath. Each of those is a
 * different fix, and a blank page distinguishes none of them.
 *
 * <h2>Why it depends on almost nothing</h2>
 * <p>
 * The snapshot, the registry and the static assets are precisely what this page exists to diagnose, so it cannot
 * require any of them to render. It is server-rendered HTML with an inline stylesheet: no JavaScript, no fetch, no
 * asset request, no dependency on the main page's code path. A diagnostic that goes blank under the conditions it
 * diagnoses is not a diagnostic.
 *
 * <h2>Passed, failed, or not-run</h2>
 * <p>
 * A check whose precondition failed reports {@link Outcome#SKIPPED}, not a failure. Reporting "no Parallel Consumer
 * meters found" as a failure when no registry was supplied at all would put three red lines on the page and hide
 * which one is the cause - the reader would start at the wrong end. Every failure carries what to do about it.
 * <p>
 * <strong>No gauge is ever evaluated here.</strong> The registry reference is used for a null check and nothing more;
 * whether Parallel Consumer's meters are present is read from the snapshot's own flag, which was computed on the
 * control thread where gauge evaluation is legal. See {@link StateRoute} for the incident behind that rule.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public final class StatusRoute implements Handler<RoutingContext> {

    /**
     * Classpath resources the page shell needs. Absent means the module's jar was repackaged in a way that dropped
     * its resources - a shaded uber-jar with a resource filter is the usual culprit - and the dashboard will serve a
     * 404 for its own page while otherwise looking healthy.
     */
    public static final List<String> DEFAULT_REQUIRED_ASSETS = Collections.unmodifiableList(Arrays.asList(
            "web/index.html",
            "META-INF/resources/webjars/uplot/1.6.30/dist/uPlot.iife.min.js",
            "META-INF/resources/webjars/uplot/1.6.30/dist/uPlot.min.css"));

    /**
     * A sample older than this many update intervals means the control loop is not running the callback any more.
     * Floored so a sub-second update interval does not make the page cry wolf on ordinary scheduling jitter.
     */
    private static final int STALE_AFTER_INTERVALS = 5;

    private static final long MINIMUM_STALE_MILLIS = 5_000L;

    private final SnapshotPublisher publisher;

    private final MeterRegistry registry;

    private final DashboardOptions options;

    private final List<String> requiredAssets;

    private final ClassLoader classLoader;

    public StatusRoute(SnapshotPublisher publisher, MeterRegistry registry, DashboardOptions options) {
        this(publisher, registry, options, DEFAULT_REQUIRED_ASSETS, StatusRoute.class.getClassLoader());
    }

    /**
     * As above, with the asset list and loader supplied - which is how a test proves the missing-asset case without
     * having to break the build's own classpath.
     */
    public StatusRoute(SnapshotPublisher publisher,
                       MeterRegistry registry,
                       DashboardOptions options,
                       List<String> requiredAssets,
                       ClassLoader classLoader) {
        this.publisher = publisher;
        this.registry = registry;
        this.options = options;
        this.requiredAssets = requiredAssets;
        this.classLoader = classLoader;
    }

    @Override
    public void handle(RoutingContext ctx) {
        ctx.response()
                .putHeader("Content-Type", "text/html; charset=utf-8")
                .putHeader("Cache-Control", "no-store")
                .putHeader("X-Content-Type-Options", "nosniff")
                .end(renderHtml(System.currentTimeMillis()));
    }

    /**
     * Runs every check, in dependency order, and returns what each one found.
     */
    public List<Check> runChecks(long nowMillis) {
        List<Check> checks = new ArrayList<>();

        boolean publisherWired = publisher != null;
        checks.add(publisherWired
                ? Check.passed("Snapshot publisher wired",
                "The dashboard server holds a SnapshotPublisher.")
                : Check.failed("Snapshot publisher wired",
                "The dashboard server was started without a SnapshotPublisher, so nothing is ever sampled.",
                "Construct the server with SnapshotPublisher.createAndRegister(parallelConsumer, meterRegistry)."));

        if (!publisherWired) {
            checks.add(Check.skipped("Meter registry supplied", "Snapshot publisher wired"));
            checks.add(Check.skipped("Control loop has sampled at least once", "Snapshot publisher wired"));
            checks.add(Check.skipped("Most recent sample is fresh", "Snapshot publisher wired"));
            checks.add(Check.skipped("Parallel Consumer meters are present", "Snapshot publisher wired"));
            checks.add(Check.skipped("Sampling is not failing", "Snapshot publisher wired"));
            checks.add(assetCheck());
            return Collections.unmodifiableList(checks);
        }

        boolean registrySupplied = registry != null;
        checks.add(registrySupplied
                ? Check.passed("Meter registry supplied",
                "A MeterRegistry was supplied to the dashboard.")
                : Check.failed("Meter registry supplied",
                "No MeterRegistry was supplied, so every metered value on the page will be absent.",
                "Pass the same MeterRegistry to ParallelConsumerOptions.meterRegistry(..) and to the dashboard. "
                        + "It must be the same instance - a registry the consumer never writes to looks exactly "
                        + "like a consumer that is doing nothing."));

        PcSnapshot current = publisher.getSnapshots().getCurrent();
        boolean hasSampled = current != null;
        if (!registrySupplied) {
            checks.add(Check.skipped("Control loop has sampled at least once", "Meter registry supplied"));
            checks.add(Check.skipped("Most recent sample is fresh", "Meter registry supplied"));
            checks.add(Check.skipped("Parallel Consumer meters are present", "Meter registry supplied"));
        } else {
            checks.add(hasSampled
                    ? Check.passed("Control loop has sampled at least once",
                    "Sample " + current.getSampleSequence() + " was captured.")
                    : Check.failed("Control loop has sampled at least once",
                    "No snapshot has ever been published, so the loop-end callback has not run.",
                    "Call SnapshotPublisher.registerWith(parallelConsumer) - or use createAndRegister - before "
                            + "the consumer starts, and check the control loop is actually running (a consumer "
                            + "that has not been given a poll loop never reaches the end of one)."));

            if (!hasSampled) {
                checks.add(Check.skipped("Most recent sample is fresh", "Control loop has sampled at least once"));
                checks.add(Check.skipped("Parallel Consumer meters are present",
                        "Control loop has sampled at least once"));
            } else {
                long ageMillis = current.ageMillis(nowMillis);
                long staleAfter = staleAfterMillis();
                checks.add(ageMillis <= staleAfter
                        ? Check.passed("Most recent sample is fresh",
                        "The last sample is " + ageMillis + "ms old (stale above " + staleAfter + "ms).")
                        : Check.failed("Most recent sample is fresh",
                        "The last sample is " + ageMillis + "ms old, past the " + staleAfter
                                + "ms staleness threshold - the page is showing a frozen reading.",
                        "The control loop has stopped completing iterations. Check whether the consumer is "
                                + "closed, blocked in the user function, or stuck in a rebalance."));

                checks.add(current.isRegistryPopulated()
                        ? Check.passed("Parallel Consumer meters are present",
                        "The registry carried Parallel Consumer's own meters at the last sample.")
                        : Check.failed("Parallel Consumer meters are present",
                        "A registry was supplied and sampling is running, but it contains none of Parallel "
                                + "Consumer's meters - so this is not the registry the consumer publishes into.",
                        "Pass the very same MeterRegistry instance to ParallelConsumerOptions.meterRegistry(..). "
                                + "Two registries in one process is the usual cause."));
            }
        }

        long failures = publisher.getSampleFailureCount();
        checks.add(failures == 0
                ? Check.passed("Sampling is not failing", "No sampling attempt has thrown.")
                : Check.failed("Sampling is not failing",
                failures + " sampling attempt(s) have thrown, so the published snapshot is not advancing.",
                "Look for a WARN from SnapshotPublisher in the consumer's log - it carries the exception, "
                        + "rate-limited so it cannot flood."));

        checks.add(assetCheck());
        return Collections.unmodifiableList(checks);
    }

    /**
     * The whole page. Plain HTML with an inline stylesheet - see the class javadoc for why it borrows nothing from
     * the main page.
     */
    public String renderHtml(long nowMillis) {
        List<Check> checks = runChecks(nowMillis);
        StringBuilder html = new StringBuilder(4096);
        html.append("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">")
                .append("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">")
                .append("<title>Parallel Consumer dashboard - self diagnostic</title><style>")
                .append("body{font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;margin:0;")
                .append("padding:2rem;background:#ffffff;color:#111111}")
                .append("h1{font-size:1.2rem;margin:0 0 .25rem}")
                .append(".sub{color:#555555;margin:0 0 1.5rem}")
                .append("table{border-collapse:collapse;width:100%;max-width:60rem}")
                .append("th,td{text-align:left;padding:.5rem .75rem;border-bottom:1px solid #dddddd;vertical-align:top}")
                .append("th{font-weight:600;color:#555555;font-size:.8rem;text-transform:uppercase}")
                .append(".passed{color:#0a6b2e;font-weight:600}")
                .append(".failed{color:#aa1111;font-weight:600}")
                .append(".skipped{color:#777777;font-weight:600}")
                .append(".remedy{color:#aa1111}")
                .append("footer{margin-top:2rem;color:#555555;max-width:60rem}")
                .append("@media(prefers-color-scheme:dark){body{background:#111111;color:#eeeeee}")
                .append("th{color:#aaaaaa}.sub,footer{color:#aaaaaa}td,th{border-bottom-color:#333333}")
                .append(".passed{color:#4ade80}.failed{color:#f87171}.skipped{color:#999999}.remedy{color:#f87171}}")
                .append("</style></head><body>");

        html.append("<h1>Parallel Consumer dashboard - self diagnostic</h1>");
        html.append("<p class=\"sub\">EXPERIMENTAL module. Read-only. Bound to ")
                .append(escape(options.getBindHostLiteral()))
                .append(" over ").append(escape(options.getScheme()))
                .append(", sampling every ").append(options.getUpdateInterval().toMillis()).append("ms.</p>");

        html.append("<table><thead><tr><th>Check</th><th>Result</th><th>Detail</th></tr></thead><tbody>");
        for (Check check : checks) {
            html.append("<tr><td>").append(escape(check.getName())).append("</td>")
                    .append("<td class=\"").append(check.getOutcome().cssClass).append("\">")
                    .append(check.getOutcome().label).append("</td><td>")
                    .append(escape(check.getDetail()));
            if (check.getRemedy() != null) {
                html.append("<div class=\"remedy\">").append(escape(check.getRemedy())).append("</div>");
            }
            html.append("</td></tr>");
        }
        html.append("</tbody></table>");

        // plan R42: the accuracy notice belongs here too, not only on the state document - somebody reading the
        // diagnostic is exactly somebody about to draw a conclusion from these numbers
        html.append("<footer><p>").append(escape(SnapshotJson.NOTICE)).append("</p>")
                .append("<p>This page reads only the published snapshot. It evaluates no meter and touches no "
                        + "Parallel Consumer state, so loading it cannot perturb the consumer it is describing.</p>")
                .append("</footer></body></html>");
        return html.toString();
    }

    private Check assetCheck() {
        List<String> missing = new ArrayList<>();
        for (String asset : requiredAssets) {
            if (classLoader.getResource(asset) == null) {
                missing.add(asset);
            }
        }
        if (missing.isEmpty()) {
            return Check.passed("Static assets resolve on the classpath",
                    requiredAssets.size() + " required resource(s) found.");
        }
        return Check.failed("Static assets resolve on the classpath",
                "Missing from the classpath: " + missing + ".",
                "The dashboard jar's resources are not where they were packaged. A shaded or repackaged jar that "
                        + "filters META-INF/resources, or a jar rebuilt without its resources, is the usual cause. "
                        + "The API endpoints keep working; only the page is affected.");
    }

    private long staleAfterMillis() {
        return Math.max(MINIMUM_STALE_MILLIS, options.getUpdateInterval().toMillis() * STALE_AFTER_INTERVALS);
    }

    /**
     * HTML-escapes text for insertion into element content or an attribute value. Topic names, group ids and
     * exception messages all reach this page and all are arbitrary user-controlled strings.
     */
    static String escape(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '&':
                    out.append("&amp;");
                    break;
                case '<':
                    out.append("&lt;");
                    break;
                case '>':
                    out.append("&gt;");
                    break;
                case '"':
                    out.append("&quot;");
                    break;
                case '\'':
                    out.append("&#x27;");
                    break;
                default:
                    out.append(c);
            }
        }
        return out.toString();
    }

    /**
     * What a check found. {@link #SKIPPED} is a first-class outcome, not a variety of failure - see the class
     * javadoc.
     * <p>
     * Experimental: the dashboard module is opt-in and its API may change without notice.
     */
    @InterfaceStability.Unstable
    public enum Outcome {

        PASSED("passed", "PASSED"),

        FAILED("failed", "FAILED"),

        SKIPPED("skipped", "NOT RUN");

        private final String cssClass;

        private final String label;

        Outcome(String cssClass, String label) {
            this.cssClass = cssClass;
            this.label = label;
        }

        public String getLabel() {
            return label;
        }
    }

    /**
     * One diagnostic result.
     * <p>
     * Experimental: the dashboard module is opt-in and its API may change without notice.
     */
    @InterfaceStability.Unstable
    @Value
    public static class Check {

        String name;

        Outcome outcome;

        /**
         * What was observed, in the reader's terms.
         */
        String detail;

        /**
         * What to do about it, or null when there is nothing to do. A failure always carries one.
         */
        String remedy;

        public static Check passed(String name, String detail) {
            return new Check(name, Outcome.PASSED, detail, null);
        }

        public static Check failed(String name, String detail, String remedy) {
            return new Check(name, Outcome.FAILED, detail, remedy);
        }

        public static Check skipped(String name, String blockedBy) {
            return new Check(name, Outcome.SKIPPED,
                    "Not run, because an earlier check failed: " + blockedBy + ".", null);
        }
    }
}
