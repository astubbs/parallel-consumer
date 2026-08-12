package io.confluent.parallelconsumer.dashboard.integrationTests.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardServer;
import io.confluent.parallelconsumer.dashboard.server.DashboardTestSupport;
import io.confluent.parallelconsumer.dashboard.snapshot.LifecycleSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.PartitionSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.PcSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.confluent.parallelconsumer.dashboard.snapshot.StateSampler;
import io.confluent.parallelconsumer.dashboard.snapshot.WorkSnapshot;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * The browser half of the UI harness (plan U15, KTD22): a real headless Chrome, a real DOM, and screenshots.
 *
 * <h2>Why a browser at all, when the model already has a unit suite</h2>
 * <p>
 * The page's arithmetic is gated without a browser by {@code io.confluent.parallelconsumer.dashboard.ui} on the unit
 * side. What that cannot see is the DOM: whether a panel is present, whether the value the model computed reached
 * the element a reader looks at, and whether the page's state classes - idle, stale, error - are actually applied.
 * Those are the assertions that catch a renderer that stopped rendering, and there is no way to make them without
 * executing the page. Screenshots come along for free once a browser is running, and they cover the visual
 * breakage no assertion anticipated.
 *
 * <h2>No broker, and a snapshot injected rather than sampled</h2>
 * <p>
 * These tests serve a fixed {@link PcSnapshot} through the ordinary server. That is deliberate on two counts. It
 * needs no Docker and no Kafka, so the suite is fast and its failures are about rendering rather than about a
 * broker - {@code ShowcaseScenarioIT} already covers the live path. And it is the only way to put an offset above
 * 2<sup>53</sup> on the page at all: every offset that arrives through Micrometer has been through a {@code double}
 * before this module ever sees it, so a meter-built fixture could not exercise the precision the wire format exists
 * to protect.
 *
 * <h2>It never skips</h2>
 * <p>
 * If the browser cannot be started, these tests FAIL. They do not abort, and there is no opt-out property. A skipped
 * suite is indistinguishable from a passing one, which is how a harness becomes decoration; and a build tool the
 * project needs is a build tool everyone working on it should have.
 * <p>
 * There is also nothing to install. Selenium Manager (built in since 4.6, and able to fetch Chrome for Testing itself
 * since 4.11) resolves both the driver and the browser on first use and caches them under {@code ~/.cache/selenium}.
 * So a machine without Chrome gets one; a failure here means something genuinely wrong - no network on a cold cache,
 * a sandbox that forbids the download, an incompatible platform - and every one of those is worth failing on rather
 * than quietly not testing the page.
 */
@Slf4j
abstract class DashboardUiTestBase {

    /**
     * Where screenshots land. A build product, never a source artefact - see the entry this suite added to
     * {@code .gitignore}, and {@code ScreenshotOutputTest}, which fails if that entry is ever removed.
     */
    static final String SCREENSHOT_DIRECTORY_PROPERTY = "dashboard.ui.screenshots";

    static final String DEFAULT_SCREENSHOT_DIRECTORY = "target/ui-screenshots";

    /**
     * Fixed, so a screenshot taken on one machine is comparable with one taken on another, and so a layout that only
     * breaks at a particular width breaks reproducibly.
     */
    private static final String VIEWPORT = "1440,1200";

    /**
     * Long enough to absorb a slow first paint on a loaded CI runner, short enough that a genuinely broken page
     * reports it rather than hanging out the lane's timeout.
     */
    private static final Duration RENDER_TIMEOUT = Duration.ofSeconds(20);

    private static final Duration POLL = Duration.ofMillis(100);

    /**
     * One browser for the whole JVM, closed on exit. Starting Chrome costs about a second, and every test in this
     * suite navigates afresh anyway, so per-test browsers would buy isolation the tests do not need at a cost they
     * would feel.
     */
    /**
     * The exclusive resource every UI suite must take, because the driver above is shared across the whole JVM.
     * <p>
     * {@code @Execution(SAME_THREAD)} serialises the methods WITHIN one class; it says nothing about two classes
     * running at the same time, and this repository executes tests concurrently outside the {@code ci} profile. So
     * {@code OffsetRibbonUiIT} and {@code PageShellUiIT} could drive one {@code ChromeDriver} from two threads at
     * once - which is not thread-safe, and whose symptom is a navigation landing in the middle of another test's
     * assertions. It would flake on a developer's {@code mvn verify} and never in CI, which is the worst place for a
     * race to live. A named resource lock is what actually serialises them against each other.
     */
    static final String BROWSER_RESOURCE = "dashboard-ui-shared-chrome-driver";

    private static volatile ChromeDriver sharedDriver;

    private DashboardServer server;

    /**
     * Per test rather than per class, so a browser that cannot start fails every test in the class rather than
     * reporting the class as having no tests at all. "Tests run: 0" is exactly how a suite disappears without anyone
     * noticing.
     */
    @BeforeEach
    void requireABrowser() {
        browser();
    }

    @AfterEach
    void stopTheServer() {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    /* ---------------------------------------------------------------- server */

    /**
     * Starts a dashboard serving exactly {@code snapshot}, and returns its URL.
     * <p>
     * The snapshot is injected by overriding the sampler rather than by seeding a meter registry, for the reason in
     * the class javadoc: the meter path cannot carry an offset above 2<sup>53</sup>.
     */
    String serve(PcSnapshot snapshot) {
        SnapshotPublisher publisher = new SnapshotPublisher(fixedSampler(snapshot));
        // one publish, and only one: the page's staleness is "no new document has arrived", so a fixture that
        // republished on a timer could never be observed going stale
        publisher.sampleOnce();
        server = new DashboardServer(publisher, null, DashboardTestSupport.testOptions().build()).start();
        return server.getUrl();
    }

    /**
     * Stops the server this test started, leaving the already-loaded page in the browser. This is how the error
     * state is reached honestly: the page is healthy, and then the instance it is talking to goes away.
     */
    void stopServing() {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    /**
     * The state document as the page itself would have fetched it. Assertions compare what is drawn against this,
     * so a formatting bug that drops precision between the wire and the screen has somewhere to be caught.
     */
    JsonObject stateDocument(String pageUrl) {
        return new JsonObject(get(pageUrl + DashboardServer.STATE_PATH));
    }

    /* --------------------------------------------------------------- browser */

    static ChromeDriver browser() {
        if (sharedDriver != null) {
            return sharedDriver;
        }
        synchronized (DashboardUiTestBase.class) {
            if (sharedDriver == null) {
                sharedDriver = newDriver();
                Runtime.getRuntime().addShutdownHook(new Thread(DashboardUiTestBase::quitBrowser,
                        "dashboard-ui-browser-shutdown"));
            }
        }
        return sharedDriver;
    }

    private static ChromeDriver newDriver() {
        ChromeOptions options = new ChromeOptions();
        options.addArguments(
                "--headless=new",
                "--disable-gpu",
                // containers - including the CI runner's - do not always grant the sandbox's namespaces
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--window-size=" + VIEWPORT,
                // a scrollbar's width differs between platforms and would otherwise move every element in the shot
                "--hide-scrollbars",
                "--force-device-scale-factor=1");
        return new ChromeDriver(options);
    }

    private static void quitBrowser() {
        ChromeDriver driver = sharedDriver;
        sharedDriver = null;
        if (driver != null) {
            try {
                driver.quit();
            } catch (RuntimeException e) {
                log.debug("Ignoring a failure while closing the dashboard UI browser.", e);
            }
        }
    }

    /* ------------------------------------------------------------ navigation */

    /**
     * Loads the page under the given colour scheme and waits until the shell has stopped saying "connecting".
     *
     * @param scheme {@code light} or {@code dark}, emulated at the browser level exactly as the reader's own
     *               operating-system preference would be - so the page's {@code prefers-color-scheme} rules are the
     *               thing under test, rather than a class this test switched on for it
     */
    void openPage(String url, String scheme) {
        emulateColourScheme(scheme);
        browser().get(url);
        awaitCondition(() -> !"loading".equals(statusPhase()),
                "the page to get past its connecting state");
    }

    void openPage(String url) {
        openPage(url, "light");
    }

    private void emulateColourScheme(String scheme) {
        Map<String, Object> feature = new HashMap<>();
        feature.put("name", "prefers-color-scheme");
        feature.put("value", scheme);
        Map<String, Object> media = new HashMap<>();
        media.put("media", "screen");
        media.put("features", Collections.singletonList(feature));
        browser().executeCdpCommand("Emulation.setEmulatedMedia", media);
    }

    /* ------------------------------------------------------------ inspection */

    /**
     * The page's own phase, as the shell publishes it. This is the programmatic form of every state the page has -
     * loading, live, idle, stale, error, off, nodata - and it is what makes those states assertable rather than
     * merely visible.
     */
    String statusPhase() {
        List<WebElement> status = browser().findElements(By.id("status"));
        return status.isEmpty() ? null : status.get(0).getAttribute("data-phase");
    }

    String textOf(String cssSelector) {
        return browser().findElement(By.cssSelector(cssSelector)).getText().trim();
    }

    int count(String cssSelector) {
        return browser().findElements(By.cssSelector(cssSelector)).size();
    }

    /**
     * Reads every partition row into plain Java values in one round trip, keyed by the label the row shows and by
     * the label each value cell shows.
     * <p>
     * Extracted by the labels the reader sees rather than by child index: an assertion written against
     * {@code .pt-value:nth-child(3)} passes for the wrong reason the moment a cell is inserted, and fails for the
     * wrong reason the moment one is reordered.
     */
    List<Map<String, Object>> partitionRows() {
        Object raw = script(""
                + "return Array.from(document.querySelectorAll('#panel-offsets .pt')).map(row => {"
                + "  const values = {};"
                + "  row.querySelectorAll('.pt-value').forEach(cell => {"
                + "    values[cell.querySelector('em').textContent.trim()] = cell.querySelector('span').textContent.trim();"
                + "  });"
                + "  return {"
                + "    name: row.querySelector('.pt-name').textContent.trim(),"
                + "    won: row.querySelector('.pt-won').textContent.trim(),"
                + "    note: row.querySelector('.pt-note').textContent.trim(),"
                + "    order: row.style.order,"
                + "    collapsed: row.getAttribute('data-collapsed'),"
                + "    axis: row.querySelector('.pt-axis').textContent.trim(),"
                + "    caveatHidden: row.querySelector('.badge-caveat').hasAttribute('hidden'),"
                + "    spans: Array.from(row.querySelectorAll('.pt-span'))"
                + "      .filter(span => span.style.display !== 'none')"
                + "      .map(span => span.getAttribute('data-kind')),"
                + "    markers: Array.from(row.querySelectorAll('.pt-marker'))"
                + "      .filter(marker => marker.style.display !== 'none')"
                + "      .map(marker => marker.getAttribute('data-kind')),"
                + "    values: values"
                + "  };"
                + "});");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Object row : (List<?>) raw) {
            @SuppressWarnings("unchecked")
            Map<String, Object> typed = (Map<String, Object>) row;
            rows.add(typed);
        }
        return rows;
    }

    static Map<String, Object> rowNamed(List<Map<String, Object>> rows, String name) {
        for (Map<String, Object> row : rows) {
            if (name.equals(row.get("name"))) {
                return row;
            }
        }
        throw new AssertionError("No row for " + name + " - the page drew " + names(rows));
    }

    static List<String> names(List<Map<String, Object>> rows) {
        List<String> names = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            names.add(String.valueOf(row.get("name")));
        }
        return names;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> valuesOf(Map<String, Object> row) {
        return (Map<String, Object>) row.get("values");
    }

    /**
     * One of a row's list-valued attributes - the kinds of span or marker it is currently drawing - as strings.
     */
    static List<String> listOf(Map<String, Object> row, String attribute) {
        List<String> values = new ArrayList<>();
        for (Object value : (List<?>) row.get(attribute)) {
            values.add(String.valueOf(value));
        }
        return values;
    }

    Object script(String javascript) {
        return ((JavascriptExecutor) browser()).executeScript(javascript);
    }

    /* --------------------------------------------------------------- waiting */

    /**
     * Waits for a condition, and says what it was waiting for when it does not arrive. A bare timeout in a UI suite
     * is the least informative failure there is; this one names the expectation and reports the page's phase, which
     * is usually the whole diagnosis.
     */
    void awaitCondition(BooleanSupplier condition, String description) {
        awaitCondition(condition, description, RENDER_TIMEOUT);
    }

    void awaitCondition(BooleanSupplier condition, String description, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
                lastFailure = null;
            } catch (RuntimeException e) {
                // a node the page has not built yet is not a failure until the deadline passes
                lastFailure = e;
            }
            sleep(POLL);
        }
        throw new AssertionError("Timed out after " + timeout + " waiting for " + description
                + ". The page's status phase was '" + statusPhase() + "'."
                + (lastFailure == null ? "" : " Last error while checking: " + lastFailure));
    }

    void awaitPhase(String phase) {
        awaitCondition(() -> phase.equals(statusPhase()), "the page to report its '" + phase + "' state");
    }

    void awaitPhase(String phase, Duration timeout) {
        awaitCondition(() -> phase.equals(statusPhase()), "the page to report its '" + phase + "' state", timeout);
    }

    static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the dashboard page", e);
        }
    }

    /* ----------------------------------------------------------- screenshots */

    /**
     * Writes a full-viewport screenshot and returns where it went. Nothing asserts on the pixels: the point of a
     * screenshot is the breakage no assertion anticipated, and an image comparison would fail on font hinting long
     * before it caught anything real.
     */
    Path screenshot(String name) {
        Path directory = screenshotDirectory();
        try {
            Files.createDirectories(directory);
            Path file = directory.resolve(name + ".png");
            Files.write(file, browser().getScreenshotAs(OutputType.BYTES));
            log.info("Dashboard UI screenshot: {}", file.toAbsolutePath());
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write the dashboard UI screenshot " + name + " to " + directory,
                    e);
        }
    }

    static Path screenshotDirectory() {
        return Paths.get(System.getProperty(SCREENSHOT_DIRECTORY_PROPERTY, DEFAULT_SCREENSHOT_DIRECTORY));
    }

    /* -------------------------------------------------------------- fixtures */

    /**
     * An offset one above {@code Number.MAX_SAFE_INTEGER}. Everything about the wire format being strings, and about
     * the page computing in {@code BigInt}, exists so that a number like this survives to the screen.
     */
    static final long ABOVE_SAFE_INTEGER = 9007199254740993L;

    static final long HUGE_COMPLETED = 9007199254741999L;

    static final long HUGE_SEEN = 9007199254742100L;

    /**
     * The partition whose offsets are past the safe-integer boundary, and which therefore carries the whole
     * precision argument on its own.
     */
    static final String HUGE_PARTITION = "orders-0";

    static final String ORDINARY_PARTITION = "orders-1";

    /**
     * The caught-up one: everything seen has been processed and committed, which must read as healthy rather than as
     * an error. See {@link #caughtUpPartition} for why its committed offset is not equal to the other three.
     */
    static final String CAUGHT_UP_PARTITION = "payments-7";

    /**
     * The offset every marker on {@link #CAUGHT_UP_PARTITION} sits at - except the committed gauge, which reads one
     * above it.
     */
    static final long CAUGHT_UP_OFFSET = 900L;

    static final String INSTANCE_ID = "pc-ui-fixture-1";

    /**
     * A busy instance: work in flight, a partition running a thousand records ahead of its commit point past
     * 2<sup>53</sup>, an ordinary partition, and one that is completely caught up.
     */
    static PcSnapshot busyInstance() {
        return snapshot(
                WorkSnapshot.builder()
                        .inflightRecords(9L)
                        .waitingRecords(31L)
                        .incompleteOffsetsTotal(11L)
                        .shards(4L)
                        .shardsSize(40L)
                        .pausedPartitions(0L)
                        .numberOfPartitions(3L)
                        .dynamicLoadFactor(2.5d)
                        .build(),
                partition("orders", 0, ABOVE_SAFE_INTEGER + 1, ABOVE_SAFE_INTEGER, HUGE_COMPLETED, HUGE_SEEN, 7L),
                partition("orders", 1, 50L, 55L, 60L, 64L, 4L),
                caughtUpPartition("payments", 7, 900L));
    }

    /**
     * A connected instance with nothing to do: nothing in flight, nothing waiting, nothing incomplete. This is a
     * healthy state and the page has to say so - it must never be confused with a stalled one, which is why the
     * suite asserts the two are told apart programmatically.
     */
    static PcSnapshot idleInstance() {
        return snapshot(
                WorkSnapshot.builder()
                        .inflightRecords(0L)
                        .waitingRecords(0L)
                        .incompleteOffsetsTotal(0L)
                        .shards(0L)
                        .shardsSize(0L)
                        .pausedPartitions(0L)
                        .numberOfPartitions(1L)
                        .build(),
                caughtUpPartition("payments", 7, 900L));
    }

    private static PcSnapshot snapshot(WorkSnapshot work, PartitionSnapshot... partitions) {
        return PcSnapshot.builder()
                .captureEpochMillis(System.currentTimeMillis())
                .sampleSequence(1L)
                .registryPopulated(true)
                .lifecycle(LifecycleSnapshot.builder()
                        .state("RUNNING")
                        .stateValue(1)
                        .pollerState("RUNNING")
                        .pollerStateValue(1)
                        .closedOrFailed(false)
                        .instanceId(INSTANCE_ID)
                        .build())
                .work(work)
                .partitions(Arrays.asList(partitions))
                .build();
    }

    /**
     * A partition whose commit has caught up with its processing - the shape a real instance produces, and the one no
     * fixture here used to have.
     * <p>
     * <strong>Its committed gauge reads {@code highestDone + 1}.</strong> Kafka's committed offset is the next offset
     * to consume, and core publishes {@code highestSequentialSucceeded + 1} into the gauge the dashboard samples. A
     * fixture setting all four markers equal describes a state no running instance produces, and it is what let an
     * off-by-one survive in both the ordering invariant and the page's model.
     */
    private static PartitionSnapshot caughtUpPartition(String topic, int number, long highestDone) {
        return partition(topic, number, highestDone + 1, highestDone, highestDone, highestDone, 0L);
    }

    private static PartitionSnapshot partition(String topic,
                                               int number,
                                               long committed,
                                               long sequential,
                                               long completed,
                                               long seen,
                                               long incomplete) {
        return PartitionSnapshot.builder()
                .topic(topic)
                .partition(number)
                .lastCommittedOffset(committed)
                .highestSequentialSucceededOffset(sequential)
                .highestCompletedOffset(completed)
                .highestSeenOffset(seen)
                .incompleteOffsets(incomplete)
                .assignmentEpoch(3L)
                .processedRecords(120d)
                .failedRecords(0d)
                .slowRecords(0d)
                .build();
    }

    /**
     * A sampler that hands back one prepared snapshot. Subclassing rather than mocking: the publisher, the routes and
     * the page below them are all the real ones, and only the reading itself is fixed.
     */
    private static StateSampler fixedSampler(PcSnapshot snapshot) {
        return new StateSampler((AbstractParallelEoSStreamProcessor<?, ?>) null, null) {
            @Override
            public PcSnapshot sample() {
                return snapshot;
            }
        };
    }

    private static String get(String url) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestProperty("Accept", "application/json");
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder body = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    body.append(line);
                }
                return body.toString();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + url, e);
        }
    }
}
