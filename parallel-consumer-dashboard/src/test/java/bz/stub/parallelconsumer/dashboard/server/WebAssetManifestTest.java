package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The page's asset manifest, derived rather than declared.
 * <p>
 * There is no build step for the page - the browser loads exactly the files that are on the classpath - so a
 * renamed or moved file produces a {@code 404} at runtime and a silently half-rendered dashboard. Nothing else in
 * the build would notice: the Java compiles, the server starts, the tests pass, and the panel simply never appears.
 * <p>
 * So this test walks the page's own references instead of trusting a hand-maintained list. It starts at
 * {@code index.html}, follows every {@code href}, {@code src} and ES-module {@code import} it finds, and asserts
 * three things about the result:
 * <ul>
 *   <li>every reference resolves to something that really is on the classpath;</li>
 *   <li>every file shipped under {@code web/} is reachable from the entry point, so a file that nothing loads any
 *       more is a failure rather than dead weight in the jar;</li>
 *   <li>no asset names an absolute {@code http} or {@code https} URL, which is plan R15 (works air-gapped, no CDN,
 *       no fonts, no telemetry) enforced mechanically rather than by review.</li>
 * </ul>
 */
class WebAssetManifestTest {

    private static final String ENTRY_POINT = "index.html";

    /** {@code href="..."} and {@code src="..."}. */
    private static final Pattern HTML_REFERENCE = Pattern.compile("(?:href|src)\\s*=\\s*[\"']([^\"']+)[\"']");

    /** {@code import x from '...'} and the side-effect form {@code import '...'}. */
    private static final Pattern MODULE_IMPORT =
            Pattern.compile("\\bimport\\s+(?:[^'\"()]*?\\bfrom\\s*)?[\"']([^\"']+)[\"']");

    /** {@code url(...)} in a stylesheet. */
    private static final Pattern CSS_URL = Pattern.compile("url\\(\\s*[\"']?([^\"')]+)[\"']?\\s*\\)");

    private static final Pattern ABSOLUTE_URL = Pattern.compile("https?://", Pattern.CASE_INSENSITIVE);

    @Test
    void everyReferenceThePageMakesResolvesToSomethingOnTheClasspath() {
        for (String asset : allPageAssets()) {
            for (String reference : referencesIn(asset)) {
                String resolved = resolve(asset, reference);
                if (resolved == null) {
                    continue;
                }
                assertThat(classpathResource(resolved))
                        .as("%s references %s, which resolves to classpath resource %s", asset, reference, resolved)
                        .isNotNull();
            }
        }
    }

    @Test
    void everyShippedFileIsReachableFromTheEntryPoint() {
        Set<String> reachable = reachableFromEntryPoint();

        assertThat(new TreeSet<>(allPageAssets()))
                .as("every file under %s should be loaded by the page, directly or transitively - an unreferenced "
                        + "one is either dead weight in the jar or a rename that broke a reference",
                        DashboardServer.PAGE_CLASSPATH_ROOT)
                .isEqualTo(new TreeSet<>(reachable));
    }

    @Test
    void theEntryPointLoadsTheStylesheetAndTheApplication() {
        List<String> references = referencesIn(ENTRY_POINT);

        assertThat(references).contains(DashboardServer.ASSETS_PREFIX + "/style.css",
                DashboardServer.ASSETS_PREFIX + "/app.js");
    }

    @Test
    void noPageAssetNamesAnAbsoluteUrl() {
        List<String> offenders = new ArrayList<>();
        for (String asset : allPageAssets()) {
            Matcher matcher = ABSOLUTE_URL.matcher(read(asset));
            if (matcher.find()) {
                offenders.add(asset + " (at index " + matcher.start() + ")");
            }
        }

        assertThat(offenders)
                .as("the page must make no request to any host other than the instance serving it, so no asset may "
                        + "carry an absolute URL - not a CDN, not a font service, not telemetry")
                .isEmpty();
    }

    /**
     * Every file shipped under the page's classpath root, as paths relative to it.
     */
    private static Set<String> allPageAssets() {
        Path root = pageRoot();
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .map(path -> root.relativize(path).toString().replace('\\', '/'))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (IOException e) {
            throw new UncheckedIOException("Could not walk the dashboard page assets under " + root, e);
        }
    }

    private static Set<String> reachableFromEntryPoint() {
        Set<String> reachable = new LinkedHashSet<>();
        Deque<String> pending = new ArrayDeque<>();
        pending.add(ENTRY_POINT);
        while (!pending.isEmpty()) {
            String asset = pending.removeFirst();
            if (!reachable.add(asset)) {
                continue;
            }
            for (String reference : referencesIn(asset)) {
                String resolved = resolve(asset, reference);
                if (resolved != null && resolved.startsWith(DashboardServer.PAGE_CLASSPATH_ROOT + "/")) {
                    pending.add(resolved.substring(DashboardServer.PAGE_CLASSPATH_ROOT.length() + 1));
                }
            }
        }
        return reachable;
    }

    /**
     * Every reference a page asset makes, in the syntax appropriate to its kind.
     */
    private static List<String> referencesIn(String asset) {
        String content = read(asset);
        List<String> references = new ArrayList<>();
        if (asset.endsWith(".html")) {
            collect(HTML_REFERENCE, content, references);
        } else if (asset.endsWith(".js")) {
            collect(MODULE_IMPORT, content, references);
        } else if (asset.endsWith(".css")) {
            collect(CSS_URL, content, references);
        }
        return references;
    }

    private static void collect(Pattern pattern, String content, List<String> into) {
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            into.add(matcher.group(1));
        }
    }

    /**
     * Turns a reference as written into the classpath resource it must resolve to, or null for references that are
     * not classpath resources at all ({@code data:} URIs, and the API endpoints the page fetches).
     * <p>
     * The URL-space-to-classpath mapping is taken from {@link DashboardServer}'s own mount constants, so a change to
     * where the assets are served from breaks this test rather than silently invalidating it.
     */
    private static String resolve(String fromAsset, String reference) {
        String target = reference.trim();
        if (target.isEmpty() || target.startsWith("data:") || target.startsWith("#")) {
            return null;
        }
        if (target.startsWith(DashboardServer.ASSETS_PREFIX + "/")) {
            return DashboardServer.PAGE_CLASSPATH_ROOT + "/"
                    + target.substring(DashboardServer.ASSETS_PREFIX.length() + 1);
        }
        if (target.startsWith(DashboardServer.VENDOR_PREFIX + "/uplot/")) {
            return DashboardServer.UPLOT_CLASSPATH_ROOT + "/"
                    + target.substring((DashboardServer.VENDOR_PREFIX + "/uplot/").length());
        }
        if (target.startsWith("/")) {
            // an endpoint rather than a file - /api/state.json and friends are served by handlers, not the classpath
            return null;
        }
        // relative, which for an ES module means relative to the importing module's own location
        Path base = Paths.get(DashboardServer.PAGE_CLASSPATH_ROOT + "/" + fromAsset).getParent();
        Path resolved = base == null ? Paths.get(target) : base.resolve(target).normalize();
        return resolved.toString().replace('\\', '/');
    }

    private static URL classpathResource(String resource) {
        return WebAssetManifestTest.class.getClassLoader().getResource(resource);
    }

    private static String read(String asset) {
        Path file = pageRoot().resolve(asset);
        try {
            return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read dashboard page asset " + file, e);
        }
    }

    /**
     * The page's classpath root as a directory on disk. Resolved through the class loader rather than from a
     * hard-coded source path, so this reads what will actually be packaged.
     */
    private static Path pageRoot() {
        URL entry = classpathResource(DashboardServer.PAGE_CLASSPATH_ROOT + "/" + ENTRY_POINT);
        assertThat(entry)
                .as("%s/%s must be on the test classpath", DashboardServer.PAGE_CLASSPATH_ROOT, ENTRY_POINT)
                .isNotNull();
        assertThat(entry.getProtocol().toLowerCase(Locale.ROOT))
                .as("this test reads the page assets as files, so it expects them unpacked on the test classpath")
                .isEqualTo("file");
        try {
            return Paths.get(entry.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Could not resolve the dashboard page assets from " + entry, e);
        }
    }

    /**
     * The one way the checks above could quietly stop working: a new kind of asset that
     * {@link #referencesIn(String)} has no parser for contributes no references, so the reachability walk passes it
     * vacuously and anything it alone loads becomes invisible. So the set of parseable kinds is asserted directly.
     */
    @Test
    void everyShippedFileIsOfAKindThisTestKnowsHowToParse() {
        List<String> known = Arrays.asList(".html", ".js", ".css");

        for (String asset : allPageAssets()) {
            assertThat(known.stream().anyMatch(asset::endsWith))
                    .as("%s is a kind of asset this test cannot follow references out of, so adding it would make "
                            + "the reachability check weaker without anybody noticing. Teach referencesIn() about "
                            + "it, or state deliberately why it has no references.", asset)
                    .isTrue();
        }
    }
}
