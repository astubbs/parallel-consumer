package bz.stub.parallelconsumer.dashboard.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The browser harness's screenshots are a build product, and this is what keeps them out of the repository.
 * <p>
 * The same reasoning that keeps the chart library out of git applies here with more force: a checked-in screenshot
 * of a page is stale the first time the page changes, and nothing in a review notices a picture that is merely
 * out of date. Ignoring the output directory is the whole guard, and a guard nothing checks is one deletion away
 * from being gone - so this asserts the entry is still there.
 * <p>
 * A unit test rather than part of the browser suite on purpose: it must run on a machine with no browser, which is
 * exactly the machine on which somebody could remove the entry without ever seeing a screenshot appear.
 */
class ScreenshotOutputTest {

    /**
     * Must match the pattern in {@code .gitignore}, and the directory name in {@code DashboardUiTestBase}. Named
     * here rather than imported from the harness because the harness lives on the integration-test source path and
     * this is the unit suite - and because a constant shared between the thing and its check would let both move
     * together, which is the one thing this test exists to prevent.
     */
    private static final String SCREENSHOT_DIRECTORY_NAME = "ui-screenshots";

    @Test
    void theScreenshotDirectoryIsIgnoredByGit() {
        Path gitignore = repositoryGitignore();

        List<String> entries = read(gitignore);

        assertThat(entries)
                .as("%s must ignore the UI harness's screenshot output - a screenshot is a build product and "
                        + "committing one puts a picture in every clone that goes stale the first time the page "
                        + "changes", gitignore)
                .contains(SCREENSHOT_DIRECTORY_NAME + "/");
    }

    private static List<String> read(Path gitignore) {
        try {
            List<String> lines = Files.readAllLines(gitignore, StandardCharsets.UTF_8);
            lines.replaceAll(String::trim);
            return lines;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + gitignore, e);
        }
    }

    /**
     * The repository's own {@code .gitignore}, found by walking upward from wherever the test was started. No module
     * here carries one of its own, so the first one found going up is the repository's.
     */
    private static Path repositoryGitignore() {
        Path directory = Paths.get("").toAbsolutePath();
        while (directory != null) {
            Path candidate = directory.resolve(".gitignore");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            directory = directory.getParent();
        }
        throw new IllegalStateException("No .gitignore found above " + Paths.get("").toAbsolutePath()
                + " - this test needs to run from inside the repository.");
    }
}
