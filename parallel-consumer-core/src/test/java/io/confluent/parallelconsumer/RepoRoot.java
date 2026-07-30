package io.confluent.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Locates the repository root from a test's working directory (surefire runs tests with the module
 * directory as cwd; forks may vary) by walking up to a known repo-root marker file.
 */
final class RepoRoot {

    private RepoRoot() {
    }

    static Path find() {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null && !Files.exists(dir.resolve("bin/quarantined-test.sh"))) {
            dir = dir.getParent();
        }
        if (dir == null) {
            throw new IllegalStateException("could not locate repo root (marker bin/quarantined-test.sh) above "
                    + Paths.get("").toAbsolutePath());
        }
        return dir;
    }
}
