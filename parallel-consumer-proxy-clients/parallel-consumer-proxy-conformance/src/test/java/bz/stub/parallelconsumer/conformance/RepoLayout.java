package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Where things are on disk, resolved rather than assumed: a foreign runner lives in its own language's
 * module, which this module has no Maven dependency on and cannot ask Maven about.
 * <p>
 * The working tree is found by walking up for {@code .git}, which is a FILE in a worktree and a directory
 * in a plain clone - every agent session here runs from a worktree, so testing for a directory would find
 * nothing.
 *
 * @author Antony Stubbs
 */
public final class RepoLayout {

    /** The clients aggregator, the parent of every language's module. */
    public static Path clientsRoot() {
        return workingTreeRoot().resolve("parallel-consumer-proxy-clients");
    }

    /** This module's scratch space, for the sidecar shims the suite writes per run. */
    public static Path scratch() {
        return Paths.get(System.getProperty("user.dir")).resolve("target").resolve("conformance");
    }

    public static Path workingTreeRoot() {
        var dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        for (var candidate = dir; candidate != null; candidate = candidate.getParent()) {
            if (Files.exists(candidate.resolve(".git"))) {
                return candidate;
            }
        }
        throw new IllegalStateException("no git working tree above " + dir
                + " - the conformance suite locates each language's runner relative to it");
    }

    private RepoLayout() {
    }
}
