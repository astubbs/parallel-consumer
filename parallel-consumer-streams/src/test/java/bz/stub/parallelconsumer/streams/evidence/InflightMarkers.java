package bz.stub.parallelconsumer.streams.evidence;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The machine-readable markers in {@code docs/inflight/}, which is where this lane's judgement calls live.
 * <p>
 * <b>The lane needs two things it cannot derive from code: which test cases are known to flake, and why each
 * remaining seam-on divergence is expected.</b> Both are judgements about the world rather than facts about a
 * class, both are reviewed as prose, and both stop being true when somebody changes the semantics underneath.
 * Keeping them in the ledger rather than in the classifier means the rung that closes one of them edits the
 * entry in the same pull request that closes it - and means a reviewer reads the reasoning rather than a
 * regular expression.
 * <p>
 * A marker is an HTML comment, so it is invisible in rendered markdown and sits beside the note's other
 * metadata:
 * <pre>
 * &lt;!-- flaky-case: fully.qualified.ClassName#methodName --&gt;
 * &lt;!-- seam-on-divergence-class: some-name = one line saying why a failure of this shape is expected --&gt;
 * &lt;!-- seam-on-divergence: fully.qualified.ClassName#methodName = some-name --&gt;
 * </pre>
 *
 * @author Antony Stubbs
 * @see DivergenceMechanism
 */
@Getter
public final class InflightMarkers {

    /**
     * One marker, with the note it came from - so the lane can say WHERE an expectation is written down
     * rather than merely that one exists.
     */
    @RequiredArgsConstructor
    @Getter
    public static final class Marker {

        private final String key;

        private final String value;

        private final String note;
    }

    private static final Pattern MARKER =
            Pattern.compile("<!--\\s*([a-z-]+):\\s*(.+?)\\s*-->", Pattern.DOTALL);

    /** Null when no {@code docs/inflight} was found above the working directory - reported, never assumed empty. */
    private final Path directory;

    private final List<Marker> markers;

    private InflightMarkers(final Path directory, final List<Marker> markers) {
        this.directory = directory;
        this.markers = Collections.unmodifiableList(markers);
    }

    /**
     * Walk up from the working directory to whichever ancestor holds {@code docs/inflight}, and read every
     * note in it.
     * <p>
     * Surefire runs with the module as its working directory and this lane only ever runs from inside the
     * repository, so a miss means the ledger has moved. That is reported through {@link #getDirectory()}
     * returning null and every mechanism saying so in its own description - never silently treated as "no
     * expectations were recorded", which would turn every ledgered case into an unexplained one on a run that
     * simply looked in the wrong place.
     */
    public static InflightMarkers load() {
        Path candidate = Paths.get("").toAbsolutePath();
        while (candidate != null) {
            final Path inflight = candidate.resolve("docs").resolve("inflight");
            if (Files.isDirectory(inflight)) {
                return new InflightMarkers(inflight, read(inflight));
            }
            candidate = candidate.getParent();
        }
        return new InflightMarkers(null, new ArrayList<>());
    }

    /**
     * Read a named directory of notes. The lane itself never calls this - it uses {@link #load()} - but the
     * classifier's own tests need a ledger they control, and reaching into the repository's real one would
     * make those tests fail whenever somebody edited a note.
     */
    public static InflightMarkers from(final Path directory) {
        return new InflightMarkers(directory, read(directory));
    }

    private static List<Marker> read(final Path directory) {
        final List<Marker> found = new ArrayList<>();
        try (DirectoryStream<Path> notes = Files.newDirectoryStream(directory, "*.md")) {
            for (final Path note : notes) {
                final String text = new String(Files.readAllBytes(note), StandardCharsets.UTF_8);
                final Matcher matcher = MARKER.matcher(text);
                while (matcher.find()) {
                    found.add(new Marker(matcher.group(1), matcher.group(2),
                            "docs/inflight/" + note.getFileName()));
                }
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Could not read the inflight ledger under " + directory, e);
        }
        return found;
    }

    public List<Marker> withKey(final String key) {
        final List<Marker> matching = new ArrayList<>();
        for (final Marker marker : markers) {
            if (marker.getKey().equals(key)) {
                matching.add(marker);
            }
        }
        return matching;
    }

    /** For a mechanism's description, so a reader of the report knows where the expectations came from. */
    public String describeSource() {
        return directory == null
                ? "NO LEDGER FOUND - no docs/inflight directory above " + Paths.get("").toAbsolutePath()
                : directory.toString();
    }
}
