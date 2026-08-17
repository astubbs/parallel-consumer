package bz.stub.parallelconsumer.dashboard.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.dashboard.DashboardServer;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Runs the dashboard page's own ES modules on the JVM, so the logic they encode can be asserted by the ordinary unit
 * suite with no browser present.
 *
 * <h2>Why a JavaScript engine and not a DOM parser</h2>
 * <p>
 * The served HTML is a shell: every interesting node on the page is built by JavaScript after it has fetched the
 * state document. A test that parsed the served markup would therefore be asserting against an empty
 * {@code <main>} and would stay green while the page was completely broken - which is worse than no test, because it
 * reads like coverage. Anything that tests this page has to actually execute its JavaScript.
 *
 * <h2>Why GraalVM's engine specifically</h2>
 * <p>
 * The modules are ES modules and the arithmetic is {@code BigInt}, and on the JVM that combination rules out the
 * alternatives: Nashorn (standalone, since the JDK dropped it) has neither ES modules nor {@code BigInt}, and Rhino
 * has {@code BigInt} but no ES modules. Graal runs them <strong>as written</strong> - the same files the server
 * serves, imported by their own relative specifiers, with no bundler, no transpile and no shim. There is no build
 * step in this repository and this harness does not introduce one.
 *
 * <h2>What it deliberately does not do</h2>
 * <p>
 * There is no DOM here, so nothing that touches {@code document} or {@code window} can be loaded: that is the
 * browser suite's job (see {@code integrationTests.ui}). The split is deliberate. This half is fast, needs no
 * environment, and therefore <em>gates</em>; the browser half sees what a reader sees, and skips where there is no
 * browser. Neither substitutes for the other.
 */
final class PageModules {

    /**
     * The page's shared helpers. DOM-free at module scope - {@code element()} touches {@code document}, but only when
     * called - so it loads here.
     */
    static final String UI = "ui.js";

    /**
     * The offsets view model: the geometry, the ordering and every quantity the reader is shown.
     */
    static final String OFFSET_MODEL = "panels/offsets/model.js";

    /**
     * How long each tween runs for. Pure arithmetic over millisecond numbers, and DOM-free precisely so that the
     * page's motion can be gated here rather than only by watching it.
     */
    static final String TWEEN = "tween.js";

    private final Context context;

    private final Path webRoot;

    private PageModules(Context context, Path webRoot) {
        this.context = context;
        this.webRoot = webRoot;
    }

    /**
     * A fresh engine over the page assets on the test classpath.
     * <p>
     * The assets are found through {@link DashboardServer#PAGE_CLASSPATH_ROOT} rather than by a hardcoded source
     * path, so this harness reads exactly the files the server would serve - if the mount point moves, this moves
     * with it instead of silently testing a copy that is no longer shipped.
     */
    static PageModules open() {
        Context context = Context.newBuilder("js")
                // the modules import each other by relative path, so the engine has to be able to read the
                // directory they live in
                .allowIO(IOAccess.ALL)
                // makes evaluating a module return its exports, which is how a Java assertion gets hold of an
                // exported function without the modules needing a test-only entry point added to them
                .allowExperimentalOptions(true)
                .option("js.esm-eval-returns-exports", "true")
                // this harness runs on a stock JVM, where Truffle has no JIT. It says so, loudly, once per context;
                // the page's model code is a few dozen operations per call and does not care.
                .option("engine.WarnInterpreterOnly", "false")
                .build();
        return new PageModules(context, locateWebRoot());
    }

    /**
     * Evaluates one module and returns its exports.
     */
    Value module(String relativePath) {
        File file = webRoot.resolve(relativePath).toFile();
        if (!file.isFile()) {
            throw new IllegalStateException("The dashboard page module " + relativePath + " is not on the test "
                    + "classpath under " + DashboardServer.PAGE_CLASSPATH_ROOT + "/ - looked in " + webRoot
                    + ". Has it been renamed, or is this running against a stale target/classes?");
        }
        try {
            return context.eval(Source.newBuilder("js", file)
                    .mimeType("application/javascript+module")
                    .build());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read the dashboard page module " + file, e);
        }
    }

    /**
     * Parses a JSON document into a guest-language value, which is how a Java-built fixture becomes something the
     * page's own functions will accept. Deliberately {@code JSON.parse} of the real wire form rather than a
     * hand-built guest object: it is the same operation the page performs on an arriving state document, so a value
     * that only survives as a Java type cannot creep into a fixture.
     */
    Value parseJson(String json) {
        return context.eval("js", "JSON.parse").execute(json);
    }

    /**
     * A guest {@code BigInt} from its digits, for feeding a function that - correctly - refuses anything else.
     */
    Value bigInt(String digits) {
        return context.eval("js", "BigInt").execute(digits);
    }

    /**
     * The guest value as the string JavaScript itself would print. Needed because a {@code BigInt} has no Java
     * equivalent {@link Value} conversion - and asserting on its digits rather than on a lossy numeric conversion is
     * the entire point of the offsets being {@code BigInt} in the first place.
     */
    String stringify(Value value) {
        return context.eval("js", "(v) => v === null || v === undefined ? null : String(v)")
                .execute(value)
                .asString();
    }

    void close() {
        context.close();
    }

    private static Path locateWebRoot() {
        String marker = DashboardServer.PAGE_CLASSPATH_ROOT + "/" + UI;
        URL resource = PageModules.class.getClassLoader().getResource(marker);
        if (resource == null) {
            throw new IllegalStateException("Could not find " + marker + " on the test classpath. The dashboard page "
                    + "assets are served from the classpath, so if they are not there the server cannot serve them "
                    + "either.");
        }
        try {
            return Paths.get(resource.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("The dashboard page assets resolved to " + resource + ", which is not a "
                    + "readable directory. This harness reads the modules as files so their relative imports "
                    + "resolve; running it from inside a jar is not supported.", e);
        }
    }
}
