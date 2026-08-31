package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The registry says where ten runners live; this is what makes it say something true.
 * <p>
 * <b>Nothing else on this rung can check these entries, and that is the whole reason this exists.</b> The
 * suite cannot spawn a runner here - there is no engine for one to reach - so a registry entry naming a
 * module that was renamed, or a binary path the language's build stopped writing to, would sit correct-looking
 * and wrong until the engine landed and somebody had to debug ten cells at once. Every claim below is one a
 * checkout can answer today.
 * <p>
 * <b>The completeness assertion runs both ways, deliberately.</b> A client module with no registry entry is
 * a language the suite will silently never drive; an entry naming a module that is gone is a lane that will
 * fail far from its cause. Reading the client modules off disk rather than listing them here is what makes an
 * eleventh language fail this test on the day its directory appears, rather than on the day somebody
 * remembers.
 *
 * @author Antony Stubbs
 * @see LanguageRunners
 */
class LanguageRunnerRegistryTest {

    /** The Java reference client is not a spawned runner - it is driven in-process, by its own bindings. */
    private static final String NOT_A_SPAWNED_RUNNER = "java";

    @Test
    void everyClientModuleOnDiskHasARunnerRegisteredAndTheReverse() {
        var registered = LanguageRunners.all().stream().map(LanguageRunner::language).sorted().toList();
        var onDisk = clientModuleLanguages();

        assertWithMessage("the registry and the client modules must be the same set: a module with no entry "
                        + "is a language this suite will never drive, and an entry with no module is a lane "
                        + "that fails far from its cause. On disk: %s; registered: %s", onDisk, registered)
                .that(registered).containsExactlyElementsIn(onDisk);
    }

    @Test
    void everyEntryBuildsInsideItsOwnModuleAndLandsInside() {
        for (var runner : LanguageRunners.all()) {
            var module = RepoLayout.clientsRoot().resolve("parallel-consumer-proxy-client-" + runner.language());

            assertWithMessage("%s builds in a directory that is not its own module", runner.language())
                    .that(runner.workingDirectory().normalize()).isEqualTo(module.normalize());
            assertWithMessage("%s's runner lands outside its module (%s), so a `mvn clean` of that module "
                            + "would not remove it and a stale binary could pass for a fresh one",
                    runner.language(), runner.executable())
                    .that(runner.executable().normalize().startsWith(module.normalize())).isTrue();
        }
    }

    /**
     * The two JVM entries carry no build command on purpose - the Maven reactor builds them - and every other
     * entry must carry one, because nothing else will. An empty command on a language the reactor does not
     * build is a runner nobody builds, which fails at use time as "missing binary" rather than as the
     * omission it is.
     */
    @Test
    void onlyTheReactorBuiltLanguagesCarryNoBuildCommand() {
        var withoutABuild = LanguageRunners.all().stream()
                .filter(runner -> runner.buildCommand().isEmpty())
                .map(LanguageRunner::language)
                .toList();

        assertWithMessage("only the JVM clients are built by the reactor that runs this suite; any other "
                        + "language with no build command is a runner nobody builds")
                .that(withoutABuild).containsExactly("kotlin", "scala");
    }

    /**
     * The negative control, and it is the behaviour the whole registry exists for: a runner whose binary is
     * absent must FAIL, naming the command that would have built it. A skip here would report a clean run for
     * a client nobody had started.
     */
    @Test
    void anAbsentRunnerFailsRatherThanSkipping() {
        var absent = LanguageRunners.absent();

        var thrown = assertThrows(LanguageRunner.RunnerUnavailableException.class, absent::ensureAvailable);

        assertWithMessage("the failure names the binary that is not there")
                .that(thrown).hasMessageThat().contains("no-such-conformance-runner");
        assertWithMessage("and says outright that absence is not a pass")
                .that(thrown).hasMessageThat().contains("is not a language that passed");
    }

    /** The other half: a binary that IS there satisfies the same check, so the control above can fail. */
    @Test
    void aRunnerThatIsThereIsAccepted() throws IOException {
        var directory = Files.createTempDirectory("pc-runner-registry");
        var executable = SidecarShim.write(directory, "stand-in", 1);
        var present = LanguageRunners.deliberatelyFailing(executable);

        present.ensureAvailable();
    }

    /** The client module directories, by language, as the checkout actually holds them. */
    private static List<String> clientModuleLanguages() {
        try (Stream<Path> modules = Files.list(RepoLayout.clientsRoot())) {
            return modules.map(path -> path.getFileName().toString())
                    .filter(name -> name.startsWith("parallel-consumer-proxy-client-"))
                    .map(name -> name.substring("parallel-consumer-proxy-client-".length()))
                    .filter(language -> !NOT_A_SPAWNED_RUNNER.equals(language))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("listing the client modules under " + RepoLayout.clientsRoot(), e);
        }
    }
}
