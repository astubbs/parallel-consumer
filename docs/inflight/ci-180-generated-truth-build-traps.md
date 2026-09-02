# Generated Truth assertions: what documenting astubbs#180 did NOT fix

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

[astubbs#180](https://github.com/astubbs/parallel-consumer/issues/180) /
[confluentinc#861](https://github.com/confluentinc/parallel-consumer/issues/861) -
*"ManagedTruth.assertThat not found"* - is answered by documentation
([`docs/building.md`](../building.md)) plus two small guard-rails: an actionable `<message>` on the
enforcer's `reactorModuleConvergence` rule, and `-am` in `bin/build.sh`'s own usage example, which
was previously the broken invocation. The **underlying build shape is unchanged**, so the trap can
still be sprung; it is now just signposted. Recorded here so a future session does not re-triage it
from scratch, and knows which fixes were considered and why they were not taken.

**One of them IS being taken - the maintainer's ruling, 2026-09-02:** make `truth-generator-maven-plugin`
clean only the `managed` tree, then flip `cleanTargetDir` here. That is the open item in this note;
the other two bullets stay as recorded decisions, not work.

## To verify anything about a fresh clone, isolate the local repository

The first pass recorded `./mvnw compile` as a *silent* success that generated nothing. That was an
artefact of the machine: a developer `~/.m2` already holding
`parallel-consumer-core-<version>-tests.jar` from an earlier `install`. On a genuinely empty
repository the same command dies at `parallel-consumer-vertx` instead. Both are real, and
`docs/building.md` records both.

Hard-link a copy of the local repository and delete the project's own snapshots from it -
`cp -al ~/.m2/repository <tmp>`, `rm -rf <tmp>/bz/stub/parallelconsumer`, build with
`-Dmaven.repo.local=<tmp>`. That leaves the shared `~/.m2` intact and re-downloads no third-party
dependency. Two other traps found the same way: `-o` (offline) changes the error text, and a stale
`target/test-classes` from an earlier run can mask the IDE-shaped failure entirely.

## Still open

- **The IDE path has no guard.** The enforcer message only fires for `-pl` without `-am`. An IDE that
  compiles test sources itself never runs Maven's `validate`, so the *most likely* way a newcomer
  meets this bug is the one path that still produces only a raw `cannot find symbol`. Considered and
  rejected for now: a `maven-enforcer` `requireFilesExist` on the generated directory (fires at
  `validate`, which the IDE also skips - so it would add a failure mode for CLI users while doing
  nothing for the people who need it), and checking the generated sources into git (defeats the
  generator, and they drift silently against the classes they assert on - **re-proposed and
  rejected again by the maintainer on 2026-09-02**, so do not bring it a third time without a new
  argument). A real fix is probably an
  IDE run configuration or an `.idea/` pre-build step, which is per-IDE and unverifiable in CI. Not
  worth it until someone reports the docs did not land.

- **`cleanTargetDir=false` does not self-repair a partial `target/`.** Reproducible, and re-verified
  on 2026-09-01: from a good build, `rm -rf parallel-consumer-core/target/generated-test-sources`
  then `./mvnw install -DskipTests` regenerates only part of the set and fails with a few hundred
  `cannot find symbol` errors *inside the generated code* - `ManagedTruth.java` referencing
  `ParallelConsumerOptionsSubject`, `CommitModeSubject` and friends that were not regenerated.
  `./mvnw clean install -DskipTests` is fine. Documented as "don't hand-delete parts of `target/`"
  for now, because flipping `cleanTargetDir` to `true` as it stands would wipe the
  `truth-assertions-templates` output on every build, and the templates are the half a human is meant
  to be able to extend.

  **DECIDED, 2026-09-02 - this is the fix to make, and it is two steps in two repos.** (1) In
  `truth-generator` (the maintainer's own project): a mode that cleans only the `managed` output
  directory and leaves `templates` alone - the plugin already knows the two apart, since it writes
  them to separate roots. (2) Here, once that ships: bump `truth-generator-maven-plugin.version` in
  the root `pom.xml` and flip this module's `cleanTargetDir`. Then the partial-`target/` case
  self-repairs and the "don't hand-delete" paragraph in `docs/building.md` comes out. This fixes only
  this trap; the IDE path and the test-jar coupling are unchanged by it.

- **The test-jar coupling itself** ([astubbs#132](https://github.com/astubbs/parallel-consumer/issues/132),
  [confluentinc#162](https://github.com/confluentinc/parallel-consumer/issues/162)) is untouched. That
  mirror's title - *"mvn compile fails if test-jar of parallel-consumer-core was not previously
  installed"* - is the fresh-clone `compile` failure, filed from the other end. The
  vertx/reactor/mutiny/example modules depend on
  `bz.stub.parallelconsumer:parallel-consumer-core:jar:tests`, which exists only after core is
  packaged and installed. The *release* pipeline's instance is handled (`preparationGoals` in the
  root `pom.xml`); the contributor path is handled only by "run install first". Removing the coupling
  means extracting the shared test fixtures into their own module - a real refactor, and a
  `docs/refactoring.md` candidate if it is ever wanted, not a doc fix.
