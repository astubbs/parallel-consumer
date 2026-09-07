#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
#
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
#        INTEGRATION_SHARD=heavy bin/ci-integration-test.sh [extra-maven-args...]
#        INTEGRATION_SHARD=rest  bin/ci-integration-test.sh [extra-maven-args...]
#
# SHARDING: ONE NAMED SET, ONE CATCH-ALL - not N balanced shards.
#
# `heavy` runs exactly the classes in HEAVY_CLASSES below; `rest` runs everything EXCEPT them.
# Unset runs the whole suite, which is what a local invocation and every non-CI caller gets.
#
# Why this shape rather than the chaos suite's four sized bins. A balanced N-way split has to be
# re-sized whenever the suite changes, and its failure mode is silent: add a class and it belongs
# to no shard, so it stops running and nothing goes red. Here the catch-all is defined by
# SUBTRACTION, so a new test runs by default and the only way to lose one is to name it in
# HEAVY_CLASSES and then delete it - which the report check below turns into a hard failure.
# The list is also small enough to reason about, which is the point: it is the handful of classes
# that dominate the wall, not a bin-packing whose contents nobody can predict.
#
# Sizing, measured (docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md): core's
# failsafe is ~1450-1530s of test time over 42 classes, and each JOB re-pays ~136s of serial build
# plus ~20s of job overhead - so a split only pays while the largest remaining class is smaller
# than the work it takes off the critical path.
#
# HOW THIS SHAPE WAS ARRIVED AT, in order, because the order is the finding. The 857 probe was the
# binding constraint: at ~342s in ONE unsplittable class it set the floor for whichever shard held
# it, which capped a two-way split at ~20%. Splitting that class four ways moved the floor to
# PartitionStateCommittedOffsetIT's ~160s, and only THEN was a larger heavy set worth deriving.
# So the probe split came first and the seven-class HEAVY_CLASSES below was re-derived after it,
# not extended by guess - which is also why the maintenance guide further down says to re-derive
# rather than append. Measured end to end: 620s -> 519s (two shards) -> 450s (+ probe split) ->
# 416s (+ the re-derived heavy set).
#
# Extra args are forwarded to Maven. Forked per-broker mode - each JVM fork gets its
# own broker, reliable AND parallel - is requested with -DforkCount=N -DreuseForks=true,
# and .github/workflows/maven.yml's gating Integration Tests lane passes forkCount=4.
# This script itself passes none, so a bare invocation runs failsafe at its default of 1.
#
# forkCount=4 is a measured ceiling, not a floor: raising it to 6 was measured on the hosted
# runner at 469s of failsafe against a 420s baseline, because the forks contend (the same
# tests cost 11% more CPU time), and it produced a first-ever timeout failure in
# ManagedPCInstanceLifecycleTest. See docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md
# before raising it. See also docs/self-hosted-runner.md.

set -euo pipefail

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default - the two lists
# drifting is silent in the direction that matters: a lane the pom excludes but this does not runs
# here, in the GATING suite, which is how the Lincheck lane would have arrived if nobody looked.
# The publishing artefacts are dead weight in a TEST lane. delombok, javadoc:jar and source:jar
# are bound in the root pom's always-active <build> section rather than behind the release
# profile, so `clean verify` builds a javadoc jar and a sources jar for all eleven modules on
# every run. Nothing here consumes either.
#
# TAKEN ON FIRST PRINCIPLES, NOT ON A MEASUREMENT, and the distinction is the point. The skips
# provably do less work - a run with them builds 0 javadoc jars against 9 without (two of the
# eleven reactor poms are aggregators and produce none either way), and delombok reports skipping
# 11 times against 2 - so this cannot be slower. The SIZE is an estimate and not a measurement:
# per-goal attribution puts javadoc:jar at 14s and delombok at 8s, so roughly 22s. Nothing measured
# it, because this lane's wall time cannot resolve 22s - three CONCURRENT samples of IDENTICAL code
# spread 119 seconds. Do not go looking for this in a before/after timing; it is not visible there
# and never will be.
#
# Property names matter, and two of the three are non-obvious: maven-source-plugin reads
# maven.source.skip (NOT source.skip, which silently does nothing and shipped in the first cut of
# this change), and lombok-maven-plugin reads lombok.delombok.skip. Verify a change to these by
# counting artefacts in the log, never by timing the job.
#
# Deliberately NOT skipped: compiler:testCompile, 60s. It looks like the same waste, since
# -DskipUTs=true means the unit tests never run - but the integration sources import 19 classes
# out of src/test/java, including ParallelEoSStreamProcessorTestBase and its transitive tail, so
# that compilation is load-bearing. Checked before spending a CI run on it.
#
# docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md holds the measurements.
# ---------------------------------------------------------------------------------------------
# SHARD MEMBERSHIP: one NAMED heavy set, and a catch-all defined by SUBTRACTION.
#
# WHEN TO ADD A CLASS TO HEAVY_CLASSES - the whole guide, because a list nobody knows how to
# maintain rots:
#
#   1. ONLY when it is the thing bounding the catch-all. A shard's wall is
#      max(its slowest class, its total work / forkCount), so a class is worth moving only when
#      its OWN wall exceeds the catch-all's work/4. Absolute slowness is not the test: a 90s class
#      is irrelevant while the catch-all is work-bound at 275s, and a 300s class is the whole
#      problem. `bin/check-integration-shard-balance.mjs` prints both numbers.
#   2. Keep the two shards' WALLS close, not their class counts or their totals. The heavy set is
#      seven classes against the catch-all's thirty-odd, and that is correct - the point is
#      balance of wall-clock, and a shard of few big classes packs differently from many small ones.
#   3. Think in FORKS. forkCount is 4, so a heavy set of seven means three forks run two classes
#      and one runs one; what matters is that no fork is handed two of the BIGGEST. The
#      pre-rebalance set was sized at five for exactly that reason, and the rebalance to seven was
#      taken from measured walls, not from this rule - which is rule 5.
#      (History: an earlier revision of this guide still said "five" after the list had grown to
#      seven, and a maintainer applying rule 3 literally would have removed two classes and undone
#      the measured rebalance. The number in this guide is descriptive; the list is the truth.)
#   4. REMOVING is as much a part of this as adding. A class that got faster, or a suite that grew
#      around it, leaves the list over-weighted; the balance checker reports drift in both
#      directions and does not care which way you fix it.
#   5. Re-derive from MEASURED times, never from reading the code. Every sizing guess in this
#      lane's history has been wrong, including the one that said splitting the 857 probe would
#      give four 89s classes - they measured 138-166s, because the repetitions carry per-class
#      fixed cost that used to be paid once.
#
# Sized from the measured per-class times of run 33829977814, which is where rule 1 above came from
# rather than being an example of it. That run measured heavy 330s against the catch-all's 450s, and
# the heavy shard was TAIL-BOUND: its slowest class (PartitionStateCommittedOffsetIT, 208s) exceeded
# its own work/4 of 178s, so three of its four forks were partly idle. Work added to a tail-bound
# shard is FREE until its total reaches forkCount x its slowest class - 834s here, against the 710s
# it held. Two classes moved across from the catch-all, which is work-bound and hands back every
# second removed.
#
# The catch-all is the complement, so a NEW test runs there by default and can never belong to no
# shard - the inversion of N explicit bins, whose failure mode is a class running nowhere with
# nothing going red.
#
# WHY TWO SHARDS AND NOT FOUR, measured rather than assumed: four shards with this same probe
# split measured 355s against two shards' ~440s, but cost 1318s of runner time against ~880s AND
# manufactured 500s of extra test work - per-shard fixed costs are paid per shard. 85s of critical
# path is not worth a 50% machine-time increase and four lists to keep straight instead of one.
# The four-way arrangement is preserved on branch ci/shard-integration-four if that trade ever
# looks different.
# ---------------------------------------------------------------------------------------------
# The groups the gating run excludes. ONE list, handed both to failsafe and to the coverage gate
# below - a class tagged into one of these is not expected to report, and two copies of this list
# would drift in the direction that matters: a group excluded here but not there fails every build.
readonly EXCLUDED_GROUPS=performance,chaos,quarantined,lincheck
readonly HEAVY_CLASSES="PartitionStateCommittedOffsetIT,Rebalance857CommitSyncDeadlockProbe3IT,Rebalance857CommitSyncDeadlockProbe2IT,TransactionAndCommitModeTest,MultiInstanceRebalanceTest,RebalanceEoSDeadlockTest,Rebalance857CommitSyncDeadlockProbeIT"

# A class in two lists would run twice and be paid for twice, and both shards would pass. With one
# list this cannot happen, but the check costs nothing and survives the list being split again.
dupes=$(echo "$HEAVY_CLASSES" | tr ',' '\n' | sort | uniq -d)
if [ -n "$dupes" ]; then
    echo "ci-integration-test: FAILED - class(es) named more than once in HEAVY_CLASSES:" >&2
    printf '    %s\n' $dupes >&2
    exit 2
fi

SHARD_ARGS=()
SHARD_EXPECT=""
case "${INTEGRATION_SHARD:-}" in
    heavy)
        # -Dfailsafe.failIfNoSpecifiedTests=false is REQUIRED: the reactor builds ten modules
        # containing none of these classes and the first would otherwise fail the build before the
        # requested tests run. bin/chaos-test.sh's header records the same trap. The report checks
        # below are what stop that flag turning a shard that selected NOTHING into a silent pass.
        SHARD_EXPECT="$HEAVY_CLASSES"
        SHARD_ARGS=(-Dit.test="${HEAVY_CLASSES}" -Dfailsafe.failIfNoSpecifiedTests=false)
        ;;
    rest)
        # NOT -Dit.test=!Class. Setting it.test REPLACES failsafe's <includes>, and both test source
        # roots compile into the same target/test-classes - so the negated form silently drops the
        # `**/integrationTest*/**` restriction and runs the entire UNIT suite under failsafe too.
        # Measured before it was understood: 168 classes and 981 tests instead of 42 and 204, and it
        # PASSED, because running more tests than you meant to fails nothing. it.excluded.classes
        # feeds failsafe's <excludes>, which leaves <includes> intact.
        SHARD_ARGS=(-Dit.excluded.classes="$(echo "$HEAVY_CLASSES" | sed 's#[^,][^,]*#**/&.java#g')")
        ;;
    "") ;;  # whole suite - local runs and any caller that does not opt in
    *)
        echo "ci-integration-test: INTEGRATION_SHARD must be 'heavy', 'rest' or unset (got '${INTEGRATION_SHARD}')" >&2
        exit 2
        ;;
esac

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dmaven.javadoc.skip=true \
  -Dmaven.source.skip=true \
  -Dlombok.delombok.skip=true \
  -Dexcluded.groups="${EXCLUDED_GROUPS}" \
  ${SHARD_ARGS[@]+"${SHARD_ARGS[@]}"} \
  "$@"

# A SHARD THAT RAN NOTHING MUST NOT READ AS A PASS. failIfNoSpecifiedTests is disabled above -
# it has to be, because the reactor builds ten modules containing none of these classes and the
# first would otherwise fail the build - so Maven exits 0 whether the selection matched every class
# or none of them. These checks are the only thing between a renamed class and a permanently green
# job that tests nothing. The mutation lane shipped exactly that bug once ("nothing to mutate,
# skipping", green forever); bin/chaos-test.sh guards the same shape.
# EVERY module's reports, not core's. Integration tests live in parallel-consumer-vertx and the
# example modules too - VertxConcurrencyIT and CoreAppMetricsIntegrationTest among them - and a
# core-only glob reports those as MISSING while they ran and passed elsewhere in the reactor. That
# is a false RED, and it failed two shards on the first four-way run.
# Enumerated ONCE, after the build: every guard below reads the same list, and re-running a tree-wide
# find per class name was up to nine identical traversals for one answer (simplify pass, astubbs#442).
REPORTS="$(find . -path '*/target/failsafe-reports/TEST-*.xml' -not -path './.git/*' 2>/dev/null)"
all_reports() { printf '%s\n' "$REPORTS"; }
report_exists() {  # report_exists <simple class name>
    # Herestring, NOT `all_reports | grep -q`. Under `set -o pipefail` that pipeline inverts its own
    # answer: grep -q exits on the first match, find takes EPIPE and dies 141, and pipefail promotes
    # that - so a MATCH reports failure. bin/check-source-patterns.mjs caught it here, and its rule
    # exists because check-review-posted.sh shipped the same bug and reported "no review posted" on
    # four PRs that had one. It would have been the same false RED this commit is fixing.
    grep -q "/TEST-.*\.$1\.xml$" <<<"$(all_reports)"
}

if [ -n "${SHARD_EXPECT}" ]; then
    # A NAMED shard must contain every class it was assigned. A rename fails HERE, loudly, while
    # the catch-all quietly keeps running the test - so the suite stays complete and the list is
    # what gets reported as wrong. That asymmetry is deliberate and is why the catch-all exists.
    missing=""
    for c in ${SHARD_EXPECT//,/ }; do
        report_exists "$c" || missing="${missing} ${c}"
    done
    if [ -n "$missing" ]; then
        echo "ci-integration-test: FAILED - the heavy shard produced no failsafe report for:${missing}" >&2
        echo "  The shard ran, exited 0, and tested less than it was assigned. Usually a renamed or" >&2
        echo "  deleted class still named in its shard list - the catch-all is still running it, so" >&2
        echo "  nothing is untested, but the list is now wrong." >&2
        exit 1
    fi
elif [ "${INTEGRATION_SHARD:-}" = "rest" ]; then
    # The catch-all must contain NONE of the named classes and still not be empty.
    # `-z`, not `wc -l`: the cached list is printed with a trailing newline, so an EMPTY tree still
    # counts one line and a count-based guard can never fire (found by review after the caching).
    if [ -z "$REPORTS" ]; then
        echo "ci-integration-test: FAILED - the catch-all shard produced no failsafe reports at all." >&2
        exit 1
    fi
    for c in ${HEAVY_CLASSES//,/ }; do
        if report_exists "$c"; then
            echo "ci-integration-test: FAILED - '${c}' is in a named shard but ran in the catch-all too." >&2
            echo "  Both shards are paying for it, and both will pass." >&2
            exit 1
        fi
    done
fi

# EVERY INTEGRATION TEST FILE IN THE TREE MUST HAVE PRODUCED A REPORT. This is the completeness
# half, and it is the only check here that starts from the SOURCE rather than from the run: the
# others all ask whether what ran was right, and none of them can see a test that ran nowhere.
#
# The gap it closes is not sharding-specific and predates it. Failsafe selects by package
# (`**/integrationTest*/**`), so a class moved to a neighbouring package silently stops being
# collected - in the single-job arrangement as much as in this one - and every shard passes having
# never run it. "A test that never runs is not a passing test, and nothing goes red to tell you"
# is AGENTS.md's phrasing, and this is that rule made mechanical for this lane.
#
# Only the catch-all runs it, because only the catch-all is supposed to hold everything not named.
#
# WHAT COUNTS AS "SHOULD HAVE RUN" is four subtractions, and getting them wrong makes this a gate
# that fails every build rather than one that catches anything. The first draft demanded 55 classes
# against the 38 that run, because an integrationTest package holds helpers and fixtures as well as
# tests. Dry-run any change to these against a real run before trusting it:
#   - HEAVY_CLASSES        - ran in the other shard, by design
#   - abstract bases       - never collected; their subclasses are what run
#   - chaos / performance  - excluded by GROUP, not by package, so package-walking still finds them
#   - @Quarantined         - the quarantine lane owns them
#   - no @Test/@RepeatedTest/@ParameterizedTest - a helper class, not a test
# Verified against run 33831283169: 37 required, 0 missing.
if [ "${INTEGRATION_SHARD:-}" = "rest" ]; then
    # Every test class the COMPILER produced must have a report in some shard. This used to be a shell
    # scan of the .java sources and every defect it had was a text-matching one - a file-wide grep,
    # a filename read as a class name, an `extends` matched inside a javadoc sentence. The gate reads
    # bytecode instead, where those shapes do not exist; its header and self-test own the detail.
    SHARD_COVERAGE_ENFORCE=1 node bin/check-integration-shard-coverage.mjs --heavy-classes "$HEAVY_CLASSES" --excluded-groups "$EXCLUDED_GROUPS" || {
        echo "ci-integration-test: FAILED - the coverage gate did not pass (see above)." >&2
        exit 1
    }
fi

# EVERY class that ran must live in an integrationTest package, in EVERY shard. This is the guard
# that caught the -Dit.test bug described above, and none of the checks before it would have: they
# ask whether the RIGHT tests ran, and that failure was 126 EXTRA ones. A "ran at least N" gate
# cannot see it either - more is not fewer - which is why this asks about PROVENANCE rather than
# count. It is the only check here that is not about the shard lists at all.
if [ -n "${INTEGRATION_SHARD:-}" ]; then
    strays=$(all_reports | grep -v '\.integrationTest' || true)
    if [ -n "$strays" ]; then
        echo "ci-integration-test: FAILED - shard '${INTEGRATION_SHARD}' ran classes from outside an" >&2
        echo "  integrationTest package. Failsafe's <includes> restriction has been lost - check whether" >&2
        echo "  something set -Dit.test, which REPLACES it. Offenders:" >&2
        printf '    %s\n' $strays >&2
        exit 1
    fi
fi
