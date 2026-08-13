#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Performs the fork's package rename - io.confluent.* -> bz.stub.* - on whatever branch it is run in.
#
# BRINGING AN OPEN PR BRANCH ACROSS? The complete procedure is the section titled BRINGING AN OPEN
# BRANCH ACROSS, below. It is eight numbered steps, each one a command. Start there and read nothing
# else first; the sections before it are the measurements behind the steps, not prerequisites.
#
# READ THE PROJECT'S IN-FLIGHT ENTRY BEFORE YOU RUN THIS: `docs/inflight/branch-package-rename.md`.
# It is the canonical entry for the whole package-rename project and it records findings this run
# NEEDS - references deliberately left for the rename to fix rather than fixed in place, and the
# reasoning for each. This script cannot infer any of that from the tree, and the sweeps below will
# not tell you which survivors were a decision and which were an oversight.
#
# WHY THIS IS A SCRIPT AND NOT A ONE-OFF `sed`
#
# The rename has to land on master AND on every open PR branch. Done by hand, once, on master, the
# other branches each turn into a conflict storm at merge: their files still live under the old path
# and their diffs still say `io.confluent`. Done by a re-runnable script, each branch gets the SAME
# transformation, so both sides of every merge agree on where the files live and what they are
# called, and the merge is trivial. That is the whole design constraint - re-runnability on an
# arbitrary branch - and it is why nothing here is a hardcoded manifest: a PR that adds a new file
# under the old package path must be picked up by globbing the tree, not by a list written today.
#
# See docs/plans/2026-08-11-001-refactor-package-rename-plan.md for the full task inventory and the
# Apache 2.0 analysis. Two findings from it govern the code below.
#
#   1. THREE OF THE REFERENCES ARE INVISIBLE TO THE OBVIOUS SWEEP. `bin/ci-mutation-test.sh` and
#      `bin/lib/quarantine-common.sh` encode the package as an ESCAPED REGEX - the literal characters
#      io\.confluent\.parallelconsumer\. - which a find-and-replace on the dotted form does not
#      match, and which the habitual verification sweep `grep -rn "io\.confluent"` also cannot see:
#      it reports success while `bin/lib/quarantine-common.sh` is not even in its output. Stale, the
#      mutation lane matches nothing, prints "no core main-source classes changed - nothing to
#      mutate, skipping" and EXITS 0 - green forever while scoring zero mutants, indistinguishable in
#      the job summary from a real pass. So the rewrite has an explicit rule for the escaped shape,
#      and the completeness check at the end uses a PERMISSIVE pattern that tolerates it.
#
#   2. THE PACKAGE IS SPELT WRONG IN AT LEAST ONE PLACE. `.idea/runConfigurations/` has carried
#      io.confluent.parallalconsumer - an `a` where the second `e` belongs. Treat misspellings as a
#      category rather than a one-off: the rewrite normalises the known variant before renaming (so
#      the typo is fixed, not carried forward), and the completeness check's pattern stops at
#      `conflu` so that a variant nobody has seen still cannot hide from it.
#
# WHAT IT DOES, IN ORDER
#
#   preflight  - refuse to run on a dirty tree (the commit would sweep in unrelated work), and
#                refuse if any legacy package is UNMAPPED - see NO FALLBACK RULE below
#   moves      - `git mv` every file under a mapped legacy package directory to its new home
#   rewrite    - every textual form, in every tracked text file, minus an explicit frozen list
#   headers    - Apache 2.0 s4(c): add the Modifications Copyright line to modified upstream files
#   regenerate - docs/todo-index.md and README.adoc, both of which must never be hand-edited
#   commit     - see COMMIT SHAPE below
#   verify     - assert git recorded the moves as RENAMES, then run the completeness check
#   residue    - a ONE-OFF report over every tracked file, for a human to judge. Not a gate.
#
# NO FALLBACK RULE, AND THE REFUSAL THAT REPLACES IT
#
# PKG_MAP used to end with a catch-all mapping the second upstream-owned prefix onto a same-named
# prefix under bz.stub. That is a rename that preserves the string it is renaming away from: every
# package the catch-all caught came out still carrying upstream's mark, one level down. It has been
# deleted, every legacy package is now named explicitly, and anything that STILL matches the legacy
# prefix once the table has been applied stops the run and is named.
#
# The two are not interchangeable, and the difference is the whole point. A fallback answers "I have
# no rule for this" by inventing one and carrying on, which is how a package nobody decided about
# acquires a permanent home - silently, in a commit of several hundred mechanical edits, where no
# reviewer will find it. Refusing puts the decision back in the table, in a diff, with a name on it.
# Do not re-introduce a quieter version of the fallback to make the refusal go away.
#
# THE RESIDUE REPORT IS DELIBERATELY NOT A `bin/check-*.sh`
#
# The sweep at the end reports, for judgement, and does not gate. Two reasons, and the second is
# the load-bearing one. First, this is a one-off migration: a standing checker outlives its subject
# and becomes a file nobody can explain. Second, a permanent checker forbidding a token would have
# to CONTAIN that token to test for it - a check that is itself the last instance of what it
# forbids, which cannot be satisfied and would have to be excused, at which point it is decoration.
#
# COMMIT SHAPE, AND WHY THE DEFAULT IS TWO COMMITS
#
# Measured, not assumed - both shapes have a plausible story and the cost of guessing wrong is a
# conflict storm across every open PR. The experiment, with a control arm: perform the rename as two
# commits (pure `git mv` first, content edits second) and verify rename detection; then squash the
# two and re-run the IDENTICAL verification on the result. Exactly one term changes - whether the
# content edits are folded into the move. Run against master at bd021cb2 on 2026-08-11, git 2.51.2,
# with an explicit diff.renameLimit of 65535 so no result is a limit artefact.
#
#   two commits  move commit     233 moved, 233 renames, ALL R100 (exact), 0 A, 0 D
#                content commit  0 renames, 0 A, 0 D, 253 modifications
#   squashed     one commit      233 moved, 232 renames, 1 A + 1 D, lowest similarity R061
#
# THE SQUASH LOST A RENAME, and the prediction about which file held. `TestConventionsArchTest.java`
# exists once per module - five near-identical files whose whole body is an `@AnalyzeClasses`
# annotation and an empty class - so once the package string is rewritten, each one resembles its
# four siblings about as much as it resembles its own former self. Git paired them into a cycle:
#
#   streams/TestConventionsArchTest.java  ->  metrics/...   R071
#   vertx/...                             ->  mutiny/...    R073
#   mutiny/...                            ->  reactor/...   R073
#   reactor/...                           ->  vertx/...     R073
#   metrics/...                           ->  DELETED, and streams/... reported as a new file
#
# Four renames recorded as CROSS-MODULE moves that never happened, and one file dropped out of the
# permutation as an add/delete pair. A bare count of R entries would have caught only the last of
# those, which is why the verification below also asserts that every rename's old path maps to its
# new path under the SAME transformation the script applied. A rename that is detected but wrong is
# worse than one that is missed.
#
# WHAT THE SPLIT DOES NOT BUY, ALSO MEASURED. `git merge` does rename detection over the merge base
# to tip TREE DELTA, and that delta is identical however many commits it is spread across. Merging
# each shape into the same PR branch produced byte-identical outcomes in every scenario tried. So the
# split does not rescue a merge; what it buys is an accurate HISTORY - `git log --follow`,
# `git blame -C`, and anything else reading the log get 233 true renames instead of four false ones
# and a fabricated deletion. That is worth the cost of a first commit that does not compile.
#
# TWO COMMITS IS THE ONLY SUPPORTED SHAPE. `--single-commit` is NOT an option to choose: it exists
# solely as the self-test's experiment arm, where it is the negative control proving the mis-pairing
# detector actually fires. On this repo it cannot succeed - the verification catches the four
# invented renames and refuses.
#
# It is rejected for a reason that outlives the measurement above: git's rename detection cannot be
# configured from the repository. There is no `.gitattributes` mechanism for `-M`, `-B`,
# `merge.renameLimit` or `diff.renames`, so any "it is safe if you set X" answer holds on the clone
# that set it and silently does not on CI, on a contributor's machine, or in the next worktree. The
# default behaviour has to be correct, which leaves one shape.
#
# RUNNING THIS ON EVERY OPEN PR BRANCH IS MANDATORY, NOT A CONVENIENCE. Merging a renamed master into
# a PR branch that has NOT been renamed reported ZERO conflicts and silently applied the PR's edit to
# the streams module's ArchUnit test INTO THE MUTINY MODULE'S FILE - the mis-pairing above, playing
# out as data loss with no warning. When both sides have run the script, the same case surfaces as an
# ordinary rename/rename conflict on the right file, with the PR's edit intact, for a human to
# resolve. Loud and correct beats silent and wrong: rename the branch, then merge.
#
# WHY THE VERIFICATION IS NOT OPTIONAL. Git does not RECORD renames, it DETECTS them at read time.
# "We used git mv" is therefore not evidence of anything; the question is whether `git show -M` finds
# them, and that depends on similarity scores and on `diff.renameLimit`, which git exceeds SILENTLY -
# reporting add+delete instead. So the script asserts the rename count equals the moved-file count,
# asserts every pairing is the one the script actually performed, asserts there are no stray
# add/delete pairs, prints the lowest similarity it saw, and prints the rename limits a future MERGE
# will use. And because a check nobody has watched fail is decoration, bin/test-rename-packages.sh
# includes a negative control where a planted stale reference is expected to make the completeness
# check go red.
#
# WHAT IT DELIBERATELY WILL NOT DO
#
# Three sentences in the tree are CLAIMS that become false the moment the rename lands - the README's
# "the Java API and package are unchanged from upstream", and the changelog's "the only required
# change is the Maven groupId" and "the library API is otherwise unchanged from upstream". Rewriting
# them mechanically turns each into a confident false statement in a published artifact, which is
# worse than leaving it stale. So they are PROSE GUARDS: the script stops before touching anything
# and prints what to write instead.
#
# A guard is only as good as the sentence it still matches. astubbs/parallel-consumer#280 inherited
# aa61238a, which rewrote the changelog entry this guard was aimed at and deleted its wording
# outright - the guard then matched nothing and would have gone on reporting "none found" forever.
# Re-point a guard when its prose is reworded; a guard that matches nothing is not a passing check.
#
# `--defer-prose` proceeds, and it is worth being exact about what that means rather than soothing:
# the bulk rewrite then TOUCHES those sentences, so the README's drop-in claim comes out saying
# "the Java API and package (`bz.stub.parallelconsumer`) are unchanged from upstream 0.5.x" - the
# same falsehood, in the new spelling. It is not left alone; it is deferred, and the script lists it
# by file and line under MANUAL FOLLOW-UPS. That is the right trade only where the prose will be
# corrected elsewhere: fix it once on master, and pass the flag on the PR branches, where the
# corrected sentence arrives from master at merge. (The changelog is frozen for the rewrite, so its
# claim is merely left stale.)
#
# BRINGING AN OPEN BRANCH ACROSS
#
# For the author - human or agent - of one of the open PRs. This is the whole procedure. Every step
# is a command to RUN, not a state to recognise. Run them in order, and stop at the first surprise:
# step 8 says what counts as one.
#
# 1. DO I NEED TO DO ANYTHING? Run this rather than reasoning about it:
#
#      git fetch origin
#      git diff --quiet origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh \
#        && echo "CURRENT - skip to step 3" \
#        || echo "STALE - do step 2 first"
#
#    Run it BEFORE you rename anything. After the rename both files legitimately differ from master's
#    copies and the answer stops meaning anything.
#
#    It compares CONTENT rather than testing `-f`, because a branch can hold an OLDER copy of both
#    files - present, out of date, and indistinguishable from current by any existence check.
#
# 2. HOW DO I GET THE SCRIPT?
#
#      git fetch origin
#      git checkout origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh
#      git commit -m "chore: take the rename tooling from master"
#
#    NOT a merge. Merging master into a branch that has not been renamed is the exact operation this
#    script exists to prevent - see RUNNING THIS ON EVERY OPEN PR BRANCH above. `git checkout <ref>
#    -- <paths>` copies those paths into the index and working tree and records NOTHING about <ref>
#    in history: no merge base moves, no other file is touched.
#
#    NOT a cherry-pick. The tooling arrived over eleven non-merge commits, so there is no single
#    commit that contains it.
#
#    A REF, NOT A SHA. `origin/master` always names the tooling that produced master's current
#    layout. A sha has to be published by somebody, rots the moment the tooling is touched again, and
#    a mistyped one silently hands you an older script.
#
#    BOTH FILES. This script never calls the copyright checker - it only edits it - so taking just
#    the script looks sufficient and is not. Without the checker's provenance normalisation every
#    moved upstream file loses its fork-point lookup and its retained Confluent header becomes a
#    violation: 197 of them, measured. They heal the moment master merges in, which is precisely what
#    makes them look like the rename broke something, and gets them "fixed" wrongly.
#
# 3. WHAT DO I RUN?
#
#      bin/rename-packages.sh --dry-run --defer-prose   # read the work set, change nothing
#      bin/rename-packages.sh --defer-prose             # apply: move commit, then content commit
#
#    `--defer-prose` is required on the DRY RUN as well, not only on the applying run: the prose
#    guards are checked BEFORE the dry-run exit, so a bare `--dry-run` aborts with `FAIL:` and never
#    prints the work set. On your branch those sentences are master's to fix - the corrected wording
#    reaches you at the merge in step 4.
#
#    IF IT REFUSES because `io.confluent.csid.asyncconsumer.BrokerPollSystem` has no rule, this is
#    EXPECTED on your branch and it has one right answer. Your branch predates the fix, which lands on
#    master as part of the rename itself - and you cannot merge master to get it, because that is step
#    4 and it must come after the rename. So apply the fix locally, in
#    `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/
#     integrationTests/KafkaSanityTests.java`, replacing the `@link io.confluent.csid.asyncconsumer...`
#    javadoc line with EXACTLY this:
#
#          /**
#           * Exercises {@code pollBrokerForRecords} on
#           * {@link io.confluent.parallelconsumer.internal.BrokerPollSystem}.
#           */
#
#    Verbatim matters. It is character-for-character what master carries, so the step 4 merge sees
#    identical content on both sides and raises no conflict. Improve the wording and you have made
#    work for yourself.
#
#    Then commit it - the next run refuses on a dirty tree otherwise. If the copyright checker then
#    reports that file missing a `Modifications Copyright ... Antony Stubbs and contributors` line, add
#    it: you have just edited an upstream-derived file, and `./mvnw` fails without it.
#
#    Add `--skip-readme-regen` ONLY if you have no JDK. It excuses README.adoc from the completeness
#    check, so the check stops being total; the run lists it under MANUAL FOLLOW-UPS. With a JDK,
#    letting the regen run leaves your README.adoc agreeing with master's, which is one less conflict.
#
# 4. WHAT DO I DO NEXT? Merge master - AFTER the rename, NEVER before.
#
#      git merge origin/master
#
#    The order is the whole point. Renamed-branch meeting renamed-master gives git a rename on each
#    side to pair against. The reverse gives it a rename on one side and edits at the old paths on
#    the other, and that resolves silently and wrongly.
#
# 5. WHAT WILL I SEE? Conflicts - and they are the good outcome, because they are loud:
#      - CONFLICT (rename/delete) and CONFLICT (add/add) on the NEW path, with your edit intact.
#      - Conflicts on the guarded prose sentences. MASTER'S WORDING WINS: discard your side, it is
#        the old false claim mechanically respelt.
#
# 6. WHAT DOES WRONG LOOK LIKE? A merge that reports ZERO conflicts on a branch that skipped step 3.
#    That is not good luck. That is the silent cross-module corruption described above, already
#    applied to your tree. Do not push it. Report it.
#
# 7. WHERE DO I STOP? Rename your branch, merge master in, resolve, commit, and push YOUR PR BRANCH.
#
#      DO NOT merge the PR. DO NOT open a PR. DO NOT touch master.
#
#    Landing is a human decision behind review and CI. It is not part of this procedure, and an
#    instruction that merely omitted it would get supplied by a helpful reader.
#
# 8. WHEN DO I STOP AND ASK? Report and stop - do not use judgement - on any of these:
#      - any refusal at all (dirty tree, or a package with no rule in PKG_MAP)
#      - `mis-paired` reporting anything other than 0
#      - a conflict whose correct resolution is not obvious from step 5
#      - ZERO conflicts where step 5 told you to expect them (that is step 6)
#
# Usage:
#   bin/rename-packages.sh                  # apply and commit (move commit + content commit)
#   bin/rename-packages.sh --dry-run        # report the work set, touch nothing
#   bin/rename-packages.sh --no-commit      # apply to the working tree, do not commit
#   # (--single-commit is not listed here on purpose - it is a self-test arm, not a usage mode)
#   bin/rename-packages.sh --verify-only    # completeness check only, change nothing
#   bin/rename-packages.sh --defer-prose --skip-readme-regen
#
# Exit codes: 0 = applied (or already applied), 1 = a check failed, 2 = refused to start.
#
# Self-test: bin/test-rename-packages.sh

set -euo pipefail

# --------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------

# The rename, in DOT form. Everything else - the path form, the escaped-regex form, the directory
# moves, the scan patterns - is DERIVED from this table, so what is on disk and what is written in
# the files cannot drift apart.
#
# THE SECOND UPSTREAM-OWNED PREFIX IS IN THE TABLE ON PURPOSE, PACKAGE BY PACKAGE. It is a DIFFERENT
# prefix from `io.confluent.parallelconsumer`, and every automation scoped to the latter misses it -
# which is how `META-INF/services/org.junit.platform.launcher.TestExecutionListener` gets left behind
# naming a listener under it, a resource no compiler and no IDE refactor will ever touch. Leaving it
# also defeats the reason for the rename, since it is still upstream's mark inside our namespace.
#
# BOTH of its packages fold INTO the library's internals rather than landing beside them:
#
#   ...csid.utils           -> bz.stub.parallelconsumer.internal.utils
#                              JavaUtils, KafkaUtils, StringUtils, ... - shared plumbing, not a
#                              second product surface, so they belong inside the library.
#   ...csid.testcontainers  -> bz.stub.parallelconsumer.internal.testcontainers
#                              test-only, one class (FilteredTestContainerSlf4jLogConsumer), same
#                              reasoning. It used to take the deleted catch-all, which would have
#                              parked it at a top-level bz.stub.<legacy-token>.testcontainers.
#
# THOSE TWO ARE THE WHOLE OF THAT PREFIX IN THIS TREE, and that is verified rather than asserted:
# `git ls-files | grep -oE '(^|/)io/confluent/csid/[^/]+' | sort -u` returns exactly `utils` and
# `testcontainers`, with no files directly in the parent. Nothing is left for a fallback to catch,
# which is what makes deleting the fallback possible rather than merely desirable.
#
# ORDER. The three rules are mutually DISJOINT prefixes, so this table is order-independent as it
# stands - measured, with a control arm, by reversing it and diffing the result: byte-identical
# (bin/test-rename-packages.sh asserts the same property on the fixture). It was NOT order-
# independent while the catch-all existed, because PKG_MAP is a program applied top to bottom
# against the same text: the general rule ran first, turned the string into its new spelling, and
# every later rule still hunting for the old spelling matched nothing and was silently shadowed.
# Keep writing specific rules above general ones anyway, and keep asserting DESTINATIONS in the
# self-test - a completeness check passes either way, so it cannot see a shadowed rule.
PKG_MAP="\
io.confluent.parallelconsumer|bz.stub.parallelconsumer
io.confluent.csid.utils|bz.stub.parallelconsumer.internal.utils
io.confluent.csid.testcontainers|bz.stub.parallelconsumer.internal.testcontainers"

MODS_HOLDER="Antony Stubbs and contributors"
MODS_YEAR="${RENAME_MODS_YEAR:-$(date +%Y)}"

# The permissive completeness pattern. It matches io.conflu, io/conflu, io\.conflu and ioconflu, and
# it stops at `conflu` so that a misspelling further along cannot hide from it. The habitual
# `io\.confluent` cannot see the backslash form at all, which is the trap this exists to avoid.
SWEEP_ERE='io[\\./]*conflu'

# --------------------------------------------------------------------------------------------------
# The one-off residue report (see THE RESIDUE REPORT IS DELIBERATELY NOT A `bin/check-*.sh` above)
# --------------------------------------------------------------------------------------------------
#
# `name|ERE|a string the pattern MUST match|full or summary`.
#
# POSIX ERE ONLY, AND THE THIRD COLUMN IS WHY. `\b` is not a word boundary here. `git grep -E`
# accepts a pattern containing it, matches NOTHING, and exits 1 - which is byte-for-byte what a
# clean tree looks like. That is not a hypothetical: a sibling branch published a false clean result
# from exactly this. So every pattern is first proven against a string it MUST match; only then is
# the tree swept, and only then does "no hits" mean anything at all.
#
# The fourth column is how much to print. `full` lists every surviving occurrence - these are the
# migration's own targets and there should be almost nothing left. `summary` is for the deliberately
# broad final pattern, which matches every retained copyright notice in the tree; listing those in
# full would bury the findings under several hundred lines the licence REQUIRES us to keep.
RESIDUE_PATTERNS='general-utils-token|csid|io.confluent.csid.utils.StringUtils|full
pre-rename-project-name|asyncconsumer|io.confluent.csid.asyncconsumer.WorkManager|full
upstream-package-prefix|io[\\./]*confluent|import io.confluent.parallelconsumer.ParallelConsumer;|full
upstream-maven-repository|packages[.]confluent[.]io|https://packages.confluent.io/maven/|full
any-upstream-mention|[Cc]onfluent|Copyright (C) 2020-2022 Confluent, Inc.|summary'

# A surviving mention of the upstream organisation is LEGITIMATE in exactly two shapes. Both are
# recognised by PATTERN rather than by a file allow-list, so a file added tomorrow gets the same
# answer as one that exists today:
#
#   a retained copyright notice - Apache 2.0 s4(b) REQUIRES these to stay. Removing one is a licence
#                                 violation dressed up as tidying.
#   a reference to upstream's repository, issues or PRs - those name history, and history stays true.
#                                 Matched on the ORGANISATION name rather than on `org/repo` or
#                                 `org#123`, because this repo's own citation rule (AGENTS.md, the
#                                 issue-reference gate) writes them as `confluentinc PR #548` and
#                                 `confluentinc issue #857` - the two forms a punctuation-anchored
#                                 pattern would have filed as findings, burying the real ones.
#
# Anything else is a FINDING: printed, counted, and left for a human. NOT an error - the vendor's
# name legitimately appears in prose, in links to its blog and documentation, and in a module that
# still resolves against its package repository. The report does not gate the run; the completeness
# check above it is the gate.
RESIDUE_EXPECTED_ERE='Copyright \(C\).*Confluent|Confluent, Inc|confluentinc'

# `substring|why` - occurrences a human has already decided to keep, named individually so they read
# as decisions rather than as noise the report failed to classify. Justify every addition here.
#
#   confluentinc/cp-kafka   the LIVE integration-test broker image. Swapping it for another vendor's
#                           image is a testing-infrastructure change with its own risk, tracked
#                           separately; bundling it into a mechanical rename would put the
#                           integration suite at risk inside a refactor.
RESIDUE_KNOWN='confluentinc/cp-kafka|the live integration-test broker image, tracked separately - not part of a mechanical rename'

# TWO LISTS, AND THEY ARE NOT THE SAME LIST. Conflating them is how a generated file gets quietly
# excused from the check that would have caught it being stale.
#
# FROZEN_PREFIXES - never touched by the bulk rewrite. Some of these are still IN SCOPE for the
# completeness check, because something else is responsible for making them correct:
#
#   README.adoc         GENERATED from src/docs/README_TEMPLATE.adoc at process-sources, so it must
#                       never be hand-edited - but it must still come out CLEAN, which is what proves
#                       the regeneration actually ran. Checked.
#   docs/todo-index.md  GENERATED by bin/todo-index.sh, gated by `--check` in CI. Same: not rewritten,
#                       but regenerated and then checked.
#   NOTICE, LICENSE     Apache 2.0 s4(a) and s4(d). Untouched, always. They contain no `io.confluent`
#                       today, and if they ever do a human should decide, so they stay checked.
#   .semaphore/         legacy Confluent internal CI, retained but inactive on the fork. Also checked,
#                       for the same reason: it holds no reference today.
FROZEN_PREFIXES="\
CHANGELOG.adoc
README.adoc
NOTICE
LICENSE
docs/plans/
docs/solutions/
docs/inflight/
docs/todo-index.md
.semaphore/
bin/check-copyright-headers.sh
bin/test-check-copyright-headers.sh"

# SWEEP_EXCLUDE - the narrow set the completeness check is allowed to ignore, because `io.confluent`
# survives there LEGITIMATELY. The check prints every match it skipped in each of these, so the
# exclusion is auditable rather than a silent hole. Justify any addition, in writing, here:
#
#   CHANGELOG.adoc      release notes. The `=== Breaking` entry NAMES the old Maven coordinate as
#                       history and must keep saying so. AGENTS.md separately forbids a PR editing
#                       this file at all, bar correcting a claim that has become false - which is
#                       what the PROSE_GUARDS below are for, a human's job, not something to sweep.
#   docs/plans/         dated plan, investigation and solution records, and the in-flight notes. They
#   docs/solutions/     said `io.confluent` because that is what was true when they were written, and
#   docs/inflight/      rewriting them makes them say something they did not say. The plan is
#                       explicit that the answer for everything under docs/plans/ is "leave it".
#   check-copyright-    its RENAMED_FROM_UPSTREAM entries are `newpath|oldpath-at-fork-point` pairs,
#   headers.sh          and the oldpath side names a path in the UPSTREAM tree. Rewrite it and
#                       `git cat-file blob $FORK_POINT:$oldpath` stops resolving - which fails by
#                       deciding the file is fork-original, turning a REQUIRED Confluent header into
#                       a violation. A targeted edit moves only the newpath half, so the oldpath
#                       halves legitimately still read io/confluent, and the check prints them all.
#                       Its PACKAGE_MOVES table names BOTH spellings for the same reason: it is the
#                       rule that maps a moved file back to its fork-point path.
#   test-check-         its fixtures move a file from io/confluent to bz/stub and assert the scanner
#   copyright-          still resolves provenance across the move. That fixture IS the negative
#   headers.sh          control for the rule above, so rewriting the old spelling out of it does not
#                       update a test, it deletes one - and the deletion reads as a pass.
#   AGENTS.md           both DESCRIBE the rename rather than reference the package: the in-flight
#   repo-hygiene.yml    section says the fork is "moving io.confluent.* to bz.stub.*", and both
#                       quote the sweep pattern `grep -rn "io\.confluent"` as the trap it is.
#                       Neither matches any rewrite rule (they are not whole package names), and
#                       rewriting them would turn each sentence into nonsense - "moving bz.stub.*
#                       to bz.stub.*". They are NOT frozen, so a real package reference added to
#                       either is still rewritten; this only silences the residue, and the check
#                       prints it.
# README.adoc is deliberately NOT here. It is generated, and the first instinct is to excuse it on the
# grounds that its template is the checked artefact - but the generation step resolves `include::`
# directives rather than rendering to HTML, so the freeze markers below survive into the generated file
# and protect exactly the lines they protect in the template. Verified, not assumed. Excusing it would
# have dropped a real check over ~1000 lines to save a mechanism that already works.
SWEEP_EXCLUDE="\
CHANGELOG.adoc
docs/plans/
docs/solutions/
docs/inflight/
bin/check-copyright-headers.sh
bin/test-check-copyright-headers.sh
AGENTS.md
.github/workflows/repo-hygiene.yml"

# FREEZE REGIONS - the line-level exemption, for text that must deliberately name the OLD package
# inside a file that is otherwise rewritten.
#
# The whole-file lists above cannot express this. Upgrade instructions are the case that forces it:
# "the packages move FROM io.confluent.parallelconsumer TO bz.stub.parallelconsumer", and the sed
# one-liner a reader runs, are only useful while they still say the old name. The bulk rewrite turns
# both into a statement that a package moves to itself and a sed that does nothing - MEASURED, and it
# passed the completeness check, the rename pairing and the copyright check, because the sweep hunts
# old spellings that SHOULD have been rewritten and this is one that must NOT be. It is structurally
# invisible to every gate this script has.
#
# Freezing README_TEMPLATE.adoc wholesale would be wrong: a real package reference added to it later
# must still be rewritten. So the exemption is a REGION, not a file, and it is visible in the file it
# applies to rather than in a list somewhere else.
#
#     // rename-packages: freeze-begin(<id>) - <why this text must keep the old spelling>
#     ...
#     // rename-packages: freeze-end(<id>)
#
# Any line-comment syntax works; the markers are matched as text. Both the rewrite and the
# completeness check honour them.
#
# THE ID IS REQUIRED, AND COUNTING MARKERS IS NOT ENOUGH. An earlier version balanced markers with a
# single open/closed flag, which is defeated by TWO INDEPENDENT authoring mistakes in one file: forget
# one freeze-end, leave an unrelated stray freeze-end further down, and the sequence reads
# begin/end/begin/end - perfectly balanced. Everything between the orphaned begin and the unrelated end
# then joins the frozen set that BOTH the rewrite and the check consume, so a live io.confluent
# reference in that span is never rewritten and never reported, while the run prints "every freeze
# region opens and closes" and "no stale references outside the excluded set". MEASURED, on this script.
#
# So a freeze-end must name the region it closes. Two markers cannot pair unless they were written as a
# pair, and an id that closes nothing, or closes the wrong region, is a hard refusal.
#
# A MARKER LINE MAY NOT CARRY A PACKAGE REFERENCE either. Both the line-set builder and the rewrite
# treat the whole marker line as frozen, so a reference written as a trailing note on the freeze-end
# line would be silently exempt with no region to audit it against.
#
# An unclosed region is refused for the original reason: it would exempt the rest of its file from both
# the rewrite and the check, and the check would then report clean over text it never looked at - the
# same class of quiet hole as a sweep pattern that matches nothing.
FREEZE_BEGIN_ERE='rename-packages:[[:space:]]*freeze-begin'
FREEZE_END_ERE='rename-packages:[[:space:]]*freeze-end'
# The parens are bracket expressions, NOT `\(` `\)`, and that is not style. This regex reaches awk
# through `-v`, which runs C-string escape processing over the value first, and `\(` is not a defined
# escape - POSIX leaves it implementation-defined. mawk 1.3.4 keeps the backslash, so the regex arrives
# intact; gawk drops it, so the literal parens become GROUPS, `match()` never fires, every region's id
# reads as empty, and a correctly written freeze region is refused with "must name its region". That is
# eight self-test failures on a machine whose only difference is which awk is installed - the whole
# freeze feature, dead, on an environment the author never sees. A bracket expression carries no
# backslash, so nothing is left for -v to eat.
FREEZE_ID_ERE='freeze-(begin|end)[(]([A-Za-z0-9_.-]+)[)]'

# Excluded from both the rewrite and the completeness check, because they must carry the old spelling
# as DATA. Matched on BASENAME, not on a hardcoded path, so moving or renaming this script cannot
# silently switch the exclusion off - the lesson bin/check-shell-sigpipe.sh already learned.
SELF_BASENAMES="\
rename-packages.sh
test-rename-packages.sh"

# Claims a mechanical rewrite would turn into confident falsehoods:
#   `path | claim-pattern | corrected-pattern | what to write instead`
#
# FORWARD-COMPATIBLE BY DESIGN, because these are three known outliers, not a mechanism with a
# lifecycle. Each entry carries BOTH spellings, so the same declaration is correct before the sentence
# is corrected and after it - on any branch, in either order, with no retirement step and no second
# list to keep in sync. Three states:
#
#   claim present                  the claim is live      -> refuse, or defer and list it
#   claim absent, corrected present already corrected     -> pass, and it must STAY corrected
#   claim absent, corrected absent reworded into something nobody declared -> REFUSE and name it
#
# The third state is the one that used to pass silently: a guard whose sentence was reworded rather
# than corrected matches nothing, and "none found" reads exactly like a clean tree.
#
# DELIBERATELY EMPTY, and that is not the same as the parser having drifted. All three original guards
# are RETIRED: their sentences were corrected in the same change-set that landed the rename, using the
# wording plan s8 pre-drafted, so there is no longer a false claim for them to catch. A guard that
# matches nothing is not a passing check - this file says so about itself above - so retiring them was
# the required move rather than an optional tidy-up.
#
# The MECHANISM stays. The next claim a mechanical rewrite would falsify goes here in the same
# `path|ERE|what to write instead` form, and --defer-prose keeps working for it.
PROSE_GUARDS="\
src/docs/README_TEMPLATE.adoc|drop-in replacement.*package.*are unchanged|Java packages \*move\*|The drop-in claim stops being TRUE and must not merely be qualified. Plan s8 drafts the replacement: say the packages MOVE from io.confluent.parallelconsumer to bz.stub.parallelconsumer, that the API itself is unchanged, and give the one-line sed under == Upgrading.
CHANGELOG.adoc|only required change is the Maven groupId|two changes are required|The rename adds a second required change - every import moves - so this becomes a factual error the moment it lands. AGENTS.md allows exactly one changelog edit in a PR: correcting an existing claim that is now false. Rewrite that sentence. Do NOT add a new entry - the 0.6.0.0 section is generated at release time from the commit log.
CHANGELOG.adoc|library API is otherwise unchanged|Rewrite your imports|Same class as the sentence above, in the === Breaking bullet: after the rename the caller's code changes too, so 'otherwise unchanged' reads as a promise the release does not keep. Correct it in place."

# Rename detection is a similarity matrix over unmatched paths, and git gives up above
# diff.renameLimit with a warning that is easy to miss, then reports add+delete. 264 files move
# today, so the default of 1000 is not truncating - but the assertion must not DEPEND on ambient
# config, so verification pins its own generous limit and separately reports the repo's.
VERIFY_RENAME_LIMIT=65535

HEADER_WINDOW=8 # lines from the top of a file searched for copyright notices, as in the checker

# --------------------------------------------------------------------------------------------------
# Options
# --------------------------------------------------------------------------------------------------

APPLY=true
DO_COMMIT=true
SPLIT_COMMITS=true # measured default - see COMMIT SHAPE in the header
DEFER_PROSE=false
REGEN_README=true
VERIFY_ONLY=false
SWEEP_EXCLUDE_EXTRA=""

usage() {
    cat <<'USAGE'
bin/rename-packages.sh - apply the io.confluent.* -> bz.stub.* package rename to this branch.

By default it makes TWO commits: a pure `git mv` move, then the content edits. Measured - squashing
them makes git mis-pair the five near-identical TestConventionsArchTest.java files across modules and
lose one rename outright.

  --dry-run             report the work set and exit; change nothing
  --no-commit           apply to the working tree, leave it uncommitted
  --single-commit       SELF-TEST ARM ONLY, not a usage mode. Rejected shape: git mis-pairs the
                        renames and the run will refuse. See COMMIT SHAPE in the header.
  --defer-prose         proceed past the prose guards. The guarded sentences are then REWRITTEN
                        mechanically (same false claim, new spelling) and listed as follow-ups
  --skip-readme-regen   do not run ./mvnw -N process-sources for README.adoc
  --verify-only         run the completeness check only
  -h, --help            this text

BRINGING AN OPEN PR BRANCH ACROSS - the short form. The full eight steps, with the reason behind each,
are in the BRINGING AN OPEN BRANCH ACROSS section of this file's header. Read them before step 2.

  1. Need to do anything? Run it, do not guess - and run it BEFORE you rename:
       git fetch origin
       git diff --quiet origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh \
         && echo "CURRENT - skip to 3" || echo "STALE - do 2 first"
  2. Get the tooling. A file checkout, NOT a merge and NOT a cherry-pick. BOTH files:
       git checkout origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh
  3. Rename your branch (--defer-prose is required on the dry run too, or it aborts unread):
       bin/rename-packages.sh --dry-run --defer-prose
       bin/rename-packages.sh --defer-prose
  4. THEN merge master, never before:  git merge origin/master
  5. Expect conflicts on the new path with your edit intact, and on the guarded prose - master wins.
  6. ZERO conflicts after skipping step 3 is silent corruption, not luck. Report it.
  7. Push YOUR PR BRANCH. Do NOT merge the PR, do NOT open a PR, do NOT touch master.
  8. Any refusal, any non-zero `mis-paired`, or any unclear conflict: STOP and report. Do not improvise.

Read the header of this file for the measurement behind the default, for why running this on every
open PR branch is mandatory rather than convenient, and for the two references a naive
find-and-replace cannot see. Self-test: bin/test-rename-packages.sh
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)           APPLY=false ;;
        --no-commit)         DO_COMMIT=false ;;
        --single-commit)     SPLIT_COMMITS=false ;;
        --defer-prose) DEFER_PROSE=true ;;
        --skip-readme-regen) REGEN_README=false ;;
        --verify-only)       VERIFY_ONLY=true ;;
        -h|--help)           usage; exit 0 ;;
        *) echo "unknown option: $1 (try --help)" >&2; exit 2 ;;
    esac
    shift
done

cd "$(git rev-parse --show-toplevel)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

MOVES="$TMP/moves.tsv"
REWRITES="$TMP/rewrites.txt"
MANUAL_FOLLOWUPS="$TMP/manual.txt"
: > "$MANUAL_FOLLOWUPS"

note_manual() { printf '  - %s\n' "$1" >> "$MANUAL_FOLLOWUPS"; }
section()     { echo; echo "== $*"; }
die()         { echo; echo "FAIL: $*" >&2; exit 1; }

count_lines() { # <file>
    local n
    n=$(wc -l < "$1")
    printf '%s' "${n//[[:space:]]/}"
}

# --------------------------------------------------------------------------------------------------
# The substitution program - built once from PKG_MAP, used for BOTH file paths and file contents
# --------------------------------------------------------------------------------------------------
#
# One program, two consumers, so a path and the string that names it can never be rewritten
# differently. Perl rather than sed because sed's -i differs between GNU and BSD and this has to run
# unchanged on a maintainer's Mac and on a CI runner.
build_perl_program() {
    local prog='' old new esc_re esc_rep dot_re path_re path_rep i n
    local ifs_save

    # Normalise the known misspelling FIRST, so the rules below stay clean and so the typo is fixed
    # rather than carried across the rename. `.idea/runConfigurations/All_examples.xml` shipped
    # `io.confluent.parallalconsumer.examples.core.*` and had been broken long before this rename.
    prog+='s{(confluent[\\]?[./])parallalconsumer}{$1parallelconsumer}g;'

    while IFS='|' read -r old new; do
        [ -n "$old" ] || continue

        ifs_save="$IFS"
        IFS='.'
        # shellcheck disable=SC2206 # deliberate word splitting on IFS='.'
        local oldseg=($old)
        # shellcheck disable=SC2206
        local newseg=($new)
        IFS="$ifs_save"
        n=${#oldseg[@]}

        # (1) The ESCAPED-REGEX form: the literal characters io\.confluent\.parallelconsumer, as they
        #     appear inside a grep/ERE pattern in a shell script. Invisible to a plain replace, and
        #     invisible to the sweep people habitually verify with. The backslash run is CAPTURED and
        #     replayed, so both `io\.x` and `io\\.x` survive with their own escaping intact; a
        #     mismatched pair falls through to the completeness check rather than being mangled.
        esc_re="${oldseg[0]}"
        esc_rep="${newseg[0]}"
        for ((i = 1; i < n; i++)); do
            if [ "$i" -eq 1 ]; then esc_re+='(\\+)\.'; else esc_re+='\1\.'; fi
            esc_re+="${oldseg[$i]}"
            esc_rep+='$1.'"${newseg[$i]}"
        done
        prog+="s{\\b${esc_re}}{${esc_rep}}g;"

        # (2) The plain dotted form: package declarations, imports, ArchUnit @AnalyzeClasses strings,
        #     the beAssignableTo FQCN, logback logger names, the truth-generator <class> list,
        #     entryPointClassPackage, PIT target packages, the META-INF/services listener.
        dot_re=${old//./\\.}
        prog+="s{\\b${dot_re}}{${new}}g;"

        # (3) The path form: javadoc links, README includes, workflow comments, docs/todo-index.md.
        #     Named by file, never by the marker word it is named after: bin/todo-index.sh scans
        #     this script, and its matcher exempts the hyphenated spelling but not the bare one, so
        #     writing that word in prose here indexes work that does not exist and fails --check.
        #     Its own header calls that the index-of-work trap.
        path_re=${old//./\/}
        path_rep=${new//./\/}
        prog+="s{\\b${path_re}}{${path_rep}}g;"
    done <<EOF
$PKG_MAP
EOF

    printf '%s' "$prog"
}

PERL_PROG="$(build_perl_program)"

# Which tracked paths sit under an old package directory - derived from the same table.
build_path_scan_ere() {
    local ere='' old dir
    while IFS='|' read -r old _; do
        [ -n "$old" ] || continue
        # Not `${old//./\/}` here: that expansion sits inside double quotes below, where bash keeps
        # the backslash and emits `io\/confluent`, which is undefined in POSIX ERE even though GNU
        # and BSD grep happen to accept it.
        dir=$(tr '.' '/' <<<"$old")
        [ -n "$ere" ] && ere+='|'
        ere+="(^|/)${dir}/"
    done <<EOF
$PKG_MAP
EOF
    printf '%s' "$ere"
}

PATH_SCAN_ERE="$(build_path_scan_ere)"

# The legacy prefix the refusal below is written against: the longest dotted prefix that EVERY
# old-side entry shares. Derived, not spelt out again - PATH_SCAN_ERE only knows the packages the
# table names, so it is structurally incapable of noticing one the table has MISSED, and a second
# hardcoded copy of the prefix would be the thing that goes stale when a rule is added.
build_legacy_prefix() {
    local old common="" seen=0
    while IFS='|' read -r old _; do
        [ -n "$old" ] || continue
        if [ "$seen" -eq 0 ]; then common="$old"; seen=1; continue; fi
        # Shorten the candidate a segment at a time until it is a prefix of this entry too. The
        # trailing dot on both sides stops `io.confluentx` counting as a match for `io.confluent`.
        while [ -n "$common" ]; do
            case "${old}." in "${common}".*) break ;; esac
            case "$common" in
                *.*) common="${common%.*}" ;;
                *)   common="" ;;
            esac
        done
    done <<EOF
$PKG_MAP
EOF
    printf '%s' "$common"
}

LEGACY_DOT_PREFIX="$(build_legacy_prefix)"
[ -n "$LEGACY_DOT_PREFIX" ] || { echo "PKG_MAP entries share no common prefix - cannot derive the legacy prefix" >&2; exit 2; }
LEGACY_PATH_PREFIX="$(tr '.' '/' <<<"$LEGACY_DOT_PREFIX")"
# Every separator becomes the same permissive class SWEEP_ERE uses, so the one pattern sees the
# dotted form, the path form AND the escaped-regex form - the three spellings the rewrite itself
# handles. Anything this matches after the rewrite has run is, by definition, unmapped.
LEGACY_SCAN_ERE="$(sed 's#\.#[\\\\./]*#g' <<<"$LEGACY_DOT_PREFIX")"
LEGACY_TOKEN_ERE="${LEGACY_SCAN_ERE}"'([\\./]+[A-Za-z0-9_]+)*'

is_self() { # <path> - matched on BASENAME so a move cannot disable the exclusion
    local b base
    b="$(basename "$1")"
    while IFS= read -r base; do
        [ -n "$base" ] || continue
        [ "$b" = "$base" ] && return 0
    done <<EOF
$SELF_BASENAMES
EOF
    return 1
}

has_prefix_in() { # <path> <newline-separated prefix list>
    local p="$1" prefix
    while IFS= read -r prefix; do
        [ -n "$prefix" ] || continue
        case "$p" in "$prefix"*) return 0 ;; esac
    done <<EOF
$2
EOF
    return 1
}

is_frozen() { # <path> - excluded from the bulk rewrite
    has_prefix_in "$1" "$FROZEN_PREFIXES" && return 0
    is_self "$1"
}

is_sweep_excluded() { # <path> - excluded from the COMPLETENESS CHECK. Deliberately a shorter list.
    has_prefix_in "$1" "$SWEEP_EXCLUDE" && return 0
    # Set at runtime by --skip-readme-regen: a README nobody regenerated is stale by construction, so
    # excusing it is a decision the operator made explicitly, and it is recorded as a follow-up.
    [ -n "${SWEEP_EXCLUDE_EXTRA:-}" ] && has_prefix_in "$1" "$SWEEP_EXCLUDE_EXTRA" && return 0
    is_self "$1"
}

frozen_lines() { # <file> -> line numbers inside a freeze region, markers included
    awk -v b="$FREEZE_BEGIN_ERE" -v e="$FREEZE_END_ERE" '
        $0 ~ b { f = 1; print NR; next }
        $0 ~ e { f = 0; print NR; next }
        f      { print NR }
    ' "$1"
}

# The frozen-line set as an awk BEGIN block, shared verbatim by both filters below instead of being
# copy-pasted into each. They differ only in which field carries the line number, and the thing that
# must never happen is the rewrite and the completeness check disagreeing about what is frozen - so
# they read the set from one place.
AWK_FROZEN_SET='BEGIN { n = split(frozen, a, "\n"); for (i = 1; i <= n; i++) if (a[i] != "") fz[a[i]] = 1 }'

has_freeze_marker() { # <file> - cheap gate, so the line-set scan runs only where it can matter
    grep -qE "$FREEZE_BEGIN_ERE" "$1" 2>/dev/null
}

# An unclosed freeze-begin exempts everything after it, and a stray freeze-end silently un-exempts the
# text above it. Either way the file stops being checked and nothing says so - so this refuses rather
# than reporting a clean sweep it did not actually perform.
validate_freeze_markers() {
    local f bad=0
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        # is_self, for the same reason every other whole-tree scan in this file does it: this script
        # documents the marker syntax and the self-test embeds deliberately malformed markers as fixture
        # TEXT. Without the filter those literals are read as real regions, and the tree currently
        # validates only because two unrelated fixtures happen to cancel out - luck, not design.
        is_self "$f" && continue
        awk -v b="$FREEZE_BEGIN_ERE" -v e="$FREEZE_END_ERE" -v idre="$FREEZE_ID_ERE" \
            -v sweep="$SWEEP_ERE" -v path="$f" '
            function id_of(line) {
                if (match(line, idre)) {
                    seg = substr(line, RSTART, RLENGTH)
                    sub(/^freeze-(begin|end)[(]/, "", seg)
                    sub(/[)]$/, "", seg)
                    return seg
                }
                return ""
            }
            $0 ~ b || $0 ~ e {
                # The marker line is frozen whole, by both the rewrite and the check, so a reference
                # riding along on it would be exempt with nothing to audit it against.
                if ($0 ~ sweep) {
                    print "      " path ":" NR ": marker line also carries a package reference"; rc = 1
                }
            }
            $0 ~ b {
                this = id_of($0)
                if (this == "") { print "      " path ":" NR ": freeze-begin must name its region, as freeze-begin(<id>)"; rc = 1 }
                if (open) { print "      " path ":" NR ": freeze-begin(" this ") while (" openid ") is still open"; rc = 1 }
                open = 1; openid = this; openline = NR
                next
            }
            $0 ~ e {
                this = id_of($0)
                if (!open) { print "      " path ":" NR ": freeze-end(" this ") closes nothing"; rc = 1 }
                else if (this != openid) {
                    print "      " path ":" NR ": freeze-end(" this ") does not close freeze-begin(" openid ") from line " openline; rc = 1
                }
                open = 0; openid = ""
                next
            }
            END { if (open) { print "      " path ":" openline ": freeze-begin(" openid ") is never closed"; rc = 1 } exit rc }
        ' "$f" || bad=1
    done < <(git grep -lIE "$FREEZE_BEGIN_ERE|$FREEZE_END_ERE" -- . || true)

    if [ "$bad" -ne 0 ]; then
        die "unbalanced freeze markers, listed above. A freeze region that is not closed exempts the
     rest of its file from BOTH the rewrite and the completeness check, and the check would then
     report clean over text it never looked at. Close the region, or delete the stray marker."
    fi
}

# --------------------------------------------------------------------------------------------------
# Discovery - always from the TREE, never from a manifest, so a PR's new files are picked up too
# --------------------------------------------------------------------------------------------------

discover_moves() {
    : > "$MOVES"
    git ls-files | { grep -E "$PATH_SCAN_ERE" || true; } > "$TMP/old-paths.txt"
    [ -s "$TMP/old-paths.txt" ] || return 0
    perl -pe "$PERL_PROG" < "$TMP/old-paths.txt" > "$TMP/new-paths.txt"
    paste "$TMP/old-paths.txt" "$TMP/new-paths.txt" | awk -F'\t' '$1 != $2' > "$MOVES"
}

discover_rewrites() {
    : > "$REWRITES"
    local f
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_frozen "$f" && continue
        # A file whose ONLY matches sit in freeze regions has nothing rewritable in it, and counting it
        # anyway is not cosmetic: on an already-renamed tree it keeps n_rewrites above zero, so the run
        # skips the "already applied, nothing to do" exit, rewrites nothing, and then fails at `git
        # commit` with nothing to commit. Re-running is the property the whole fan-out rests on, and it
        # would have reported failure on every branch that carries migration prose.
        has_rewritable_match "$f" || continue
        printf '%s\n' "$f" >> "$REWRITES"
    done < <(git grep -lIE "$SWEEP_ERE" -- . || true)
}

has_rewritable_match() { # <file> - true when at least one SWEEP_ERE match is OUTSIDE a freeze region
    has_freeze_marker "$1" || return 0
    awk -v frozen="$(frozen_lines "$1")" -v ere="$SWEEP_ERE" "$AWK_FROZEN_SET"'
        $0 ~ ere && !(NR in fz) { found = 1; exit }
        END { exit !found }
    ' "$1"
}

# --------------------------------------------------------------------------------------------------
# Preflight: is every legacy package actually MAPPED?
# --------------------------------------------------------------------------------------------------
#
# This is the refusal that replaced the deleted catch-all rule. It runs BEFORE anything moves,
# because the alternative - discovering it afterwards - means a few hundred files have already been
# moved and committed around the package nobody had a rule for.
#
# TWO ARMS, BECAUSE THE TWO FAILURES LOOK NOTHING ALIKE.
#
#   paths       An unmapped package DIRECTORY is invisible to discover_moves(), which globs by the
#               table: its files simply do not move, and no counter goes down. The tree ends up with
#               a live package under the old root that nothing reports.
#   references  An unmapped package NAME in text (a logger name, an ArchUnit string, a services
#               resource) is not rewritten. The completeness check does catch this one - but only
#               after the run, and it names the FILE. This arm predicts it beforehand and names the
#               PACKAGE, which is the thing a human has to make a decision about.
#
# The reference arm predicts by construction rather than by reasoning: it takes the lines that
# mention the legacy prefix, runs the SAME substitution program the rewrite will run, and asks what
# still matches. There is no second model of the rename to drift from the first one.
check_unmapped_legacy() {
    local n_paths n_refs f

    git ls-files | { grep -E "(^|/)${LEGACY_PATH_PREFIX}/" || true; } > "$TMP/legacy-paths.txt"
    : > "$TMP/unmapped-paths.txt"
    if [ -s "$TMP/legacy-paths.txt" ]; then
        perl -pe "$PERL_PROG" < "$TMP/legacy-paths.txt" > "$TMP/legacy-paths-mapped.txt"
        paste "$TMP/legacy-paths.txt" "$TMP/legacy-paths-mapped.txt" |
            awk -F'\t' -v ere="(^|/)${LEGACY_PATH_PREFIX}/" '$2 ~ ere { print $1 }' \
            > "$TMP/unmapped-paths.txt"
    fi

    # Only files that WILL be rewritten and ARE policed afterwards. A file the completeness check
    # excuses - a dated plan, the changelog, this script, the provenance manifest - is supposed to
    # keep the old spelling, so predicting a survivor there would be a false alarm, and a guard that
    # cries wolf gets bypassed.
    : > "$TMP/predict-set.txt"
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_sweep_excluded "$f" && continue
        printf '%s\n' "$f" >> "$TMP/predict-set.txt"
    done < "$REWRITES"

    : > "$TMP/unmapped-refs.txt"
    if [ -s "$TMP/predict-set.txt" ]; then
        : > "$TMP/legacy-refs.txt"
        tr '\n' '\0' < "$TMP/predict-set.txt" |
            xargs -0 git grep -nIE "$LEGACY_SCAN_ERE" -- >> "$TMP/legacy-refs.txt" || true
        # Split each `path:line:text` hit, rewrite the TEXT ONLY, and keep the hit only if the TEXT
        # still names the legacy prefix afterwards.
        #
        # The split is load-bearing, and getting it wrong fails in the direction that looks like
        # working code: the PATH of a hit is itself full of the legacy prefix (that is where these
        # files live), so testing the whole `path:line:text` line reports every mapped reference in
        # a moved file as unmapped. The address is kept in its ORIGINAL spelling on purpose - it is
        # where the operator has to go and look, and that file has not moved yet.
        LEGACY_ERE="$LEGACY_SCAN_ERE" perl -ne '
            chomp;
            if (/^(.*?):([0-9]+):(.*)$/) {
                my ($p, $l) = ($1, $2);
                $_ = $3;
                '"$PERL_PROG"'
                print "$p:$l:$_\n" if /$ENV{LEGACY_ERE}/;
            }
        ' < "$TMP/legacy-refs.txt" > "$TMP/unmapped-refs.txt"
    fi

    n_paths=$(count_lines "$TMP/unmapped-paths.txt")
    n_refs=$(count_lines "$TMP/unmapped-refs.txt")

    if [ "$n_paths" -eq 0 ] && [ "$n_refs" -eq 0 ]; then
        echo "  legacy prefix      ${LEGACY_DOT_PREFIX}   (derived from PKG_MAP, not hardcoded)"
        echo "  VERDICT            every legacy package in this tree is named by a rule"
        return 0
    fi

    echo "  legacy prefix      ${LEGACY_DOT_PREFIX}   (derived from PKG_MAP, not hardcoded)"
    echo
    echo "  PACKAGES WITH NO RULE:"
    # The DIRECTORY of an unmapped file, and the TEXT of an unmapped reference. Not the reference's
    # address - a hit's own path is full of the legacy prefix, and feeding it in here would name the
    # file's package as if it were the thing with no rule.
    : > "$TMP/unmapped-subjects.txt"
    if [ "$n_paths" -gt 0 ]; then
        sed 's#/[^/]*$##' "$TMP/unmapped-paths.txt" >> "$TMP/unmapped-subjects.txt"
    fi
    sed 's#^[^:]*:[0-9]*:##' "$TMP/unmapped-refs.txt" >> "$TMP/unmapped-subjects.txt"
    { grep -oE "$LEGACY_TOKEN_ERE" "$TMP/unmapped-subjects.txt" || true; } |
        sort -u | sed 's/^/      /'

    if [ "$n_paths" -gt 0 ]; then
        echo
        echo "  unmapped FILES (they would not have moved at all):"
        sed 's/^/      /' "$TMP/unmapped-paths.txt"
    fi
    if [ "$n_refs" -gt 0 ]; then
        echo
        echo "  unmapped REFERENCES (shown at their address, with the text as it would survive):"
        sed 's/^/      /' "$TMP/unmapped-refs.txt"
    fi

    die "the packages named above match the legacy prefix and NO rule in PKG_MAP maps them.
     There is no catch-all any more, on purpose: the one that used to absorb these mapped them
     onto a same-named prefix under the new root, so the token the rename exists to remove
     survived the rename by design. Decide what each package should become and add an explicit
     rule - or, where the reference is configuration naming a package that will no longer exist,
     delete the reference rather than carrying dead configuration into the new namespace.
     Do NOT add a general fallback to make this message go away."
}

# --------------------------------------------------------------------------------------------------
# The residue report - a one-off, for judgement. NOT a gate, and NOT a standing check.
# --------------------------------------------------------------------------------------------------

RESIDUE_LIVENESS="$TMP/residue-liveness.txt"
RESIDUE_PRE="$TMP/residue-pre.tsv"

# Proven ONCE, early, and recorded - the report at the end prints the recording. Running it early is
# the point: a dead pattern is discovered before the tree is transformed, not after, when the only
# evidence of what the tree used to contain has already been rewritten.
#
# PROVEN WITH `git grep`, NOT WITH `grep`, AND THAT IS NOT PEDANTRY. They are different regex
# engines, and whether they agree about a given construct depends on the machine. \b is the case
# that caused the false clean result: on a BSD box `grep -qE '\bcsid\b'` MATCHES while
# `git grep -cE '\bcsid\b'` over the same tree returns NOTHING and exits 1 - so proving the pattern
# with grep certifies it live and then the sweep runs on the engine that cannot match it, a
# liveness check that manufactures the false negative it exists to catch. On glibc the two agree
# (both match), which is why that specific disagreement cannot be relied on to demonstrate the
# hazard - see the \d control in the self-test - but the rule survives the platform difference:
# prove each pattern with the engine that will sweep with it.
# The sample is written to a scratch file so `git grep --no-index` can be pointed at it, which is
# the only way to reach that engine without a tracked file.
residue_prove_patterns() {
    local name ere sample _mode dead=0
    : > "$RESIDUE_LIVENESS"
    while IFS='|' read -r name ere sample _mode; do
        [ -n "$name" ] || continue
        printf '%s\n' "$sample" > "$TMP/residue-sample.txt"
        if git -C "$TMP" grep --no-index -qE "$ere" -- residue-sample.txt; then
            printf '      %-26s %-26s live\n' "$name" "$ere" >> "$RESIDUE_LIVENESS"
        else
            printf '      %-26s %-26s DEAD - it does not even match its own sample\n' \
                "$name" "$ere" >> "$RESIDUE_LIVENESS"
            dead=$((dead + 1))
        fi
    done <<EOF
$RESIDUE_PATTERNS
EOF
    if [ "$dead" -gt 0 ]; then
        cat "$RESIDUE_LIVENESS" >&2
        die "${dead} residue pattern(s) match nothing at all, so a sweep with them would report a
     clean tree no matter what is in it. The usual cause is a PCRE-ism in a POSIX ERE - \\b in
     particular is NOT a word boundary to git grep; it accepts the pattern, matches nothing, and
     exits 1, which is indistinguishable from a tree that has nothing left to find. Note that
     plain grep -E may well MATCH the same pattern, so testing it in a shell proves nothing."
    fi
}

# Captured BEFORE anything is transformed. A pattern is allowed to be at zero here - that is the
# goal state for a package already cleaned up - but the TOTAL is not, because a total of zero means
# the sweep as a whole is measuring nothing, and "nothing to find" is indistinguishable from
# "cannot find anything".
residue_capture_pre_counts() {
    local name ere _sample _mode n total=0
    residue_prove_patterns
    : > "$RESIDUE_PRE"
    while IFS='|' read -r name ere _sample _mode; do
        [ -n "$name" ] || continue
        n=$( { git grep -cIE "$ere" -- . || true; } | awk -F: '{ s += $NF } END { print s + 0 }')
        printf '%s\t%s\n' "$name" "$n" >> "$RESIDUE_PRE"
        total=$((total + n))
    done <<EOF
$RESIDUE_PATTERNS
EOF
    if [ "$total" -eq 0 ]; then
        die "every residue pattern found zero matching lines in the tree BEFORE the transformation.
     A sweep that starts at zero cannot demonstrate anything afterwards, so this is treated as a
     broken sweep rather than a clean tree. Check the patterns in RESIDUE_PATTERNS."
    fi
}

residue_pre_count_of() { # <name>
    awk -F'\t' -v n="$1" '$1 == n { print $2; exit }' "$RESIDUE_PRE"
}

# Which bucket does one `path:line:text` hit belong in? Printed in this order of precedence, and the
# order matters: a named decision should read as a decision even when it would also have matched the
# general "legitimate mention" pattern.
#
# Sets RESIDUE_CLASS rather than echoing it, and tests with bash's own matching rather than a grep:
# the broadest pattern here matches every retained copyright notice in the tree, so this runs
# thousands of times in one sweep and a command substitution plus a grep per line would be thousands
# of process spawns.
RESIDUE_CLASS=""
residue_classify() { # <hit line> -> sets RESIDUE_CLASS to known|expected|finding
    local hit="$1" sub _why path
    while IFS='|' read -r sub _why; do
        [ -n "$sub" ] || continue
        case "$hit" in *"$sub"*) RESIDUE_CLASS=known; return 0 ;; esac
    done <<EOF
$RESIDUE_KNOWN
EOF
    path="${hit%%:*}"
    # A file the completeness check excuses carries the old spelling as DATA or as a dated record.
    if is_sweep_excluded "$path"; then RESIDUE_CLASS=expected; return 0; fi
    if [[ "$hit" =~ $RESIDUE_EXPECTED_ERE ]]; then RESIDUE_CLASS=expected; return 0; fi
    RESIDUE_CLASS=finding
}

residue_report() {
    local name ere _sample mode pre known expected finding hit
    local total_findings=0

    echo "  A ONE-OFF MIGRATION REPORT, NOT A CHECK THAT WILL RUN AGAIN. It does not gate this run:"
    echo "  the completeness check above is the gate. Read the findings and decide."
    echo
    echo "  pattern liveness (proven against a sample BEFORE the tree was transformed):"
    cat "$RESIDUE_LIVENESS"
    echo
    echo "  sweep scope        every tracked text file - not just *.java. The silent survivors live"
    echo "                     in logback*.xml, junit-platform.properties and META-INF/services/..."
    echo

    while IFS='|' read -r name ere _sample mode; do
        [ -n "$name" ] || continue
        pre="$(residue_pre_count_of "$name")"
        : > "$TMP/res-hits.txt"
        { git grep -nIE "$ere" -- . || true; } > "$TMP/res-hits.txt"

        known=0; expected=0; finding=0
        : > "$TMP/res-known.txt"; : > "$TMP/res-expected.txt"; : > "$TMP/res-finding.txt"
        while IFS= read -r hit; do
            [ -n "$hit" ] || continue
            residue_classify "$hit"
            case "$RESIDUE_CLASS" in
                known)    printf '%s\n' "$hit" >> "$TMP/res-known.txt";    known=$((known + 1)) ;;
                expected) printf '%s\n' "$hit" >> "$TMP/res-expected.txt"; expected=$((expected + 1)) ;;
                *)        printf '%s\n' "$hit" >> "$TMP/res-finding.txt";  finding=$((finding + 1)) ;;
            esac
        done < "$TMP/res-hits.txt"
        total_findings=$((total_findings + finding))

        echo "  ${name}   (${ere})"
        echo "      before ${pre} line(s)   after $((known + expected + finding)) line(s)   ->   ${known} known, ${expected} expected, ${finding} FINDING(S)"
        if [ "$mode" = full ]; then
            if [ "$known" -gt 0 ]; then
                echo "      known:"
                sed 's/^/          /' "$TMP/res-known.txt"
            fi
            if [ "$expected" -gt 0 ]; then
                echo "      expected:"
                sed 's/^/          /' "$TMP/res-expected.txt"
            fi
        fi
        if [ "$finding" -gt 0 ]; then
            echo "      FINDINGS:"
            sed 's/^/          /' "$TMP/res-finding.txt"
        fi
        echo
    done <<EOF
$RESIDUE_PATTERNS
EOF

    echo "  known exclusions, each a decision someone made:"
    sed 's/^/      /' <<EOF
$RESIDUE_KNOWN
EOF
    echo
    echo "  TOTAL FINDINGS     ${total_findings}"
    if [ "$total_findings" -eq 0 ]; then
        echo "  Every survivor is either a retained copyright notice, a reference to upstream's own"
        echo "  issues or repository, a file that carries the old spelling as data, or a named"
        echo "  exclusion above."
    else
        echo "  Each finding is a survivor that is NOT a retained copyright notice, NOT a reference to"
        echo "  upstream's issues or repository, and NOT a named exclusion. Judge them individually."
    fi
}

# --------------------------------------------------------------------------------------------------
# Phase: move
# --------------------------------------------------------------------------------------------------

do_moves() {
    local old new moved=0 d
    while IFS=$'\t' read -r old new; do
        [ -n "$old" ] || continue
        mkdir -p "$(dirname "$new")"
        # `git mv`, never `mv` + `git add`. git mv stages the delete and the add as one operation
        # against the index; the hand-rolled version is where a half-staged move comes from, and a
        # half-staged move is exactly the thing that shows up as delete+create at merge time.
        # Per FILE, not per directory, so a branch where SOME of the tree has already moved (a PR
        # that added one new file under the old path after merging the rename) is handled by the
        # same code path as a clean branch.
        git mv "$old" "$new"
        moved=$((moved + 1))
    done < "$MOVES"

    # git tracks files, not directories, so each emptied io/confluent/... is left behind on disk.
    # Harmless to git, confusing to a human, and it makes a later "already applied" check ambiguous.
    # rmdir -p walks up and stops at the first non-empty parent, which is the source root.
    while IFS= read -r d; do
        [ -n "$d" ] || continue
        rmdir -p "$d" 2>/dev/null || true
    done < <(cut -f1 "$MOVES" | sed 's#/[^/]*$##' | sort -ru)

    echo "  moved ${moved} file(s) with git mv"
}

# --------------------------------------------------------------------------------------------------
# Phase: content rewrite
# --------------------------------------------------------------------------------------------------

do_rewrite() {
    local n
    n=$(count_lines "$REWRITES")
    if [ "$n" -gt 0 ]; then
        # Region-aware: the substitutions are gated on not being inside a freeze region, and the
        # marker lines themselves are never rewritten. $FROZEN resets per FILE via $ARGV rather than
        # per line, because `perl -i` keeps one interpreter across the whole argument list and state
        # left set by one file would silently exempt the start of the next.
        tr '\n' '\0' < "$REWRITES" | xargs -0 perl -i -pe "
            if (\$ARGV ne \$PREV_FILE) { \$PREV_FILE = \$ARGV; \$FROZEN = 0 }
            if (/${FREEZE_BEGIN_ERE}/) { \$FROZEN = 1 }
            elsif (/${FREEZE_END_ERE}/) { \$FROZEN = 0 }
            elsif (!\$FROZEN) { $PERL_PROG }
        "
    fi
    echo "  rewrote references in ${n} file(s)"
}

# bin/check-copyright-headers.sh is frozen for the sweep because half of each RENAMED_FROM_UPSTREAM
# entry must NOT move: the format is `newpath|oldpath-at-fork-point`, and the oldpath names a path in
# the UPSTREAM tree, looked up with `git cat-file blob $FORK_POINT:$oldpath`. So: rewrite everything
# up to the first `|`, leave the rest exactly as it was. Lines with no `|` are the
# EXTRACTED_FROM_UPSTREAM block, which lists CURRENT paths, so those move whole. Idempotent: a second
# run finds nothing left to change on the head side.
retarget_copyright_manifest() {
    local f="bin/check-copyright-headers.sh" prog
    [ -f "$f" ] || return 0
    prog='if (m{^\s*parallel-consumer\S*}) { my $tail = q{}; if (index($_, q{|}) >= 0) { ($_, $tail) = split(/\|/, $_, 2); $tail = q{|} . $tail; } '
    prog+="$PERL_PROG"
    prog+=' $_ .= $tail; }'
    perl -i -pe "$prog" "$f"
    echo "  retargeted the newpath side of the copyright provenance manifest"
}

# --------------------------------------------------------------------------------------------------
# Phase: copyright headers (Apache 2.0 s4(c))
# --------------------------------------------------------------------------------------------------
#
# The rule the repo already enforces: an upstream-derived file MODIFIED on the fork keeps its
# Confluent notice and ADDS `Modifications Copyright (C) <year> Antony Stubbs and contributors`.
# This run has just modified a few hundred of them, so it owes them the line.
#
# "Upstream-derived" needs no fork-point lookup here, because the repo has a stronger invariant to
# lean on: bin/check-copyright-headers.sh FAILS any fork-original file that claims Confluent
# copyright. So a file carrying a Confluent notice IS upstream-derived, by construction - and that
# test also catches the renamed and extracted files a path lookup would miss.
#
# THE COMMENT SYNTAX IS COPIED FROM THE FILE'S OWN CONFLUENT LINE rather than chosen from a table.
# Taking the prefix that already sits in front of `Copyright (C)` makes it structurally impossible to
# emit `//` into a YAML file or `#` into an XML comment: the new line is, by construction, in
# whatever syntax the old one was in. The extension table is then a SECOND, INDEPENDENT check - it
# says what a file of this type is allowed to look like, and the script refuses rather than guesses
# when the two disagree, or when it meets an extension it has never seen.

comment_style_for() { # <path> -> java|hash|xml|adoc, non-zero for "I do not know"
    case "$1" in
        *.java|*.kt|*.groovy|*.js|*.ts) echo java ;;
        *.xml|*.html|*.md|*.markdown)   echo xml ;;
        *.yml|*.yaml|*.sh|*.bash|*.properties|*.conf|*.cfg|*.toml|*.ini) echo hash ;;
        *.adoc|*.asciidoc)              echo adoc ;;
        *) return 1 ;;
    esac
}

prefix_matches_style() { # <style> <prefix> - strict: an unrecognised shape is an error, not a default
    local re
    case "$1" in
        java|adoc) re='^[[:space:]]*(\*|//|/\*)' ;;
        # inside an <!-- --> block the continuation lines carry no marker at all, so whitespace-only
        # is the NORMAL shape for XML; `<!--` on the same line is the other legal one.
        xml)       re='^[[:space:]]*(<!--)?[[:space:]]*$' ;;
        hash)      re='^[[:space:]]*#' ;;
        *)         return 1 ;;
    esac
    [[ "$2" =~ $re ]]
}

MODS_LINE_ADDED=0
MODS_LINE_PRESENT=0

ensure_modifications_line() { # <path>
    local f="$1" header style prefix
    [ -f "$f" ] || return 0

    header="$(head -"$HEADER_WINDOW" "$f")"
    # Herestrings, never `printf | grep -q`: grep -q exits at its first match, the writer takes
    # SIGPIPE, and under pipefail the pipeline then reads as FALSE - so a file that DID match is
    # classified as one that did not. bin/check-shell-sigpipe.sh fails the build over this.
    grep -q "Copyright (C).*Confluent" <<<"$header" || return 0
    if grep -q "Modifications Copyright (C).*${MODS_HOLDER}" <<<"$header"; then
        MODS_LINE_PRESENT=$((MODS_LINE_PRESENT + 1))
        return 0
    fi

    if ! style="$(comment_style_for "$f")"; then
        die "refusing to guess a comment syntax for '${f}' (unrecognised extension).
     It carries a Confluent copyright notice and this run modified it, so Apache 2.0 s4(c) wants
     a Modifications Copyright line - but emitting the wrong comment marker would corrupt the
     file. Add the extension to comment_style_for() in bin/rename-packages.sh, or add the line
     by hand."
    fi

    prefix="$(head -"$HEADER_WINDOW" "$f" | sed -n 's/^\(.*\)Copyright (C).*Confluent.*$/\1/p' | sed -n '1p')"
    if ! prefix_matches_style "$style" "$prefix"; then
        die "the Confluent notice in '${f}' is prefixed with '${prefix}', which is not a ${style}
     comment opener. Refusing to insert a line I cannot place correctly."
    fi

    MODS_PREFIX="$prefix" \
    MODS_TEXT="Modifications Copyright (C) ${MODS_YEAR} ${MODS_HOLDER}" \
    HEADER_WINDOW="$HEADER_WINDOW" \
    perl -i -pe '
        BEGIN { $done = 0 }
        if (!$done && $. <= $ENV{HEADER_WINDOW} && /Copyright \(C\).*Confluent/) {
            $_ .= "$ENV{MODS_PREFIX}$ENV{MODS_TEXT}\n";
            $done = 1;
        }
    ' "$f"
    MODS_LINE_ADDED=$((MODS_LINE_ADDED + 1))
}

do_headers() {
    local f
    # Every file this run touched - the moved ones (their package declaration changed) and the
    # rewritten ones. Files the run did NOT modify get nothing: s4(c) is triggered by modifying a
    # file, and stamping untouched files would be an unrelated policy change riding along.
    {
        [ -s "$MOVES" ] && cut -f2 "$MOVES"
        cat "$REWRITES"
    } | sort -u > "$TMP/touched.txt"

    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_self "$f" && continue
        ensure_modifications_line "$f"
    done < "$TMP/touched.txt"

    echo "  modifications-copyright line: ${MODS_LINE_ADDED} added, ${MODS_LINE_PRESENT} already present"
}

# --------------------------------------------------------------------------------------------------
# Phase: regenerate what must never be hand-edited
# --------------------------------------------------------------------------------------------------

do_regenerate() {
    if [ -f bin/todo-index.sh ]; then
        bash bin/todo-index.sh > /dev/null
        echo "  regenerated docs/todo-index.md"
    fi

    [ -f README.adoc ] || return 0
    if [ "$REGEN_README" != true ]; then
        note_manual "README.adoc NOT regenerated (--skip-readme-regen), and therefore EXCUSED from the completeness check. Run: ./mvnw -N process-sources"
        SWEEP_EXCLUDE_EXTRA="README.adoc"
        echo "  skipped README.adoc regeneration (--skip-readme-regen); it is excused from the check"
        return 0
    fi
    if [ ! -x ./mvnw ]; then
        note_manual "README.adoc NOT regenerated (no ./mvnw in this tree), and therefore EXCUSED from the completeness check. Run: ./mvnw -N process-sources"
        SWEEP_EXCLUDE_EXTRA="README.adoc"
        return 0
    fi
    # -N: the asciidoc-template plugin is <inherited>false</inherited> and bound to the ROOT pom, so
    # a non-recursive run regenerates the README without building the reactor.
    #
    # NO -Dcopyright.skip. This used to be mandatory, and it was the sharp edge of the whole rename:
    # bin/check-copyright-headers.sh is bound to the `validate` phase via exec-maven-plugin, so it
    # runs before ANY goal, and it resolved provenance by exact path against the fork-point tree -
    # so the moment the move landed, every upstream-derived file missed the lookup, was judged
    # fork-original, and its REQUIRED Confluent header became a violation. 197 of them, and maven
    # died in validate before doing anything at all. That scanner now maps a path back through its
    # PACKAGE_MOVES table before every lookup, so the rename is invisible to it and the gate runs
    # here exactly as it does on any other build - which is the point: the run that changes ~200
    # copyright headers is the last run that should be skipping the copyright gate.
    echo "  regenerating README.adoc (./mvnw -N process-sources) ..."
    if ! ./mvnw -N -q process-sources; then
        note_manual "./mvnw -N process-sources FAILED - README.adoc is stale. The completeness check below will fail on it; that is correct. Fix the build, regenerate, commit. If it died in the validate phase, read the copyright violations it printed: the provenance model is what is broken, and -Dcopyright.skip=true would only hide it."
        echo "  WARNING: README regeneration failed; the completeness check will report it" >&2
    fi
}

# --------------------------------------------------------------------------------------------------
# Verification: did git actually record the moves as RENAMES?
# --------------------------------------------------------------------------------------------------

verify_renames() { # <rev> <expected-moves> <label>
    local rev="$1" expected="$2" label="$3"
    local statuses n_r n_a n_d n_m sorted lowest n_mispaired
    local raw="$TMP/raw-$$.txt"

    git -c diff.renameLimit="$VERIFY_RENAME_LIMIT" show --raw -M --no-color --format='' "$rev" > "$raw"
    statuses="$(awk '/^:/ { print $5 }' "$raw")"

    n_r=$(grep -c '^R' <<<"$statuses" || true)
    n_a=$(grep -c '^A' <<<"$statuses" || true)
    n_d=$(grep -c '^D' <<<"$statuses" || true)
    n_m=$(grep -c '^M' <<<"$statuses" || true)

    # sort reads its whole input, so no early-exiting reader and nothing for pipefail to promote.
    sorted="$(grep '^R' <<<"$statuses" | sed 's/^R//' | sort -n || true)"
    lowest="${sorted%%$'\n'*}"
    [ -n "$lowest" ] || lowest="n/a"

    echo "  rev                $(git rev-parse --short "$rev")   (${label})"
    echo "  files moved        ${expected}"
    echo "  renames (R)        ${n_r}"
    echo "  adds (A)           ${n_a}"
    echo "  deletes (D)        ${n_d}"
    echo "  modifies (M)       ${n_m}"
    echo "  lowest similarity  R${lowest}   (git's detection threshold is 50)"

    # THE COUNT IS NOT ENOUGH. Rename detection can pair the RIGHT NUMBER of files WRONGLY: five
    # near-identical TestConventionsArchTest.java files, one per module, were observed being paired
    # into a cross-module cycle (streams -> metrics, vertx -> mutiny, ...). A count of R entries sees
    # nothing amiss in that; it is only visible if you ask whether each pairing is the move the
    # script actually made. So: re-derive the expected new path from the old one with the SAME
    # substitution program, and demand it match.
    awk -F'\t' '/^:/ { split($1, a, " "); if (a[5] ~ /^R/) print $2 }' "$raw" > "$TMP/pair-old.txt"
    awk -F'\t' '/^:/ { split($1, a, " "); if (a[5] ~ /^R/) print $3 }' "$raw" > "$TMP/pair-new.txt"
    if [ -s "$TMP/pair-old.txt" ]; then
        perl -pe "$PERL_PROG" < "$TMP/pair-old.txt" > "$TMP/pair-expected.txt"
        paste "$TMP/pair-old.txt" "$TMP/pair-expected.txt" "$TMP/pair-new.txt" |
            awk -F'\t' '$2 != $3' > "$TMP/mispaired.txt"
    else
        : > "$TMP/mispaired.txt"
    fi
    n_mispaired=$(count_lines "$TMP/mispaired.txt")
    echo "  mis-paired         ${n_mispaired}   (renames git invented, pairing a file with one it never came from)"

    if [ "$n_mispaired" -gt 0 ]; then
        echo
        echo "  MIS-PAIRED RENAMES (old / expected new / what git actually paired it with):"
        sed 's/^/      /' "$TMP/mispaired.txt"
        die "git paired ${n_mispaired} file(s) with a destination this script never moved them to.
     The commit's history now asserts a move that did not happen, and a merge will apply a PR's
     edit to the WRONG FILE without conflicting. Re-run with the two-commit default (the pure
     move commit is 100% similar, so it cannot be mis-paired)."
    fi

    if [ "$n_r" -ne "$expected" ]; then
        die "git detected ${n_r} rename(s) but ${expected} file(s) were moved.
     The shortfall shows up as a delete+add pair, and every open PR touching one of those files
     conflicts instead of merging - which is the entire failure this script exists to prevent.
     Check the rename limits printed above. If this happened under --single-commit, drop the
     flag: the two-commit default has a move commit that is 100% similar by construction, so its
     detection cannot fail."
    fi
    if [ "$n_a" -ne 0 ] || [ "$n_d" -ne 0 ]; then
        die "expected no add/delete pairs, got A=${n_a} D=${n_d}.
     Inspect: git show --raw -M $(git rev-parse --short "$rev")"
    fi
    if [ "$expected" -eq 0 ]; then
        echo "  VERDICT            no moves here, and none detected - this commit cannot dilute its parent"
    else
        echo "  VERDICT            every move recorded as a rename, correctly paired, no delete/create pairs"
    fi
}

report_rename_limits() {
    local d m
    d="$(git config diff.renameLimit || true)"
    m="$(git config merge.renameLimit || true)"
    echo "  diff.renameLimit   ${d:-<unset - git default 1000>}"
    echo "  merge.renameLimit  ${m:-<unset - git default 7000>}"
    echo "  The assertion below pins its own limit of ${VERIFY_RENAME_LIMIT}, so it does not depend on ambient"
    echo "  config. The two values above are what a future MERGE will use: if either is below the"
    echo "  moved-file count, raise it before merging the open PR branches."
}

# --------------------------------------------------------------------------------------------------
# Verification: the completeness check
# --------------------------------------------------------------------------------------------------
#
# The one that has to be un-foolable. The two references it exists for - the escaped regex in the
# mutation lane and a misspelt package - both survive a naive sweep AND both fail GREEN afterwards,
# so nothing downstream will tell you they were missed.

completeness_check() {
    local f hits=0 skipped=0
    local live="$TMP/live-hits.txt" skipped_list="$TMP/skipped-hits.txt"
    local one="$TMP/hits-one.txt" frozen_list="$TMP/frozen-hits.txt" frozen_files=0
    : > "$live"
    : > "$skipped_list"
    : > "$frozen_list"

    while IFS= read -r f; do
        [ -n "$f" ] || continue
        if is_sweep_excluded "$f"; then
            {
                printf '      %s\n' "$f"
                git grep -nIE "$SWEEP_ERE" -- "$f" | sed 's/^/          /' || true
            } >> "$skipped_list"
            skipped=$((skipped + 1))
            continue
        fi
        git grep -nIE "$SWEEP_ERE" -- "$f" > "$one" || true

        # Drop matches sitting inside a freeze region, and print what was dropped. Same rule as the
        # excluded set above: an exemption that is not shown is a hole nobody can audit.
        if has_freeze_marker "$f"; then
            # Both files are created up front because awk only creates an output file it actually
            # writes to, and the emptiness of each is what the caller tests. Named outputs rather than
            # the shorter `print > "/dev/stderr"` trick: stderr is the error channel, and borrowing it
            # to carry data means a real awk error lands in the frozen-hits list and gets reported as
            # protected text - a malfunction presenting as a clean exemption.
            : > "$one.frozen"
            : > "$one.kept"
            awk -v frozen="$(frozen_lines "$f")" -v froz="$one.frozen" -v kept="$one.kept" -F: "$AWK_FROZEN_SET"'
                { if ($2 in fz) print > froz; else print > kept }
            ' "$one"
            if [ -s "$one.frozen" ]; then
                {
                    printf '      %s  (inside a freeze region)\n' "$f"
                    sed 's/^/          /' "$one.frozen"
                } >> "$frozen_list"
                frozen_files=$((frozen_files + 1))
            fi
            mv "$one.kept" "$one"
        fi

        if [ -s "$one" ]; then
            cat "$one" >> "$live"
            hits=$((hits + 1))
        fi
    done < <(git grep -lIE "$SWEEP_ERE" -- . || true)

    echo "  pattern            ${SWEEP_ERE}"
    echo "                     (tolerates io\\.conflu and io/conflu, and stops before the package"
    echo "                      name so a misspelling downstream of it cannot hide)"
    echo
    echo "  EXCLUDED, with everything each one hid - audit this, do not skim it:"
    if [ -s "$skipped_list" ]; then
        cat "$skipped_list"
    else
        echo "      (nothing matched inside the excluded set)"
    fi
    echo
    echo "  FROZEN REGIONS, with everything each one held - text that must keep the old spelling:"
    if [ -s "$frozen_list" ]; then
        cat "$frozen_list"
    else
        echo "      (no freeze region matched)"
    fi
    echo
    echo "  excluded files with matches      ${skipped}"
    echo "  files with frozen-region matches ${frozen_files}"
    echo "  NON-excluded files with matches  ${hits}"

    if [ "$hits" -gt 0 ]; then
        echo
        echo "  STALE REFERENCES:"
        sed 's/^/      /' "$live"
        die "the completeness check found matches in ${hits} file(s) outside the excluded set.
     Each is either a MISS - fix it, and say which sweep should have caught it - or a legitimate
     survivor, in which case add it to FROZEN_PREFIXES with a written justification."
    fi
    echo "  VERDICT            no stale references outside the excluded set"
}

# --------------------------------------------------------------------------------------------------
# Prose guards
# --------------------------------------------------------------------------------------------------

check_prose_guards() {
    local path ere fixed advice found=0 orphaned=""
    while IFS='|' read -r path ere fixed advice; do
        [ -n "$path" ] || continue
        [ -f "$path" ] || continue
        if ! grep -qE "$ere" "$path"; then
            # Corrected, or silently reworded into something nobody declared. Only the first is a pass.
            grep -qE "$fixed" "$path" || orphaned="${orphaned}
      ${path}: neither the claim (${ere}) nor its corrected form (${fixed}) is present"
            continue
        fi
        if grep -nE "$ere" "$path" > "$TMP/prose.txt"; then
            found=$((found + 1))
            echo
            echo "  ${path}"
            sed 's/^/      /' "$TMP/prose.txt"
            echo "      -> ${advice}"
            note_manual "prose in ${path}:$(sed -n '1s/:.*//p' "$TMP/prose.txt") - ${advice}"
        fi
    done <<EOF
$PROSE_GUARDS
EOF

    # A guard matching NEITHER spelling means one of two very different things, and which one depends
    # entirely on whether this is master or a branch.
    #
    # On master it is the failure this check exists for: somebody reworded the sentence, the guard now
    # matches nothing, and "none found" reads exactly like a clean tree. Hard refusal.
    #
    # On a branch running --defer-prose it usually means the branch simply PREDATES the sentence. The
    # three guarded claims all arrived in one commit; a branch cut before it never had them to reword.
    # Refusing there is the guard failing closed on a tree that has nothing to guard - and it cannot be
    # worked around, because --defer-prose was checked AFTER this die, so the one flag that means
    # "prose is master's problem" could not reach it. MEASURED on astubbs#38, which had every package
    # mapped and still could not run.
    #
    # So --defer-prose now covers this case too, which is what the flag already promises: the corrected
    # wording arrives from master at merge. Writing the missing sentence onto the branch would be
    # authoring master's prose from a branch, which is the thing the guards exist to prevent.
    if [ -n "$orphaned" ]; then
        if [ "$DEFER_PROSE" != true ]; then
            echo
            die "a guarded sentence was reworded rather than corrected:${orphaned}
     A guard that matches neither spelling reports \"none found\", which reads exactly like a clean
     tree. Re-point the claim pattern at the new wording, or add the corrected form it should have.
     On a PR branch that simply predates the sentence, pass --defer-prose."
        fi
        echo
        echo "  --defer-prose: guarded sentence(s) absent in BOTH spellings, carried as follow-ups.${orphaned}"
        while IFS= read -r line; do
            [ -n "${line# }" ] || continue
            note_manual "absent prose guard -${line#      }"
        done <<EOF
$orphaned
EOF
    fi

    if [ "$found" -eq 0 ]; then
        echo "  none found - every guarded claim is already corrected"
        return 0
    fi
    if [ "$DEFER_PROSE" = true ]; then
        echo
        echo "  --defer-prose: continuing. The ${found} claim(s) above are manual follow-ups."
        return 0
    fi
    echo
    die "${found} sentence(s) in the tree CLAIM the packages are unchanged. Rewriting them
     mechanically produces a confident false statement in a published artifact, which is worse
     than leaving them stale, so this script will not do it. Edit the prose and re-run - or pass
     --defer-prose to proceed and carry them as follow-ups, which is what you want on a PR
     branch, where the corrected prose arrives from master at merge."
}

# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------

echo "== package rename: io.confluent.* -> bz.stub.*"
echo "   branch $(git rev-parse --abbrev-ref HEAD)   tree $(pwd)"

# BEFORE any path that can print a clean verdict, not just before the applying run. completeness_check
# HONOURS freeze regions, so if it runs without this having run, an unclosed freeze-begin silently
# freezes to end-of-file and the check reports "no stale references outside the excluded set" over text
# it never examined. --verify-only and the "already applied, nothing to do" exit both used to reach that
# state, because the only call site sat after both of them.
section "preflight: are the freeze regions well formed?"
validate_freeze_markers
echo "  VERDICT            every freeze region opens and closes, and names what it closes"

if [ "$VERIFY_ONLY" = true ]; then
    section "verification: completeness"
    completeness_check
    echo
    echo "--verify-only: nothing was changed."
    exit 0
fi

discover_moves
discover_rewrites
n_moves=$(count_lines "$MOVES")
n_rewrites=$(count_lines "$REWRITES")

# --- idempotency ----------------------------------------------------------------------------------
# "Already applied" is decided by the TREE, not by a marker file or a commit message, so it is
# correct on any branch at any time - including a branch that merged the rename and THEN grew a new
# file under the old path, which must come out as "there is work to do", not as "already done".
if [ "$n_moves" -eq 0 ] && [ "$n_rewrites" -eq 0 ]; then
    section "already applied, nothing to do"
    echo "  no tracked path matches   ${PATH_SCAN_ERE}"
    echo "  no rewritable file matches  ${SWEEP_ERE}"
    echo
    echo "  Re-running is a no-op by design. This script is meant to be run on master and on every"
    echo "  open PR branch, and again on a branch that later adds a file under the old path."
    exit 0
fi

section "work set"
echo "  files to move      ${n_moves}"
echo "  files to rewrite   ${n_rewrites}"
echo "  frozen (never rewritten - generated files here are REGENERATED, not excused):"
sed 's/^/      /' <<EOF
$FROZEN_PREFIXES
EOF
echo "  excused from the completeness check (a shorter list, on purpose):"
sed 's/^/      /' <<EOF
$SWEEP_EXCLUDE
EOF

section "preflight: is every legacy package actually MAPPED?"
check_unmapped_legacy

section "prose that a mechanical rewrite would falsify"
check_prose_guards

if [ "$APPLY" != true ]; then
    section "dry run"
    echo "  would move:"
    head -5 "$MOVES" | sed 's/^/      /'
    [ "$n_moves" -gt 5 ] && echo "      ... and $((n_moves - 5)) more"
    echo "  would rewrite:"
    head -10 "$REWRITES" | sed 's/^/      /'
    [ "$n_rewrites" -gt 10 ] && echo "      ... and $((n_rewrites - 10)) more"
    echo
    echo "--dry-run: nothing was changed."
    exit 0
fi

if [ "$DO_COMMIT" = true ] && [ -n "$(git status --porcelain)" ]; then
    {
        echo "ERROR: working tree is not clean."
        echo "  This script commits what it changes, and a dirty tree would sweep unrelated work into"
        echo "  the rename commit - which is exactly what makes a rename un-mergeable. Commit or"
        echo "  stash first, or pass --no-commit."
    } >&2
    exit 2
fi

MOVE_REV=""
CONTENT_COMMITTED=false

# BEFORE anything changes. After the rewrite these numbers cannot be recovered from the tree, and a
# report with no "before" column cannot tell a clean sweep from a broken one.
residue_capture_pre_counts

section "phase 1: move"
do_moves
if [ "$SPLIT_COMMITS" = true ] && [ "$DO_COMMIT" = true ] && [ "$n_moves" -gt 0 ]; then
    git commit -q -m "refactor: move io.confluent.* to bz.stub.* (pure rename, no content changes)" \
        -m "Directory move only, so every path is 100% similar and git's exact-rename detection
cannot fail on it. The content edits follow in the next commit.

Generated by bin/rename-packages.sh."
    MOVE_REV="$(git rev-parse HEAD)"
    echo "  committed the moves on their own"
fi

section "phase 2: rewrite"
# Recomputed against the POST-MOVE tree: the moved files are at their new paths now, and the move
# itself changed nothing textual, so they are still in the rewrite set - under their new names.
discover_rewrites
n_rewrites=$(count_lines "$REWRITES")
do_rewrite
retarget_copyright_manifest
do_headers
do_regenerate

section "commit"
if [ "$DO_COMMIT" != true ]; then
    echo "  --no-commit: the changes are in the working tree and index, uncommitted."
    echo "  Rename verification is SKIPPED, because there is no commit to inspect. Commit the moves"
    echo "  and the edits TOGETHER (the measured default) and then run:"
    echo "      git show --raw -M HEAD"
    echo "  expecting ${n_moves} R-entries and no A/D pairs."
else
    git add -A
    # A run that changed nothing is a successful no-op, not a failed check. Without this, re-running on
    # a settled tree dies here: `git commit` refuses an empty commit and the script exits 1, so the
    # per-branch instruction "any refusal, STOP and report" fires on a branch that is already correct.
    #
    # The "already applied, nothing to do" exit above does NOT catch this case on the real tree, and the
    # self-test fixture cannot show why: AGENTS.md and .github/workflows/repo-hygiene.yml both DESCRIBE
    # the rename, so they match the sweep pattern permanently while matching no rewrite rule. n_rewrites
    # therefore never reaches zero here, however finished the rename is, and the fixture contains
    # neither file.
    if git diff --cached --quiet; then
        echo "  nothing to commit - the tree already carries this change, so this run was a no-op."
        echo "  Re-running is a no-op by design; see the idempotency note above."
    elif [ "$SPLIT_COMMITS" = true ]; then
        CONTENT_COMMITTED=true
        git commit -q -m "refactor: rename io.confluent.* references to bz.stub.* (content only)" \
            -m "Text edits only. No file moves in this commit, so it cannot dilute the rename
detection in its parent.

Generated by bin/rename-packages.sh."
    else
        CONTENT_COMMITTED=true
        git commit -q -m "refactor: rename packages io.confluent.* to bz.stub.*" \
            -m "Moves the package directories and rewrites every reference in one atomic commit, so
the tree compiles at every point in history and a PR branch has one commit to reconcile
rather than two rewrites of the same file. Both shapes were measured before choosing:
folding the content edits into the move left every rename detected, worst similarity 96%,
against git's 50% threshold.

Covers the forms a plain find-and-replace cannot see: the escaped-regex spelling in the
mutation and quarantine scripts, the io/confluent path form, ArchUnit @AnalyzeClasses
package strings, the truth-generator <class> list, the META-INF/services listener, the
logback logger names, and a misspelt variant.

Generated by bin/rename-packages.sh."
    fi
    if [ "$CONTENT_COMMITTED" = true ]; then
        echo "  committed $(git rev-parse --short HEAD)"
    fi
fi

section "verification: did git record the moves as RENAMES?"
report_rename_limits
echo
if [ "$DO_COMMIT" != true ]; then
    echo "  SKIPPED - nothing was committed (--no-commit). See the note above."
elif [ "$n_moves" -eq 0 ]; then
    echo "  no files moved on this branch, so there is nothing to detect."
elif [ -n "$MOVE_REV" ] && [ "$CONTENT_COMMITTED" != true ]; then
    # Moves happened, but phase 2 staged nothing, so HEAD is still the MOVE commit. Verifying it as a
    # "content only" commit asserts 0 renames against a commit that legitimately has n_moves of them,
    # and dies with a diagnostic that is simply false - on a branch that was handled correctly.
    echo "  [move commit]"
    verify_renames "$MOVE_REV" "$n_moves" "pure move"
    echo
    echo "  [content commit]  none was needed - phase 2 changed nothing, so there is nothing to verify."
elif [ -n "$MOVE_REV" ]; then
    echo "  [move commit]"
    verify_renames "$MOVE_REV" "$n_moves" "pure move"
    echo
    echo "  [content commit]"
    verify_renames HEAD 0 "content only"
else
    verify_renames HEAD "$n_moves" "single atomic commit"
fi

section "verification: completeness"
completeness_check

section "migration residue report (one-off - read it, it does not gate this run)"
residue_report

section "summary"
echo "  moved              ${n_moves} file(s)"
echo "  rewrote            ${n_rewrites} file(s)"
echo "  header lines       ${MODS_LINE_ADDED} added, ${MODS_LINE_PRESENT} already present"
if [ -s "$MANUAL_FOLLOWUPS" ]; then
    echo
    echo "  MANUAL FOLLOW-UPS - this script could not do these and will not pretend otherwise:"
    cat "$MANUAL_FOLLOWUPS"
fi
echo
echo "  NOT covered here, and NOT green merely because this exited 0:"
echo "   - The mutation lane EXITS 0 when it matches nothing. On the first PR after this lands,"
echo "     change a class under the decidable packages and read the job summary for a mutation score"
echo "     and a survivor list. A green tick carrying 'nothing to mutate, skipping' is the FAILURE"
echo "     mode, not the pass."
echo "   - ArchUnit rules pin package names as STRINGS and pass vacuously when they select nothing."
echo "     Break one on purpose and watch it go red before believing the suite still guards anything."
echo
echo "done."
