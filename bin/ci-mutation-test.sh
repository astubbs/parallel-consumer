#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run PIT mutation testing (advisory / non-gating). PIT is CPU-bound and runs mutation
# analysis across parallel minion JVMs, so it scales with cores - which is why it belongs
# on the self-hosted multi-core runner, not GitHub's 2-core gate.
#
# Threads default to the machine's CPU core count (1 minion per core - optimal for CPU-bound
# work; oversubscribing hurts). Override with PIT_THREADS=N. NOTE ON RAM: each minion gets
# -Xmx2g (the coverage minion OOMs at 1g), so peak heap is roughly threads x 2g - make sure the
# runner has the RAM (e.g. 12 cores => ~24g). Lower PIT_THREADS if the box is RAM-constrained.
#
# Usage: bin/ci-mutation-test.sh [extra-maven-args...]   (e.g. -Dverbose=true)
#
# Env overrides: PIT_THREADS, PIT_BASE_REF, PIT_FULL_SWEEP, PIT_TARGET_CLASSES, PIT_TARGET_TESTS,
# PIT_DECIDABLE_PACKAGES (regex: which changed classes a PR run will mutate - see below),
# PIT_MIN_MUTANTS (the floor a run must score to count as a run; default 1), PIT_DRY_RUN_LOG
# (self-test seam - see bin/test-ci-mutation-test.sh).
# Prefer PIT_TARGET_CLASSES / PIT_TARGET_TESTS over passing a second -DtargetClasses in "$@": duplicate
# -D flags resolve by last-wins, which is implicit and easy to get backwards.
#
# WHERE THIS RUNS: exactly one per-PR lane - maven.yml -> "Mutation Tests (PIT, PR-scoped)", advisory.
# The full sweep is manual-only (.github/workflows/mutation-full-sweep.yml) because it has never
# completed. See docs/plans/2026-08-03-002-mutation-testing-plan.md.
#
# ---------------------------------------------------------------------------------------------
# THE EXIT CODE IS THE VERDICT. Four states, and none of them share a code:
#
#   0  PIT ran and EVALUATED at least PIT_MIN_MUTANTS mutants. Survivors do NOT change this - a
#      score is a conversation starter, not a gate, and that is unchanged.
#   2  CANNOT RUN. The scope matches nothing that exists, the tree has no main sources, PIT was
#      asked to mutate something and produced no statistics / zero mutants, or it generated mutants
#      and evaluated none of them (every result MEMORY_ERROR / RUN_ERROR / NON_VIABLE, or no
#      mutations.xml written at all).
#   3  NOTHING IN SCOPE. No core main-source class changed, or none of the ones that did are in a
#      package where a survivor is decidable. Legitimate, common, and deliberately not 0.
#   *  PIT's own exit status, passed through.
#
# WHY, measured 2026-08-25: across the last 40 `maven.yml` pull_request runs the
# "Mutation Tests (PIT, PR-scoped)" check reported success 40 times and scored ZERO mutants 40
# times - 23 runs with no core main-source change, 14 outside the decidable packages, the rest
# still in flight. Each skip was individually correct. The problem is that "correct skip", "stale
# scope that can never match again" and "185 mutants killed" were all the same green tick, which is
# precisely the shape AGENTS.md warns about under "Confirm the mutation lane scored mutants rather
# than trusting its tick". Separating the codes is what lets maven.yml react differently to each.
#
# The 2 follows bin/check-shell-lint.sh and bin/lib/node-gate.sh: "the tool could not run" must
# never share a code with "the tool ran and the code is clean".
# ---------------------------------------------------------------------------------------------

set -euo pipefail

# One transform, two callers. Converts a repo-relative .java path to a fully-qualified class name.
# It was written out twice - once in the inert-config guard and once in the changed-class scan - and
# a transform that exists twice is one that can be corrected in one place only.
path_to_fqcn() { sed -E 's#.*/src/main/java/##; s#/#.#g; s#\.java$##'; }

# Append to the GitHub job summary when running in Actions; no-op locally. A mutation run's output is
# otherwise a log nobody reads behind a tick that means "nothing to mutate" at least as often as it
# means "all mutants killed" - so every exit path below says which one it was.
summary() { printf '%s\n' "$*" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"; }

# Captured run output, read back afterwards for the statistics. Outside the repo by default so it can
# never be committed or picked up by a report-globbing artifact upload.
PIT_LOG="${PIT_LOG:-$(mktemp -t pit-run.XXXXXX)}"

# Cross-platform core count (Linux nproc / macOS sysctl), default 2.
if command -v nproc >/dev/null 2>&1; then
  CORES=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
  CORES=$(sysctl -n hw.ncpu)
else
  CORES=2
fi
THREADS="${PIT_THREADS:-$CORES}"
MIN_MUTANTS="${PIT_MIN_MUTANTS:-1}"
echo "PIT: using ${THREADS} thread(s) (cores=${CORES}); minion heap -Xmx2g => ~$((THREADS * 2))g peak"

# DECIDABLE PACKAGES. Which changed classes a PR run will mutate; see the long argument at the point
# of use below. Read here rather than there so the inert-config guard can validate it on EVERY run,
# including the ones that would otherwise skip before ever looking at it.
#
# Still offsets.* alone. `state.*` was the standing next candidate and was MEASURED on 2026-08-25
# rather than argued: it hangs, exactly as internal.* does. The numbers and the control arm are in
# docs/inflight/ci-mutation-testing.md - do not widen it back without reading them.
DECIDABLE="${PIT_DECIDABLE_PACKAGES:-^bz\.stub\.parallelconsumer\.offsets\.}"

# ---------------------------------------------------------------------------------------------
# INERT-CONFIG GUARD: does the scope still match anything that exists?
#
# Every scoping decision below is a regex or a glob over fully qualified class names, and the
# package has already moved once (io.confluent.* -> bz.stub.*). A scope that no longer matches
# anything does not error - it falls through to "nothing to mutate, skipping" and exits 0, which in
# the checks list is the same green tick as a run that killed every mutant. AGENTS.md names this
# exact failure.
#
# So the scope is validated against the classes actually in the tree BEFORE it decides anything,
# on every run - including runs that would have skipped. A stale scope is then caught by the next
# PR of any kind, rather than by the next PR that happens to touch the one package it names.
# ---------------------------------------------------------------------------------------------
ALL_MAIN_CLASSES=$(git ls-files -- parallel-consumer-core/src/main/java 2>/dev/null \
  | { grep -E '\.java$' || true; } \
  | path_to_fqcn)
if [ -z "$ALL_MAIN_CLASSES" ]; then
  echo "PIT: no core main-source classes found under parallel-consumer-core/src/main/java - CANNOT RUN." >&2
  echo "PIT: this is NOT a pass. Either the checkout is wrong or the module moved." >&2
  summary "### Mutation (PIT): CANNOT RUN"
  summary "No core main-source classes were found under \`parallel-consumer-core/src/main/java\`."
  summary "Either the checkout is wrong or the module moved. **This is not a clean result.**"
  exit 2
fi
DECIDABLE_IN_TREE=$(printf '%s\n' "$ALL_MAIN_CLASSES" | { grep -cE "$DECIDABLE" || true; })
if [ "$DECIDABLE_IN_TREE" -eq 0 ]; then
  echo "PIT: the decidable scope '${DECIDABLE}' matches NOTHING in the tree - CANNOT RUN." >&2
  echo "PIT: the scope is stale (a package rename is the usual cause), so every run would 'skip'." >&2
  summary "### Mutation (PIT): CANNOT RUN - the scope is stale"
  summary "\`PIT_DECIDABLE_PACKAGES\` is \`${DECIDABLE}\`, which matches **no class** in"
  summary "\`parallel-consumer-core/src/main/java\`. Every run would report \"nothing to mutate\""
  summary "and exit green while scoring nothing. Fix the regex; do not silence this."
  exit 2
fi
echo "PIT: decidable scope '${DECIDABLE}' matches ${DECIDABLE_IN_TREE} class(es) in the tree"

# Scope: on a PR, mutate ONLY the core main-source classes CHANGED vs the base branch. The full sweep is
# impractically slow (it has never completed on CI). Set PIT_BASE_REF to override; GITHUB_BASE_REF is set
# automatically on pull_request. No base ref => full sweep, which now only happens on a manual dispatch.
# "Knee-cap to changed-only now; walk the scope back up as it proves fast enough." The scope should walk
# SIDEWAYS first, to where mutants are decidable (offsets.*), rather than up to everything.
BASE_REF="${PIT_BASE_REF:-${GITHUB_BASE_REF:-}}"
# Explicit full-sweep override: PIT_FULL_SWEEP=true ignores the PR base ref and mutates the whole
# target glob (the manual mutation-full-sweep.yml job uses this; PR runs are otherwise scoped).
if [ "${PIT_FULL_SWEEP:-}" = "true" ]; then BASE_REF=""; fi
# Full-sweep target (manual dispatch only - a PR run derives its target from the changed classes below
# and never reaches this default).
#
# offsets.*, NOT internal.*. internal.* is the concurrency core and close to the worst possible target:
# mutants to locks, loop conditions and timeouts HANG by construction rather than dying fast, which is
# why that sweep has never completed - and its timing-based tests make a survivor unfalsifiable anyway
# ("nothing asserts this" is indistinguishable from "the race didn't happen this run"). The offset
# encoders/decoders are the opposite on both counts: deterministic tests, so a survivor is a REAL gap,
# and high stakes, since a silent bug there means lost or duplicated records.
TARGET_CLASSES="${PIT_TARGET_CLASSES:-bz.stub.parallelconsumer.offsets.*}"
# What gets RUN to compute coverage - the dominant cost of any run (~332s of instrumented full-suite
# execution), paid up front whether you mutate two classes or two hundred. Narrowing this is the only
# lever that touches that cost; caching mutant verdicts does not.
TARGET_TESTS="${PIT_TARGET_TESTS:-bz.stub.parallelconsumer.*}"
if [ -n "$BASE_REF" ]; then
  git fetch --no-tags -q origin "$BASE_REF" 2>/dev/null || true
fi
# Only PR-scope if the base ref actually resolved. On a failed fetch / shallow checkout without the ref,
# the git diff below would hard-crash under `set -euo pipefail` (unknown revision -> exit 128) instead of
# the intended skip/full-sweep - so fall back to the full sweep when the ref is missing (review finding).
# BEWARE what that fallback means: a shallow checkout silently promotes a scoped run into a full sweep.
# That is why mutation was removed from the `local` lane, whose checkout is shallow. The one PR lane
# (maven.yml) checks out with fetch-depth: 0 - keep it that way.
if [ -n "$BASE_REF" ] && git rev-parse --verify -q "origin/${BASE_REF}^{commit}" >/dev/null 2>&1; then
  # --diff-filter=d drops deletions: a deleted class has no target/classes entry, and pitest's
  # failWhenNoMutations (default true) would then fail the goal outright instead of the "nothing to mutate" skip.
  CHANGED_ALL=$(git diff --name-only --diff-filter=d "origin/${BASE_REF}" HEAD -- parallel-consumer-core/src/main/java/ 2>/dev/null \
    | path_to_fqcn \
    | { grep -E '^bz\.stub\.parallelconsumer\.' || true; })
  if [ -z "$CHANGED_ALL" ]; then
    echo "PIT: no core main-source classes changed vs origin/${BASE_REF} - nothing to mutate, skipping."
    summary "### Mutation (PIT): skipped"
    summary "No core main-source classes changed vs \`origin/${BASE_REF}\`, so there was nothing to mutate."
    summary "**This green tick is not evidence about test quality** - it means no work was done."
    # 3, not 0 - "nothing in scope" is a distinct outcome from "scored mutants" and the caller has to
    # be able to tell them apart. See the exit-code contract in this file's header.
    exit 3
  fi

  # DECIDABLE PACKAGES ONLY. Mutating what changed is the right instinct, but it is not free of the
  # §3.2 problem: in the concurrency core, mutants to locks, loop conditions and timeouts HANG rather
  # than dying, and the covering tests are timing-based, so a survivor cannot be told apart from a race
  # that did not happen. A finding you cannot act on is not worth a runner slot - and the job timeout is
  # no protection, it just converts "slow" into "cancelled with nothing scored", which is the same zero
  # signal the full sweep produced for months.
  #
  # So the lane mutates changed classes only where a survivor is DECIDABLE. The scope walks SIDEWAYS
  # on evidence, never up by argument; DECIDABLE is read at the top of this file so the inert-config
  # guard can validate it, and docs/inflight/ci-mutation-testing.md records what each widening
  # measured before it was made.
  CHANGED=$(printf '%s\n' "$CHANGED_ALL" | { grep -E "$DECIDABLE" || true; })
  SKIPPED=$(printf '%s\n' "$CHANGED_ALL" | { grep -Ev "$DECIDABLE" || true; })
  if [ -z "$CHANGED" ]; then
    echo "PIT: changed classes are all outside the decidable packages (${DECIDABLE}) - skipping."
    printf '%s\n' "$SKIPPED" | sed 's/^/PIT:   not mutated: /'
    summary "### Mutation (PIT): skipped"
    summary "Every changed class is outside the packages where a mutation survivor is decidable"
    summary "(currently \`${DECIDABLE}\`), so mutating them would buy hang-prone mutants and survivors"
    summary "that cannot be told apart from a race that did not happen."
    summary ""
    summary "Not mutated:"
    summary '```'
    summary "$SKIPPED"
    summary '```'
    # 3, not 0 - see the exit-code contract in this file's header. The scope regex has already been
    # proved live against the tree above, so this really is "your PR touched nothing decidable"
    # rather than "the scope is broken".
    exit 3
  fi
  # Emit "Foo,Foo$*" per FQCN: the class itself PLUS its nested/synthetic members (Lombok @Builder inner
  # classes, anonymous classes, lambdas). A bare "Foo*" would over-match siblings that merely share the
  # prefix (PIT globs '*' as unbounded '.*', not '.'/'$'-bounded) - e.g. PartitionState* sweeps in
  # PartitionStateManager - attributing mutants to code the PR never touched (ce-review finding).
  TARGET_CLASSES=$(printf '%s\n' "$CHANGED" | sed 's/.*/&,&$*/' | paste -sd, -)
  echo "PIT: PR-scoped to CHANGED decidable classes -> ${TARGET_CLASSES}"
  # Never silently cap coverage: say what was dropped, or a partial run reads as a complete one.
  if [ -n "$SKIPPED" ]; then
    printf '%s\n' "$SKIPPED" | sed 's/^/PIT:   changed but NOT mutated (not decidable): /'
  fi
else
  echo "PIT: no resolvable base ref - full sweep of ${TARGET_CLASSES}"
  # The same inert-config question for the full-sweep path, whose target is a PIT glob rather than a
  # regex. Only the literal prefix before the first '*' can be checked, which is enough to catch the
  # rename class: `bz.stub.parallelconsumer.offsets.` either names classes that exist or it does not.
  # pitest's own failWhenNoMutations would eventually notice, but as a maven failure at the end of a
  # coverage pass rather than as a stale-scope diagnosis before it.
  #
  # It is a COMMA-SEPARATED LIST, and one entry matching is enough. Treating it as a single string
  # was the first version of this guard and it rejected a perfectly good `Foo,Foo$*` target - caught
  # by running the lane rather than by the self-test, which is why both cases are now arms in
  # bin/test-ci-mutation-test.sh.
  TC_MATCHED=0
  TC_PREFIXES=""
  IFS=',' read -r -a TC_PARTS <<< "$TARGET_CLASSES"
  for tc_part in "${TC_PARTS[@]}"; do
    tc_prefix="${tc_part%%\**}"   # everything before the first glob star
    tc_prefix="${tc_prefix%\$}"   # a trailing '$' marks nested classes; it is not part of a name
    if [ -z "$tc_prefix" ]; then TC_MATCHED=1; break; fi   # a bare '*' targets everything
    TC_PREFIXES="${TC_PREFIXES}${TC_PREFIXES:+, }${tc_prefix}"
    if grep -qF "$tc_prefix" <<< "$ALL_MAIN_CLASSES"; then TC_MATCHED=1; break; fi
  done
  if [ "$TC_MATCHED" -eq 0 ]; then
    echo "PIT: full-sweep target '${TARGET_CLASSES}' matches NOTHING in the tree - CANNOT RUN." >&2
    echo "PIT: this is NOT a pass. The target is stale - a package rename is the usual cause." >&2
    summary "### Mutation (PIT): CANNOT RUN - the sweep target is stale"
    summary "No class under \`parallel-consumer-core/src/main/java\` starts with any of \`${TC_PREFIXES}\`."
    summary "Every sweep would score nothing. Fix the target; **this is not a clean result.**"
    exit 2
  fi
fi

# The Lincheck lane is excluded BY NAME in -DexcludedTestClasses below, not by tag, for the reason the
# next note gives:
# whether pitest honours excluded.groups is unverified, and being wrong is expensive here rather than
# merely inaccurate. Each Lincheck class runs a scheduler-controlled search taking seconds, and pitest
# re-runs its covering tests once per mutant.
#
# NB: pitest does not honour excluded.groups - @Quarantined (and chaos/performance) tests are only
# excluded here coincidentally, via the integrationTests source-dir glob. If a quarantined UNIT test
# ever exists when this runs, wire excludedGroups into pitest explicitly (ce-review P3 finding).
# NOTE: pitest-maven's parseSurefireConfig defaults to TRUE and its SurefireConfigConverter DOES read
# surefire's <excludedGroups>, which our pom sets - so this may already be handled and the comment above
# may be obsolete. Unverified: it hinges on whether the ${excluded.groups} property is interpolated in
# the raw Xpp3Dom pitest reads. Verify before either relying on it or "fixing" it.
#
# skipFailingTests is set in the POM, not here: its PitMojo @Parameter declares a defaultValue and no
# `property`, so -DskipFailingTests is SILENTLY IGNORED - no warning, it just doesn't apply. Check for a
# `property` before concluding that some other pitest setting "doesn't work" from the command line.
if [ -n "${PIT_DRY_RUN_LOG:-}" ]; then
  # SELF-TEST SEAM, and nothing in CI sets it. bin/test-ci-mutation-test.sh needs to prove that the
  # verdict below can go red - "PIT produced no statistics" and "PIT generated zero mutants" - and
  # neither is reachable in seconds through a real build. So the run is replaced by a canned log and
  # everything downstream is exercised unchanged. It announces itself loudly for the same reason the
  # skip paths do: a seam that can silently stand in for the real thing is the defect this file is
  # about.
  echo "PIT: DRY RUN - reading a canned PIT log from ${PIT_DRY_RUN_LOG} instead of building."
  cp "$PIT_DRY_RUN_LOG" "$PIT_LOG"
  STATUS=0
else
  set +e
  ./mvnw --batch-mode -Pci test-compile org.pitest:pitest-maven:mutationCoverage \
    -Djacoco.skip=true \
    -DtargetClasses="${TARGET_CLASSES}" \
    -DtargetTests="${TARGET_TESTS}" \
    # The Lincheck harnesses are excluded as PIT TESTS (astubbs#347, inherited via master):
    # each asserts that a violation IS found, so using them as the suite PIT mutates against
    # is meaningless and slow. They remain mutable TARGETS - this excludes them from the test
    # set, not from the target set.
    -DexcludedTestClasses="bz.stub.parallelconsumer.integrationTests.*,bz.stub.parallelconsumer.state.*Lincheck*" \
    -DjvmArgs=-Xmx2g \
    -DoutputFormats=XML,HTML \
    -DtimeoutConstant=30000 -DtimeoutFactor=3.0 \
    -Dthreads="${THREADS}" \
    -pl parallel-consumer-core -am \
    "$@" 2>&1 | tee "$PIT_LOG"
  STATUS=${PIPESTATUS[0]}
  set -e
fi

# Report what actually happened. PIT prints ">> " lines twice over: a per-class running tally during
# analysis, then the run totals under a "- Statistics" banner at the end. Take only the final block -
# the per-class lines look deceptively like totals ("Generated 4 Killed 2") and there are as many of
# them as there are mutated classes. Empty => the run died before scoring anything, which is the
# failure mode most worth naming out loud, since it is what "never completes" looks like.
STATS=$(awk '/^- Statistics$/ {found = 1; out = ""; next}
             found && /^>> / {out = out $0 "\n"}
             END {printf "%s", out}' "$PIT_LOG")
# THE NUMBER, not the presence of the block. A tidy, complete statistics block over zero mutants is
# the harder half of the vacuity problem to spot: everything reads normal and the score line says
# nothing was measured. `tail -1` because a mutated-class tally can carry the same wording, and the
# final block is the run total. Missing block => 0, which the floor below then rejects.
GENERATED=$(sed -nE 's/^>> Generated ([0-9]+) mutations.*/\1/p' <<< "$STATS" | tail -1)
GENERATED="${GENERATED:-0}"
summary "### Mutation (PIT)"
summary ""
summary "Mutated: \`${TARGET_CLASSES}\`"
summary "Coverage run: \`${TARGET_TESTS}\` (minus integrationTests)"
if [ -n "${SKIPPED:-}" ]; then
  # State the cap every time. A run that mutated one of five changed classes must not read like a run
  # that covered the PR.
  summary ""
  summary "Changed but **not** mutated (outside the decidable packages):"
  summary '```'
  summary "$SKIPPED"
  summary '```'
fi
summary ""
if [ -n "$STATS" ]; then
  summary '```'
  summary "$STATS"
  summary '```'
  # skipFailingTests=true (pom) keeps a red test from aborting the whole lane, but it does so by
  # DISCARDING that test's coverage, and pitest logs nothing when it does. So a mutant that only the
  # failing test covered silently becomes "no coverage" rather than killed. The flake itself is not
  # hidden - it still reddens the Unit gate in the same PR run, which is where a flake belongs.
  summary ""
  summary "> Mutations with no coverage can also mean a test failed and was skipped (\`skipFailingTests\`)."
  summary "> Check the Unit gate before treating a no-coverage jump as a real gap."

  # THE SURVIVOR LIST IS THE PRODUCT. A score cannot be acted on - "50% killed" tells nobody which half,
  # so it reads as a grade rather than a work item. Each line below names a specific behaviour that can
  # be broken without any test noticing, which is something a person can actually go and fix.
  #
  # No XML parser needed: pitest writes one <mutation> element per line, so sed suffices. Anything not
  # KILLED or TIMED_OUT survived (PIT counts a timeout as a kill - see the timeoutConstant note above).
  REPORT="parallel-consumer-core/target/pit-reports/mutations.xml"
  if [ -f "$REPORT" ]; then
    # Emit file/line as separate fields so the sort is NUMERIC on the line - sorting "File:119" as text
    # puts line 119 above line 38, which makes the table read as though it were in no order at all.
    # The description's fully-qualified paths are stripped: file:line already locates the mutant, so
    # "for bz/stub/parallelconsumer/offsets/Foo::bar" is 60 characters of noise per row.
    PARSED=$(sed -n "s|.*status='\([A-Z_]*\)'.*<sourceFile>\([^<]*\)</sourceFile>.*<lineNumber>\([0-9]*\)</lineNumber>.*<description>\(.*\)</description>.*|\2\t\3\t\1\t\4|p" "$REPORT")
    # SURVIVED / NO_COVERAGE are findings: something is untested. MEMORY_ERROR / RUN_ERROR / NON_VIABLE
    # are NOT - they mean the mutant could not be evaluated (minion died, bytecode unloadable), which is
    # an infrastructure problem, not a test gap. Listing them under "a behaviour nothing asserts" would
    # be the lane misreporting itself, so they are counted separately (ce-review finding).
    SURVIVORS=$(printf '%s\n' "$PARSED" \
      | awk -F'\t' '$3 == "SURVIVED" || $3 == "NO_COVERAGE"' \
      | sed 's/&quot;/"/g; s/&apos;/'"'"'/g; s/&lt;/</g; s/&gt;/>/g; s/&amp;/\&/g' \
      | sed -E 's#([A-Za-z0-9$_]+/)+([A-Za-z0-9$_]+::)#\2#g' \
      | sort -t'	' -k1,1 -k2,2n || true)
    if [ -n "$SURVIVORS" ]; then
      TOTAL=$(printf '%s\n' "$SURVIVORS" | wc -l | tr -d ' ')
      summary ""
      summary "#### Survived (${TOTAL}) - each is a behaviour nothing asserts"
      summary '```'
      # Cap the table, but never silently: a truncated list that looks complete is worse than no list.
      #
      # The cap is awk's, NOT `head -50`, and that is a correctness fix rather than a tidy-up. Under
      # `set -o pipefail`, `printf ... | head -50` lets head exit after 50 lines; once the unread
      # remainder exceeds the 64 KiB pipe buffer, printf takes SIGPIPE, the pipeline reports 141, and
      # pipefail promotes that to the script's exit status - killing the run BEFORE the mutation
      # verdict below. A large but perfectly valid PIT report would have made this lane look broken.
      # awk consumes its whole input and simply stops printing after NR 50, so nothing closes early.
      #
      # bin/check-shell-sigpipe.sh guards the sibling case (`| grep -q`) but deliberately scopes
      # `head` out as "common, usually harmless" - correct as a blanket rule, and this is the site
      # where it was not harmless. Found by an independent review, not by the gate.
      printf '%s\n' "$SURVIVORS" \
        | awk -F'\t' 'NR <= 50 { printf "%-12s %-34s %s\n", $3, $1 ":" $2, $4 }' >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
      summary '```'
      if [ "$TOTAL" -gt 50 ]; then
        summary "_Showing the first 50 of ${TOTAL}. Full detail in the HTML report under \`${REPORT%/*}\`._"
      fi
    fi
    # Never fold these into the survivor count: an unevaluated mutant is neither killed nor surviving,
    # and quietly treating it as either is how a broken run comes to look like a measured one.
    UNEVALUATED=$(printf '%s\n' "$PARSED" \
      | awk -F'\t' '$3 != "KILLED" && $3 != "TIMED_OUT" && $3 != "SURVIVED" && $3 != "NO_COVERAGE"' \
      | awk -F'\t' '{ print $3 }' | sort | uniq -c | sed 's/^ *//' || true)
    if [ -n "$UNEVALUATED" ]; then
      summary ""
      summary "#### Could not be evaluated - infrastructure, not test gaps"
      summary '```'
      summary "$UNEVALUATED"
      summary '```'
      summary "These mutants were neither killed nor survived; the score above is over a smaller set than it looks."
    fi
  fi
else
  summary "**No mutants were scored** - the run did not reach the statistics stage."
  # The green-suite abort should no longer be reachable (skipFailingTests is set in the pom), so if it
  # IS reached, say so precisely rather than leaving someone to hunt through the log: it means something
  # turned that setting off - most likely a <testFailureIgnore> added to the surefire config, which
  # parseSurefireConfig imports straight over the top of it.
  # `testClass=` filters to PIT's per-test SEVERE lines - maven's own [ERROR] summary repeats the same
  # phrase without naming anything, and would otherwise be listed as if it were a test.
  # `|| true` because this is the EXPECTED-empty case: we are already on the failure path, and under
  # `set -euo pipefail` a no-match grep would abort the script here - losing the summary below and
  # replacing PIT's exit status with grep's.
  FAILED_TESTS=$(grep 'did not pass without mutation' "$PIT_LOG" 2>/dev/null \
    | grep 'testClass=' \
    | sed -E 's/.*testClass=([^,]+).*\[method:([^]]+)\].*/\1.\2/' | sort -u || true)
  if [ -n "$FAILED_TESTS" ]; then
    summary "Cause: **the suite was not green**, so PIT refused to attribute kills to mutants."
    summary "This should be unreachable - check that \`skipFailingTests\` is still set on pitest-maven."
    summary '```'
    summary "$FAILED_TESTS"
    summary '```'
  else
    summary "Usual causes: the coverage pass never finished, or minions died (MEMORY_ERROR / TIMED_OUT)."
  fi
fi

# ---------------------------------------------------------------------------------------------
# THE VERDICT. Everything above reports; this decides. See the exit-code contract in the header.
#
# PIT's own failure wins, because it is the more specific diagnosis - a maven or minion failure is
# a different thing from "ran fine, measured nothing", and collapsing them would lose which.
# ---------------------------------------------------------------------------------------------
if [ "$STATUS" -ne 0 ]; then
  echo "PIT: exited ${STATUS} after generating ${GENERATED} mutant(s)." >&2
  exit "$STATUS"
fi
# GENERATED counts mutants CREATED, which is not the same as mutants EVALUATED, and the verdict must
# turn on the second. A run whose minions all die returns MEMORY_ERROR or RUN_ERROR for every mutant:
# it generated plenty, scored none, and against a generated-count floor it exits 0. That is not
# hypothetical here - the plan that scoped this lane recorded `state.*` hanging with minions dying,
# which is exactly the shape. An independent review caught the verdict still reading GENERATED while
# the survivor table twenty lines up already drew the right distinction.
#
# KILLED / SURVIVED / TIMED_OUT / NO_COVERAGE are all real outcomes (PIT scores a timeout as a kill,
# and no-coverage is a genuine finding). MEMORY_ERROR / RUN_ERROR / NON_VIABLE are infrastructure
# failures or invalid bytecode - the mutant was never actually tried.
EVALUATED=0
UNEVALUATED=0
VERDICT_REPORT="parallel-consumer-core/target/pit-reports/mutations.xml"
if [ -f "$VERDICT_REPORT" ]; then
  # The `|| true` is load-bearing under `set -o pipefail`: a grep that matches nothing exits 1, which
  # kills the whole script mid-assignment and produces an exit 1 with no output at all - neither the
  # pass this run might deserve nor the exit 2 a vacuous one must give. An all-KILLED report has no
  # MEMORY_ERROR in it, so the no-match case is the NORMAL case here, not the exceptional one.
  EVALUATED=$({ grep -oE "status='(KILLED|SURVIVED|TIMED_OUT|NO_COVERAGE)'" "$VERDICT_REPORT" || true; } | wc -l | tr -d ' ')
  UNEVALUATED=$({ grep -oE "status='(MEMORY_ERROR|RUN_ERROR|NON_VIABLE)'" "$VERDICT_REPORT" || true; } | wc -l | tr -d ' ')
fi

if [ "$EVALUATED" -lt "$MIN_MUTANTS" ]; then
  echo "PIT: the run scored NOTHING - ${GENERATED} mutant(s) generated, ${EVALUATED} EVALUATED, floor is ${MIN_MUTANTS}." >&2
  if [ "$UNEVALUATED" -gt 0 ]; then
    echo "PIT: ${UNEVALUATED} mutant(s) came back MEMORY_ERROR / RUN_ERROR / NON_VIABLE - the minions died or the bytecode would not load." >&2
    echo "PIT: that is an infrastructure failure, not a clean run. Generating mutants is not scoring them." >&2
  fi
  echo "PIT: exiting 2. A lane that measured nothing must not report the same result as one that did." >&2
  summary ""
  summary "**This run scored NOTHING** (${GENERATED} generated, ${EVALUATED} evaluated, floor \`${MIN_MUTANTS}\`),"
  summary "even though it had classes to mutate. That is a broken lane, not a clean pull request."
  exit 2
fi
if [ "$UNEVALUATED" -gt 0 ]; then
  echo "PIT: warning - ${UNEVALUATED} of ${GENERATED} mutant(s) were never evaluated (minion death or unloadable bytecode)." >&2
fi
echo "PIT: scored ${EVALUATED} evaluated mutant(s) of ${GENERATED} generated - the lane did work."
exit 0
