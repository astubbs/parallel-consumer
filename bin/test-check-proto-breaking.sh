#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-proto-breaking.sh - the freeze gate for the proxy wire schema.
#
# The freeze unit's own text required this ("verify the failure case so the gate is proven able to
# say no") and the demonstration was a one-off by hand, which proves the gate could say no ONCE, on
# one machine, to one person. Nothing has re-run it since, and a gate nobody can see fail is a gate
# nobody knows has stopped working - the repo's green-without-having-run class,
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md.
#
# Mutates the real schema one way at a time, asserts the gate's verdict, and restores it:
#    1. baseline, schema untouched                                  -> pass (0)
#    2. a field DELETED                                             -> FAIL (1)
#    3. a field RENUMBERED                                          -> FAIL (1)
#    4. a field RENAMED                                             -> FAIL (1)
#    5. a field ADDED at a fresh number                             -> pass (0)
#    6. the module RENAMED in the baseline, and a field deleted     -> FAIL (1)
#    7. a baseline that genuinely carries no schema                 -> pass (0), with the notice
#
# Case 4 pins the CATEGORY choice: a rename is wire-compatible, so `buf breaking` in WIRE would
# pass it. FILE is what the compatibility promise needs, because ten client languages sit on the
# generated APIs, not on the bytes - and nothing else would notice the config being weakened.
#
# Cases 6 and 7 are the two halves of the silent-disarm hole the gate shipped with. It armed only
# when the schema was at ONE hardcoded path on the baseline ref, so a post-freeze PR that renamed
# the module, updated the constant and deleted a frozen field re-entered the "nothing frozen yet"
# branch and passed green. Case 6 is that exact PR, and it must go red; case 7 is the branch it was
# abusing, which must still pass while the freeze is genuinely not on master yet - as it is not,
# today, on this branch.
#
# Baselines are built without touching the shared git index: the fixture refs are commit objects
# written with `commit-tree` against a temp GIT_INDEX_FILE, so a parallel agent's staged work is
# never disturbed and no branch is moved. They carry an identity supplied with `git -c`, because
# `commit-tree` refuses to write an object when git cannot resolve a committer and a GitHub-hosted
# runner has none configured - which is how cases 6 and 7 failed on every CI run while passing on
# every developer box. Neither fixture's verdict may be read unless its ref was actually built: an
# empty ref reaches the gate as PROTO_BREAKING_BASELINE_REF= and its `:-` default quietly
# substitutes origin/master, which is the grace branch case 7 asserts - so case 7 reported ok while
# testing nothing at all.
#
# Run: bin/test-check-proto-breaking.sh   (CI runs it before the gate it protects)
#
# A developer box has a configured git user and a hosted runner does not, so the green that matters
# is the one WITHOUT one - and the runner's condition cannot be reproduced locally by unsetting
# config, which every worktree of this clone shares. Run this arm instead, which is stricter than
# the runner (an empty environment identity outranks every config level) and must also pass:
#   GIT_AUTHOR_NAME='' GIT_COMMITTER_NAME='' bin/test-check-proto-breaking.sh

set -uo pipefail

cd "$(git rev-parse --show-toplevel)"

GATE=bin/check-proto-breaking.sh
MODULE=parallel-consumer-proxy-protocol
PROTO=$MODULE/src/main/proto/parallelconsumer/proxy/v1/proxy.proto
failures=0

if ! command -v buf >/dev/null 2>&1; then
  echo "test-check-proto-breaking: buf is not installed; the gate cannot run, so neither can its self-test." >&2
  echo "CI installs it via bufbuild/buf-action; locally it is a managed toolchain." >&2
  exit 2
fi

WORK=$(mktemp -d)
PRISTINE=$WORK/pristine

# Restore by copying bytes back, for the reason bin/test-check-docs-data.sh sets out: a variable
# round trip through command substitution strips trailing newlines and silently rewrites the file.
cleanup() {
  if [ -f "$WORK/proto.orig" ]; then cp "$WORK/proto.orig" "$PROTO"; fi
  rm -rf "$WORK"
}
trap cleanup EXIT

cp "$PROTO" "$WORK/proto.orig"
mkdir -p "$PRISTINE"
cp -r "$MODULE" "$PRISTINE/$MODULE"

restore() { cp "$WORK/proto.orig" "$PROTO"; }

# expect <expected-exit> <label> [env assignments...] - runs the gate and reports what it said
expect() {
  local want=$1 label=$2; shift 2
  local got output
  output=$(env "$@" "$GATE" 2>&1)
  got=$?
  if [ "$got" -eq "$want" ]; then
    printf 'ok:   %s (exit %s)\n' "$label" "$got"
  else
    printf 'FAIL: %s - expected exit %s, got %s. The gate said:\n' "$label" "$want" "$got"
    printf '%s\n' "$output" | sed 's/^/      | /'
    failures=$((failures + 1))
  fi
}

# expect_mentioning <expected-exit> <pattern> <label> [env assignments...] - verdict AND the reason
#
# An exit code alone cannot tell "passed because the schema only gained" from "passed because the
# gate decided it had nothing to compare against", which is precisely the confusion cases 6 and 7
# exist to separate.
expect_mentioning() {
  local want=$1 pattern=$2 label=$3; shift 3
  local got output
  output=$(env "$@" "$GATE" 2>&1)
  got=$?
  # Herestring, not `printf | grep -q`: grep exits on its first match, printf takes EPIPE, and
  # pipefail promotes 141 to the pipeline's status - so finding the pattern would FAIL the case.
  # bin/check-shell-sigpipe.sh enforces this across the directory.
  if [ "$got" -eq "$want" ] && grep -q "$pattern" <<<"$output"; then
    printf 'ok:   %s (exit %s, mentioning %s)\n' "$label" "$got" "$pattern"
  else
    printf 'FAIL: %s - expected exit %s mentioning "%s", got exit %s. The gate said:\n' \
      "$label" "$want" "$pattern" "$got"
    printf '%s\n' "$output" | sed 's/^/      | /'
    failures=$((failures + 1))
  fi
}

# mutate <python-expression-on-t> - edits the schema in place
mutate() {
  python3 - "$PROTO" "$1" <<'PYTHON'
import pathlib
import re
import sys

path, expression = sys.argv[1], sys.argv[2]
p = pathlib.Path(path)
t = p.read_text()
new = eval(expression, {"t": t, "re": re})  # noqa: S307 - fixture mutation, inputs are literals here
if new == t:
    print(f"self-test fixture did not change {path}", file=sys.stderr)
    sys.exit(2)
p.write_text(new)
PYTHON
  if [ $? -ne 0 ]; then
    printf 'FAIL: could not apply fixture to %s\n' "$PROTO"
    failures=$((failures + 1))
  fi
}

# fixture_commit_tree <commit-tree args...> - `git commit-tree` that works with no configured user
#
# The identity is supplied per-invocation in the ENVIRONMENT, which nothing outranks. Not written
# to config, because config is shared by every worktree of this clone and setting user.name here
# would reach into a parallel agent's checkout; and not passed with `git -c`, which config-level
# precedence lets an empty GIT_COMMITTER_NAME in the environment beat - so the runner's condition
# would be fixed while the stricter arm below still failed. Who stamps a throwaway fixture is
# irrelevant to every assertion in this file; that it can be stamped at all is not.
# bin/test-check-copyright-headers.sh solves the same problem by configuring the fixture repo its
# `new_repo` creates - it has one to configure, and this file deliberately does not.
fixture_commit_tree() {
  GIT_AUTHOR_NAME='proto-gate self-test'    GIT_AUTHOR_EMAIL='self-test@invalid' \
  GIT_COMMITTER_NAME='proto-gate self-test' GIT_COMMITTER_EMAIL='self-test@invalid' \
    git commit-tree "$@"
}

# commit_fixture <prefix> - a commit object holding HEAD's module at <prefix>, on no branch
#
# GIT_INDEX_FILE points at a path that does NOT exist yet: git rejects an existing empty file as a
# malformed index, so `mktemp` (which creates one) fails here where `mktemp -u` works.
commit_fixture() {
  local prefix=$1 idx tree
  idx=$(mktemp -u)
  GIT_INDEX_FILE=$idx git read-tree --prefix="$prefix/" "HEAD:$MODULE" || return 1
  tree=$(GIT_INDEX_FILE=$idx git write-tree) || return 1
  rm -f "$idx"
  fixture_commit_tree "$tree" -m "self-test fixture: the frozen module at $prefix"
}

empty_fixture() {
  fixture_commit_tree "$(git mktree </dev/null)" -m "self-test fixture: a baseline with no schema"
}

expect 0 "baseline: the working tree matches the frozen module" "PROTO_BREAKING_AGAINST=$PRISTINE/$MODULE"

mutate 't.replace("  optional int32 max_failure_history = 17;\n", "", 1)'
expect 1 "a deleted field is caught" "PROTO_BREAKING_AGAINST=$PRISTINE/$MODULE"
restore

mutate 't.replace("optional int32 max_failure_history = 17;", "optional int32 max_failure_history = 117;", 1)'
expect 1 "a renumbered field is caught" "PROTO_BREAKING_AGAINST=$PRISTINE/$MODULE"
restore

# Wire-compatible, and therefore invisible to the WIRE category - this case is what pins FILE.
mutate 't.replace("optional string pc_instance_tag = 27;", "optional string pc_instance_label = 27;", 1)'
expect 1 "a renamed field is caught - the reason the category is FILE, not WIRE" \
  "PROTO_BREAKING_AGAINST=$PRISTINE/$MODULE"
restore

mutate 't.replace("  optional string pc_instance_tag = 27;\n", "  optional string pc_instance_tag = 27;\n  optional string self_test_addition = 99;\n", 1)'
expect 0 "an additive field passes - the wire may gain" "PROTO_BREAKING_AGAINST=$PRISTINE/$MODULE"
restore

# Case 6: the silent-disarm PR. The baseline holds the module under a DIFFERENT name, exactly as a
# renaming PR leaves it, and the working tree deletes a frozen field. Discovery must find it anyway.
renamed_baseline=$(commit_fixture renamed-proxy-protocol)
if [ -z "${renamed_baseline:-}" ]; then
  printf 'FAIL: could not build the renamed-module baseline fixture\n'
  failures=$((failures + 1))
else
  mutate 't.replace("  optional int32 max_failure_history = 17;\n", "", 1)'
  expect_mentioning 1 "the module moved since" \
    "a renamed module does not re-enter the grace branch - the deletion is still caught" \
    "PROTO_BREAKING_BASELINE_REF=$renamed_baseline"
  restore
fi

# Case 7: the grace branch itself, which must survive the hardening - master genuinely has no
# frozen schema until the freeze merges, and this gate may not go red on every PR until then.
#
# Guarded exactly as case 6 is, and for a sharper reason: this is the one case whose expected
# verdict the gate ALSO reaches when handed nothing. An unbuilt fixture arrives as
# PROTO_BREAKING_BASELINE_REF= , the gate's `:-origin/master` default replaces it, and today's
# origin/master genuinely carries no frozen schema - so the case printed ok for exactly the reason
# it was written to distinguish from.
empty_baseline=$(empty_fixture)
if [ -z "${empty_baseline:-}" ]; then
  printf 'FAIL: could not build the empty baseline fixture - the case below would pass by falling back to the default baseline\n'
  failures=$((failures + 1))
else
  expect_mentioning 0 "nothing frozen to compare against" \
    "a baseline with no schema anywhere passes, and says that is why" \
    "PROTO_BREAKING_BASELINE_REF=$empty_baseline"
fi

expect 0 "restored: the working tree matches the frozen module again" "PROTO_BREAKING_AGAINST=$PRISTINE/$MODULE"

if [ "$failures" -ne 0 ]; then
  printf '\ntest-check-proto-breaking: %s case(s) failed\n' "$failures"
  exit 1
fi
printf '\ntest-check-proto-breaking: all cases pass\n'
