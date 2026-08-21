#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-toolchain-versions.sh.
#
# The gate's whole job is to notice a difference between two files, so the case that matters is not
# "does it pass on a healthy repo" - it is "can it be made to go red, for each way the two can
# disagree". bin/AGENTS.md: a regression test that has never failed proves nothing.
#
# Every case drives the gate against crafted fixtures through PC_MISE_TOML / PC_CLIENTS_WORKFLOW, so
# none of it depends on the state of the real repository - a self-test for a drift checker must not
# itself drift when someone legitimately bumps a version.
#
# Run: bin/test-check-toolchain-versions.sh

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GATE="$REPO_ROOT/bin/check-toolchain-versions.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# A healthy pair, which every case below mutates in exactly one way.
write_mise() { # <path> [extra lines...]
    {
        echo '[tools]'
        echo 'go = "1.25.13"'
        echo 'ruby = "3.4.4"'
        echo 'node = "22.17.0"'
        shift
        for line in "$@"; do echo "$line"; done
    } >"$1"
}

write_workflow() { # <path> <go-version> <ruby-version> <node-version> <swift-version>
    cat >"$1" <<EOF
jobs:
  clients:
    strategy:
      matrix:
        include:
          - language: go
            module: parallel-consumer-proxy-client-go
            toolchain: '$2'
          - language: ruby
            module: parallel-consumer-proxy-client-ruby
            toolchain: '$3'
          - language: typescript
            module: parallel-consumer-proxy-client-typescript
            toolchain: '$4'
          - language: swift
            module: parallel-consumer-proxy-client-swift
            toolchain: '$5'
EOF
}

verdict() { # <mise> <workflow> -> PASS | FAIL
    if PC_MISE_TOML="$1" PC_CLIENTS_WORKFLOW="$2" "$GATE" >/dev/null 2>&1; then
        echo PASS
    else
        echo FAIL
    fi
}

# The baseline. Without this the red cases below could all be red for some unrelated reason.
write_mise "$TMP/ok.toml"
write_workflow "$TMP/ok.yml" "1.25.13" "3.4.4" "22.17.0" ""
assert "matching declarations pass" PASS "$(verdict "$TMP/ok.toml" "$TMP/ok.yml")"

# The failure the gate was written for, at the size it actually occurred (a whole major version).
write_workflow "$TMP/major.yml" "1.25.13" "3.4.4" "25.9.0" ""
assert "a major-version disagreement is caught" FAIL "$(verdict "$TMP/ok.toml" "$TMP/major.yml")"

# ...and at the smallest size, because these are two declarations of one number: a patch difference
# between them is a mistake, not tolerable drift.
write_workflow "$TMP/patch.yml" "1.25.14" "3.4.4" "22.17.0" ""
assert "a single patch disagreement is caught" FAIL "$(verdict "$TMP/ok.toml" "$TMP/patch.yml")"

# The typescript -> node mapping is the one place the two files use different names for one thing.
# If it broke, the baseline case above would fail; this asserts the mapping is real rather than the
# gate ignoring anything it does not recognise.
write_mise "$TMP/nonode.toml"
grep -v '^node' "$TMP/nonode.toml" >"$TMP/nonode2.toml" && mv "$TMP/nonode2.toml" "$TMP/nonode.toml"
assert "a language CI pins but mise omits is caught" FAIL "$(verdict "$TMP/nonode.toml" "$TMP/ok.yml")"

# A tool nobody builds with is dead weight that will rot; it is also how a rename hides.
write_mise "$TMP/extra.toml" 'elixir = "1.18.0"'
assert "a mise tool no language uses is caught" FAIL "$(verdict "$TMP/extra.toml" "$TMP/ok.yml")"

# Swift builds in a container. If it ever gains a host pin, that is either a real change or a stale
# copy-paste, and both need a human.
write_workflow "$TMP/swiftpin.yml" "1.25.13" "3.4.4" "22.17.0" "6.1.0"
assert "a container language gaining a CI pin is caught" FAIL "$(verdict "$TMP/ok.toml" "$TMP/swiftpin.yml")"

write_mise "$TMP/swiftmise.toml" 'swift = "6.1.0"'
assert "a container language appearing in mise.toml is caught" FAIL "$(verdict "$TMP/swiftmise.toml" "$TMP/ok.yml")"

# A pinned language silently losing its version reads as "no toolchain needed", which is exactly the
# state the four container/JVM languages are in - so it must not be inferred.
write_workflow "$TMP/lost.yml" "" "3.4.4" "22.17.0" ""
assert "a pinned language losing its CI version is caught" FAIL "$(verdict "$TMP/ok.toml" "$TMP/lost.yml")"

# THE MOST IMPORTANT CASE. If the workflow format changes under it, a regex-based gate parses zero
# languages and every comparison trivially succeeds - a green run that checked nothing, which is
# worse than no gate because it is indistinguishable from a real pass.
echo 'jobs: {}' >"$TMP/empty.yml"
assert "a workflow it cannot parse FAILS rather than passing vacuously" FAIL "$(verdict "$TMP/ok.toml" "$TMP/empty.yml")"

# Same argument for the inputs themselves.
assert "a missing mise.toml fails"  FAIL "$(verdict "$TMP/does-not-exist.toml" "$TMP/ok.yml")"
assert "a missing workflow fails"   FAIL "$(verdict "$TMP/ok.toml" "$TMP/does-not-exist.yml")"

# The real repository must be consistent, or the gate is about to fail CI for everyone.
if "$GATE" >/dev/null 2>&1; then real=PASS; else real=FAIL; fi
assert "this repository's own declarations agree" PASS "$real"

echo
if [ "$failures" -eq 0 ]; then
    echo "All check-toolchain-versions self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
