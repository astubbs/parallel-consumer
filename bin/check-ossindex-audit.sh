#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Decides whether an ossindex-maven-plugin run actually SCANNED, and renders what it found.
#
# WHY THIS EXISTS
#
# `ossindex-maven-plugin` cannot be configured to fail when the scanner is unreachable. Its `fail`
# parameter covers "vulnerable components were found", not "the request failed". Verified directly,
# with a cold client cache and an expired token:
#
#     [WARNING] Failed to fetch component-reports
#     org.sonatype.ossindex.service.client.transport.Transport$TransportException:
#         Unexpected response; status: HTTP/1.1 401 Unauthorized
#     [INFO] BUILD SUCCESS
#
# So a green Maven run is NEVER by itself evidence that the audit ran, and a token that expires six
# months from now degrades silently to green-but-not-scanning. That is the defect this repo already
# shipped once - the audit sat on `validate` with `fail=false`, 401ing on every module while every
# build stayed green. A CI job without this check would rebuild it somewhere more expensive.
#
# Beware the client's own on-disk report cache (macOS: ~/Library/Application Support/Sonatype/
# Ossindex; Linux: under $XDG_CACHE_HOME). A warm cache serves results even with a bogus base-URL,
# so any experiment about reachability must clear it first, and CI must never cache that directory.
#
# THE SPLIT THIS SCRIPT ENCODES
#
#   scan did not run          -> exit 1  (red: the check itself is broken, and that IS actionable)
#   scan ran, found nothing   -> exit 0
#   scan ran, found problems  -> exit 0, findings rendered  (reporting only, deliberately)
#
# Findings are not fatal today. The original reason was a standing backlog - a job that goes red on
# every PR for known debt is ignored inside a week - but astubbs/parallel-consumer#281 triaged it, so
# the audit now reports zero findings and gating would NOT be permanently red. What remains is a
# weaker, deliberate caution: the exclusions carrying that backlog are only as good as their stated
# retirement conditions, and this lane has not yet run clean over time. Red stays reserved for "this
# check stopped working", which is rare and therefore still means something. Flipping findings to
# fatal is a one-line change here, and the argument for it is now stronger than the argument against
# - a NEW finding, on a tree whose known debt is already excluded with reasons, is by construction
# something nobody has looked at.
#
# HOW "DID IT ACTUALLY SCAN" IS DECIDED
#
# Three independent legs, all fail-closed:
#
#   1. NEGATIVE, on the log:  `Failed to fetch component-reports` anywhere -> did not scan.
#   2. POSITIVE, structural:  every exported report must carry a non-empty `reports` map. This is
#      the leg that survives a wording change upstream. On a 401 the plugin still writes the report
#      file, but writes `{ }` - so "the file exists" proves nothing and "the file has content" does.
#   3. POSITIVE, coverage:    one exported report per module the log says it checked. Catches a
#      module that silently produced nothing while its siblings scanned fine.
#
# Leg 2 is the one a token expiry trips. Both directions are exercised by
# bin/test-check-ossindex-audit.sh against real 401-shaped and success-shaped fixtures.
#
# USAGE
#
#   bin/check-ossindex-audit.sh <maven-log-file> [tree-root]
#
# The Maven run must export reports. Clear the previous run's first: leg 3 counts every
# `ossindex-report.json` under the tree with no run-identity check, so a stale one - from an earlier
# run, or from a wider one when you are now building a subset - is counted as this run's output. It
# can then either mask a module that genuinely produced nothing, or invent a shortfall that is not
# real. CI is immune (fresh checkout every time); a local re-run is not.
#
#   find . -type f -name ossindex-report.json -delete
#   ./mvnw --batch-mode test-compile \
#       -Dossindex.skip=false -Dossindex.fail=false -Dossindex.authId=ossindex \
#       -Dossindex.reportFile=target/ossindex-report.json | tee audit.log
#
# `test-compile` rather than `validate`: the audit mojo resolves TEST-scope dependencies, and the
# modules depend on `parallel-consumer-core:jar:tests`, so on a machine without those snapshots
# installed a bare `ossindex:audit` dies with "Could not resolve dependencies". Verified.
#
# Markdown report goes to stdout; CI appends it to $GITHUB_STEP_SUMMARY. Progress goes to stderr.

set -euo pipefail

LOG="${1:?usage: check-ossindex-audit.sh <maven-log-file> [tree-root]}"
ROOT="${2:-$(cd "$(dirname "$0")/.." && pwd)}"

# The line the plugin logs when the component-report request fails, whatever the cause - 401, DNS,
# timeout. A named constant because the self-test uses it to build its negative fixture.
FETCH_FAILURE_MARKER="Failed to fetch component-reports"
# The line it logs once per module it audits. Feeds the coverage leg only.
MODULE_AUDITED_MARKER="Checking for vulnerabilities;"

if [ ! -f "$LOG" ]; then
    echo "FAIL: Maven log not found: $LOG" >&2
    exit 1
fi

# grep with a FILE argument, never `cat ... | grep -q`: an early-exiting reader on a pipe takes the
# writer down with EPIPE, and under `set -o pipefail` a MATCH then reads as failure. See
# bin/AGENTS.md -> "Scripts that guard other scripts".
scan_failed=0
if grep -qF "$FETCH_FAILURE_MARKER" "$LOG"; then
    scan_failed=1
fi

modules_in_log=$(grep -cF "$MODULE_AUDITED_MARKER" "$LOG" || true)

# Collect the exported reports. -print0 / read -d '' so a path containing a space cannot split.
reports=()
while IFS= read -r -d '' f; do
    reports+=("$f")
done < <(find "$ROOT" -type f -name 'ossindex-report.json' -print0)

# Facts out of the JSON, then the findings table. Emitted as `key=value` lines, a `---` sentinel,
# then markdown - so bash keeps the verdict and python keeps only the parsing.
facts=$(python3 - "${reports[@]+"${reports[@]}"}" <<'PY'
import json, sys

paths = sorted(sys.argv[1:])
empty = 0
components = 0
# coordinates -> {"advisories": {id: score}, "modules": set()}
found = {}


def top_of(entry):
    """Highest CVSS on a component, 0.0 when nothing on it carries a score.

    A coordinate can appear under `vulnerable` with an empty (or absent) `vulnerabilities` list -
    an advisory OSS Index has not scored yet - which leaves `advisories` empty. Bare `max()` on
    that raises ValueError, and since the caller runs under `set -euo pipefail` that would kill
    the whole guard with a traceback: a scan that DID run, reported as an unexplained red.
    """
    scores = entry["advisories"].values()
    return max(scores) if scores else 0.0


for p in paths:
    try:
        with open(p) as fh:
            doc = json.load(fh)
    except (OSError, ValueError) as e:
        print("parse_error=%s: %s" % (p, e), file=sys.stderr)
        doc = {}
    if not isinstance(doc, dict):
        doc = {}
    reports = doc.get("reports") or {}
    if not reports:
        empty += 1
        continue
    components += len(reports)
    module = p.rsplit("/target/", 1)[0].rsplit("/", 1)[-1]
    for coords, report in (doc.get("vulnerable") or {}).items():
        entry = found.setdefault(coords, {"advisories": {}, "modules": set()})
        entry["modules"].add(module)
        for i, vuln in enumerate(report.get("vulnerabilities") or []):
            # Positional fallback rather than a single shared "?": OSS Index does not always carry
            # an id, and collapsing every unidentified advisory onto one key silently overwrites
            # scores and under-counts findings. Positional keeps the same advisory de-duplicating
            # across modules, which is what the shared key was getting right by accident.
            vuln_id = vuln.get("id") or ("unidentified-%d" % i)
            entry["advisories"][vuln_id] = vuln.get("cvssScore") or 0.0

advisories = sum(len(e["advisories"]) for e in found.values())
scores = [s for e in found.values() for s in e["advisories"].values()]

print("report_files=%d" % len(paths))
print("empty_reports=%d" % empty)
print("components=%d" % components)
print("vulnerable=%d" % len(found))
print("advisories=%d" % advisories)
print("top_score=%s" % (max(scores) if scores else 0))
print("---")

if found:
    print("| Component | Advisories | Top CVSS | Modules |")
    print("|---|---|---|---|")
    # Worst first, so the thing to act on is the first thing read.
    for coords, e in sorted(found.items(), key=lambda kv: -top_of(kv[1])):
        ids = ", ".join("`%s`" % i for i in sorted(e["advisories"]))
        print("| `%s` | %s | %s | %s |" % (
            coords, ids, top_of(e), ", ".join(sorted(e["modules"]))))
PY
)

# One pass over the `key=value` block rather than one `sed` per fact. The name whitelist keeps the
# parser's output from assigning arbitrary variables, and the emptiness check below turns a fact
# renamed above but not here into a loud failure - previously it just became an empty variable, with
# nothing tying the two halves together.
empty_reports='' report_files='' components='' vulnerable='' advisories='' top_score=''
while IFS='=' read -r key value; do
    case "$key" in
        ---) break ;;
        empty_reports|report_files|components|vulnerable|advisories|top_score)
            printf -v "$key" '%s' "$value" ;;
    esac
done <<< "$facts"

for fact in empty_reports report_files components vulnerable advisories top_score; do
    if [ -z "${!fact}" ]; then
        echo "FAIL: internal error - the audit parser emitted no '${fact}' fact." >&2
        exit 1
    fi
done

findings_table=$(sed -n '/^---$/,$p' <<< "$facts" | tail -n +2)

problems=()
if [ "$scan_failed" -eq 1 ]; then
    problems+=("the plugin logged \"${FETCH_FAILURE_MARKER}\" - the scanner was unreachable, or rejected our credentials")
fi
if [ "$report_files" -eq 0 ]; then
    problems+=("no ossindex-report.json was exported anywhere under ${ROOT} - the audit did not run at all")
fi
if [ "$empty_reports" -gt 0 ]; then
    problems+=("${empty_reports} of ${report_files} exported report(s) hold zero component reports - that is the shape a 401 leaves behind")
fi
if [ "$report_files" -gt 0 ] && [ "$modules_in_log" -ne "$report_files" ]; then
    problems+=("the log says ${modules_in_log} module(s) were checked but ${report_files} report(s) were exported - a module produced nothing")
fi

if [ "${#problems[@]}" -gt 0 ]; then
    echo "## :x: OSS Index audit did not run"
    echo
    echo "**This is not a vulnerability finding.** The scan did not happen, so an empty result"
    echo "means nothing. Likely causes, in order: \`OSSINDEX_TOKEN\` expired or was revoked; the"
    echo "\`ossindex\` server id is not reaching Maven; ossindex.sonatype.org is down."
    echo
    for p in "${problems[@]}"; do
        echo "- ${p}"
    done
    echo
    echo "Reproduce locally with an \`ossindex\` \`<server>\` in \`~/.m2/settings.xml\`:"
    echo
    cat <<'REPRO'
```
find . -type f -name ossindex-report.json -delete   # stale reports are counted as this run's
./mvnw --batch-mode test-compile -Dossindex.skip=false -Dossindex.fail=false \
    -Dossindex.authId=ossindex -Dossindex.reportFile=target/ossindex-report.json | tee audit.log
bin/check-ossindex-audit.sh audit.log
```
REPRO
    echo "FAIL: the OSS Index audit did not scan - ${#problems[@]} problem(s), see the report above." >&2
    exit 1
fi

if [ "$vulnerable" -eq 0 ]; then
    echo "## :white_check_mark: OSS Index audit: no vulnerable components"
else
    echo "## :warning: OSS Index audit: ${vulnerable} vulnerable component(s), ${advisories} advisory(ies)"
fi
echo
echo "Scanned **${components}** resolved components across **${report_files}** module(s)."
if [ "$vulnerable" -gt 0 ]; then
    echo
    echo "Highest CVSS score seen: **${top_score}**."
    echo
    printf '%s\n' "$findings_table"
fi
echo
echo "_Findings are **reported, not blocking**. This check goes red only when the scan fails to"
echo "run - see the header of \`bin/check-ossindex-audit.sh\` for why._"

echo "ok:   OSS Index audit ran - ${report_files} module report(s), ${components} components, ${vulnerable} vulnerable" >&2
