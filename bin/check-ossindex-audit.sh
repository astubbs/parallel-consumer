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
# TWO REDS, AND THEY MUST NOT BE CONFUSED
#
#   scan did not run          -> exit 1  (red: the CHECK is broken - an empty result means nothing)
#   scan ran, found nothing   -> exit 0
#   scan ran, found problems  -> exit 2  (red: the TREE has an advisory nobody has looked at)
#
# Distinct exit codes, distinct headings and distinct stderr lines, because they demand opposite
# responses: exit 1 means go fix the credentials or the lane and learn nothing about the tree; exit 2
# means the lane worked and there is a real advisory to triage. Collapsing them into one
# undifferentiated red would make the second unreadable - the reader could no longer tell whether the
# scanner had anything to say. When both are true at once, exit 1 wins: findings from a scan that
# cannot be proven to have happened are not evidence of anything, in either direction.
#
# WHY FINDINGS ARE FATAL
#
# They were not, once, and the reason was a standing backlog: a job that goes red on every PR for
# known debt is ignored inside a week. astubbs/parallel-consumer#281 retired that backlog - every
# item is now either fixed or an explicit `excludeVulnerabilityIds` entry in the root pom carrying a
# stated retirement condition. That inverts the argument. On a tree whose known debt is already
# excluded *with reasons*, a finding is by construction something nobody has looked at, and each one
# is either a real advisory or an exclusion that needs writing down. Nothing red is left standing.
#
# And PR-time rather than the schedule alone, because this repo has one maintainer: a weekly digest
# nobody is obliged to read is not a control, it is mail. The PR gate is the only channel that
# reliably has attention, so that is where the finding has to land. The schedule still matters for
# what a PR cannot see - an unchanged tree acquiring a new advisory - but it is the second channel,
# not the first.
#
# The cost is a false positive blocking unrelated work, and it is real: OSS Index produced two false
# positives and one CVE id with no public record in a single run against this repo
# (docs/solutions/security-issues/oss-index-reports-need-reading-before-acting-2026-08-12.md). The escape hatch is the same one the backlog used -
# add the id to `excludeVulnerabilityIds` in the root pom with a reason and a retirement condition -
# which is a deliberate, reviewable act rather than a flag flip.
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
# `-Dossindex.fail=false` is NOT laxity, and must stay - it is what makes this script the single
# place that decides red. The pom defaults `ossindex.fail` to `true`, and letting Maven fail on
# findings kills the pipeline (`set -o pipefail`) BEFORE the guard runs: on exactly the runs that
# have something to say, you would lose both the did-it-actually-scan verdict and the rendered
# findings table. Maven stays lenient; this script gates.
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
# Advisory ids the root pom's `excludeVulnerabilityIds` suppressed, as the plugin itself resolved
# them. Collected across every module and reported as a count, so a growing exclusion list stays
# visible in the summary rather than quietly absorbing the whole point of the check.
excluded_ids = set()


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

    # `excludeVulnerabilityIds` in the root pom is the documented escape hatch for a finding that
    # does not apply, is disputed, or has no fixed version. The plugin honours it when deciding
    # which COMPONENTS land in `vulnerable`, but the per-component `vulnerabilities` array it
    # exports is unfiltered - an excluded advisory is still listed there, and is also repeated in
    # this top-level `excludedVulnerabilities` block. Verified on a live authenticated run: with
    # CVE-2026-54518 excluded in the pom, the plugin logged it under "Excluded vulnerabilities:"
    # and still wrote it into `vulnerable`. Gating on the raw array would therefore go red on
    # exactly the debt that was deliberately triaged, and no pom edit could ever clear it.
    module_excluded = {
        v.get("id") for v in (doc.get("excludedVulnerabilities") or []) if v.get("id")}
    excluded_ids |= module_excluded

    for coords, report in (doc.get("vulnerable") or {}).items():
        listed = report.get("vulnerabilities") or []
        live = [v for v in listed if v.get("id") not in module_excluded]
        # Every advisory on this component was excluded, with a reason, in the pom. The plugin
        # would not normally list such a component at all; belt and braces so an upstream change
        # to that behaviour cannot resurrect suppressed findings as a red gate.
        if listed and not live:
            continue
        entry = found.setdefault(coords, {"advisories": {}, "modules": set()})
        entry["modules"].add(module)
        for i, vuln in enumerate(live):
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
print("excluded=%d" % len(excluded_ids))
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
empty_reports='' report_files='' components='' vulnerable='' advisories='' excluded='' top_score=''
while IFS='=' read -r key value; do
    case "$key" in
        ---) break ;;
        empty_reports|report_files|components|vulnerable|advisories|excluded|top_score)
            printf -v "$key" '%s' "$value" ;;
    esac
done <<< "$facts"

for fact in empty_reports report_files components vulnerable advisories excluded top_score; do
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

# Past here the scan is PROVEN to have run, so anything red below is a statement about the tree, not
# about the lane. Kept visibly separate - different heading, different exit code, different stderr
# line - so the two reds can never be mistaken for each other.
if [ "$vulnerable" -eq 0 ]; then
    echo "## :white_check_mark: OSS Index audit: no vulnerable components"
else
    echo "## :x: OSS Index audit found ${vulnerable} vulnerable component(s), ${advisories} advisory(ies)"
fi
echo
echo "Scanned **${components}** resolved components across **${report_files}** module(s)."
# Never silent: an exclusion list is the one way a finding can legitimately disappear, so it is
# stated on every run - green ones included - or it stops being reviewable. The retirement
# conditions on that list are no longer only prose: bin/check-cve-exclusions.sh dates the temporary
# entries and fails once one outlives its window. That check lives in repo-hygiene.yml rather than
# here on purpose - this job is skipped for fork PRs and dies early on a broken lane, which is
# exactly when an unwatched list would rot. See its header.
if [ "$excluded" -gt 0 ]; then
    echo
    echo "**${excluded}** advisory(ies) were suppressed by \`excludeVulnerabilityIds\` in the root pom."
    echo "Each carries a reason and a retirement condition there; they are excluded from the verdict"
    echo "above, not overlooked. Temporary entries are dated and expire -"
    echo "\`bin/check-cve-exclusions.sh\` (Repo Hygiene) fails once one outlives its window, so those"
    echo "conditions are enforced rather than merely written down."
fi
if [ "$vulnerable" -eq 0 ]; then
    echo
    echo "_The scan ran and the tree is clean. This check goes red two different ways: **the scan"
    echo "could not be proven to have run** (broken lane, exit 1), or **the scan found something**"
    echo "(exit 2) - see the header of \`bin/check-ossindex-audit.sh\`._"
    echo "ok:   OSS Index audit ran - ${report_files} module report(s), ${components} components, 0 vulnerable, ${excluded} excluded" >&2
    exit 0
fi

echo
echo "**The scan ran cleanly - this is a real finding, not a broken check.** Known debt is already"
echo "carried as \`excludeVulnerabilityIds\` entries in the root pom and filtered out above, so"
echo "anything reaching this table is an advisory nobody has looked at."
echo
echo "Highest CVSS score seen: **${top_score}**."
echo
printf '%s\n' "$findings_table"
echo
echo "Either fix it (upgrade, or drop the dependency), or - if it does not apply, is disputed, or has"
echo "no fixed version - add the id to \`excludeVulnerabilityIds\` in the root pom **with a reason and"
echo "a retirement condition**, the way astubbs/parallel-consumer#281 did for the original backlog."

echo "FAIL: OSS Index audit found ${vulnerable} vulnerable component(s), ${advisories} advisory(ies), top CVSS ${top_score} - the scan itself ran fine." >&2
exit 2
