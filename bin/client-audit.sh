#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Runs one foreign client's OWN ecosystem vulnerability auditor, and decides red.
#
#   bin/client-audit.sh go|rust|typescript|ruby|python|dotnet|swift|cpp|kotlin|scala
#
# WHY THIS EXISTS
#
# The Maven side of this repo has had a dependency CVE scan for a while - `ossindex-maven-plugin`,
# driven by .github/workflows/dependency-audit.yml and guarded by bin/check-ossindex-audit.sh.
# Nothing scanned Go, Rust, npm, Ruby, Python, .NET or Swift dependencies at all (astubbs#242), and
# a stale gRPC pin with a published CVE shipped on the proxy branch as a result. Dependabot cannot
# close that particular gap even once .github/dependabot.yml declares those ecosystems, because
# Dependabot reads the DEFAULT BRANCH: a pin chosen at the start of a long-lived branch goes stale
# during that branch's life, where only a check running ON the branch can see it. This is that
# check, one row per language in .github/workflows/clients.yml.
#
# NOT ONE SCANNER FOR EVERYTHING. Each ecosystem's own auditor reads that ecosystem's lockfile and
# its own advisory database - `govulncheck` even resolves whether the vulnerable SYMBOL is reachable,
# which no cross-ecosystem scanner does. The repo's rule against adding a dependency to make
# something work applies here too: npm and .NET ship theirs, and where a tool has to be added it is
# pinned in the MODULE'S OWN manifest (go.mod's `tool` directives, the Gemfile, pyproject's dev
# extra) so a developer and this script run the same version, exactly as the static-analysis lane
# already does with staticcheck, rubocop and ruff.
#
# THE EXIT CODE OF AN AUDITOR IS NOT A VERDICT - MEASURED, NOT ASSUMED
#
# This is the whole reason the script exists rather than a one-line `run:` per row. Each of the six
# was run against a deliberately vulnerable fixture and against a clean one:
#
#   govulncheck -format json    11 findings, EXIT 0    <- json mode never signals; only text does
#   dotnet list --vulnerable    2 High findings, EXIT 0 <- prints a table, exits success, always
#   npm audit --json            findings exit 1 ... and a MISSING LOCKFILE also exits 1
#   bundler-audit               findings exit 1 ... and a MISSING Gemfile.lock also exits 1
#   cargo audit --json          findings exit 1
#   pip-audit --format json     findings exit 1
#
# Two of them report success while holding findings, and two more use one exit code for "found
# something" and "could not look". Wiring any of these into a workflow bare would have produced
# instance ten of the class in
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md. So every
# language is classified STRUCTURALLY, from the report rather than from the exit status, and each
# language's rule below names the evidence that proves the scan happened - not merely that the
# command returned.
#
# EXIT CODES - the split bin/check-ossindex-audit.sh established, and for the same reason:
#
#   0  the audit ran, and found nothing
#   1  the audit could NOT be proven to have run - tool absent, lockfile missing, report
#      unparseable. The LANE is broken and nothing was learned about the dependencies.
#   2  the audit ran and FOUND something. The lane worked; there is something to triage.
#
# When both could apply, 1 wins: findings from a scan that cannot be proven to have happened are
# evidence of nothing, in either direction.
#
# WHAT TO DO WITH AN EXIT 2. Bump the dependency - these are small, curated dependency sets and a
# finding is by construction something nobody has looked at yet. Where a finding is genuinely not
# applicable, suppress it in THAT ECOSYSTEM'S OWN ignore mechanism, with a comment, rather than
# here: `.cargo/audit.toml` (cargo), `.bundler-audit.yml` (bundler), `--ignore-vuln` (pip-audit).
# A second suppression registry in this repo would drift from the tool's, which is the defect the
# root pom's exclusion list already has a guard for.
#
# LANGUAGES WITH NO AUDITOR are listed in NO_AUDITOR and exit 0 printing the reason on every run,
# including green ones - the arrangement clients.yml already uses for the Swift row's static
# analysis. An UNKNOWN language exits 1: a new client row must make an audit decision, and silence
# is not one of them.

set -euo pipefail

repo_root="${PC_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

if [ "$#" -ne 1 ]; then
    echo "usage: bin/client-audit.sh <language>" >&2
    exit 1
fi

python3 - "$repo_root" "$1" <<'PYTHON'
import json
import os
import subprocess
import sys

root, language = sys.argv[1], sys.argv[2]
CLIENTS = "parallel-consumer-proxy-clients"


def module(name):
    return os.path.join(root, CLIENTS, f"parallel-consumer-proxy-client-{name}")


# ── Languages with no auditor - stated, never silently skipped ────────────────────────────
NO_AUDITOR = {
    "swift": "Swift Package Manager has no audit command, and Swift has no mature standalone "
             "dependency auditor - the same finding this repo already recorded for Swift static "
             "analysis. Its only coverage is the `swift` ecosystem in .github/dependabot.yml, "
             "which starts working when these modules reach master.",
    "cpp": "no package manager at all: CMakeLists.txt resolves through find_package/PkgConfig "
           "against system packages from the runner image, so there is no manifest, no lockfile "
           "and nothing to audit. Nothing covers it, by construction.",
    "kotlin": "a Maven module - covered by the whole-tree OSS Index scan "
              "(.github/workflows/dependency-audit.yml), which resolves the full reactor.",
    "scala": "a Maven module - covered by the whole-tree OSS Index scan "
             "(.github/workflows/dependency-audit.yml), which resolves the full reactor.",
}


def broken(message):
    print(f"::error::clients: {language} dependency audit could not run - {message}", file=sys.stderr)
    print(f"### :x: {language}: the dependency audit did not run")
    print()
    print("**This is not a clean result.** The scan did not happen, so finding nothing means "
          "nothing.")
    print()
    print(f"- {message}")
    sys.exit(1)


def run(command, cwd):
    """Run an auditor. A missing binary is a broken lane, never a pass."""
    try:
        completed = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        broken(f"`{command[0]}` is not on PATH. It is pinned in the module's own manifest, so "
               f"this usually means the module's dependencies are not installed yet - run its "
               f"ordinary build first (`./mvnw package -pl :parallel-consumer-proxy-client-"
               f"{language} -am -Dpc.foreignClients`).")
    return completed


def report(findings, scanned, detail):
    """One rendering for every language: what was scanned, and what was found."""
    if findings:
        print(f"### :x: {language}: {len(findings)} dependency vulnerability finding(s)")
        print()
        print(f"- scanned: {scanned}")
        for line in findings:
            print(f"- {line}")
        print(f"::error::clients: {language} dependency audit found {len(findings)} "
              f"vulnerability finding(s)", file=sys.stderr)
        sys.exit(2)
    print(f"### :white_check_mark: {language}: no known vulnerable dependencies")
    print()
    print(f"- scanned: {scanned}")
    if detail:
        print(f"- {detail}")
    sys.exit(0)


def require_file(path, what):
    if not os.path.exists(path):
        broken(f"{what} is not at `{os.path.relpath(path, root)}` - the audit has nothing to read, "
               f"and an audit of nothing must never report clean")


def load_json(text, what):
    try:
        return json.loads(text)
    except ValueError:
        broken(f"{what} did not produce parseable JSON. The tool failed before it scanned, or its "
               f"output format changed; its first line was: "
               f"{(text.strip().splitlines() or ['<empty>'])[0][:200]}")


# ── Go: govulncheck, pinned as a `tool` directive in go.mod ───────────────────────────────
# JSON mode EXITS 0 WITH FINDINGS - only the text format sets exit 3 - so the exit code is
# deliberately ignored here. Evidence the scan happened: the `config` message, which names the
# scanner version and the advisory database it reached. No config message means it never started.
def audit_go():
    directory = module("go")
    require_file(os.path.join(directory, "go.mod"), "go.mod")
    completed = run(["go", "tool", "govulncheck", "-format", "json", "./..."], directory)
    decoder, text, index, messages = json.JSONDecoder(), completed.stdout, 0, []
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        try:
            message, index = decoder.raw_decode(text, index)
        except ValueError:
            broken("govulncheck did not produce a parseable JSON message stream; its stderr began: "
                   f"{(completed.stderr.strip().splitlines() or ['<empty>'])[0][:200]}")
        messages.append(message)
    config = next((m["config"] for m in messages if "config" in m), None)
    if config is None:
        broken("govulncheck emitted no `config` message, so it never reached the vulnerability "
               "database - this is what a failed start looks like, and its findings list is empty "
               "for that reason rather than because the module is clean")
    osv = {m["osv"]["id"]: m["osv"] for m in messages if "osv" in m}
    findings = []
    for message in messages:
        if "finding" not in message:
            continue
        finding = message["finding"]
        trace = (finding.get("trace") or [{}])[0]
        # A symbol-level trace means the vulnerable function is reachable from this module's code;
        # package or module level means the vulnerable version is present but not called. Both are
        # reported and both fail - we ship the version either way - but the summary says which.
        depth = "CALLED" if trace.get("function") else (
            "imported" if trace.get("package") else "present")
        summary = (osv.get(finding["osv"], {}).get("summary") or "").strip()
        findings.append(f"`{finding['osv']}` ({depth}) in {trace.get('module', '?')} "
                        f"{trace.get('version', '')} - fixed in {finding.get('fixed_version', '?')}"
                        f"{': ' + summary if summary else ''}")
    report(sorted(set(findings)),
           f"govulncheck {config.get('scanner_version', '?')} against {config.get('db', '?')} "
           f"(database last modified {config.get('db_last_modified', '?')})",
           f"{len(osv)} advisor(ies) considered, none affecting this module's resolved versions")


# ── Rust: cargo-audit against Cargo.lock ──────────────────────────────────────────────────
# Structural evidence: the advisory database must have advisories in it, and the lockfile must have
# resolved dependencies. Either at zero is a scan of nothing wearing a clean result's clothes.
def audit_rust():
    directory = module("rust")
    require_file(os.path.join(directory, "Cargo.lock"), "Cargo.lock")
    completed = run(["cargo", "audit", "--json"], directory)
    payload = load_json(completed.stdout, "cargo audit")
    advisories = (payload.get("database") or {}).get("advisory-count", 0)
    dependencies = (payload.get("lockfile") or {}).get("dependency-count", 0)
    if not advisories or not dependencies:
        broken(f"cargo audit reported {advisories} advisories against {dependencies} locked "
               "dependencies - one of those is zero, so nothing was actually compared")
    findings = []
    for entry in (payload.get("vulnerabilities") or {}).get("list", []):
        advisory, package = entry.get("advisory", {}), entry.get("package", {})
        findings.append(f"`{advisory.get('id')}` in {package.get('name')} "
                        f"{package.get('version')} - {advisory.get('title')}")
    report(findings, f"cargo audit: {dependencies} locked dependencies against {advisories} "
                     f"RustSec advisories", None)


# ── npm: the auditor that ships with npm ──────────────────────────────────────────────────
# `npm audit` exits 1 for findings AND exits 1 when there is no lockfile to audit, so the exit code
# cannot separate them. The report can: a failed run carries an `error` object and no metadata.
def audit_typescript():
    directory = module("typescript")
    require_file(os.path.join(directory, "package-lock.json"), "package-lock.json")
    completed = run(["npm", "audit", "--json"], directory)
    payload = load_json(completed.stdout, "npm audit")
    if "error" in payload:
        error = payload["error"]
        broken(f"npm audit failed before scanning ({error.get('code')}): {error.get('summary')}")
    metadata = payload.get("metadata") or {}
    counts = metadata.get("vulnerabilities") or {}
    if "total" not in counts:
        broken("npm audit returned no `metadata.vulnerabilities` block - the report shape changed, "
               "and an unrecognised report must not be read as a clean one")
    findings = []
    for name, entry in sorted((payload.get("vulnerabilities") or {}).items()):
        advisories = [via.get("url") or via.get("title") for via in entry.get("via", [])
                      if isinstance(via, dict)]
        findings.append(f"`{name}` {entry.get('severity')} via "
                        f"{', '.join(str(a) for a in advisories) or 'a transitive dependency'}")
    report(findings,
           f"npm audit: {(metadata.get('dependencies') or {}).get('total', '?')} resolved "
           f"dependencies",
           f"severity counts: {counts}")


# ── Ruby: bundler-audit, pinned in the module's Gemfile ───────────────────────────────────
# Same trap as npm - a missing Gemfile.lock also exits 1 - and here the JSON separates them the
# hard way: on that failure bundler-audit prints a message and NO JSON at all, so an unparseable
# report is the broken-lane signal rather than a formatting complaint.
def audit_ruby():
    directory = module("ruby")
    require_file(os.path.join(directory, "Gemfile.lock"), "Gemfile.lock")
    completed = run(["bundle", "exec", "bundler-audit", "check", "--update", "--format", "json"],
                    directory)
    # `--update` prints a progress banner on STDOUT ahead of the report, so the stream is not JSON
    # from byte zero. --update is not optional: the advisory database is a git clone under the
    # user's home, which a CI runner does not have, and without it bundler-audit aborts. The report
    # is the last thing printed, so the first `{` is where it starts.
    body = completed.stdout[completed.stdout.find("{"):] if "{" in completed.stdout else ""
    payload = load_json(body, "bundler-audit")
    if "results" not in payload:
        broken("bundler-audit returned JSON with no `results` key - the report shape changed")
    findings = []
    for result in payload["results"]:
        gem, advisory = result.get("gem", {}), result.get("advisory", {})
        findings.append(f"`{advisory.get('id')}` ({advisory.get('criticality', 'unknown')}) in "
                        f"{gem.get('name')} {gem.get('version')} - {advisory.get('title')}")
    report(findings, f"bundler-audit {payload.get('version', '?')} against the ruby-advisory-db, "
                     f"refreshed at {payload.get('created_at', '?')}", None)


# ── Python: pip-audit, pinned in pyproject's dev extra, run from the module's venv ────────
# Audits the venv the module's own `make deps` built, which is the environment its tests run in -
# not a re-resolution of the manifest, which could differ from what is installed. The venv's own
# presence is the "did it run" precondition; an empty dependency list is the silent-clean shape.
def audit_python():
    directory = module("python")
    executable = os.path.join(directory, ".venv", "bin", "pip-audit")
    require_file(os.path.join(directory, "pyproject.toml"), "pyproject.toml")
    require_file(executable, "the module's venv pip-audit (run `make build` in the module first)")
    completed = run([executable, "--format", "json", "--progress-spinner", "off"], directory)
    payload = load_json(completed.stdout, "pip-audit")
    dependencies = payload.get("dependencies")
    if not dependencies:
        broken("pip-audit reported zero dependencies - it audited an empty environment, which is a "
               "clean result about nothing")
    findings = []
    for dependency in dependencies:
        for vulnerability in dependency.get("vulns", []):
            fixes = ", ".join(vulnerability.get("fix_versions") or []) or "no fix published"
            findings.append(f"`{vulnerability.get('id')}` in {dependency.get('name')} "
                            f"{dependency.get('version')} - fixed in {fixes}")
    report(findings, f"pip-audit: {len(dependencies)} installed distributions in the module venv",
           None)


# ── .NET: the auditor that ships with the SDK ─────────────────────────────────────────────
# `dotnet list package --vulnerable` PRINTS ITS FINDINGS AND EXITS 0 - measured, two High-severity
# advisories, exit 0. The exit code is therefore ignored entirely and the text is parsed. The
# coverage leg is what makes a silent no-op impossible: every .csproj on disk must appear in the
# output, so a run that restored nothing fails instead of reporting a clean tree.
def audit_dotnet():
    directory = module("dotnet")
    projects = sorted(os.path.splitext(name)[0]
                      for _, _, files in os.walk(directory) for name in files
                      if name.endswith(".csproj"))
    if not projects:
        broken("no .csproj files found in the .NET module - nothing to audit, which is not the "
               "same as nothing to find")
    completed = run(["dotnet", "list", "package", "--vulnerable", "--include-transitive"], directory)
    output = completed.stdout + completed.stderr
    reported = {name for name in projects
                if f"`{name}` has the following vulnerable packages" in output
                or f"`{name}` has no vulnerable packages" in output}
    missing = [name for name in projects if name not in reported]
    if missing:
        broken(f"`dotnet list package --vulnerable` said nothing about {', '.join(missing)} - it "
               f"exits 0 whatever happens, so a project it never restored would otherwise read as "
               f"clean. Its output began: "
               f"{(output.strip().splitlines() or ['<empty>'])[0][:200]}")
    findings = [line.strip().lstrip("> ").strip() for line in output.splitlines()
                if line.strip().startswith(">")]
    report([f"`{finding}`" for finding in findings],
           f"dotnet list package --vulnerable --include-transitive, over {len(projects)} project(s): "
           f"{', '.join(projects)}", None)


AUDITORS = {
    "go": audit_go,
    "rust": audit_rust,
    "typescript": audit_typescript,
    "ruby": audit_ruby,
    "python": audit_python,
    "dotnet": audit_dotnet,
}

if language in NO_AUDITOR:
    print(f"### :grey_question: {language}: no dependency auditor exists")
    print()
    print(f"- {NO_AUDITOR[language]}")
    sys.exit(0)
if language not in AUDITORS:
    print(f"::error::no audit decision is recorded for the client language '{language}' - add it to "
          f"AUDITORS or to NO_AUDITOR (with the reason) in bin/client-audit.sh", file=sys.stderr)
    print(f"### :x: no audit decision recorded for `{language}`")
    print()
    print("A new client row must choose an auditor or record why it has none. Silence is not a "
          "third option: it would leave the row reporting green having scanned nothing.")
    sys.exit(1)

AUDITORS[language]()
PYTHON
