#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/client-audit.sh - the per-language dependency vulnerability audit.
#
# WHY EVERY CASE HERE IS A NEGATIVE CONTROL
#
# The guard exists because an auditor's exit code is not a verdict, and two of the six prove it:
# `govulncheck -format json` and `dotnet list package --vulnerable` both PRINT FINDINGS AND EXIT 0,
# measured against deliberately vulnerable fixtures. Two more - `npm audit` and `bundler-audit` -
# use exit 1 for "found something" AND for "could not find a lockfile". A guard for that class which
# has only ever been watched passing is worth nothing: the broken version passes too.
#
# So each language is driven through three states, with the exit code asserted exactly:
#
#   0  the audit ran and found nothing
#   1  the audit could not be proven to have run  (tool absent, report unparseable, empty scan)
#   2  the audit ran and found something
#
# THE REPORTS BELOW ARE REAL OUTPUT, not invented shapes - captured on 2026-08-17 by running each
# tool against a deliberately vulnerable fixture and against a clean one (lodash 4.17.11, time
# 0.1.44, jinja2 2.11.2, actionpack 5.2.0, golang.org/x/text v0.3.7, Newtonsoft.Json 12.0.2). They
# are trimmed to the fields the guard reads. A report shape invented from documentation would test
# the guard against the author's belief rather than against the tool.
#
# THE TOOLS ARE REPLACED BY SHIMS ON PATH rather than run for real, which is what makes this suite
# hermetic: no Go, Rust, Node, Ruby, Python venv or .NET SDK is needed, and no network. The whole
# script - dispatch, module resolution, manifest checks, parsing, exit code - is exercised; only the
# auditor binary is stubbed. That is also how the "tool is not installed" case is produced, by
# giving PATH no shim at all: the tenth instance of the silent-green class this repo has already
# named would be exactly this case reporting a clean tree.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
guard="${script_dir}/client-audit.sh"

failures=0
workspace="$(mktemp -d)"
trap 'rm -rf "${workspace}"' EXIT

CLIENTS="parallel-consumer-proxy-clients"

# fixture <name> <language> <manifest-relative-path>... -> prints the fixture root
fixture() {
    local name="$1" language="$2"
    shift 2
    local root="${workspace}/${name}"
    local module="${root}/${CLIENTS}/parallel-consumer-proxy-client-${language}"
    mkdir -p "${module}/shims"
    local path
    for path in "$@"; do
        mkdir -p "${module}/$(dirname "${path}")"
        : >"${module}/${path}"
    done
    printf '%s' "${root}"
}

# shim <fixture-root> <language> <binary> <exit-code> <stdout>
shim() {
    local root="$1" language="$2" binary="$3" code="$4" body="$5"
    local dir="${root}/${CLIENTS}/parallel-consumer-proxy-client-${language}/shims"
    mkdir -p "${dir}"
    {
        printf '#!/usr/bin/env bash\n'
        printf 'cat <<%s\n' "'PC_SHIM_EOF'"
        printf '%s\n' "${body}"
        printf 'PC_SHIM_EOF\n'
        printf 'exit %s\n' "${code}"
    } >"${dir}/${binary}"
    chmod +x "${dir}/${binary}"
}

# expect <description> <expected-exit> <fixture-root> <language>
expect() {
    local description="$1" expected="$2" root="$3" language="$4"
    local shims="${root}/${CLIENTS}/parallel-consumer-proxy-client-${language}/shims"
    local actual=0
    PC_REPO_ROOT="${root}" PATH="${shims}:${PATH}" \
        bash "${guard}" "${language}" >"${workspace}/out.txt" 2>&1 || actual=$?
    if [ "${actual}" -eq "${expected}" ]; then
        echo "  ok   ${description} (exit ${actual})"
    else
        echo "  FAIL ${description}: expected exit ${expected}, got ${actual}"
        sed 's/^/       | /' "${workspace}/out.txt"
        failures=$((failures + 1))
    fi
}

echo "test-client-audit.sh"

# ── Go ───────────────────────────────────────────────────────────────────────────────────
# govulncheck's JSON message stream. THE FINDINGS CASE EXITS 0 AT THE TOOL - the shim says so - and
# the guard must still call it a finding; that is the whole reason this language is not a one-liner.
GO_CONFIG='{"config":{"protocol_version":"v1.0.0","scanner_name":"govulncheck","scanner_version":"v1.7.0","db":"https://vuln.go.dev","db_last_modified":"2026-08-14T16:22:54Z"}}'
GO_FINDING='{"osv":{"id":"GO-2022-1059","summary":"Denial of service in golang.org/x/text/language"}}
{"finding":{"osv":"GO-2022-1059","fixed_version":"v0.3.8","trace":[{"module":"golang.org/x/text","version":"v0.3.7","package":"golang.org/x/text/language","function":"ParseAcceptLanguage"}]}}'

root="$(fixture go-findings go go.mod)"
shim "${root}" go go 0 "${GO_CONFIG}
${GO_FINDING}"
expect "go: govulncheck reports a finding and exits 0" 2 "${root}" go

root="$(fixture go-clean go go.mod)"
shim "${root}" go go 0 "${GO_CONFIG}"
expect "go: govulncheck scanned and found nothing" 0 "${root}" go

# No `config` message: what a govulncheck that never reached the database leaves behind. An empty
# findings list here means "did not look", and must not read as "nothing to find".
root="$(fixture go-noconfig go go.mod)"
shim "${root}" go go 0 '{"progress":{"message":"Scanning your code..."}}'
expect "go: no config message - the scan never started" 1 "${root}" go

root="$(fixture go-notool go go.mod)"
expect "go: govulncheck's driver is not on PATH" 1 "${root}" go

root="$(fixture go-nomanifest go README.md)"
shim "${root}" go go 0 "${GO_CONFIG}"
expect "go: no go.mod in the module" 1 "${root}" go

# ── Rust ─────────────────────────────────────────────────────────────────────────────────
root="$(fixture rust-findings rust Cargo.lock)"
shim "${root}" rust cargo 1 '{"database":{"advisory-count":1216},"lockfile":{"dependency-count":7},"vulnerabilities":{"found":true,"count":1,"list":[{"advisory":{"id":"RUSTSEC-2020-0071","title":"Potential segfault in the time crate"},"package":{"name":"time","version":"0.1.44"}}]}}'
expect "rust: cargo audit reports a RUSTSEC advisory" 2 "${root}" rust

root="$(fixture rust-clean rust Cargo.lock)"
shim "${root}" rust cargo 0 '{"database":{"advisory-count":1216},"lockfile":{"dependency-count":106},"vulnerabilities":{"found":false,"count":0,"list":[]}}'
expect "rust: cargo audit scanned 106 dependencies, clean" 0 "${root}" rust

# An empty advisory database compares nothing against everything and reports clean while doing it.
root="$(fixture rust-emptydb rust Cargo.lock)"
shim "${root}" rust cargo 0 '{"database":{"advisory-count":0},"lockfile":{"dependency-count":106},"vulnerabilities":{"found":false,"count":0,"list":[]}}'
expect "rust: an empty advisory database is not a clean scan" 1 "${root}" rust

# ── npm ──────────────────────────────────────────────────────────────────────────────────
root="$(fixture npm-findings typescript package-lock.json)"
shim "${root}" typescript npm 1 '{"auditReportVersion":2,"vulnerabilities":{"lodash":{"name":"lodash","severity":"critical","via":[{"title":"Command Injection in lodash","url":"https://github.com/advisories/GHSA-35jh-r3h4-6jhm"}]}},"metadata":{"vulnerabilities":{"info":0,"low":0,"moderate":0,"high":0,"critical":1,"total":1},"dependencies":{"total":2}}}'
expect "npm: audit reports a critical advisory" 2 "${root}" typescript

root="$(fixture npm-clean typescript package-lock.json)"
shim "${root}" typescript npm 0 '{"auditReportVersion":2,"vulnerabilities":{},"metadata":{"vulnerabilities":{"info":0,"low":0,"moderate":0,"high":0,"critical":0,"total":0},"dependencies":{"total":128}}}'
expect "npm: audit scanned 128 dependencies, clean" 0 "${root}" typescript

# THE TRAP THIS CASE EXISTS FOR: npm audit exits 1 with no lockfile to audit, exactly as it does
# when it finds a critical vulnerability. Only the report tells them apart.
root="$(fixture npm-enolock typescript package-lock.json)"
shim "${root}" typescript npm 1 '{"error":{"code":"ENOLOCK","summary":"This command requires an existing lockfile.","detail":"Try creating one first with: npm i --package-lock-only"}}'
expect "npm: ENOLOCK exits 1 like a finding, and is not one" 1 "${root}" typescript

# ── Ruby ─────────────────────────────────────────────────────────────────────────────────
# `--update` prints a banner on stdout before the JSON, so the report does not start at byte zero.
root="$(fixture ruby-findings ruby Gemfile.lock)"
shim "${root}" ruby bundle 1 'Updating ruby-advisory-db ...
{"version":"0.9.3","created_at":"2026-08-17 09:17:38 +0000","results":[{"type":"unpatched_gem","gem":{"name":"actionpack","version":"5.2.0"},"advisory":{"id":"CVE-2020-8164","criticality":"high","title":"Possible Strong Parameters Bypass in ActionPack"}}]}'
expect "ruby: bundler-audit reports an unpatched gem, after a banner" 2 "${root}" ruby

root="$(fixture ruby-clean ruby Gemfile.lock)"
shim "${root}" ruby bundle 0 'Updating ruby-advisory-db ...
{"version":"0.9.3","created_at":"2026-08-17 09:23:24 +0000","results":[]}'
expect "ruby: bundler-audit scanned, clean" 0 "${root}" ruby

# The same exit-1-two-ways trap as npm, and here the failure prints no JSON at all.
root="$(fixture ruby-nolock ruby Gemfile.lock)"
shim "${root}" ruby bundle 1 'Could not find "Gemfile.lock" in "/tmp/x"'
expect "ruby: a missing lockfile exits 1 like a finding, and is not one" 1 "${root}" ruby

# ── Python ───────────────────────────────────────────────────────────────────────────────
# pip-audit is invoked by absolute path out of the module's own venv, so the "shim" is that file.
python_venv() {
    local root="$1" code="$2" body="$3"
    local target="${root}/${CLIENTS}/parallel-consumer-proxy-client-python/.venv/bin/pip-audit"
    mkdir -p "$(dirname "${target}")"
    {
        printf '#!/usr/bin/env bash\n'
        printf 'cat <<%s\n' "'PC_SHIM_EOF'"
        printf '%s\n' "${body}"
        printf 'PC_SHIM_EOF\n'
        printf 'exit %s\n' "${code}"
    } >"${target}"
    chmod +x "${target}"
}

root="$(fixture python-findings python pyproject.toml)"
python_venv "${root}" 1 '{"dependencies":[{"name":"jinja2","version":"2.11.2","vulns":[{"id":"PYSEC-2021-66","fix_versions":["2.11.3"]}]},{"name":"markupsafe","version":"3.0.3","vulns":[]}],"fixes":[]}'
expect "python: pip-audit reports a PYSEC advisory" 2 "${root}" python

root="$(fixture python-clean python pyproject.toml)"
python_venv "${root}" 0 '{"dependencies":[{"name":"grpcio","version":"1.73.0","vulns":[]},{"name":"protobuf","version":"5.29.5","vulns":[]}],"fixes":[]}'
expect "python: pip-audit scanned the venv, clean" 0 "${root}" python

# An audit of an empty environment is a clean result about nothing.
root="$(fixture python-empty python pyproject.toml)"
python_venv "${root}" 0 '{"dependencies":[],"fixes":[]}'
expect "python: an empty environment is not a clean scan" 1 "${root}" python

root="$(fixture python-novenv python pyproject.toml)"
expect "python: the module venv has not been built" 1 "${root}" python

# ── .NET ─────────────────────────────────────────────────────────────────────────────────
# THE SECOND SILENT-GREEN PROOF: this is the real output of a run that found two High-severity
# advisories, and the tool exited 0 while printing it.
root="$(fixture dotnet-findings dotnet Client.sln src/Client/Client.csproj)"
shim "${root}" dotnet dotnet 0 'Project `Client` has the following vulnerable packages
   [net8.0]:
   Top-level Package      Requested   Resolved   Severity   Advisory URL
   > Newtonsoft.Json      12.0.2      12.0.2     High       https://github.com/advisories/GHSA-5crp-9r3c-p9vr'
expect "dotnet: vulnerable packages listed, tool exits 0" 2 "${root}" dotnet

root="$(fixture dotnet-clean dotnet Client.sln src/Client/Client.csproj)"
shim "${root}" dotnet dotnet 0 'The given project `Client` has no vulnerable packages given the current sources.'
expect "dotnet: no vulnerable packages" 0 "${root}" dotnet

# A project the command never restored simply does not appear in the output - and the command still
# exits 0. The coverage leg is the only thing standing between that and a clean-looking run.
root="$(fixture dotnet-silent dotnet Client.sln src/Client/Client.csproj tests/Tests/Tests.csproj)"
shim "${root}" dotnet dotnet 0 'The given project `Client` has no vulnerable packages given the current sources.'
expect "dotnet: silence about one of two projects" 1 "${root}" dotnet

root="$(fixture dotnet-noprojects dotnet Client.sln)"
shim "${root}" dotnet dotnet 0 'nothing to say'
expect "dotnet: no .csproj files at all" 1 "${root}" dotnet

# ── Languages with no auditor, and languages nobody decided about ────────────────────────
root="$(fixture swift swift Package.swift)"
expect "swift: no auditor exists, and the reason is printed" 0 "${root}" swift

root="$(fixture cpp cpp CMakeLists.txt)"
expect "cpp: no package manager to audit" 0 "${root}" cpp

root="$(fixture kotlin kotlin pom.xml)"
expect "kotlin: covered by the Maven whole-tree scan" 0 "${root}" kotlin

# A new client row that made no audit decision must fail rather than skip: skipping is how an
# unaudited language reports green.
root="$(fixture unknown perl Makefile.PL)"
expect "a language with no recorded audit decision" 1 "${root}" perl

echo
if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed"
    exit 1
fi
echo "all cases passed"
