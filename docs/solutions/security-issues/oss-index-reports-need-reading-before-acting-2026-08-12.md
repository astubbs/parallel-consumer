# OSS Index finds real advisories and invents some - read a run before acting on it

**The rule: never bump, pin or panic straight from an OSS Index finding.** Check the id against OSV,
GHSA and MITRE first, and check the affected range against the version actually resolved. This
scanner's output is worth having and is not self-certifying, and the exclusion list in the root pom
exists because some of what it reports cannot be acted on at all.

This is the scanner behind `deps: whole-tree CVE scan`
([`.github/workflows/dependency-audit.yml`](../../../.github/workflows/dependency-audit.yml)), whose
findings gate PRs. That gate is only as good as the reading behind each exclusion, which is why this
record exists.

## The evidence: the first authenticated run, checked line by line

Before this, `ossindex-maven-plugin` had been returning HTTP 401 on every build while configured
`fail=false` - reporting success without ever scanning. That defect, and the guard built for it, are
in
[`../workflow-issues/a-check-that-reports-success-without-having-run.md`](../workflow-issues/a-check-that-reports-success-without-having-run.md);
what follows is what the scan said once it could actually run.

It failed the build on **5 components, 12 CVEs**, top severity 9.3:

| Component | Scope | CVEs (severity) | Dependabot |
|---|---|---|---|
| `com.fasterxml.jackson.core:jackson-databind:2.16.2` | runtime | 54512 (9.2), 54513 (9.3), 54514 (6.9), 54515 (6.9), 54518 (6.9), 59888 (6.3), 59889 (6.3) | 5 of 7 |
| `io.micrometer:micrometer-core:1.13.15` | compile | 40984 (8.7) | yes |
| `io.netty:netty-codec-http:4.1.136.Final` | compile | 59903 (6.5) | no |
| `at.yawk.lz4:lz4-java:1.10.1` | runtime | 59949 (6.5) | no |
| `org.hdrhistogram:HdrHistogram:2.2.2` | runtime | 14683 (2.0), 14686 (2.0) | no |

Read as a scoreboard against Dependabot, that first looked like *three components Dependabot misses
entirely*, and therefore like evidence of a systematic transitive-closure failure in GitHub's
dependency graph. Checked against the advisory databases, it was not:

- **`at.yawk.lz4:lz4-java` is the one real Dependabot gap.** GHSA-xx22-p4ch-683r exists and no alert
  fired. A single genuine miss still carries the point that the two tools are not redundant - but it
  is one, not three, and one miss is not a systematic failure.
- **The jackson "5 of 7" was OSS Index being wrong, not Dependabot.** `CVE-2026-54518` was introduced
  in 2.21.0 (GHSA-rcqc-6cw3-h962) and `CVE-2026-59889` in 2.18.0 (GHSA-5gvw-p9qm-jgwh); both live in
  `UnwrappedPropertyHandler` code that does not exist in 2.16. Neither applies to 2.16.2. Dependabot's
  five was the correct number; the scanner's seven was inflated by two false positives.
- **`CVE-2026-59903` (netty-codec-http) has no public record at all.** Absent from OSV and GHSA, and
  `cveawg.mitre.org` returns `CVE_RECORD_DNE`. Its neighbours -59898, -59899 and -59901 are the real
  Netty August batch, all fixed in 4.1.136.Final - the version already in use. Unverifiable, nothing
  to do, and not a Dependabot blind spot.
- **HdrHistogram's two are self-disputed.** Both carry `isDisputed: true` in their own records, are
  VulDB-assigned and local-access-only, and say it is "still unclear if this vulnerability genuinely
  exists".

**Honest scoreboard for that run: one Dependabot miss, two scanner false positives, one phantom id,
and two disputed local-only findings.** The comparison was worth running - just not only in the
direction it was first read. The false-positive rate is itself a finding about the scanner, and it
belongs in any decision about gating on it.

Two lessons generalise past this tool:

- **An over-claim in your own favour survives longer than one against you.** The three-component
  reading confirmed the thesis the scan was run to test, so nobody re-derived it. The correction cost
  one pass over three advisory databases and should have happened first.
- **A scanner disagreeing with another scanner is not evidence either is right.** Both directions of
  the disagreement have to be checked against the primary record, or you have swapped one unverified
  source for two.

## What that means for gating

Findings are fatal (`exit 2`), so a false positive can block unrelated work. The escape hatch is
deliberate and reviewable rather than a flag flip: add the id to `excludeVulnerabilityIds` in the root
pom **with a reason and a retirement condition**. `bin/check-cve-exclusions.sh` then polices that
list, so a suppression written in a hurry cannot quietly become permanent.

The per-CVE triage - which finding is real, which is a false positive, and what each is blocked on -
lives in [`../../inflight/deps-cve-backlog.md`](../../inflight/deps-cve-backlog.md) while any of it is
still open.

## Reproducing a run locally

Nobody needs an account to build this repo; this is for when you are triaging a finding.

Add an `ossindex` server to `~/.m2/settings.xml`, matching `authId` in the root pom:

```xml
<server>
  <id>ossindex</id>
  <username>you@example.com</username>   <!-- the account email -->
  <password>your-api-token</password>    <!-- ossindex.sonatype.org -> account settings -> API token -->
</server>
```

Then run it with **`test-compile`**, not `validate`:

```bash
rm -rf ~/Library/Application\ Support/Sonatype/Ossindex   # or the cache answers for you
mvn test-compile -Dossindex.skip=false
```

Two things that otherwise waste an afternoon, both consequences of traps documented in full in
[the sibling record](../workflow-issues/a-check-that-reports-success-without-having-run.md):

- **`mvn validate` alone cannot work on a cold local repo**, despite the audit being *bound* to
  `validate`. The audit mojo declares `requiresDependencyResolution=test`, so Maven resolves the full
  test scope before it runs - which needs `parallel-consumer-core:jar:tests`. Without installed
  snapshots that artifact does not exist and the build dies at the second module. `test-compile`
  builds it on the way through. (The `<scope>runtime</scope>` in the plugin config filters what gets
  *audited*; it does not reduce what Maven has to resolve first.)
- **Clear the report cache before any run you intend to draw a conclusion from.** Without the server
  entry the command 401s and still *succeeds*; with a warm cache it will not even 401, it will hand
  back the previous run's findings. A clean-looking run is not evidence the credentials work - check
  the output says it scanned.

## Was a third scanner ever the answer?

No, and the alternative is worth recording so nobody re-derives it. OWASP dependency-check is the
usual suggestion for the whole-tree gap and is **not** the credential-free option it is assumed to be:
unkeyed NVD updates are rate-limited into uselessness, so it wants an NVD API key too, plus a
checked-in suppression file for its own CPE false positives. Whether this lane still earns its keep at
all is tracked in [`../../inflight/ci-ossindex-lane-reassessment.md`](../../inflight/ci-ossindex-lane-reassessment.md).
