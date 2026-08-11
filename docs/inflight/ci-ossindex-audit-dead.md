# OSS Index audit: a green run is not evidence it scanned

`ossindex-maven-plugin` gets `HTTP/1.1 401 Unauthorized` from `ossindex.sonatype.org` - the API no
longer serves unauthenticated component-report requests. It was bound to `validate` with
`fail=false`, so every build ran it, got 401 on every module, and stayed green: a security check
reporting success by not running.

**Wired but not switched on** (astubbs/parallel-consumer#259): `authId` now names an `ossindex`
`<server>` in `settings.xml`, `fail` is `true`, and `ossindex.skip` defaults to `true` so the audit
is off *and visibly off* instead of running and silently failing. Flip it with
`-Dossindex.skip=false`.

Note that none of that wiring is a *prerequisite* for running a scan: `authId` is a plugin user
property, so `-Dossindex.authId=ossindex -Dossindex.skip=false` behaves identically with or without
the `<authId>` element in the pom. The element earns its place as a declarative default, not as an
enabler - nobody was ever blocked on it.

`OSSINDEX_USERNAME` and `OSSINDEX_TOKEN` exist as repo secrets. **A CI job now exists on branch
`ci/ossindex-audit-job`** (astubbs/parallel-consumer#279, open, not merged) with a three-legged
did-it-actually-scan guard. The constraints it had to satisfy, kept here because they are the
reasoning rather than the code:

- **One job only, not "on in CI".** The audit is bound to `validate`, which every Maven job runs, so
  enabling it globally means six-plus scans per PR from one account and a burnt rate limit for no
  extra signal.
- **The job needs its own did-it-actually-scan check**, or it recreates the original bug somewhere
  more expensive: a green check that scanned nothing. Failing the step when
  `Failed to fetch component-reports` appears is enough.
- **Give it a `schedule:` too.** That is what closes the whole-tree gap below, and is nearly free
  once the job exists.
- Credentials reach Maven via `setup-java` (`server-id: ossindex`, `server-username`,
  `server-password`), the same pattern `publish.yml` already uses for Central.
- **Never cache the OSS Index report directory** in the job - see the second trap below.

## Two traps to know about before turning it on

Both produce the same false signal - a scan that looks like it worked while never reaching the
service - which is the whole subject of this note.

### 1. `fail=true` does not make an unreachable scanner fatal

It covers "vulnerable components were found", not "the request failed". Verified directly: forced on
with no credentials and `fail=true`, the 401 is still only a `[WARNING]` and the build reports
SUCCESS. The plugin exposes no setting that changes this.

So this cannot be made self-policing from config alone. Whoever adds the token must confirm the run
actually returned findings - and be aware that a later token expiry, revocation or outage degrades
silently back to green-but-not-scanning, with nothing to catch it. If it goes into a gating lane,
the lane needs its own check that the scan produced output.

### 2. The client caches reports on disk, and a warm cache fakes a successful scan

`ossindex-maven-plugin` keeps an on-disk report cache - on macOS
`~/Library/Application Support/Sonatype/Ossindex`. **A warm cache returns full, correct-looking
results even against a bogus base URL and an expired token.** That is not a theoretical risk: it
silently invalidated the first two negative controls run against this plugin before anyone noticed
the results were too good.

Two consequences, both easy to get wrong:

- **Any reachability or credential experiment must clear that directory first**, or a "yes, my token
  works" result means nothing. This applies to `-Dossindex.baseUrl=...` negative controls too.
- **CI must never cache it.** A cached report directory would let the job report findings - and pass
  its did-it-actually-scan guard - on a run that never contacted the service at all, rebuilding the
  original defect inside the very guard meant to prevent it.

## Running it locally (optional - nobody needs an account to build)

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
rm -rf ~/Library/Application\ Support/Sonatype/Ossindex   # or the cache lies to you - trap 2
mvn test-compile -Dossindex.skip=false
```

Two things that will otherwise waste an afternoon:

- **`mvn validate` alone cannot work on a cold local repo**, despite the audit being *bound* to
  `validate`. The audit mojo declares `requiresDependencyResolution=test`, so Maven resolves the
  full test scope before it runs - which needs `parallel-consumer-core:jar:tests`. Without installed
  snapshots that artifact does not exist yet and the build dies at the second module. `test-compile`
  builds it on the way through. (The `<scope>runtime</scope>` in the plugin config only filters what
  gets *audited*; it does not reduce what Maven has to resolve first.)
- **Clear the report cache before any run you intend to draw a conclusion from.** Without the server
  entry the command 401s and, per trap 1, still *succeeds* - and with a warm cache it will not even
  401, it will hand back the previous run's findings. A clean run is not evidence the credentials
  work. Check the output says it scanned.

## What it found the moment it was switched on

The token works, and the first authenticated run failed the build on **5 vulnerable components, 12
CVEs**, top severity **9.3**. This is why `ossindex.skip` stays `true` for now: turning it on by
default breaks every build until these are triaged.

What the scanner reported (all ids are `CVE-2026-NNNNN`):

| Component | Scope | CVEs (severity) | Dependabot |
|---|---|---|---|
| `com.fasterxml.jackson.core:jackson-databind:2.16.2` | runtime | 54512 (9.2), 54513 (9.3), 54514 (6.9), 54515 (6.9), 54518 (6.9), 59888 (6.3), 59889 (6.3) | 5 of 7 |
| `io.micrometer:micrometer-core:1.13.15` | compile | 40984 (8.7) | yes |
| `io.netty:netty-codec-http:4.1.136.Final` | compile | 59903 (6.5) | no |
| `at.yawk.lz4:lz4-java:1.10.1` | runtime | 59949 (6.5) | no |
| `org.hdrhistogram:HdrHistogram:2.2.2` | runtime | 14683 (2.0), 14686 (2.0) | no |

The per-CVE triage is written up separately as `docs/inflight/deps-cve-backlog.md`, on branch
`security/cve-backlog-triage` - open for review as astubbs/parallel-consumer#281 and **not merged**,
so it is a pointer to work you can read today, but not to anything on master. What follows is only
what the *scanner comparison* turned out to be worth, because the first reading of it was wrong.

### Correction: the Dependabot gap is one component, not three

An earlier version of this note claimed **three of the five components have no Dependabot alert in
any state**, and inferred that GitHub's Maven dependency graph must be failing to resolve the full
transitive closure. Checked against OSV, GHSA and MITRE, that over-claimed in the scanner's favour,
and the mechanism was guesswork:

- **`at.yawk.lz4:lz4-java` is the one real gap.** GHSA-xx22-p4ch-683r exists and no Dependabot alert
  fired. A single genuine miss still carries the point that the two tools are not redundant - but it
  is one, not three, and one miss does not evidence a systematic transitive-closure failure.
- **The jackson "5 of 7" was OSS Index being wrong, not Dependabot.** `CVE-2026-54518` was
  introduced in 2.21.0 (GHSA-rcqc-6cw3-h962) and `CVE-2026-59889` in 2.18.0 (GHSA-5gvw-p9qm-jgwh);
  both live in `UnwrappedPropertyHandler` code that does not exist in 2.16. Neither applies to
  2.16.2. Dependabot's five was the correct number; the scanner's seven was inflated by two false
  positives.
- **`CVE-2026-59903` (netty-codec-http) has no public record at all.** Absent from OSV and GHSA, and
  `cveawg.mitre.org` returns `CVE_RECORD_DNE`. Its neighbours -59898, -59899 and -59901 are the real
  Netty August batch and are all fixed in 4.1.136.Final - the version already in use. Unverifiable,
  nothing to do, and not a Dependabot blind spot.
- **HdrHistogram's two are self-disputed.** Both carry `isDisputed: true` in their own records, are
  VulDB-assigned and local-only, and say it is "still unclear if this vulnerability genuinely
  exists".

So the honest scoreboard for that first authenticated run is: one Dependabot miss, two scanner false
positives, one phantom id, and two disputed local-only findings. **The false-positive rate is itself
a finding about this scanner**, and belongs in any decision about gating on it. The comparison was
worth running - just not only in the direction it was first read.

**Only one of the two highest findings is a deliberately-held-back pin.** `micrometer-core` is:
the root pom holds `micrometer-core.version` at 1.13.15 to keep the family aligned with
`micrometer-registry-prometheus`, which example-metrics pins to 1.12.x until the
`io.micrometer.prometheus` -> `io.micrometer.prometheusmetrics` rename is migrated. Pins parked for a
compatibility reason are where CVEs accumulate, precisely because nobody upgrades them, and triage
needs to weigh that rather than just bump.

`jackson-databind` is **not** that, though an earlier version of this note said it was. There are
three distinct copies in the reactor and only one of them is the scanned artifact
(`mvn dependency:tree -Dincludes=com.fasterxml.jackson.core:jackson-databind`):

| Where | Version | Scope | Origin |
|---|---|---|---|
| `parallel-consumer-example-streams` | **2.16.2** | runtime | transitive, unpinned - **this is the flagged one** |
| `parallel-consumer-example-metrics` | 2.17.2 | test | explicit module-local pin |
| core, vertx, example-vertx, example-reactor | 2.13.4.2 | test | transitive via WireMock |

The "kept module-local because pinning it globally breaks `VertxTest` through `wiremock-jre8`"
reason belongs to the 2.17.2 test pin and the 2.13.4.2 WireMock copy. It is a real constraint on
adding a *global* `dependencyManagement` entry, but it is not why the flagged 2.16.2 sits where it
does - that one is simply an unpinned transitive runtime dependency of an example module that nobody
has ever set. So triaging it is a different job from unblocking a parked pin, and WireMock is a
reason not to fix it with a global pin rather than a reason it cannot be fixed.

### Why the audit still cannot simply be switched to `fail=true`

HdrHistogram 2.2.2 is the newest release, and **no fixed version exists** for either of its disputed
CVEs. Turning the audit on as a gate therefore leaves the build permanently red on a disputed,
local-only, severity-2.0 finding with no remedy available - unless an exclusion is configured first
(`excludeVulnerabilityIds`). That is the practical blocker for the CI job described above, and it is
not visible from the plugin docs.

## What is genuinely covered meanwhile

- **`deps: vulnerabilities`** (`maven.yml`, `actions/dependency-review-action@v4`,
  `fail-on-severity: high`) really does gate PRs - but only for dependencies a PR *changes*.
- **Dependabot alerts** cover the existing tree against newly published advisories.

The residual gap is therefore narrower than "no CVE scanning": there is no **whole-tree, scheduled**
scan that fails a build. Anyone reading only the 401 would over-correct here and bolt on a third
scanner duplicating the two that already work.

## If the token turns out not to be worth it

OWASP dependency-check is the usual alternative for the whole-tree gap, and is **not** the
credential-free option it is assumed to be: unkeyed NVD updates are rate-limited into uselessness,
so it wants an NVD API key too, plus a checked-in suppression file for its CPE false positives.
