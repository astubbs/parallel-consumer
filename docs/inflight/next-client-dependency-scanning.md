# Later phase: dependency scanning for the non-JVM clients — try GitHub first

The CVE audit covers the Maven tree only. The five non-JVM clients pull dependencies from npm, PyPI,
crates.io, RubyGems and NuGet, and **nothing scans any of them** — a gap that only appeared when
those clients did. It has already cost something real: a **critical** advisory in the Go client's
gRPC dependency sat unnoticed until CI triage surfaced it (fixed in the conformance wave, 1.71.1 →
1.82.1).

Owner's steer, 2026-08-15, and it is the right instinct:

> "Let's not try adding the OSS scanner from Java equivalent to every project if GitHub can do it for
> us."

## Check GitHub's native support before building anything

**Dependabot's ecosystem coverage is the thing to evaluate**, because if it covers these five the
whole job is one configuration file rather than five scanners, five authentication stories and five
CI steps to maintain. Its alerts also arrive against the repository rather than only inside a CI run,
which is where a security finding is actually useful.

What to establish before deciding:

- Which of `npm`, `pip`, `cargo`, `bundler`, `nuget` and `gomod` the current Dependabot supports for
  **alerts** as well as version updates, and whether a *library* (rather than an application) with no
  committed lockfile gets useful results — several of these clients deliberately have no lockfile,
  and that choice was made for good reasons.
- Whether alerts can **fail a pull request**, or only notify. The Maven lane blocks on findings today;
  if Dependabot only notifies, the two lanes enforce different standards and that difference should
  be a decision rather than an accident.
- Whether the noise level is tolerable — a scanner nobody reads is worse than none, because it looks
  like coverage.

**Only if GitHub's answer is genuinely inadequate** should per-ecosystem scanners be added, and then
with the same discipline as everything else here: one shape decided once, not five improvisations.

## Related

The Maven lane's own exclusion discipline (a documented rationale and a retirement condition per
excluded advisory) is the standard any new lane should meet — see the CVE backlog note and
`bin/check-cve-exclusions.sh`. Whatever tool is chosen, **a finding must be either fixed or
explicitly excluded with a reason**, never quietly tolerated.
