# Core's latent SpotBugs count: 30 recorded, 67 measured, nobody has reconciled them

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

[`static-spotbugs-latent-findings.md`](static-spotbugs-latent-findings.md) records **30** findings in
core, surfaced when the package rename made the baseline miss. A fresh unbaselined run of core on
2026-08-25, stock detectors only, reports **67** at the same `Max`/`Medium` settings.

**Not chased.** The plausible explanations are that the 30 was what the PR-annotation action
displayed rather than what SpotBugs found, or that core has grown since. Both are checkable and
neither was checked.

**Why it matters enough to keep a note open.** That other note is the triage of record for core's
latent findings. If it covers half of them, then its conclusion - "everything else here is either
noise or cosmetic" - is a claim about a smaller set than any reader assumes, and the reader has no
way to tell. A triage document that silently covers part of its subject is the same shape as a check
that reports green while blind, which is what the build-hardening register exists to catch.

**How to settle it:** re-run core unbaselined at `Max`/`Medium` with stock detectors only, count the
findings in `spotbugsXml.xml` directly rather than through the annotation action, and compare against
the 30. Whichever way it lands, correct the recorded number in
`static-spotbugs-latent-findings.md` and delete this note.

---

This is what remained open when `static-spotbugs-coverage-and-extension-detectors.md` was retired.
Everything else that note tracked - SpotBugs running in one module, test code unanalysed, no
extension detectors, javac's own analysis switched off, RacerD unexplored, and the auto-regenerating
baseline - was resolved by the branch that retired it. Its ruled-out survey table already lived in
[`ci-build-hardening-register.md`](ci-build-hardening-register.md), so nothing moved; the detector
versions are properties in `pom.xml`, and the reasoning behind each is in
[`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md).
