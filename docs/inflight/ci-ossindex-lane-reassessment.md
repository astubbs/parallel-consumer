# OSS Index lane - does it still earn its keep?

The `deps: ossindex audit` lane (added by astubbs/parallel-consumer#279) was justified on Dependabot
missing **transitive** components. That premise has partly expired, so the lane needs one deliberate
re-look rather than an indefinite assumption.

GitHub has shipped automatic Maven transitive dependency submission since
[2024-07-29](https://github.blog/changelog/2024-07-29-automatically-submit-your-maven-transitive-dependencies-to-the-dependency-graph/)
/ [2025-03-26](https://github.blog/changelog/2025-03-26-transitive-dependencies-are-now-available-for-maven/).
It was simply never switched on here. **Enabled 2026-08-11.**

## What was measured before the flip

Measured, not reasoned from the docs, via
`gh api repos/astubbs/parallel-consumer/dependency-graph/sbom`:

| Component | In the dependency graph | Dependabot alerted |
|---|---|---|
| `jackson-databind` | yes | yes |
| `micrometer-core` | yes | yes |
| `lz4-java` | **no** | no |
| `HdrHistogram` | **no** | no |
| `netty-codec-http` | **no** | no |

The overlap is exact: the graph held only what the poms declare, and the three "Dependabot blind
spots" are precisely the components absent from it. The transitive-closure gap was real. Note the
consequence was smaller than first claimed, though - only `lz4-java` had an advisory to alert on.

**Baseline at the moment of the flip**, so the re-measurement means something: 121 packages in the
graph, zero occurrences of `lz4-java` / `HdrHistogram` / `netty-codec-http`, 6 open Dependabot
alerts. Automatic submission runs on a push to the default branch, so it had not repopulated yet.

## The trigger

**After the next push to `master`**, re-run the SBOM query and check two things:

1. do `lz4-java`, `HdrHistogram` and `netty-codec-http` now appear in the graph;
2. does a Dependabot alert fire for `lz4-java` (GHSA-xx22-p4ch-683r exists for it).

## What the answer would mean

If the graph picks up the full closure and Dependabot starts covering it, the **transitive-coverage**
justification for this lane is gone, and only two remain:

- **proving the scan actually ran** - Dependabot has no equivalent, and it is the entire reason
  `bin/check-ossindex-audit.sh` exists;
- **a whole-tree scheduled scan that can fail a build.**

That is a genuine question to ask at that point, not a foregone conclusion either way. The honest
answer might be that the lane collapses to just the scheduled scan.

Weigh in the maintenance cost the dependency graph does not carry: the exclusion list in
astubbs/parallel-consumer#281 has to be kept honest as advisories are published, corrected and
retired, and OSS Index has already produced two false positives and one phantom CVE id in a single
scan.

**Decision (2026-08-11): keep the scanner.** Nothing was removed or weakened; this note exists so the
reassessment actually happens instead of being rediscovered.
