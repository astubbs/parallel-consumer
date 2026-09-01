# Every build depends on packagecloud hosting Truth Generator, and one read timeout reddens master

<!-- inflight-type: task -->
<!-- inflight-impact: deps-debt -->
<!-- inflight-state: deferred - the first step is a Maven Central release of truth-generator, which is astubbs/truth-generator's work and not this repo's; nothing here can move until it lands -->

`io.stubbs.truth:truth-generator-maven-plugin:0.1.1` and its `truth-generator-api` are resolvable
from exactly one place: `https://packagecloud.io/astubbs/truth-generator/maven2`, declared twice in
the root `pom.xml` as `astubbs-truth-generator`. Neither artifact is on Maven Central.

## What it costs

On 2026-08-20 the `cache` job on master failed its `Download all dependencies` step with
`Could not transfer artifact io.stubbs.truth:truth-generator-maven-plugin:pom:0.1.1 ... Read timed
out`. Nothing was wrong with the code - the head was a documentation-only squash - but master read
as broken, which is the expensive part: a red master that is not about the code trains everyone to
discount red masters.

The blast radius is the whole build, not one module. `go-offline` resolves it for
`parallel-consumer-core`, so when packagecloud is slow, nothing compiles anywhere. There is no
mirror and no fallback repository.

**It does not reach consumers.** Both dependencies are `<scope>test</scope>`, and the generator is a
build-time plugin, so a published `bz.stub.parallelconsumer` artifact carries no dependency on
`io.stubbs.truth`. This is a build-and-CI availability problem only - which is why it is
`deps-debt` rather than a release blocker.

## Why it is still open

Truth Generator is this project's own (`astubbs/truth-generator`), so the fix is available rather
than blocked on a third party - but it lives in that repo, not this one. Its `pom.xml` declares two
publish profiles, `github` (GitHub Packages) and `package-cloud`, the latter `activeByDefault`.
**Neither is Maven Central.** Tags are `0.1` and `0.1.1`; the pom sits at `0.1.2-SNAPSHOT`.

Verified 2026-08-20: `https://repo1.maven.org/maven2/io/stubbs/truth/` returns **404**, and Central's
search API returns 0 results for the group, for `a:truth-generator*` across all groups, and for
`g:bz.stub*` - against a control query for `org.apache.kafka:kafka-clients` that returned 1, so the
API and the network were both fine.

## What closes this

1. In `astubbs/truth-generator`: add a Central publishing profile and release `0.1.1` (or cut
   `0.1.2`) to Maven Central. Not this repo's work, and nothing here can proceed without it.
2. In this repo: delete both `astubbs-truth-generator` repository declarations from the root
   `pom.xml` and confirm a clean `~/.m2` build resolves everything from Central.

Until step 1 lands, the packagecloud declaration has to stay, so the open question is whether the
build should tolerate the flakiness meanwhile - a mirror, or retry settings on that repository. That
is a decision, not a task, and nobody has made it.

## Related

- `docs/ci.md` - what each workflow does and how to fetch a failed job's log
