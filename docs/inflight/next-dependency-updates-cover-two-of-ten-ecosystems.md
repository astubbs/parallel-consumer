# Nothing updates the foreign clients' dependencies, and the Java pins are declared three times

Raised 2026-08-17 (astubbs#242) after a gRPC CVE was found by CI rather than by a bot, on a version
pinned when the branch started. Not started.

## What is automated today

`.github/dependabot.yml` declares exactly two ecosystems:

- `maven`, directory `/`, daily
- `github-actions`, directory `/`, weekly

No Renovate config exists. So the following are covered by **nothing**:

| Ecosystem | Manifest in the tree | Lockfile |
|---|---|---|
| Go modules | `client-go/go.mod` | `go.sum` |
| Cargo | `client-rust/Cargo.toml` | `Cargo.lock` |
| npm | `client-typescript/package.json` | `package-lock.json` |
| Bundler | `client-ruby/Gemfile` | `Gemfile.lock` |
| pip | `client-python/pyproject.toml` | - |
| NuGet | the .NET module's `.csproj` files | - |
| Swift Package Manager | the Swift module's package manifest | - |
| C++ | whatever its container build resolves | - |

**Eight of ten ecosystems have no update path at all**, automated or scheduled. A CVE in any of them
surfaces only if `deps: whole-tree CVE scan` happens to see it, or if a human looks.

## Why the stale gRPC pin was never going to be caught

Two independent reasons, and the first is the one that matters:

- **Dependabot scans the default branch.** Every proxy module is unmerged, so those poms do not exist
  on `master` and have never been eligible. Nothing was missed; nothing was ever looked at.
- **`grpc.version` is declared three separate times** - in `parallel-consumer-proxy-protocol/pom.xml`,
  `parallel-consumer-proxy/pom.xml` and `parallel-consumer-proxy-client-java-grpc/pom.xml` - rather
  than once in the root pom's `dependencyManagement`. Three places to bump, three places to drift, and
  a bump that updates two of three looks green.

The version itself was not careless: it was fixed by the U1 feasibility gate, which measured gRPC
1.73.0 on a throwaway probe and discarded the probe. **A pin chosen at the start of a long branch
goes stale during the branch's life** - which is an argument for a freshness check before merge, not
for choosing differently at the time.

## What to do, roughly in value order

- **Collapse the three `grpc.version` declarations into one** managed property. Cheapest, and it is a
  correctness fix as much as a maintenance one.
- **Add the missing ecosystems to `dependabot.yml`.** Go, Cargo, npm, Bundler, pip and NuGet are all
  natively supported; Swift is supported for SwiftPM; C++ realistically is not, and should be named as
  uncovered rather than left to look covered.
- **Decide the noise budget first.** Eight ecosystems on a daily interval against eleven client
  modules is a lot of pull requests for one maintainer, and a bot whose PRs are ignored is worse than
  no bot - it trains the habit of ignoring it. Weekly, grouped by ecosystem, is the shape to start
  with.
- **A pre-merge freshness check** would have caught this one where Dependabot structurally could not,
  because it runs on the branch rather than on `master`. That is the only mechanism that covers work
  which has not landed yet.
