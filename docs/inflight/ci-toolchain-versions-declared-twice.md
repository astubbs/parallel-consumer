# Toolchain versions are declared twice: mise locally, the CI matrix on the runner

Found 2026-08-22 while getting the full suite to run locally for the first time. **Open - no fix
proposed yet, because collapsing the two needs an owner's call.**

## The two mechanisms

**Locally**, `bin/build-client.sh` expects **mise** to provide every foreign toolchain except the two
that build in containers. That decision, including the permission for agents to run `mise use -g`
themselves, is in
[`parked-containerised-toolchains-and-runtime.md`](parked-containerised-toolchains-and-runtime.md).
mise reads `~/.config/mise/config.toml` - a machine-global file, outside the repo.

**On the runner**, `.github/workflows/clients.yml` uses per-language setup actions
(`actions/setup-go`, `ruby/setup-ruby` pinned by SHA, `actions/setup-dotnet`, `actions/setup-node`,
`actions/setup-python`) and reads the version from its own matrix. **`mise` appears nowhere in
`.github/`** - a grep that seems to find it is matching the word "misused" in a comment.

Both are reasonable in isolation. Together they are two sources of truth for the same numbers, and
nothing compares them.

## The drift is real today, not hypothetical

CI matrix versus one developer machine an hour after `mise use -g`:

| | CI matrix (`clients.yml`) | this machine |
|---|---|---|
| go | `1.25.13` | 1.25.14 |
| ruby | `3.4.4` | 3.4.10 |
| dotnet | `8.0.404` | **9.0.101** |
| node | `22.17.0` | **25.9.0** |
| rust | `1.88.0` | 1.94.1 |

The go and ruby gaps are patch-level and unlikely to matter. **The dotnet and node gaps are whole
major versions**, which is the classic shape of "passed locally, failed in CI" - or worse, the
reverse, where CI is the one running the older runtime and a developer never sees the failure their
change causes.

Note the empty rows are not a gap: `swift`, `kotlin`, `scala` and `cpp` declare `toolchain: ''`
because kotlin and scala ride the Maven reactor and swift and cpp build in containers. The split in
`bin/build-client.sh` and the CI matrix already agree about *which* languages need a toolchain at
all. It is only the versions that are stated twice.

## The obvious collapse, and why it is not just done

A repo-level `mise.toml` as the single declaration, installed in CI by `jdx/mise-action`, would make
one file authoritative for both. Two things to weigh before doing it:

- **The setup actions are not merely version installers.** They carry per-language caching that the
  workflow's own header calls out (`cache: maven`, setup-go's `cache: true`, the ruby row's
  `cache-path`/`lockfile`). Replacing them with mise means either losing that caching or rebuilding
  it by hand, and a slower client matrix is a real cost paid on every pull request.
- **`ruby/setup-ruby` is pinned by commit SHA**, which is a supply-chain posture the repo chose
  deliberately. `mise` installing from a registry at build time is a different trust model, not
  obviously a worse one, but it is a change of posture rather than a refactor.

A cheaper middle option, if the full collapse is not wanted: keep both mechanisms and add a check
that the versions agree - the failure this file describes is silent drift, and a five-line gate
comparing `mise.toml` against the matrix would end it without touching how either installs anything.

## What made this visible

The suite could not run locally at all until mise was installed, because Maven fell through to
whatever was on `PATH`: a Go pinned to `GOTOOLCHAIN=local` and macOS's system Ruby 2.6. Nothing in
`AGENTS.md` said mise was required - that is fixed - but the deeper point is that **a developer can
satisfy every documented requirement and still be running different toolchains than CI**, with no
signal either way.
