# Adopt `just` as the front door to `bin/`, so the entry points are listed rather than remembered

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

**The proposal:** add a `justfile` at the repo root whose recipes wrap the existing `bin/` entry
points, so `just --list` prints the menu of things a person or an agent can run here, each with a
one-line description, and `just build`, `just check`, `just chaos` and the like replace remembering
which of ninety files under `bin/` is the one you want. Nothing moves; `bin/` stays the
implementation and the gates keep their globs.

Raised by Antony on 2026-09-09 after reading KPipe's benchmark post, where every reproduction step
is a `just` recipe (`just bench`, `just bench mode=smoke`, `just bench mode=latency`) and the README
documents the project by naming them. Prior art: KPipe's `justfile` at
<https://github.com/eschizoid/kpipe/blob/main/justfile> - fifteen recipes covering build, the Docker
stack, seeding, release and the benchmark sweep, with parameters defaulted in the recipe signature.

## The problem it answers

`bin/` holds ninety entries at the time of writing: build, CI, chaos, soak, performance, Lincheck,
quarantine, mutation, and twenty-odd `check-*` gates each paired with a `test-check-*` self-test.
There is no listing of them with intent. The routes to finding the right one are `ls bin`, which
gives names without descriptions; `AGENTS.md`, which names two dozen of them across its sections
and is loaded whole into every session for that among other reasons; and each script's own header
comment, which is where the description actually lives. An agent that does not already know
`bin/lincheck-test.sh` exists will not find it by looking for "how do I run the Lincheck lane".

That is the same failure `AGENTS.md` describes for docs - the commonest mistake is never opening
the thing - applied to executables. `just --list` is the pull form of an entry-point index, and a
`just` recipe with a `# comment` above it is the description in the one place nothing else reads.

## What it would look like

```just
# Quick local build: compile plus unit tests
build:
    bin/build.sh

# Every gate in bin/, concurrently - run before you push
check *ARGS:
    bin/check-all.sh {{ARGS}}

# The chaos suite; SEED is optional
chaos SEED="":
    bin/chaos-test.sh {{SEED}}

# The Lincheck lane, with the JVM args it needs
lincheck:
    bin/lincheck-test.sh
```

Recipes are thin. The rule would be **a recipe never carries logic**, so `bin/` remains the only
place behaviour lives and the justfile cannot drift into a second implementation.

## What it costs, and what has to be true first

- **A toolchain decision.** `just` is not on this box and not in `mise` here; toolchains are
  Ansible-managed, so adding it is a playbook change, not a `cargo install`. A `justfile` nobody can
  run is worse than none.
- **The permission allowlist keys on the script path.** `bin/AGENTS.md` records that the reviewer's
  grant is `Bash(bin/check-*.sh:*)`, and that a Node gate invoked as `node bin/check-x.mjs` silently
  matches nothing. `just check` matches nothing either. Either the allowlist gains `Bash(just *)`, which
  is a broad grant, or `just` is a convenience for people and agents keep calling `bin/` directly - in
  which case the discoverability benefit for agents is only `just --list`, which is still the point.
- **A second list that can drift.** Every new `bin/` entry point needs a recipe, and nothing would
  check that. The fix is the same shape as `bin/check-all.sh`'s glob discipline: a gate that every
  runnable `bin/*` outside `lib/` and `test-*` appears in the justfile, or the justfile is generated
  from script headers. Decide which before writing the first recipe, because the generated form
  wants the header comments in a fixed shape and half of them are prose.
- **CI and the hooks do not benefit.** They call scripts by path and should keep doing so; the
  justfile is for the interactive front door only.

## What would settle it

Wrap the ten entry points `AGENTS.md` names most, ship the drift gate with them, and watch one week
of sessions: do agents reach for `just --list` unprompted, or does the session-start hook have to
inject it? If the hook has to inject it, the listing belongs in the hook and the justfile has earned
nothing.

## Related

- [`process-adopt-external-harness.md`](process-adopt-external-harness.md) - the same question one
  level up: whether to keep hand-building the harness at all.
- [`../../bin/AGENTS.md`](../../bin/AGENTS.md) owns the `bin/` conventions this would sit in front of,
  including the allowlist trap above.
- The rest of what KPipe's toolchain has that this repo does not is in chat with Antony on
  2026-09-09 and not yet recorded; each candidate gets its own note if it survives a look.
