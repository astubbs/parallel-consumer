# Next: wire every language into the shared conformance suite, and core with them

The suite exists and works, but proves only **Go** conforms. This note is the ready-to-execute brief
for finishing it, written down rather than carried in a session's head.

Read first: `parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/README.md` (the
runner contract), and the authoring guide's §7 (scenario names).

## The work

**Five runners** — python, typescript, rust, ruby, dotnet — each a small program against the fixed
contract: five flags, three exit statuses (0 did it, 1 could not, 2 usage), one observation line per
delivery on stdout, four behaviour tokens, and the fixed literals **including the 3-second
`report-nothing` hold**. Registry entries are already sketched as comments in `LanguageRunners.java`.
Nothing in the conformance module itself needs to change: the scenarios and assertions are
language-blind.

**Do it in one agent, not five.** The work is repetitive against a settled contract; one context
writes the second through fifth far faster and keeps them consistent, which is the entire point of a
shared suite.

## Core is a binding, not an afterthought

**One scenario definition, many bindings.** A scenario's seeds, prescribed behaviour and assertions
are written **once**; they run once per binding — in-process core, java-direct, java-grpc, then each
foreign runner. The same assertion executing many times is the goal; the same assertion being
*written* many times is the thing to prevent.

Adding **core itself** as a binding gives the suite a control arm: **if a scenario fails against
core, the scenario is wrong, not the client.** Without it, a bad assertion costs hours of debugging
an innocent client. It also makes the suite the single place "correct" is defined for this product
rather than a client-only harness.

## CI: parallel by matrix, not by threads

CI parallelism comes from the existing per-language rows in `.github/workflows/clients.yml`, each
installing only its own toolchain and carrying its own cache. One Java job running every language
would install six toolchains before it could start, and bury any single failure among the others.

So **the suite must be runnable for one language at a time** — a system property or JUnit tag — with
the matrix row selecting its own. In-process parallelism stays valuable locally, where every
toolchain is present and all bindings run in seconds.

Each row must also build the sidecar jar it spawns; the Go row already needed that fix, and its
`go-e2e-harness` profile is the model.

## Make a non-run impossible to mistake for a pass

Three distinct holes, all the same failure class — this repo has seven recorded instances of checks
that reported success without having run:

- **An unmatched or misspelled selector must fail loudly.** A row that silently evaluates zero
  scenarios and reports green is the worst outcome available here.
- **A registered runner that is missing or crashes must fail** — as it already does, with a message
  that says it fails rather than skips. Absence looks exactly like agreement.
- **A module with real source must not still be deferred.** The clients matrix derives each row's
  skip from the module's own deferral fragment, which is right for an unstarted skeleton — but
  nothing enforces the reverse, so a started module whose fragment was never lifted would be skipped
  indefinitely while looking green. `lifted_by` is a note to a human, not a check. Add one:
  source present beyond the skeleton **and** `deferred:` still set → fail.

## Expose everything in one run

`--fail-at-end` is used nowhere today, so a reactor build stops at the first failing module and hides
every module after it — one problem learned per run, then fix, push, and discover the next. Add it to
the CI build lanes so every independent branch of the reactor reports together.

**And assert the run still ends red.** `-fae` preserves Maven's exit code, but a pipe without
`pipefail` loses it; the repo has been bitten by that shape before. Prove the failing case exits
non-zero rather than assuming it.

## Verification bar

Each new runner proven **red before green** on every scenario — and inherit the two traps the Go wave
found, both recorded in the scenario javadoc: a runner that exits too fast kills its own report in
flight and the sabotage passes; and "records sharing a key" cannot be sabotaged by declining to hold,
because the engine dispatches both shards in one wave regardless — the real sabotage is a mutex
around the processor.
