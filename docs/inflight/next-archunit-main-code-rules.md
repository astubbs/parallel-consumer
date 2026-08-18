# ArchUnit is wired up and barely used

Nothing is switched off. There is no `archunit.properties`, no `FreezingArchRule`, no `@Disabled`,
no `allowEmptyShould` suppression anywhere in the repo. Every rule that exists runs on every PR at
ArchUnit's defaults, and they pass because the codebase complies.

The reason no findings are ever seen is simply that **there are four rules, and three of them are
about test hygiene**:

| Rule | Where | Polices |
|---|---|---|
| Integration tests live in an `integrationTest` package | `TestConventionRules` | test hygiene |
| Tests must not use shaded libraries | `TestConventionRules` | test hygiene |
| Test classes named so surefire collects them | `TestConventionRules` | test hygiene |
| `WorkContainer#getFuture()` has no production readers | `WorkContainerFutureIsWriteOnlyArchTest` | one main-code invariant |

The harness is in good shape and the hard part is already done: `TestConventionRules` is a shared
rule library shipped in the core test-jar, and each module has a thin `TestConventionsArchTest` that
points `@AnalyzeClasses` at its own packages. Core, vertx, reactor and mutiny all wire it up. Adding
a rule is a few lines in one place and it applies everywhere.

**No main-code architecture is policed.** Not layering, not package boundaries, not the dependency
direction between `internal` and `state`, not the God-class boundaries `docs/refactoring.md` wants
to decompose.

## Why this is worth picking up

`docs/refactoring.md` carries **"Decompose the God class - `AbstractParallelEoSStreamProcessor`
(1533 lines)"**, marked high risk, plus a thread-model rework and a list of SpotBugs findings for
non-atomic and stale-thread state. Every one of those is a structural constraint that is currently
enforced by review attention alone. A decomposition with no rule holding the new boundaries will
drift back, and the drift is invisible until someone reads the whole class again.

The precedent for the value is on this branch: `WorkContainerFutureIsWriteOnlyArchTest` was written
because a comment saying "nothing reads this" is not enforcement, and mutation-checking it proved it
fires. That is one invariant. The class it guards has several more that nothing checks.

## Candidate rules, cheapest first

Each needs its own judgement about whether it is true today; a rule that fails on arrival needs the
code fixed or the rule narrowed, not a suppression.

- **Dependency direction** between `internal`, `state`, `offsets` and the public API package - state
  should not reach back into the controller.
- **No raw `Thread` / executor construction outside the places that own lifecycle**, so the
  thread-model rework has a boundary to hold.
- **Non-volatile shared-state guard** for fields written by one thread and read by another - the
  SpotBugs findings in `docs/refactoring.md` (`lastWorkRequestWasFulfilled`,
  `ConsumerManager.commitRequested`, `RetryQueue.closed`) name the current offenders, and `state` in
  `AbstractParallelEoSStreamProcessor` is the one that makes the astubbs#209 race window
  unpredictable.
- **Public API surface** - what may be `public` outside the documented API packages, which would have
  caught the `// todo make private` in `WorkManager` before it became a `todo`.

## When

**After 0.6.0.0.** Adding architecture rules during a release cut turns the build red for reasons
that have nothing to do with the release, and the God-class decomposition these rules would guard is
itself post-v6 work. The `next-` prefix on this file is that decision.

**One exception, and it is already in use:** a rule that pins an invariant created by work landing
now. `WorkContainerFutureIsWriteOnlyArchTest` was added mid-branch for exactly that reason - a
comment saying "nothing reads this" is not enforcement. Keep doing that per-invariant; it is not the
programme this file describes, and it does not wait for v6.

## The trap to avoid

Do not add rules in bulk and freeze the failures. `FreezingArchRule` exists and would let a wide net
land green on day one, but a frozen violation is a documented invariant nobody enforces - the exact
pattern this repo already rejects for quarantined tests, where the registry is "enforced, not
advisory". Add one rule at a time, green on arrival, each with a mutation check proving it can fail.
