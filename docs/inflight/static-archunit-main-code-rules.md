# ArchUnit pins three invariants and no architecture

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - after v6, tooling investment -->


Nothing is switched off. There is no `archunit.properties`, no `FreezingArchRule`, no `@Disabled`,
no `allowEmptyShould` suppression anywhere in the repo. Every rule that exists runs on every PR at
ArchUnit's defaults, and they pass because the codebase complies.

The reason no findings are ever seen is that the rules police **hygiene and a handful of named
invariants**, not structure:

| Rule | Where | Polices |
|---|---|---|
| Integration tests live in an `integrationTest` package | `TestConventionRules` | test hygiene |
| Tests must not use shaded libraries | `TestConventionRules` | test hygiene |
| Test classes named so surefire collects them | `TestConventionRules` | test hygiene |
| `WorkContainer#getFuture()` has no production readers | `WorkContainerFutureIsWriteOnlyArchTest` | one main-code invariant |
| `ShardManager`'s shard-map setter has no production callers | `ShardMapIsNeverReplacedArchTest` | one main-code invariant |
| `ShardManager.processingShards` is assigned only at construction | `ShardMapIsNeverReplacedArchTest` | one main-code invariant |

The harness is in good shape and the hard part is already done: `TestConventionRules` is a shared
rule library shipped in the core test-jar, and each module has a thin `TestConventionsArchTest` that
points `@AnalyzeClasses` at its own packages. Core, vertx, reactor and mutiny all wire it up. Adding
a rule is a few lines in one place and it applies everywhere.

**No main-code architecture is policed.** Not layering, not package boundaries, not the dependency
direction between `internal` and `state`, not the God-class boundaries `docs/refactoring.md` wants
to decompose. The three invariant rules above are each one named member; none of them constrains a
shape.

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
code fixed or the rule narrowed, not a suppression. Read the granularity limits below before
costing any of them - one of the four is not expressible in ArchUnit at all.

- **Dependency direction** between `internal`, `state`, `offsets` and the public API package - state
  should not reach back into the controller.
- **No raw `Thread` / executor construction outside the places that own lifecycle**, so the
  thread-model rework has a boundary to hold.
- **Non-volatile shared-state guard** for fields written by one thread and read by another - the
  SpotBugs findings in `docs/refactoring.md` (`lastWorkRequestWasFulfilled`,
  `ConsumerManager.commitRequested`, `RetryQueue.closed`) name the current offenders, and `state` in
  `AbstractParallelEoSStreamProcessor` is the one that makes the astubbs#209 race window
  unpredictable. ArchUnit can see a field's modifiers, so "these named fields must be volatile" is
  writable; **which thread reads a field is not in the model**, so the general form is not, and
  SpotBugs `IS2_INCONSISTENT_SYNC` is the detector for that class.
- **Public API surface** - what may be `public` outside the documented API packages, which would have
  caught the `// todo make private` in `WorkManager` before it became a `todo`.

## What ArchUnit's granularity rules out, measured

**ArchUnit sees a field ACCESS - origin code unit, target field, get or set. It does not see what is
invoked on the value that access loaded.** So no rule can distinguish a keyed `map.get(k)` from a
bulk `map.values()`, and no rule can require that lookups of a field go through an accessor.

This kills the "mandated accessor" form of the shard-map rule, and the measurement is worth keeping
because the form reads as obviously implementable until you look at the accesses. Exactly one
main-code access to `ShardManager.processingShards` is `getShard`; nearly all the rest are bulk
operations over `values()`, `keySet()`, `size()` or the whole map, which `getShard(key)` cannot
express, and the keyed ones that remain include both writes (`computeIfAbsent`, `remove`). A rule
requiring the accessor is therefore red on arrival, and the only way to green it is an allowlist of
the methods already there - a frozen baseline under a different name, which the trap below rules
out.

The check-then-get seam that motivated the idiom (astubbs#345) is caught instead by fb-contrib's
`MUI_CONTAINSKEY_BEFORE_GET`, which names the method statically - see
[`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md). **When a candidate rule is
about what happens to a value rather than about who may touch a member, SpotBugs is the tool and
ArchUnit is not.**

What ArchUnit could still pin about the same field is who may replace it, and that is what
`ShardMapIsNeverReplacedArchTest` does.

## Pin the names the rules depend on

**ArchUnit does not check that a `String`-named target exists.** Rename the field or drop the Lombok
annotation that generates the accessor, and the rule matches nothing, passes, and has asserted
nothing. Measured: pointing both shard-map rules at names that do not exist leaves both **green**.

Every rule naming a member as a string therefore ships with a reflective `@Test` asserting the
member is still there. Lombok-generated targets need it most - one annotation deletes them and no
compile error anywhere says so.

## When

**After 0.6.0.0.** Adding architecture rules during a release cut turns the build red for reasons
that have nothing to do with the release, and the God-class decomposition these rules would guard is
itself post-v6 work.

**One exception, and it is already in use:** a rule that pins an invariant created by work landing
now. `WorkContainerFutureIsWriteOnlyArchTest` and `ShardMapIsNeverReplacedArchTest` were both added
mid-branch for exactly that reason. Keep doing that per-invariant; it is not the programme this file
describes, and it does not wait for v6.

## The trap to avoid

Do not add rules in bulk and freeze the failures. `FreezingArchRule` exists and would let a wide net
land green on day one, but a frozen violation is a documented invariant nobody enforces - the exact
pattern this repo already rejects for quarantined tests, where the registry is "enforced, not
advisory". Add one rule at a time, green on arrival, each with a mutation check proving it can fail.

**An allowlist of the methods that already violate a rule is a frozen baseline wearing a source-code
disguise**, and it is the form the trap actually takes when `FreezingArchRule` is off the table. The
shard-map accessor rule above is the worked example of turning one down.
