# ArchUnit runs against test code only - main code has no architectural rules at all

Every `@AnalyzeClasses` in the tree carries `ImportOption.OnlyIncludeTests`:

```
parallel-consumer-core/src/test/.../TestConventionsArchTest.java
parallel-consumer-vertx/src/test/.../TestConventionsArchTest.java
parallel-consumer-reactor/src/test/.../TestConventionsArchTest.java
parallel-consumer-mutiny/src/test/.../TestConventionsArchTest.java
parallel-consumer-examples/*/src/test/.../TestConventionsArchTest.java   (x2)
```

Six files, all pointing at the same rule library (`TestConventionRules`), all test-scoped: integration
tests live in the right package, tests do not use Testcontainers' shaded libraries, test classes are
named so surefire collects them. Useful rules - and **not one of them looks at the product**.

So the tool most able to enforce a rule mechanically is, today, pointed away from the code where a
broken rule actually costs a user something. That matters here more than in most projects, because
this repo's rules exist mostly where nothing catches a miss (`AGENTS.md`, "For anything situational,
the deciding question is what catches a miss").

## astubbs#29 introduces the missing half - build on it, do not duplicate it

astubbs#29 (draft) adds `parallel-consumer-core/src/test/.../ArchitectureTest.java` with
`ImportOption.DoNotIncludeTests` - the first main-code analysis in the repo. Its opening rule is a
thread-safety one: only `ConsumerManager`, `ThreadConfinedConsumer` and the options classes may hold
a `Consumer` field, so nothing can accidentally reach past the thread-confinement wrapper.

**That is the seam.** Once it lands, a main-code rule is a few lines in an existing file rather than
new scaffolding, which is what makes the candidates below cheap. Do not stand up a second main-code
analysis class in parallel.

## Candidate rules, in the order they become worth it

**1. Only `ThrowableUtils` may call `Throwable.getMessage()`.** Turns "do not log bare messages" into
"go through the utility", which is the enforceable form. `e.getMessage()` alone drops the type, the
cause chain and the stack, and prints `null` for a message-less throwable - the original complaint
behind astubbs#267, and a fresh instance of it appeared on astubbs#29's revoke path while that PR was
in draft, which is the argument for a rule rather than vigilance.

Feasible today - counted, not estimated:

| | |
|---|---|
| `getMessage()` occurrences in main | 8 |
| of those, comments/javadoc | 4 |
| inside `ThrowableUtils` | 3 |
| genuine call sites elsewhere | **1** (`ProducerManager`, grep `lastErrorSavedForRethrow`) |

So: one exemption, or migrate that site to `describeWithRootCause`. `StringUtils`' use is
`MessageFormatter.arrayFormat(...).getMessage()` on an SLF4J `FormattingTuple`, not a `Throwable`, so
an owner-typed rule excludes it without an exemption.

**2. No raw `Throwable` handed to a `Logger`.** Expressible precisely - match calls whose target owner
is `Logger` and whose raw parameter types are `(String, Throwable)`. **Not worth it, and possibly
never**: see [`parked-blanket-safe-logging.md`](parked-blanket-safe-logging.md) for why the decision
went the other way. Recorded here only so the next person costing it out finds the answer instead of
re-deriving it.

## What ArchUnit cannot do here, so nobody tries

**Statement ordering inside a method.** ArchUnit's model is classes, methods, calls and dependencies -
it cannot see that a log call happens *before* the bookkeeping that must not be skipped. That is the
shape of the worst defect astubbs#267 fixed (`runUserFunction`, grep
`Exception caught in user function running stage`), and it is structurally invisible to any
ArchUnit rule. Only a test catches it, which is why that PR added one.

Worth stating explicitly because "add an ArchUnit rule" is a tempting answer to a class of problem it
provably cannot address.
