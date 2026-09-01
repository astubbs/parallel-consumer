# A pre-fork build script pins JDK 13 and calls system maven - the other one is already handled

<!-- inflight-type: bug -->
<!-- inflight-impact: config-lie -->
<!-- inflight-state: parked - the keep-or-delete call is the maintainer's; the sibling is gone, so this note is now about one file -->

**Half of this is already done, on a branch that reached it independently.** The module-scoped
sibling is DELETED by `e7b1dda88` on astubbs/parallel-consumer#200 - read it at
`git show e7b1dda88^:bin/build-parallel-consumer-core-without-tests.sh`, since the path no longer
resolves in the working tree. Do not redo that work; what is left here is the survivor below.

**`bin/build-without-tests.sh`** - the whole-tree variant - survives, and breaks several of this
project's stated build rules at once. Nothing runs it, nothing references it, and nothing goes red.

```
export JAVA_HOME=$(/usr/libexec/java_home -v13)
mvn clean install -Dmaven.test.skip=true
```

## What is wrong with them

- **They pin JDK 13.** `AGENTS.md` requires **JDK 17** - the build uses Jabel to compile 17 source to
  8 bytecode, and the mutiny module's floor is 17 outright.
- **`/usr/libexec/java_home` is the call this project has already been bitten by**, independently of
  the version argument: on the maintainer's machine `-v 17` resolves to JDK 26 and delombok then
  fails in a module you never touched. `bin/lib/chaos-experiment-common.sh` pins an explicit SDKMAN
  path for exactly this reason and says so in its header.
- **They call system `mvn`, not `./mvnw`.** `AGENTS.md`, "Build Requirements": *Maven via wrapper -
  do not use system Maven.*
- **They are macOS-only and fail silently elsewhere.** `/usr/libexec/java_home` does not exist on
  Linux, so `JAVA_HOME` is exported empty rather than the script stopping - the
  degrade-loudly rule in [`bin/AGENTS.md`](../../bin/AGENTS.md) is written against precisely this shape.
- **No `set -euo pipefail`**, so the failed `java_home` does not stop the maven run either.

**A fourth defect, found by astubbs/parallel-consumer#200 and NOT by this note, applies only to the
deleted sibling** - worth recording because it is the sharper finding of the two. That script ran a
bare `mvn -pl parallel-consumer-core` with no `-am`, which cannot succeed against this tree at all:
the enforcer's `ReactorModuleConvergence` rule fails it with "Module parents have been found which
could not be found in the reactor". That was measured there rather than assumed, and independently
reproduced here on 2026-09-01 - the same invocation shape, without `-am`, failed exactly that way
while checking a different change. So the module-scoped script was not merely mis-pinned; it could
never have run. `bin/build-without-tests.sh` builds the whole reactor and does not have that
particular fault, which is precisely why it needs its own decision rather than inheriting one.

## Why they are still here

It is a **pre-fork upstream file**, introduced by `2aa4da956` ("major: Batching feature and Event
system improvements") and still carrying `Copyright (C) 2020-2022 Confluent, Inc.` JDK 13 was
plausible when it was written. Nothing has run it since, and nothing told anyone it had
stopped being true - which is the whole of the problem: a build script that pins a wrong JDK is worse
than an absent one, because someone eventually runs it and debugs the fallout somewhere else
entirely.

Found on 2026-09-01 by an audit asking which scripts in `bin/` are referenced from no doc, no
workflow and no other script. The two `*-without-tests.sh` scripts were the only non-self-test
answers - everything else on that list is a self-test, which `bin/check-all.sh --with-tests` globs
and therefore finds by construction.

**Two independent routes reached the same pair on the same day**, from opposite directions: a
discoverability audit here, and a defect-class sweep on astubbs/parallel-consumer#200 looking for
other instances of a bare `-pl`. That is worth noting rather than tidying away - it is the argument
for doing the sweep, and it means the survivor is not an oversight by either party but a file whose
fate simply has not been decided.

## What already replaces them

`bin/build.sh` is the documented local build, takes extra maven arguments, and does the same job
correctly:

```
bin/build.sh -Dmaven.test.skip=true                            # the whole tree
bin/build.sh -pl parallel-consumer-core -Dmaven.test.skip=true # one module
```

## The decision, which is the maintainer's

**Deleting it is the honest option** and needs no replacement, since `bin/build.sh` already covers
it - and astubbs/parallel-consumer#200 has already made that call for the sibling, which is a
precedent rather than a decision here. Two things to weigh first, neither blocking:

- It is an upstream file with a Confluent header, so removal is a deliberate fork divergence rather
  than a tidy-up. [`docs/copyright.md`](../copyright.md) owns what that means; nothing else in the
  tree depends on them, so the cost is only that the fork stops carrying them.
- If it is kept for provenance rather than use, the JDK pin needs correcting and `mvn` swapping for
  `./mvnw` in the same change - a kept script that cannot work is the state this note exists to end,
  and keeping it unfixed just moves the note.

**Ordering is no longer a consideration:** astubbs/parallel-consumer#200 has taken the sibling, so
this note now covers a single file and whichever branch answers the question can simply delete it.
The conflict this paragraph used to warn about was between that PR and a second branch, and there is
no longer a second edit to collide with.

Parked deliberately - the tag above carries the same disposition, so the index agrees with this
paragraph: the finding is cheap and certain, the decision is not urgent, and it was not the
subject of the audit that turned it up.
