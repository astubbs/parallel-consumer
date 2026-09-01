# Two pre-fork build scripts pin JDK 13 and call system maven

<!-- inflight-type: bug -->
<!-- inflight-impact: config-lie -->

`bin/build-without-tests.sh` and `bin/build-parallel-consumer-core-without-tests.sh` are seven lines
each, and between them break three of this project's stated build rules. Nothing runs them, nothing
references them, and nothing goes red.

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

## Why they are still here

They are **pre-fork upstream files**, introduced by `2aa4da956` ("major: Batching feature and Event
system improvements") and still carrying `Copyright (C) 2020-2022 Confluent, Inc.` JDK 13 was
plausible when they were written. Nothing has run them since, and nothing told anyone they had
stopped being true - which is the whole of the problem: a build script that pins a wrong JDK is worse
than an absent one, because someone eventually runs it and debugs the fallout somewhere else
entirely.

Found on 2026-09-01 by an audit asking which scripts in `bin/` are referenced from no doc, no
workflow and no other script. These two were the only non-self-test answers.

## What already replaces them

`bin/build.sh` is the documented local build, takes extra maven arguments, and does the same job
correctly:

```
bin/build.sh -Dmaven.test.skip=true                            # the whole tree
bin/build.sh -pl parallel-consumer-core -Dmaven.test.skip=true # one module
```

## The decision, which is the maintainer's

**Deleting them is the honest option** and needs no replacement, since `bin/build.sh` already covers
both. Two things to weigh first, neither blocking:

- They are upstream files with Confluent headers, so removal is a deliberate fork divergence rather
  than a tidy-up. [`docs/copyright.md`](../copyright.md) owns what that means; nothing else in the
  tree depends on them, so the cost is only that the fork stops carrying them.
- If they are kept for provenance rather than use, they need the JDK pin corrected and `mvn` swapped
  for `./mvnw` in the same change - a kept script that cannot work is the state this note exists to
  end, and keeping them unfixed just moves the note.

Parked deliberately: the finding is cheap and certain, the decision is not urgent, and it was not
the subject of the audit that turned it up.
