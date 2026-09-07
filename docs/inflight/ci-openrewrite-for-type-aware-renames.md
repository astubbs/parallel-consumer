# Renames are done by regex, and nothing in the toolchain knows Java

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

Every rename in this repo is a `sed`/`grep` sweep whose only check is whether the build still
compiles. `bin/rename-packages.sh` is the biggest instance, and its own header records the class of
failure that follows: it silently ignored every freeze region on macOS, and the sweep for the old
package has to be spelled `grep -rnE 'io[\./]*conflu'` rather than `grep -rn "io\.confluent"`
because three files encode the package as an escaped regex and one misspells it. A text tool cannot
know which `count` is the field and which is an unrelated local.

**OpenRewrite is the tool that would.** `rewrite-maven-plugin` is not in any pom here. It runs
recipes against the compiled LST, so `ChangeMethodName`, `ChangeFieldName` and `ChangeType` are
type-aware: they rename a declaration and exactly its references, across modules, and cannot touch a
same-named symbol on another type.

```
./mvnw org.openrewrite.maven:rewrite-maven-plugin:run -Drewrite.activeRecipes=<composite>
```

**What it would actually buy, stated honestly, because the obvious pitch is wrong.** It does not
make renaming cheap. Measured against the two rename passes on
astubbs/parallel-consumer#373 - eleven identifier renames from a review - the identifier half was
roughly a third of the work. The rest was javadoc prose explaining the concept, test *method* names
describing behaviour, two notes in this directory, and one caveat paragraph that had to be rewritten
rather than renamed. OpenRewrite does none of that. **Adopt it for correctness, not for speed:** the
win is that a wide rename stops being verified by "it still compiles".

**The case that justifies it** is `bin/rename-packages.sh`, not review-sized renames. That script
already has to assert both the renames git recorded *and* their pairing, because a bare R-count
reads a mis-paired rename as healthy - a check that exists only because the renamer cannot be
trusted. Note also that a clean merge is not evidence there: merging renamed master into an
un-renamed branch reported zero conflicts and silently applied one module's ArchUnit edit into
another module's file.

**Before adopting, settle these.** Whether the plugin's own build cycle is tolerable on a reactor
that cross-compiles Java 17 source to Java 8 bytecode via Jabel; whether recipes can be checked in
and reviewed like the shell gates are, or become a second undocumented layer; and whether it can be
run without becoming a required lane, since a rename tool nobody invokes is worse than a script
everybody does.

**Not a general refactoring CLI.** IntelliJ exposes no headless refactoring, and Eclipse JDT's
scripted refactoring is old and awkward, so OpenRewrite is effectively the only maintained option -
and it is a Maven plugin rather than a `refactor rename` binary, which is why it does not feel like
the tool to reach for.
