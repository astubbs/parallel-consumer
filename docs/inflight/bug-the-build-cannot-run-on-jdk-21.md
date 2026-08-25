# Bug: the build does not run on any JDK newer than 17, and three separate things stop it

<!-- inflight-type: bug -->
<!-- inflight-impact: build -->
<!-- inflight-labels: needs-measurement -->

Measured 2026-08-22 on Temurin 21.0.9 while building the virtual-threads lane. `./mvnw -pl
parallel-consumer-core -am -DskipTests compile` fails, and fixing each blocker reveals the next.

**Nothing here blocks virtual threads**, because that lane builds on JDK 17 and only *runs* the tests
on 21 - see below. It matters for [`pr-53-java-baseline-kafka4.md`](pr-53-java-baseline-kafka4.md) and
for astubbs#181 ("PC cannot run on Java 24"), both of which assume the build can move.

## The three blockers, in the order they surface

**1. `lombok-maven-plugin` 1.18.20.0 cannot delombok on JDK 21.**

```
Failed to execute goal org.projectlombok:lombok-maven-plugin:1.18.20.0:delombok:
  NoSuchFieldError: Class com.sun.tools.javac.tree.JCTree$JCImport
                    does not have member field 'com.sun.tools.javac.tree.JCTree qualid'
```

The plugin embeds lombok 1.18.20; the project's own lombok is `${lombok.version}` 1.18.46, which is
fine on 21. The plugin's release train stopped, so the fix is pinning its dependency rather than
bumping it. Delombok exists only to feed javadoc, so it is also skippable per-invocation with
`-Dlombok.delombok.skip=true`.

**2. `maven-compiler-plugin` 3.15.0 reports javac's obsolete-target WARNING as a compilation ERROR.**

```
[ERROR] source value 8 is obsolete and will be removed in a future release
[ERROR] target value 8 is obsolete and will be removed in a future release
```

Raw `javac -source 8 -target 8` on the same JDK emits these as **warnings** and exits 0 - checked
directly. `-Dmaven.compiler.failOnWarning=false` is already the default (confirmed in the plugin's own
debug dump) and does not change it, so the promotion happens above javac. `-Xlint:-options` in
`<compilerArgs>` clears it, which is javac's own suggestion quoted in the message.

**3. Jabel 1.0.0 does not work on JDK 21 at all, and this one is not a configuration fix.**

Jabel is what lets this project write Java 17 source and emit Java 8 bytecode. It rewrites javac's
internals with Byte Buddy, and:

- its own Byte Buddy (1.12.18, imported via `byte-buddy-parent`) refuses to read a JDK 21 classfile -
  *"Java 21 (65) is not supported by the current version of Byte Buddy which officially supports Java
  20 (64)"* - thrown from `JabelCompilerPlugin.<clinit>`, which maven surfaces only as **"An unknown
  compilation problem occurred"**;
- pinning the processor path to `${byte-buddy.version}` (1.17.7, already a property here for the
  *identical* defect one layer over - mockito needed `JAVA_V21`) gets past that, and then javac dies
  in its own parser:

```
An exception has occurred in the compiler (21.0.9).
java.lang.NoSuchFieldError: Class com.sun.tools.javac.code.Source$Feature
                            does not have member field 'com.sun.tools.javac.code.Source$Feature LAMBDA'
	at com.sun.tools.javac.parser.JavacParser.checkSourceLevel
```

Jabel's rewrite removes an enum constant javac's parser then asks for. **There is no flag for this.**
It needs a Jabel version that supports 21, Jabel removed (which is astubbs#53's option 2), or the
baseline moved off 8.

## Why the virtual-threads lane does not need any of this

Virtual threads are a **runtime** capability, and Parallel Consumer reaches the JDK 21 API
reflectively precisely so the module still compiles to Java 8 bytecode. Nothing has to compile on
21 - and given blocker 3, nothing can.

Surefire forks a separate JVM for tests, and the pom already parameterises it as `${jvm.location}`.
So the lane **builds on 17 and runs the tests on 21**:

```
./mvnw -pl parallel-consumer-core -am test -Djvm.location=<jdk21-home> -Dpc.virtualThreads=true
```

That is a better lane than a JDK 21 build would have been: it exercises the **shipped Java 8
bytecode** on a JDK 21 runtime, which is exactly what a user with the option enabled does, rather than
a differently-compiled artifact nobody ships.

## What to do

1. **Nothing urgently.** No shipped artifact and no CI lane is blocked today.
2. **Blockers 1 and 2 are one-line fixes** and are worth taking whenever something else touches the
   pom - they are latent, and each cost an hour to identify from its symptom.
3. **Blocker 3 belongs to astubbs#53.** It is a hard constraint on that decision, not a detail: "keep
   Jabel and move `--release` to 11" only works while the *build* JDK stays at or below 20. Jabel is
   the reason the build JDK cannot move, and that was not previously written down anywhere.
4. **Do not switch the machine's global JDK to test this.** `sdk install` claims the default; several
   agents and the user share one machine, and a global 21 makes blocker 1 fire in unrelated worktrees
   with an error that looks nothing like "someone changed my JDK". Set `JAVA_HOME` per invocation.
