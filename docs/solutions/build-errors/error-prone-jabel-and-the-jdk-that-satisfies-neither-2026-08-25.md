---
title: "Error Prone, Jabel, and the JDK that satisfies neither"
date: 2026-08-25
category: build-errors
module: build-system
problem_type: build_error
component: development_workflow
severity: medium
symptoms:
  - "`UnsupportedClassVersionError: com/google/errorprone/ErrorProneJavacPlugin has been compiled by a more recent version of the Java Runtime (class file version 65.0)` on JDK 17"
  - "The same build on JDK 21 dies instead with `ServiceConfigurationError: com.sun.source.util.Plugin: Provider com.github.bsideup.jabel.JabelCompilerPlugin could not be instantiated`"
  - "`Java 21 (65) is not supported by the current version of Byte Buddy which officially supports Java 20 (64)`"
  - "Adding error_prone_core to annotationProcessorPaths breaks compilation even with no -Xplugin:ErrorProne anywhere"
  - "`NoClassDefFoundError: com/google/errorprone/predicates/type/DescendantOf` from NullAway, at checker construction"
  - "Error Prone crashes in StringConcatToTextBlock with a NoSuchElementException on a class that only uses Lombok's @NonNull"
  - "maven-compiler-plugin reports `COMPILATION ERROR` followed directly by a stack frame, with the exception message missing"
root_cause: dependency_conflict
resolution_type: config_change
tags:
  - error-prone
  - nullaway
  - jabel
  - lombok
  - jdk17
  - javac-plugin
  - static-analysis
  - class-file-version
---

# Error Prone, Jabel, and the JDK that satisfies neither

## Problem

Adding `com.google.errorprone:error_prone_core` at its current release to a build that compiles Java
17 source to a Java 8 target through Jabel fails, and the failure moves when you try to fix it. Three
blockers are involved, they are independent, and two of them are mutually exclusive - so the obvious
remedy for the first makes the second fire.

Everything below was measured, each with a control arm that changes one term.

## Blocker 1: Error Prone dropped JDK 17, at a nameable version

`ErrorProneJavacPlugin` is compiled to class file version **65** (Java 21) from **2.43.0** onwards.
2.42.0 is **61**. JDK 17 reads to 61, so the plugin class cannot be defined at all and javac dies
before analysing anything.

The jar states its own requirement, which is quicker than bisecting: `Build-Jdk-Spec: 21` and
`Require-Capability: osgi.ee;filter:="(&(osgi.ee=JavaSE)(version=21))"` in the manifest. Reading the
class file's version bytes across releases is the mechanical way to find the boundary, and it takes
seconds per release.

## Blocker 2: raising the JDK is not available, because Jabel refuses 21

Jabel 1.0.0 bundles Byte Buddy 1.12.18, which rejects class file 65 outright:
*"Java 21 (65) is not supported by the current version of Byte Buddy which officially supports Java
20 (64) - update Byte Buddy or set net.bytebuddy.experimental as a VM property"*.

**Control**: the same plain compile on JDK 21 with an Error-Prone-free processor path fails
identically, so this is Jabel alone and not an interaction with Error Prone. The two constraints have
no overlapping JDK.

## Blocker 3: NullAway is version-coupled to Error Prone, and nothing says so

NullAway 0.12.7 dies at checker construction against Error Prone 2.50.0 with
`NoClassDefFoundError: com/google/errorprone/predicates/type/DescendantOf`; that release ships
`com.google.errorprone.predicates.TypePredicate(s)` and no `predicates.type` sub-package at all. It
works against 2.42.0.

**The constraint is invisible to every dependency tool**, because NullAway takes Error Prone as
`compileOnly` and its published pom declares only jspecify, dataflow-nullaway and guava. An enforcer
`requireUpperBoundDeps` rule cannot see a dependency that is not declared, so this class of mismatch
will always surface at runtime. Move the two versions together, deliberately.

## Lombok is a fourth blocker, but only at the newer Error Prone

**Two-arm control**, same JDK, same flags, same processor path, one term changed:

- `public C(@lombok.NonNull String s) { ... }` - exit 4. `StringConcatToTextBlock` throws
  `NoSuchElementException` out of `Iterables.getLast`, walking the tokens of a string literal Lombok
  synthesised with no source position, inside the generated
  `throw new NullPointerException("s is marked non-null but is null")`.
- The same class with that null check written by hand - exit 0.

It does not reproduce at 2.42.0. Worth knowing before blaming the annotation processor ordering that
Lombok and Error Prone are famous for: this is not an ordering problem, it is one check mishandling a
synthetic node.

## The trap that costs the most time: the jar alone arms the plugin

Putting `error_prone_core` on `annotationProcessorPaths` breaks compilation **with the plugin
switched off**. `BasicJavacTask.initPlugins` enumerates every `com.sun.source.util.Plugin` service
provider on the processor path and loads each class *before* looking at which plugin was requested.

**Control**: identical javac invocation both sides - Error Prone on the processor path and not
enabled gives `UnsupportedClassVersionError`; removed from the processor path, exit 0.

So "add the dependency now, enable it in a later PR" is not a route, and a build that has never
written `-Xplugin:ErrorProne` can still be broken by the dependency.

## Diagnosing any of this: maven hides the message

`maven-compiler-plugin` prints `COMPILATION ERROR` followed directly by a stack frame, with the
exception's message line missing - it does not match the plugin's `file:line: error:` parser, so it is
dropped. Every failure above is unreadable through Maven and obvious the moment you run the same
`javac` by hand.

Reproduce the compiler invocation directly: `dependency:build-classpath` for the compile classpath, a
throwaway pom whose dependencies are the processor path, then `javac` with the same flags. It also
gives you somewhere to change one term at a time, which is what all the controls above needed.

## Solution

Pin Error Prone to the last release on the near side of the class-file boundary, and pin NullAway with
it. The pin is temporary: it lifts when Jabel does, and Jabel's removal is already tracked with the
Java baseline work it belongs to. What is pinned, what is switched off, and what turns each back on
live in `docs/inflight/static-error-prone-rule-registry.md`.

Configuration that turned out to be load-bearing rather than optional:

- `<fork>true</fork>` plus `-J--add-exports`/`-J--add-opens` for eight and two `jdk.compiler`
  packages. They must reach the **compiler's** JVM, which is what `-J` and forking are for. Omit them
  and the plugin dies with `IllegalAccessError` on `BasicJavacTask` rather than anything that names
  the cause.
- `-XDcompilePolicy=simple` and `--should-stop=ifError=FLOW`.
- **NullAway needs `-XepOpt:NullAway:AnnotatedPackages=<pkg>` or `-XepOpt:NullAway:OnlyNullMarked`.**
  With neither it does not degrade to silence; it crashes the compiler while constructing the checker.
- `-XepExcludedPaths:` takes a **colon**, not an equals sign. The equals form is rejected as
  `invalid flag`, through the same swallowed-message path above.
- `-Xmaxwarns`. javac caps output at 100 warnings per compilation and says nothing when it truncates.

## Prevention

- **Read a javac plugin's class file version before its changelog.** It is one field, it is
  authoritative, and it answers "will this run on our JDK" without reading a release note that may not
  mention the bump.
- **When a build tool swallows a message, go under it.** A stack trace whose first line is a frame is
  a message that was filtered, not a message that does not exist.
- **A dependency taken as `compileOnly` by a plugin you consume is a version constraint nobody
  enforces.** Pin both ends and move them together.
