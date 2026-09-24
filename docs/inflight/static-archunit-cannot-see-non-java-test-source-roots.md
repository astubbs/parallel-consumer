# `EveryModuleWiresUpArchUnitTest` only looks at `src/test/java`, and two modules now use other roots

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

`EveryModuleWiresUpArchUnitTest` walks the tree for directories ending `src/test/java`, and fails
the build for any that has `.java` sources but no wired `TestConventionsArchTest`. Its own javadoc
says why that matters: *"a module without one was not reported as unprotected - it was reported as
nothing at all, which reads exactly like passing."*

**Its scope has the same shape as the hole it closes.** The path filter is
`Paths.get("src", "test", "java")` exactly, so a module whose tests live anywhere else is invisible
to it - not reported as unprotected, reported as nothing. Until now no module had another root, so
the filter and "every module" meant the same thing.

`parallel-consumer-proxy-client-kotlin` (`src/test/kotlin`) and
`parallel-consumer-proxy-client-scala` (`src/test/scala`) are the first that do. Both have test
sources; neither wires ArchUnit; nothing goes red. The Java client module beside them does wire it,
which is what makes the asymmetry easy to miss - the tree looks covered.

**Whether they SHOULD be covered is the open question, and it is not obvious.** ArchUnit reads
bytecode, so pointing it at a Kotlin or Scala module works in principle - a `TestConventionsArchTest`
in `src/test/java` of those modules would analyse their compiled test classes. But the shared
`TestConventionRules` were written against Java test sources, and at least two of them read on
compiled non-Java classes in ways nobody has checked: the surefire-collectable naming rule against
Kotlin's backtick-quoted method names, and the one-character-method-name rule against whatever
synthetic members the Scala and Kotlin compilers emit. Wiring first and discovering that afterwards
would produce the failure mode this project dislikes most - a rule that fires for a reason unrelated
to what it is checking.

**So there are two candidate fixes and they are not the same change:**

1. Widen the walk to `src/test/<any>` and let the existing message name the module. Cheap, and it
   converts the blind spot into a red build immediately - but it fails the two modules until (2) is
   answered.
2. Decide what the shared rules mean for non-JVM-source-language test classes, then wire the two
   modules.

Whoever takes this should do (2) first and (1) with it, so the widened check lands green.
