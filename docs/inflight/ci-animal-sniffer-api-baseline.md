# Animal Sniffer: nothing checks the APIs we CALL against the Java 8 floor

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->


**The gap.** This project compiles Java 17 source to Java 8 bytecode with Jabel, so a call to a
post-Java-8 JDK method compiles cleanly, ships, and fails with `NoSuchMethodError` on a Java 8
consumer. `--release` in the root `pom.xml` (`<release.target>8</release.target>`) is the only guard
today, and Jabel exists precisely to relax what that restricts.

`animal-sniffer-maven-plugin` checks exactly this: every call site in the bytecode against a
signature baseline for the target platform. It is the opposite direction from
astubbs#315 (`ci/api-compatibility-gate`), which asks whether *our* published API breaks *our*
consumers - these are complementary, not duplicates, and neither substitutes for the other.

**It was started once and dropped.** The pre-fork branch `origin/animal-sniffer` adds the plugin
bound to the `test` phase against the `org.codehaus.mojo.signature:java18` signature. Both commit
bodies are empty, so no reason was recorded - but the second commit adds a version and, in the
same hunk, mistypes the signature groupId to `org.codehaus.mosdfsdfjo.signature`, which cannot
resolve. The tip reads as abandoned mid-debug rather than as a considered rejection. Last touched
March 2022; the plugin appears zero times in today's poms, and nothing replaced it in that
direction.

**Why it reads as a cheap win.** The configuration is about twenty lines and already written -
`git show 0b4b0f642:pom.xml` has it. What has to be decided is what the 2022 attempt did not
settle: which phase it binds to (`test` is late - `process-classes` fails faster), whether the
modules with a different floor are excluded (the Mutiny module's real runtime floor is 17), and
whether it also gets a dependency-API baseline rather than only the JDK one.

Open rather than deferred: it is small, it is independent of every other in-flight branch, and the
exposure it covers is live on every release.

## Delete when

The plugin runs in the build and a violation fails it, or the idea is rejected with the reason
written down.
