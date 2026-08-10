# `-Dtest=` silently skips the upstream Kafka suite, and the run still goes green

**A measurement hazard that produces a false pass.** Found 2026-08-11, after it cost an agent a full run.

`parallel-consumer-streams` runs Apache Kafka's own test classes against the patched classes in a dedicated
surefire execution with its own `<includes>`. That suite is the module's behaviour-preservation evidence -
the "Kafka's own tests still pass with the seam off" claim rests entirely on it.

**Passing `-Dtest=...` on the command line silently overrides that execution's `<includes>`, so the
upstream suite does not run at all.** The build succeeds, the summary looks healthy, and the number you
were trying to check was never computed. Nothing warns you.

This is the same shape as the other silent-pass defects this repo keeps finding: a green result that
certifies nothing, and is indistinguishable from a real one.

Two practical consequences:

- **Never read a scoped `-Dtest=` run as evidence about the upstream suite.** If you are checking a claim
  about Kafka's own tests, run the module's full `test` phase and confirm the upstream counts appear in
  the output.
- **State how you confirmed the suite executed**, not just that the build passed. The counts are the
  proof; their absence is invisible.

Related trap in the same area: the project's own `javadoc:javadoc` goal never sees the patched classes at
all, because the parent pom pins `sourcepath` to the delombok output. It returns BUILD SUCCESS having
documented only `io.confluent.*`. Reporting that as a javadoc check on patched code is a false negative of
the same family.

## Delete when

Either the pom makes a scoped override impossible or loud, or this hazard is documented where someone
running a scoped build will meet it (the module README's build section, or the surefire execution's own
comment).
