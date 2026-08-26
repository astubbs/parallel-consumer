# The hosted integration lane can die on a broker container exit 126, and cannot diagnose itself

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

<!-- post-merge: checked-begin -->
The GitHub-hosted `Integration Tests` gate goes red with the whole suite erroring, for a reason that
is **not the branch** and that the log does not contain enough evidence to explain. First recorded
from two occurrences on astubbs#347 (2026-08-25), on two different heads of that branch.
<!-- post-merge: checked-end -->

## The signature

`BrokerIntegrationTest`'s static initializer starts the shared `confluentinc/cp-kafka` container. When
that container dies, `<clinit>` throws and **every** subclass errors instantly:

```
Caused by: java.lang.IllegalStateException: Wait strategy failed. Container exited with code 126
Caused by: ContainerLaunchException: Timed out waiting for log output matching
           '.*\[KafkaServer id=\d+\] started.*'
```

The shape is what identifies it, and it is easy to misread:

- **one** class fails slowly, at the container start timeout (~88s), and
- **~16 more** fail in 0.001-0.011s each, because they never got a broker.

A wall of red test classes therefore means *one* failure, not sixteen. Do not start diagnosing the
sixteen. Exit **126** is "command cannot execute", so the broker's entrypoint never ran - a
Docker/runner-level fault, not a Kafka configuration or product problem, and the wait-strategy
timeout is a consequence of it rather than a second cause.

## Why the branch is ruled out, and how to rule it out again

<!-- post-merge: checked-begin -->
The cheap control is the branch's own earlier heads. On astubbs#347 the gate **passed** on two
successive heads and then **failed** on the third, across a delta of five markdown files plus one
shell script no workflow invokes - zero Java, zero pom, zero workflow. (Those heads are named in that
PR's own commit range; they are deliberately not quoted here, since a squash merge would leave the
SHAs unresolvable while the reasoning stays valid.) Run
`git diff --name-only <passing-head>..<failing-head>` before anything else; if nothing executable
changed, this is the environment.
<!-- post-merge: checked-end -->

**A re-run is the correct response here, and it is not a retry masking a flake.** Nothing about the
test changed and no assertion is being weakened - the container never started, so the suite never
ran. That is distinct from the no-retry rule, which is about tests that ran and failed
(`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`). Re-running a
lane that measured nothing is closer to re-dialling a dropped call. Do **not** quarantine anything:
there is no test to quarantine, and the ambient probe has nothing to say because no broker existed
for it to probe.

## The actual open item: the lane cannot explain its own infra failures

The container's stdout is nowhere in the job log - only Testcontainers' outside view of it. So "exit
126" is the end of the evidence, and each occurrence is rediagnosed from scratch and then forgotten,
which is how this reached a second sighting with nothing written down.

Worth doing, in rough order of value:

1. **Capture the container's logs on a startup failure.** Testcontainers can attach a log consumer,
   or the lane can `docker logs` the failed container in an `if: failure()` step. Without this, the
   next occurrence is exactly as undiagnosable as this one.
2. **Decide whether the shared-container-in-`<clinit>` shape is worth keeping.** It converts one infra
   fault into 16 red classes and an `ExceptionInInitializerError` whose stack trace names no cause.
   A failure surfaced once, with the container's own output, would be strictly more informative.
3. Only then ask whether exit 126 has a fixable local cause (image pull, runner disk, nested
   virtualisation). It is not worth guessing at before step 1 exists.
