# The hosted integration lane can die on a broker container exit 126, and cannot diagnose itself

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-09 - re-read against the tree and against a third occurrence, recorded below. Still true: the shared container starts in an unguarded static initialiser calling kafkaContainer.start(), followKafkaLogs() is @BeforeAll and DEBUG-gated so it cannot run when the class initialiser throws, and no docker logs or if: failure() capture step exists in maven.yml. PROPOSED partly-true, for the owner, on items 1 and 3: the premise under item 1 - "the container's stdout is nowhere in the job log" - is false, and was false of the astubbs#347 run this note was written from. Testcontainers prints the failed container's own output itself at GenericContainer#tryStart, one line below the Wait strategy failed line the signature block quotes, and both the 2026-09-09 job and the refetched 2026-08-25 job 97822772353 carry sh: /tmp/testcontainers_start.sh: Text file busy. So item 3 has its answer - ETXTBSY on the starter script, a Testcontainers start race, not a local cause worth hunting - and item 1 is smaller than stated: what is missing is not capture but the broker's own stdout AFTER it starts executing, which this class of failure never reaches. Item 2, the shared-container-in-clinit shape, is untouched and is what still turns one fault into a wall of red. Owner-gated because the impact is misdirection -->

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
sixteen.

**The cause line sits one line ABOVE the wait-strategy timeout in the job log, and is easy to scroll
past**: `sh: /tmp/testcontainers_start.sh: Text file busy`. The start script Testcontainers copies in
is still held open for writing when the container execs it, so the entrypoint never runs - which is
what exit 126 ("command cannot execute") is reporting. Grep the log for `Text file busy` before
anything else; finding it settles the diagnosis without reading a stack trace.

<!-- post-merge: checked-begin -->
**Sightings.** astubbs#347, two heads, 2026-08-25 (the first record, above). astubbs#496,
2026-09-09, GitHub-hosted `Integration Tests`, job `102314718122`: `Text file busy` then exit 126 at
`BrokerIntegrationTest.<clinit>`, one class failing slowly and the rest instantly. The control arm
held - that lane had passed on the PR's previous head, and the delta was one merge of master whose
only executable files were unrelated to the broker or the lane. Re-run of the failed job only.
Add a line here rather than a count: what matters is which PRs and which heads, not how many.
<!-- post-merge: checked-end --> Exit **126** is "command cannot execute", so the broker's entrypoint never ran - a
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

## Third sighting, 2026-09-09 - and the container DID say why, in this log and in the first one

<!-- post-merge: checked-begin - a dated sighting against a PR number and two job ids, all durable -->
`Integration Tests` on astubbs/parallel-consumer#446 at `dfff1d69d`,
[job 102306213003](https://github.com/astubbs/parallel-consumer/actions/runs/34300446475/job/102306213003).
The signature above, exactly: `TransactionalVisibilityIT` fell first and slowly (80.89s,
`ExceptionInInitializerError`), and every other broker class fell instantly with
`NoClassDefFoundError: Could not initialize class ...BrokerIntegrationTest`. Codecov's PR comment
renders that as **"20 Tests Failed"**, which is the misreading this note exists to pre-empt: it is
one failure. The branch is the cleanest control this note has had - its entire delta from master is
two markdown files under `docs/inflight/`, and the same lane had passed on an earlier head of the
same branch two days before.
<!-- post-merge: checked-end -->

**The container's own output was in the job log, and it names the cause.** One line below the
`Wait strategy failed` line this note quotes, Testcontainers prints it itself:

```
ERROR [main] (GenericContainer.java:549)#tryStart Log output from the failed container:
sh: /tmp/testcontainers_start.sh: Text file busy
```

`Text file busy` is `ETXTBSY` - exec refused because the file was still open for writing. The
`KafkaContainer` this suite builds runs a command that waits for its starter script to **exist** and
then executes it, so a script the daemon has created but not finished extracting is executable-shaped
and not yet executable. The shell reports that refusal as exit **126**, which is why the outside view
says "command cannot execute" and stops there. Nothing in the product, the image or the Kafka
configuration is involved, and a slower or busier runner widens the window.

**The 2026-08-25 log carries the same two lines**, refetched to check rather than assumed:
astubbs#347's job 97822772353 has `Log output from the failed container:` followed by the identical
`sh: /tmp/testcontainers_start.sh: Text file busy`. So this evidence was present in the very run this
note was written from. What failed was not the instrument but the reading - the triage stopped at the
line the note quotes and never looked at the next one.

**Rate, on the day.** Of the completed `Integration Tests` jobs between 00:52 and 01:45 UTC on
2026-09-09 - a busy hour with many branches in flight - this was the only failure; every other one
passed. That is a low-rate environment fault, not a lane that is broken.

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
