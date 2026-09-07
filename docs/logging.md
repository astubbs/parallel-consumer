# Logging: the test profiles, the siloed streams, and how to prove which config loaded

Owns the *why* behind the logback configuration in the test harnesses. `AGENTS.md` routes here.
Read it before changing a logback file or wondering why a logging change had no effect.

## Two profiles, explicitly selected - not two files racing

`parallel-consumer-core` has two test logging profiles:

| File | Used by | Selected |
|---|---|---|
| `src/test/resources/logback-test.xml` | the unit lane, and anything that does not ask otherwise | default |
| `src/test-integration/resources/logback-integration-test.xml` | integration and chaos runs wanting the louder profile | `-Dlogback.configurationFile=logback-integration-test.xml` |

**The two used to share a name - on the branch this came from, not on master.** Both copied to
`target/test-classes/logback-test.xml`, so which one applied was decided by build order and file
timestamp: the integration suite's logging was effectively nondeterministic and nothing said so. A
distinct name plus an explicit switch replaces a coin flip with a decision.
<!-- file-refs: N/A - a build-output path, and the collision at it is the point -->

**Where a reader can and cannot see that collision.** It existed on the
astubbs/parallel-consumer#29 tree, which is where the integration profile was first added. Against
master there was nothing to collide with - `src/test-integration/resources` does not exist there,
and no deletion of an integration `logback-test.xml` appears anywhere in the history. So do not go
looking for the collision in the log; the design is the same either way, and this paragraph is the
only record of which tree it happened on.
<!-- file-refs: N/A - names a path that exists on another branch and deliberately not on master; that
     absence is the correction being recorded -->

## Prove which config loaded - do not assume

    -Dlogback.statusListenerClass=ch.qos.logback.core.status.OnConsoleStatusListener

Logback then prints the URL of the file it actually read, at startup.

**Reach for this whenever a logging change appears to have had no effect.** The usual cause is that
the file was never loaded, and that failure is completely silent - the run looks normal and simply
does not contain what you added. That is the same shape as every other silent-no-op this repo has
paid for: a gate that checked nothing and exited 0, a mutation lane that scored nothing and passed, a
probe whose window never opened. Instrumentation that did not reach the run produces a confident
wrong answer, and the only defence is checking rather than believing.

## Levels are property-driven, and stay that way

    ./mvnw test -Dpc.log.level=debug -Dtest=TheOneTest

`bin/check-test-log-config.sh` guards this across the library modules. A committed `debug` default
does not go red - it floods the log, slows the run, and the volume alone breaks tests: measured at
warn, one suite emits hundreds of lines and passes; at debug it emits hundreds of thousands and three
tests fail on a latch. Nobody attributes that to a logging default, which is why it is a gate.

**So do not attach a level to a logger just to route it.** The siloed appenders below deliberately
carry no level, so they inherit and the property still governs.

## Siloed streams: copies, not diversions

Under `target/pc-logs/fork-<N>/` (`-Dpc.log.dir` moves the root). Every routed logger keeps its
default additivity, so the console stream is unchanged and complete - these are additional copies.

**One directory per surefire fork, because seven plain `FileAppender`s cannot share a path.** The
`ci` profile runs surefire at `forkCount=1C` with `reuseForks`, so without this there is one JVM per
core holding a handle on each of the same seven files - and a non-prudent `FileAppender` is not safe
for that. The two streams that promise *every* error and *every* warning are exactly the ones that
would lose writes. The core pom's surefire `argLine` appends
`-Dpc.log.dir=${pc.log.dir}/fork-${surefire.forkNumber}`; read across them with
`cat target/pc-logs/fork-*/errors.log`. `<prudent>` is the alternative and was not taken: it buys a
single shared path at the price of file-locked, unbuffered writes on all seven appenders on every
line.

**Failsafe deliberately does not get this.** It declares no `forkCount`, so it runs at the default
of one fork, and the `bin/` experiment runners read a flat `<their -Dpc.log.dir>/probes.log` that a
fork subdirectory would break.

**`-Dpc.log.dir` silently did nothing until 2026-09-01, and the shape of the bug is worth keeping.**
The config declared `<property name="pc.log.dir" value="${pc.log.dir:-target/pc-logs}"/>` - a
definition that refers to itself. Logback resolves that self-reference to the default rather than to
the system property, so every run wrote to `target/pc-logs` no matter what was passed, and the
scripts that pass a per-iteration directory were all appending into one shared set of files. It was
proved by putting `-Dpc.log.dir` on the fork command line, watching the files land in the default
directory anyway, then renaming the local property and watching them move. **Keep the local name
(`siloDir`) different from the system property it reads.** Nothing goes red if they converge again:
the appenders still work, they just ignore you.

| Stream | Holds |
|---|---|
| `errors.log` | every ERROR for the whole run |
| `warnings.log` | every WARN for the whole run |
| `probes.log` | detectors and probes - the instruments, not the product |
| `harness.log` | what the test did TO the system |
| `pc-commit-offsets.log` | committer, producer, offset encoding |
| `pc-poll-rebalance-lifecycle.log` | poll system, consumer manager, processor lifecycle |
| `pc-shard-work-state.log` | shard and work state |

**The rule for what earns a stream: anything tangential to running the product.** Probes and the
harness exist to observe, not to be the system, and in analysis they are either the only thing you
want or the only thing in the way. Split them and both problems go away.

**Errors and warnings are never truncated, deliberately.** When a ring buffer is added for the
verbose stream in a long soak, these two stay whole. A ring buffer is the right answer to volume and
the wrong answer to a warning from two days ago - which is exactly the line wanted when something
fails on day three.

**Classify errors, so an unexpected one is a finding.** An error the system expected - a retriable
failure, an induced fault - is the harness working. The signal is the error nobody forced, and it is
invisible while both share a stream. `PCRetriableException.isPresentIn` already draws that line on
the processing path; the close and revoke paths do not ask. In a long run, an unforced error is a bug
until shown otherwise.

## Adding a stream

Start from a question analysis actually asked and could not answer cheaply. The current set came from
one week's work: "did the detector fire" required several wrong greps of a very large run log, and
the finding that a detector stayed *silent* on a third of failures was noticed by accident rather
than by looking.

- Add the appender, route the logger, **set no level**.
- Keep additivity default so the master stream stays complete.
- Add a row to the table above.

Guessing a taxonomy up front produces files nobody opens. Let the set grow from what got in the way.
