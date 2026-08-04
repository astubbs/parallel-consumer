# Disabled CI jobs, and highcpu runner load

- **`Kafka Compat (experimental 4.x)` is disabled** (`if: false` in `maven.yml`) - it cannot compile
  under kafka-clients 4.x until the 0.7.x migration. Re-enable with
  `if: github.event_name == 'pull_request'` when that work starts (see `pr-53-java-baseline-kafka4.md`).
- **The `local` self-hosted PR jobs are disabled** (`pr-local-fast-feedback.yml`, `pull_request`
  trigger commented out). That runner is offline indefinitely and its suites now run on the highcpu
  runner, a strict superset. `workflow_dispatch` still works; restore the trigger if the box returns.
- **The highcpu lane's load is much lower since #111**, which cut it from six suites per branch to
  two (Performance, Chaos). Both mutation entries moved off-box - one PR-scoped lane now runs on the
  GitHub-hosted gate, the full sweep is dispatch-only - and Unit/Integration were removed as
  duplicates of the hosted gate that were measured as no faster. Jobs had been dying of
  runner-lost-communication (3+ times on #80 alone) and making chaos timing SLOs noisy; **re-check
  whether that still happens** before spending anything on a shared concurrency group. See
  `ci-mutation-testing.md`.
