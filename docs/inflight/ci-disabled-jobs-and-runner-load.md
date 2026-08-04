# Disabled CI jobs, and highcpu runner load

- **`Kafka Compat (experimental 4.x)` is disabled** (`if: false` in `maven.yml`) - it cannot compile
  under kafka-clients 4.x until the 0.7.x migration. Re-enable with
  `if: github.event_name == 'pull_request'` when that work starts (see `pr-53-java-baseline-kafka4.md`).
- **The `local` self-hosted PR jobs are disabled** (`pr-local-fast-feedback.yml`, `pull_request`
  trigger commented out). That runner is offline indefinitely and its suites now run on the highcpu
  runner, a strict superset. `workflow_dispatch` still works; restore the trigger if the box returns.
- **The highcpu lane runs six suites per branch on one box**, including mutation sweeps, and jobs
  repeatedly die of runner-lost-communication - 3+ times on #80 alone. It makes chaos timing SLOs
  noisy. Consider a shared concurrency group, or moving mutation off-box. Mutation strategy is being
  reconsidered wholesale in #111.
