# Self-Hosted Runner Setup (Proxmox Linux VM)

The heavy test suites - `Integration Tests` and `Performance Tests` - run on a
self-hosted GitHub Actions runner so we can throw real hardware at them **and
run the tests concurrently**. This document walks through the one-time setup on
a Linux VM.

Workflow: [`.github/workflows/self-hosted-tests.yml`](../.github/workflows/self-hosted-tests.yml).

You can register **more than one** machine (e.g. a Proxmox Linux VM *and* a Mac
laptop). The workflow targets only the custom `performance` label, so whichever
machine is online picks up the job - handy when the gaming PC is switched off.
See [Running on more than one machine](#running-on-more-than-one-machine).

## Why Linux (not Windows)

An earlier draft of this doc targeted Windows + Docker Desktop + WSL2. Don't do
that. TestContainers spins up Kafka in Linux containers; on a Linux host they
run **natively**, with no WSL2 translation layer and no Docker Desktop
licensing. On a Proxmox box, a small Linux VM is the fastest and simplest path.

## Where the speedup comes from

The integration and performance suites are **I/O-bound** - most of their time is
spent waiting on Kafka via TestContainers, not on CPU. The `ci` Maven profile
runs them sequentially (`parallel-tests=false` in pom.xml), most likely because
20 Kafka containers fighting over GitHub's 2 hosted cores caused resource
contention. A self-hosted machine has the cores and RAM to actually parallelise,
so the workflow does - but each suite uses a **different, suite-appropriate**
mechanism:

- **Integration: forked per-broker mode** (`-DforkCount=4 -DreuseForks=true`).
  Each JVM fork gets its **own** TestContainers broker and runs sequentially
  within itself, so tests never contend one shared broker. This is reliable
  **and** parallel - it was the fix for the flakiness described below. (Naive
  JUnit thread-parallelism on one shared broker - `-Dparallel-tests=true` - is
  ~7-10x faster but flaky, and it surfaced a real main-code deadlock, #857;
  forked mode avoids the contention without masking anything.)

- **Performance: in-JVM thread parallelism** (`-Dparallel-tests=true`). JUnit is
  configured to run these concurrently by default (see
  `junit.jupiter.execution.parallel.*` in
  `parallel-consumer-core/src/test/resources/junit-platform.properties`); the
  performance leg re-enables that on real cores.

Measure it (see [below](#measuring-the-speedup)); if a suite doesn't speed up
that tells you the bottleneck is elsewhere (Docker throughput, a genuinely
order-dependent test).

## What you get

- A GitHub Actions runner registered to your fork (`astubbs/parallel-consumer`)
- Triggered manually via `workflow_dispatch` or weekly on a schedule (never on
  PRs - see [Security](#security-notes))
- Runs the integration suite (`bin/ci-integration-test.sh`) and/or the
  performance suite (`bin/performance-test.sh`), with concurrent test execution

## Provision the VM on Proxmox

1. Create a VM: **Ubuntu 22.04/24.04 LTS Server**, 8+ vCPUs, 16+ GB RAM,
   40+ GB disk. Give it as many cores as you can spare - concurrency scales
   with them.
2. Enable nested virtualization is **not** required (containers, not VMs).
3. Install Docker Engine (the native daemon, not Docker Desktop):
   ```bash
   curl -fsSL https://get.docker.com | sh
   sudo usermod -aG docker "$USER"    # let the runner user talk to Docker
   newgrp docker                      # or log out/in
   docker run --rm hello-world        # verify
   ```
   JDK 17 does **not** need pre-installing - the workflow uses
   `actions/setup-java` and caches it in the runner work directory.

## Register the runner

### 1. Get a registration token

Fork's **Settings -> Actions -> Runners -> New self-hosted runner**, choose
**Linux / x64**. GitHub shows a snippet containing a one-time token.

### 2. Install the agent (as a normal user, not root)

Run the snippet GitHub gave you. It looks like:

```bash
mkdir actions-runner && cd actions-runner
curl -o actions-runner-linux-x64.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.x.x/actions-runner-linux-x64-2.x.x.tar.gz
tar xzf actions-runner-linux-x64.tar.gz
./config.sh --url https://github.com/astubbs/parallel-consumer --token <TOKEN>
```

### 3. Add the `performance` label

When `config.sh` prompts:

```
Enter any additional labels (ex. label-1,label-2): performance
```

The runner's full label set becomes: `self-hosted`, `Linux`, `X64`,
`performance`. The workflow targets `[self-hosted, performance]` - deliberately
**not** an OS label - so any online `performance` runner (Linux or Mac) can
serve it.

### 4. Run it as a service (survives reboots)

```bash
sudo ./svc.sh install "$USER"
sudo ./svc.sh start
sudo ./svc.sh status
```

The service runs as the user you pass - make sure that user is in the `docker`
group (step above).

## Running on more than one machine

Because the workflow targets only the `performance` label, you can register
several machines and let whichever is online serve the run. A Mac laptop is the
easiest second runner - you can reach it more often than the gaming PC.

**On the Mac** (Docker Desktop already installed and running):

1. Fork's **Settings -> Actions -> Runners -> New self-hosted runner**, choose
   **macOS** and your arch (**arm64** for Apple Silicon).
2. Run the snippet GitHub gives you, and add the `performance` label at the
   prompt (exactly as in step 3 above):
   ```bash
   ./config.sh --url https://github.com/astubbs/parallel-consumer --token <TOKEN>
   # Enter any additional labels: performance
   ```
3. Run it as a service so it survives sleep/reboot:
   ```bash
   ./svc.sh install
   ./svc.sh start
   ```

Both runners now advertise `performance`. GitHub sends the job to whichever is
idle and online. The gaming PC will be faster; the Mac is the always-reachable
fallback.

> Kafka TestContainers on a Mac run inside Docker Desktop's Linux VM, so
> throughput is lower than the native-Docker Linux VM - but it still beats
> GitHub's 2-core hosted runners, and it keeps the suite runnable when the PC is
> off.

### The `highcpu` runners (many-core Linux LXC, several instances)

The heavy runner is a many-core Linux box running a Docker LXC with **several runner instances** (one per
concurrent job), targeted by the [`highcpu`](../.github/workflows/pr-highcpu-fast-feedback.yml)
workflow (`runs-on: [self-hosted, highcpu]`, same-repo-guarded, non-gating). Unit, integration,
performance **and mutation (PIT)** run as separate matrix jobs in parallel. Provisioning it (OpenTofu +
Ansible) and the on-demand power/boot control are generic infrastructure kept in a separate private
infra repo, not here.

**Trigger:** `workflow_dispatch` only until a `[self-hosted, highcpu]` runner is registered and reliably
online - a `pull_request:` trigger with no runner would leave eternally-pending checks on every PR (the
reason `pr-local-fast-feedback.yml`'s PR trigger is also disabled). **Manually:** `gh workflow run highcpu
--ref <branch>`, or fork -> **Actions -> highcpu -> Run workflow**. Re-enable the `pull_request:` trigger
in the workflow once the runners are live.

## Fallback behaviour (important)

**There is no automatic fallback from a self-hosted runner to a GitHub-hosted
one.** If you trigger `Self-Hosted Tests` and no `performance` runner is online,
the run **queues and waits** for one - it will not silently run on github.com,
and a scheduled run in that state eventually errors out harmlessly.

This does **not** put your work at risk, because the self-hosted workflow is
*additive*, not load-bearing:

- Every **pull request** runs the integration and performance suites on
  **GitHub-hosted** runners via [`maven.yml`](../.github/workflows/maven.yml).
  That is your real gate and it never depends on your machines.
- `Self-Hosted Tests` is a manual/scheduled *bonus* - a fast full run on real
  hardware when you want it. If both your machines are off, you simply don't
  trigger it (or a scheduled run no-ops); nothing you depend on breaks.

So the safe mental model is: **PR feedback = GitHub-hosted, always. Fast heavy
runs = your machines, when they're on.** Registering the Mac as a second runner
widens the "when they're on" window.

## Triggering the workflow

**Manually:** fork -> **Actions -> Self-Hosted Tests -> Run workflow**.
Optionally pick a suite (`both` / `integration` / `performance`) and a Kafka
version override.

**Automatically:** runs every **Sunday at 02:00 UTC** via cron (defined in the
workflow).

## Measuring the speedup

Measure it on the runner itself, where the hardware and container behaviour are
real:

```bash
# baseline: sequential (what the ci profile does)
time bin/ci-integration-test.sh

# with the runner's setting: forked per-broker mode (what the workflow runs)
time bin/ci-integration-test.sh -DforkCount=4 -DreuseForks=true
```

### What we measured (2026-07-28) - short version

Integration parallelism was tested on GitHub-hosted runners (PR #66) and a
self-hosted Mac (`mac-laptop`, M2, 12 cores):

- **Naive thread-parallelism (`-Dparallel-tests=true`) is fast but flaky.** On
  the 12-core Mac it was **~7-10× faster** (~70-92 s vs ~11.5 min sequential),
  but ~2 of 104 tests flaked per run - a different set each time, all
  timing/timeout races on the **one shared broker** all ~104 tests contend.
  Lowering the parallelism factor and doubling Docker RAM had no effect. One of
  those failures (`RebalanceEoSDeadlockTest`) turned out to be a **real main-code
  deadlock (#857)**, not test flakiness - so we did not loosen timeouts to go
  green.
- **Forked per-broker mode (`-DforkCount=4 -DreuseForks=true`) is the fix** - and
  what the workflow now runs. Each JVM fork gets its own broker, so tests never
  contend: reliable **and** parallel. Measured **5/5 green** on the Mac (~4:06)
  and green on GitHub-hosted (6:16 vs ~11:38 sequential). It masks nothing -
  each test runs on an uncontended broker, just N-way in parallel.
- **On GitHub's 2-core runners, thread-parallelism was unusable** (~28 timeout
  failures from CPU starvation) - which is why the `ci` profile keeps
  `parallel-tests=false` as the sequential default.

Full diagnosis, the measured runs, and the resolution are in the findings doc:
[`docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`](solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md).

## Security & trust model

This is a **public** repository, so the core risk is: someone opens a PR and
their code runs on your machine. As currently wired, that risk **does not apply**
- `self-hosted-tests.yml` triggers only on `workflow_dispatch` and `schedule`,
**never** on `pull_request`. It only ever runs code you chose to run.

If you later add a PR-triggered FYI run (see below), understand the trust model
before you do:

- **Containerizing the runner is not a sandbox here.** Our tests need Docker
  (TestContainers spins up Kafka), so a containerized runner must mount the host
  Docker socket or run privileged Docker-in-Docker - both are effectively host
  root if the code is malicious. Run the runner in a container for convenience
  and clean teardown if you like, but do not treat it as an isolation boundary.
- **A disposable VM is the real sandbox.** On the Proxmox box, give the runner a
  dedicated, network-isolated Linux VM with nothing sensitive on it. If it's ever
  compromised, the blast radius is one throwaway VM you rebuild.
- **The Mac is your daily laptop - keep untrusted code off it.** Only ever run
  your own branches there, never external-fork PRs.
- **Same-repo guard** is what enforces that. Any future PR-triggered job must be
  guarded so only branches pushed into this repo run on your hardware:
  ```yaml
  if: github.event.pull_request.head.repo.full_name == github.repository
  ```
  External-fork PRs then skip the self-hosted job entirely and fall back to the
  GitHub-hosted checks. Pair it with the repo setting
  *Settings -> Actions -> General -> Require approval for all outside
  collaborators*.

**Planned (not wired yet):** an additive, non-required (`continue-on-error`)
self-hosted integration/performance check on PRs, same-repo guarded, running
alongside - not replacing - the required GitHub-hosted gate. Deliberately left
out for now; the GitHub-hosted suites remain the sole merge gate.

### Operational hygiene

- Don't run the runner or Docker as root.
- Keep the runner agent and Docker Engine updated.

## Appendix: remote power-on and OS switching (dual-boot host)

If the runner lives on a dual-boot machine (e.g. a gaming PC that boots Windows
natively by default and Proxmox from a second drive), you can run it fully
hands-off. The trick is to split "power on" from "pick the OS":

- **Power on** is Wake-on-LAN. It cannot choose an OS.
- **Pick the OS** is a UEFI boot-order / next-boot setting, done from software
  once an OS is running.

### 1. Make Proxmox the default boot entry

So a cold WoL power-on always lands in Proxmox (and the runner service comes up):

```bash
efibootmgr                 # list entries; note Proxmox's XXXX and Windows' YYYY
efibootmgr -o XXXX,YYYY    # persistent boot order, Proxmox first
```

### 2. Enable Wake-on-LAN

- BIOS: enable "Power On By Onboard LAN / PCIE", and **disable ErP/EuP** (its
  deep-sleep mode cuts NIC standby power and kills WoL).
- Proxmox: arm the NIC - `ethtool <iface>` should show `Wake-on: g`; set with
  `ethtool -s <iface> wol g` and persist it (systemd unit or interfaces hook).
- Trigger from anywhere on the LAN: `wakeonlan <MAC>`.

### 3. Reboot into Windows on demand (one-shot)

When you want to game, tell the running Proxmox host to reboot **once** into
Windows. UEFI's `BootNext` is consumed on the next boot and then reverts to the
Proxmox default automatically - so you never have to switch back:

```bash
#!/usr/bin/env bash
# reboot-into-windows.sh - run on the Proxmox host (needs root for efibootmgr)
set -euo pipefail
# Find the Windows entry dynamically so we don't hard-code the boot number:
WIN=$(efibootmgr | awk '/Windows Boot Manager/ { print substr($1, 5, 4); exit }')
[ -n "$WIN" ] || { echo "No Windows Boot Manager entry found" >&2; exit 1; }
efibootmgr -n "$WIN"     # BootNext = Windows, one time only
systemctl reboot
```

### 4. Trigger it from Home Assistant

Have Home Assistant WoL the machine, wait for it to come up, then SSH the script.
A minimal `shell_command` (HA already has the host's SSH key authorised, and the
runner user has `NOPASSWD` sudo for `efibootmgr`/`reboot`):

```yaml
# configuration.yaml
shell_command:
  gaming_pc_to_windows: >
    ssh -o StrictHostKeyChecking=no runner@GAMING_PC_IP
    'sudo /usr/local/bin/reboot-into-windows.sh'

# Optional: power it on first, then switch, in one automation
automation:
  - alias: "Gaming PC -> Windows"
    trigger: []                      # e.g. a button/helper you tap when you want to game
    action:
      - service: wake_on_lan.send_magic_packet
        data: { mac: "AA:BB:CC:DD:EE:FF" }
      - wait_template: "{{ true }}"  # replace with a ping/port check on the host
        timeout: "00:02:00"
      - service: shell_command.gaming_pc_to_windows
```

Net effect: the machine sits in Proxmox running the CI runner; when you want to
game you tap one Home Assistant control and it reboots straight into Windows,
then returns to Proxmox on the following boot. No keyboard, no boot-menu key.

> For remote access to the **BIOS/boot menu itself** (rare cases: a hung boot, a
> firmware change), a hardware KVM-over-IP (JetKVM, PiKVM) is the general
> solution - consumer boards have no IPMI. Not needed for the flow above.

## Troubleshooting

**Runner shows offline in GitHub:**
- `sudo ./svc.sh status`; logs in `actions-runner/_diag/`
- Restart: `sudo ./svc.sh stop && sudo ./svc.sh start`

**Tests fail with "Cannot connect to the Docker daemon":**
- `systemctl status docker`; start it with `sudo systemctl start docker`
- Confirm the runner user is in the `docker` group: `groups`

**Tests are flaky under parallelism:**
- Lower `dynamic.factor` in `junit-platform.properties`, or give the VM more RAM
- A genuinely order-dependent test is a bug - fix the test, don't disable
  parallelism globally

**Workflow can't find the runner:**
- The runner must be **online** when the workflow is triggered
- Verify labels match `runs-on:` in `.github/workflows/self-hosted-tests.yml`
