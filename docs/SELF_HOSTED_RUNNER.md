# Self-Hosted Runner Setup (Windows + Docker Desktop)

The performance test suite runs on a self-hosted GitHub Actions runner so we can throw real hardware at the volume tests. This document walks through the one-time setup on a Windows machine.

## What you get

- A GitHub Actions runner registered to your fork (`astubbs/parallel-consumer`)
- Triggered manually via `workflow_dispatch` or weekly on a schedule
- Runs the `@Tag("performance")` tests via `bin/performance-test.cmd`
- TestContainers spins up a Kafka broker via Docker Desktop (Linux containers / WSL2 backend)

## Prerequisites on the runner machine

1. **Windows 10/11** (or Server 2019+)
2. **WSL2 enabled** — required for Docker Desktop's Linux container backend
3. **Docker Desktop** for Windows, configured to use the WSL2 backend
4. **Git for Windows** (which includes Git Bash)
5. **Outbound HTTPS access** to `github.com`, `*.actions.githubusercontent.com`, Maven Central, and Docker Hub (or `images.confluent.io` for cp-kafka images)

JDK 17 does **not** need to be pre-installed — the workflow uses `actions/setup-java` to provision it on first run, and caches it in the runner work directory.

## One-time runner installation

### 1. Generate a runner token

1. Go to your fork's **Settings → Actions → Runners → New self-hosted runner**
2. Choose **Windows** and **x64**
3. GitHub will show you a PowerShell snippet that includes a one-time registration token

### 2. Install the runner agent

Open **PowerShell as your normal user** (not Administrator — the runner should not run as admin) and run the snippet GitHub provided. It looks roughly like this:

```powershell
mkdir actions-runner
cd actions-runner
Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v2.x.x/actions-runner-win-x64-2.x.x.zip -OutFile actions-runner.zip
Add-Type -AssemblyName System.IO.Compression.FileSystem ; [System.IO.Compression.ZipFile]::ExtractToDirectory("$PWD\actions-runner.zip", "$PWD")
```

### 3. Configure the runner with the right labels

When prompted by `config.cmd`, set the labels so the workflow can find this runner:

```
Enter the name of the runner group to add this runner to: [press Enter for Default]
Enter the name of runner: [press Enter for hostname, or type something descriptive]
Enter any additional labels (ex. label-1,label-2): performance,windows
```

The full label set on this runner will be: `self-hosted`, `Windows`, `X64`, `performance`, `windows`

> **Why two `windows` labels?** GitHub auto-adds `Windows` (capital W). The workflow targets the lowercase custom label `windows` to make intent explicit. Either works — pick whichever you prefer and update the workflow's `runs-on:` line if you change it.

### 4. Run the runner as a service (recommended)

So it survives reboots:

```cmd
cd actions-runner
.\svc.cmd install
.\svc.cmd start
```

The service runs as the current user by default. Make sure that user has Docker Desktop access.

### 5. Verify Docker is reachable

In the same shell:

```cmd
docker info
docker run --rm hello-world
```

If `docker info` complains about the daemon, start Docker Desktop and wait for it to finish initializing (the whale icon in the system tray should be steady, not animating).

## Triggering the workflow

### Manually

1. Go to your fork on GitHub
2. **Actions → Performance Tests → Run workflow**
3. Optionally enter a Kafka version override
4. Click **Run workflow**

### Automatically

The workflow runs every **Sunday at 02:00 UTC** via cron schedule (defined in `.github/workflows/performance.yml`).

## Security notes

- Self-hosted runners on **public repositories** are dangerous because anyone can open a PR that runs arbitrary code on your machine. The performance workflow is configured to **only** run on `workflow_dispatch` (manual) and `schedule` triggers, never on PRs, to avoid this.
- Do not run the runner as Administrator.
- Keep Docker Desktop and the runner agent updated.

## Troubleshooting

**Runner shows offline in GitHub:**
- Check the service is running: `.\svc.cmd status`
- Check logs in `actions-runner\_diag\`
- Restart: `.\svc.cmd stop && .\svc.cmd start`

**Tests fail with "Cannot connect to Docker daemon":**
- Make sure Docker Desktop is running
- Check WSL2 backend is enabled (Docker Desktop → Settings → General)
- Try `docker info` from the runner's shell

**Tests run but are very slow:**
- Performance tests are *meant* to be slow — that's why they're on dedicated hardware
- Check Docker Desktop's resource allocation (Settings → Resources) — give it enough RAM (8GB+) and CPU cores

**Workflow can't find the runner:**
- Verify the labels match what `runs-on:` expects in `.github/workflows/performance.yml`
- The runner must be online when the workflow is triggered
