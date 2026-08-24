#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse:Bash hook. Warns - never blocks - when either disk this project fills is running low.
#
# WHY IT EXISTS. A fan-out of eleven per-language demo agents took the host volume from comfortable
# to 8.8 GiB free of 926 GiB over about an hour, and took the Docker VM's virtual disk down with it.
# One agent's build died outright with `No space left on device`; two others hit it and worked around
# it by pruning under each other. Nothing warned, because nothing was watching: the session started
# with plenty of room, and the room went away while work was in progress.
#
# WHY PreToolUse AND NOT SessionStart. docs/agent-harness.md's rule is to choose the event by WHEN it
# fires. SessionStart would have reported "all clear" on the session described above - the disk was
# fine when it began. The instant that matters is just before the next command, every time, because
# that is the only instant that can see the state the previous command left behind.
#
# WHY NO `if` PREFIX FILTER. Restricting this to `Bash(docker *)` would miss `cd demo && docker
# compose up`, `./run.sh --docker`, `mvn ... -Ddocker`, and every wrapper script - which is most of
# how containers actually get built here. docs/agent-harness.md records that `if` matches a PREFIX
# and that a self-test can only prove what a script does, not whether it is reached. So this runs on
# every Bash call and buys the right to do so by being cheap. Counted with `bash -x`, the healthy
# path forks SEVEN short-lived commands on Linux - `uname`, `id`, `mkdir`, and a `df`+`awk` pair per
# filesystem it asks about - and eight on macOS with Docker Desktop installed, where one `df` pair is
# replaced by a `stat`+`awk` on the disk image and an `awk` on its settings file. It never invokes the
# `docker` CLI. The command count is the durable half of that claim and is worth re-counting after
# any change; the wall-clock half moved between 8 and 10ms across repeated 100-run batches on one
# idle Linux box, quoted as a range because a single batch median shifts by more than a millisecond
# and a figure to two decimal places would only look more certain than it is.
#
# docs/agent-harness.md points HERE for those figures rather than repeating them. An earlier draft
# said "two stat-class syscalls, measured at 7ms" in both places and it was neither: nine commands,
# and ~10ms in the same harness, because `df` was forked twice over each path. Two copies of a
# measurement drift, and this one is what justifies running on every Bash call - so it is the copy
# that has to stay true, and it is cheaper to keep one honest than two.
#
# WHY IT CAN NEVER BLOCK. A PreToolUse hook exiting 2 takes away the tool call. A disk warner that
# blocked Bash on a full disk would remove the very commands needed to clear the disk, which is the
# outage docs/agent-harness.md describes under the misplaced-`if` trap. Every path here exits 0,
# including every internal error: it fails open and silent.
#
# That is a claim `set -u` can falsify, and did. An unbound variable aborts the script with a
# non-zero status, and bash reaches one through ARITHMETIC: `$(( x ))` resolves a non-numeric `x` as
# a variable name. So every value that reaches `$(( ))` here is proven all-digits first, and the two
# that come from outside this process - the throttle stamp and `PC_DISK_VM_ALLOC_GIB` - are clamped
# at their read sites rather than trusted. `bin/test-check-agent-hooks.sh` asserts exit 0 against a
# garbled stamp and against a hostile one, not only against readings the hook takes itself.
#
# THE Docker.raw CAVEAT, which is why there are two stages. Docker Desktop's virtual disk is a sparse
# file that grows and never shrinks - pruning 17 GB of images does not shrink it by a byte. So its
# allocated size is a HIGH-WATER MARK, not live usage: it over-reports pressure after a prune and
# never under-reports it. That is the safe direction for a warner but it would nag falsely for days
# after a cleanup, so the expensive confirmation (`docker system df`, ~300ms, cached) runs ONLY when
# the cheap signal has already tripped. Cheap trigger, expensive confirmation.
#
# CROSS-PLATFORM, AND SILENT WHERE IT CANNOT SEE. Every platform-specific reading is behind a
# capability check, and an unavailable one is skipped rather than guessed: no Docker, no `stat`, an
# unrecognised OS, or a `df` that will not answer all degrade to "checked what I could". The one
# thing this must never do is report a healthy disk it did not actually measure.
#
# `stat -f` is NOT a portable idiom and must not be used as a try-then-fallback. On macOS `stat -f %b`
# is a FILE's allocated blocks; on GNU/Linux `-f` switches stat into FILESYSTEM mode, where `%b` is
# the total blocks in the filesystem - it exits 0 and returns a number three orders of magnitude too
# large. A fallback chain would therefore fail silently in the dangerous direction, which is why the
# platform is resolved once, up front, from `uname`.
#
# Run the self-test: bin/test-check-agent-hooks.sh

set -uo pipefail

# Free space below which the warning fires, in GiB. The host figures are set against what this
# project actually consumes in one go: a single Swift demo image is 3.8 GB, and a full eleven-language
# rebuild is roughly 16 GB, so 25 GiB is "one more full fan-out would not fit".
# Overridable ONLY so bin/test-check-agent-hooks.sh can drive each band on a machine whose real disk
# is healthy. bin/AGENTS.md's rule is that a guard which has never fired proves nothing, and the only
# alternative way to make this one fire is to genuinely fill a 926 GiB volume.
readonly HOST_WARN_GIB="${PC_DISK_HOST_WARN_GIB:-25}"
readonly HOST_CRIT_GIB="${PC_DISK_HOST_CRIT_GIB:-10}"

# The VM's headroom bands. CRIT is set at 5 GiB because the observed failure - a Swift toolchain
# build dying mid-export - happened at 4.6 GiB free, so the warning has to arrive above that.
readonly VM_WARN_GIB="${PC_DISK_VM_WARN_GIB:-12}"
readonly VM_CRIT_GIB="${PC_DISK_VM_CRIT_GIB:-5}"

# Re-warning on every Bash call would be noise the model learns to skip. Warn again only when this
# much time has passed, or when the situation got materially worse (see `band` below).
readonly REWARN_AFTER_SECONDS=600

# Trust a `docker system df` reading for this long. Cheap enough to refresh often, expensive enough
# not to run per call.
readonly DOCKER_DF_TTL_SECONDS=120

# Two separate concerns, deliberately not one variable:
#   REAL_UNAME decides which `stat` SYNTAX this machine speaks. Never injectable - getting it wrong
#     is the silent-wrong-number failure described in the header.
#   OS_NAME decides which Docker LAYOUT to look for. Injectable, so the self-test can exercise the
#     Docker Desktop branch on a Linux CI runner without pretending Linux has BSD stat.
readonly REAL_UNAME="$(uname -s 2>/dev/null || echo unknown)"
readonly OS_NAME="${PC_DISK_UNAME:-$REAL_UNAME}"

# ONE platform table, because two of them drift: this was a pair of functions differing only in the
# format letters, so adding a platform meant remembering to edit both. An unrecognised platform falls
# off the end of the `case` and yields nothing, which is the whole point - see the `stat -f` warning
# in the header for what guessing costs here.
stat_field() { # <bsd-format> <gnu-format> <path> -> the field, or empty
    case "$REAL_UNAME" in
        Darwin | *BSD | DragonFly) stat -f "$1" "$3" 2>/dev/null ;;
        Linux | CYGWIN* | MINGW* | MSYS*) stat -c "$2" "$3" 2>/dev/null ;;
    esac
}

file_blocks() { stat_field %b %b "$1"; } # allocated size of a FILE, in 512-byte blocks
file_mtime() { stat_field %m %Y "$1"; }  # modification time, as a Unix timestamp

# ONE `df` per path, answering both questions this hook asks of a filesystem at once: which one it
# is, and how much room is left on it. These were two functions, each forking its own `df` over the
# same path, so the Linux fast path ran three `df`s and three `awk`s to learn two things about two
# paths. Now it runs two of each, whatever the layout.
#
# `-P` forces POSIX single-line output: without it a long device name wraps onto its own line on
# Linux and the awk reads the wrong column - a wrong number rather than no number. `-k` pins the
# block size at 1 KiB so `$4` means the same thing to GNU and BSD `df`.
#
# The device is only ever COMPARED, never shown, so `df`'s own first column is enough and needs no
# platform branch. Both fields are gated on `$4` being numeric TOGETHER, so a `df` that answers
# unparseably yields nothing rather than half an answer - the device column alone was ungated
# before, and only the caller's ordering kept that from mattering.
#
# The `-e` test stays even though `df` on a missing path already yields nothing here: it costs no
# fork, and it makes the missing-path case a property of this script rather than of whether the
# local `df` prints its header before failing. Nobody has run this against BSD `df` yet.
# FREE SPACE FIRST, device second, because a device name CAN contain a space and a mount point
# always can - so the caller splits on the FIRST space, never the last, and the device keeps whatever
# spaces it really has. Columns are located by the capacity field rather than counted from the left:
# POSIX `df -P` guarantees exactly one field ending in `%`, with Available immediately before it. The
# earlier `$4` was the Used column for any device containing a space (macOS autofs reports
# `map auto_home`; a CIFS share can too), which is a CONFIDENT WRONG NUMBER in the dangerous
# direction - it reports space in USE as space free, so the hook goes quiet on a filling disk.
fs_probe() { # <path> -> "<free-gib> <device>", or empty when the filesystem will not answer
    [ -e "$1" ] || return 0
    df -Pk "$1" 2>/dev/null | awk 'NR==2 {
        for (i = 1; i <= NF; i++)
            if ($i ~ /^[0-9]+%$/) {
                if (i >= 5 && $(i-1) ~ /^[0-9]+$/) {
                    dev = $1
                    for (j = 2; j <= i - 4; j++) dev = dev " " $j
                    printf "%d %s", $(i-1) / 1048576, dev
                }
                exit
            }
    }'
}

# Docker Desktop's configured ceiling, in GiB, from its settings JSON. One `awk` rather than the
# `grep -o | head -1 | grep -o | awk` chain this replaces: four processes for one integer, on a path
# that runs on every Bash call on a live Docker Desktop box, and `head -1` in front of a `grep`
# writer is the early-exiting-reader shape bin/AGENTS.md warns about under `pipefail`.
desktop_ceiling_gib() { # <settings-json> -> GiB, or empty
    awk 'match($0, /"[Dd]iskSizeMiB"[ \t]*:[ \t]*[0-9]+/) {
             mib = substr($0, RSTART, RLENGTH)
             sub(/^.*:[ \t]*/, "", mib)
             printf "%d", mib / 1024
             exit
         }' "$1" 2>/dev/null
}

# Owner-only from here down. Everything this script writes is read back on a LATER run from a
# PREDICTABLE path in a shared /tmp, which makes the directory's mode part of its contract rather
# than a detail. `umask` is a shell builtin, so this costs nothing on the fast path.
umask 077

state_dir="${PC_DISK_STATE_DIR:-${TMPDIR:-/tmp}/claude-disk-warn-$(id -u 2>/dev/null || echo 0)}"
mkdir -p "$state_dir" 2>/dev/null || exit 0

# `mkdir -p` SUCCEEDS against a directory that already exists and belongs to someone else, so it is
# not the check it looks like. Without this, any local user can pre-create the predictable path above
# and then owns every byte this script reads back - see the `last_at` guard in the throttle for what
# that buys them - while a symlinked `last-warning` turns the write at the end of this file into
# arbitrary file truncation. Both were reproduced against the unguarded version.
[ -O "$state_dir" ] || exit 0

# Everything below is best-effort. `exit 0` on any surprise, silently.
target="${CLAUDE_PROJECT_DIR:-$PWD}"
[ -d "$target" ] || target="$PWD"

# Nothing readable means nothing to say. Never assume healthy.
target_probe="$(fs_probe "$target")"
[ -n "$target_probe" ] || exit 0
host_free_gib="${target_probe%% *}"
target_device="${target_probe#* }"

# --- the disk Docker fills, which is a different thing on each platform -------------------------
# macOS / Windows (Docker Desktop): a sparse virtual disk with a configured ceiling. Its allocated
#   size is a HIGH-WATER MARK - see the header - so it needs the correction stage below.
# Linux (native engine): no VM at all; the daemon writes straight into the host filesystem. When
#   that is a separate mount from the project it is worth reading, and the reading is LIVE, so the
#   correction must not be applied to it. When it is the same mount, the host check above already
#   covers it and reporting it twice would just be noise.
# Anything else: skipped, silently. A platform we cannot read is not a platform that is fine.
vm_free_gib=""
vm_total_gib=""
vm_label=""
vm_is_high_water="no"

docker_desktop_raw=""
docker_desktop_settings=""
case "$OS_NAME" in
    Darwin)
        docker_desktop_raw="${PC_DISK_DESKTOP_RAW:-${HOME:-}/Library/Containers/com.docker.docker/Data/vms/0/data/Docker.raw}"
        if [ -n "${PC_DISK_DESKTOP_SETTINGS:-}" ]; then
            docker_desktop_settings="$PC_DISK_DESKTOP_SETTINGS"
        else
            for candidate in \
                "${HOME:-}/Library/Group Containers/group.com.docker/settings-store.json" \
                "${HOME:-}/Library/Group Containers/group.com.docker/settings.json"; do
                [ -f "$candidate" ] && { docker_desktop_settings="$candidate"; break; }
            done
        fi
        ;;
    CYGWIN* | MINGW* | MSYS*)
        # Docker Desktop on WSL2 keeps a .vhdx whose ceiling is a WSL setting rather than a Docker
        # one, and is commonly left at the 1 TB default - a headroom figure computed against that
        # would be meaningless. Deliberately not read; the host volume check still applies.
        ;;
esac

if [ -n "$docker_desktop_raw" ] && [ -f "$docker_desktop_raw" ] && [ -n "$docker_desktop_settings" ]; then
    # PC_DISK_VM_ALLOC_GIB exists only for bin/test-check-agent-hooks.sh. The high-water-mark
    # correction can only be driven from outside by a disk image with GIGABYTES genuinely allocated,
    # and writing a multi-GiB fixture to prove a branch - on a machine the hook exists because it ran
    # out of disk - is a worse trade than one seam. The `file_blocks` path it bypasses is covered by
    # the other Docker Desktop cases, which use a real file and the real stat.
    vm_alloc_gib="${PC_DISK_VM_ALLOC_GIB:-$(file_blocks "$docker_desktop_raw" | awk '$1 ~ /^[0-9]+$/ {printf "%d", $1*512/1073741824}')}"
    # Reaches `$(( ))` below, so it obeys the same all-digits rule as the throttle's `last_at`; the
    # `awk` path already guarantees it, but the env override does not. Same defect class, swept per
    # AGENTS.md rather than fixed only where it was found.
    case "${vm_alloc_gib:-}" in "" | *[!0-9]*) vm_alloc_gib="" ;; esac
    vm_total_gib="$(desktop_ceiling_gib "$docker_desktop_settings")"
    if [ -n "${vm_alloc_gib:-}" ] && [ -n "${vm_total_gib:-}" ] && [ "$vm_total_gib" -gt 0 ]; then
        vm_free_gib=$((vm_total_gib - vm_alloc_gib))
        # A sparse image can exceed its own configured ceiling (the ceiling was lowered after the
        # image grew), so clamp HERE too and not only after the correction below - this figure
        # reaches the message directly whenever the host is what tripped and the correction is
        # skipped, and "~-3 GiB headroom" is not a thing to show anyone.
        [ "$vm_free_gib" -lt 0 ] && vm_free_gib=0
        vm_label="Docker VM disk"
        vm_is_high_water="yes"
    fi
elif [ "$OS_NAME" = "Linux" ]; then
    docker_root="${PC_DISK_DOCKER_ROOT:-/var/lib/docker}"
    docker_probe=""
    [ -d "$docker_root" ] && docker_probe="$(fs_probe "$docker_root")"
    if [ -n "$docker_probe" ] && [ "${docker_probe#* }" != "$target_device" ]; then
        vm_free_gib="${docker_probe%% *}"
        vm_label="Docker data filesystem"
    fi
fi

# --- decide whether anything is worth saying ---------------------------------------------------
# Which disk tripped is tracked separately from how bad it is, because the post-prune correction
# below must know whether the HOST is a reason to warn. An earlier version inferred that by
# re-comparing the host figure against HOST_WARN_GIB, which silently swallowed a host-critical
# reading whenever the VM looked healthy; bin/test-check-agent-hooks.sh has the case.
host_tripped="no"
vm_tripped="no"
band="ok"

if [ "$host_free_gib" -lt "$HOST_WARN_GIB" ]; then
    host_tripped="yes"
    band="warn"
fi
if [ -n "$vm_free_gib" ] && [ "$vm_free_gib" -lt "$VM_WARN_GIB" ]; then
    vm_tripped="yes"
    band="warn"
fi
if [ "$host_free_gib" -lt "$HOST_CRIT_GIB" ]; then
    host_tripped="yes"
    band="critical"
fi
if [ -n "$vm_free_gib" ] && [ "$vm_free_gib" -lt "$VM_CRIT_GIB" ]; then
    vm_tripped="yes"
    band="critical"
fi

[ "$band" = "ok" ] && exit 0

# One clock from here down. Both the Docker cache TTL and the re-warn throttle need "how long ago",
# and reading `date` twice in one run can straddle a second boundary as well as costing a fork.
now="$(date +%s)"

# The high-water-mark correction. If the ONLY reason we are here is the VM figure, ask docker what
# it is really holding before saying anything - the sparse file cannot tell the difference between
# 36 GiB in use and 36 GiB that was freed by a prune an hour ago.
if [ "$host_tripped" = "no" ] && [ "$vm_tripped" = "yes" ] && [ "$vm_is_high_water" = "yes" ]; then
    df_cache="$state_dir/docker-df"
    # `${...:-0}` rather than `|| echo 0`, which could not fire: on a platform whose `stat` syntax is
    # unrecognised, `file_mtime` exits 0 with EMPTY output, so the fallback was skipped and the
    # arithmetic became `$(( <now> -  ))` - a syntax error, whose empty result made the `-gt` test
    # error out too and quietly settle on "do not refresh". Treating an unreadable mtime as epoch 0
    # forces a refresh instead, which is the direction that cannot serve a stale figure.
    df_cached_at="$(file_mtime "$df_cache")"
    if [ ! -f "$df_cache" ] ||
        [ "$((now - ${df_cached_at:-0}))" -gt "$DOCKER_DF_TTL_SECONDS" ]; then
        # Built in a temp file and RENAMED, never truncated in place. `rename(2)` within one
        # directory is atomic, so a concurrent session - and this repo routinely runs several - reads
        # either the old snapshot or the new one, never a half-written one. It matters more than it
        # looks: a PARTIAL read UNDERCOUNTS what Docker holds, which inflates the corrected free
        # figure and takes the `exit 0` below, so the hook says nothing at all about a disk that is
        # filling. That is the false "fine" the header forbids. An EMPTY read was always safe - the
        # `-gt 0` guard skips the correction - so truncate-in-place hid this behind its own best case.
        df_tmp="$df_cache.$$"
        # BOUNDED, because this talks to the daemon at precisely the moment the disk is already known
        # to be low - the state in which dockerd is likeliest to be wedged - and a PreToolUse hook
        # that blocks here blocks the agent's next command. `timeout` is GNU coreutils and absent from
        # stock macOS, so it is used when present and skipped when not, like every other capability
        # in this file; a killed call leaves an empty cache, which already degrades to warning from
        # the high-water figure. Unquoted on purpose, so an empty value expands to no word at all.
        docker_timeout=""
        command -v timeout >/dev/null 2>&1 && docker_timeout="timeout 5"
        if command -v docker >/dev/null 2>&1; then
            # shellcheck disable=SC2086
            $docker_timeout docker system df --format '{{.Size}}' >"$df_tmp" 2>/dev/null || : >"$df_tmp"
        else
            : >"$df_tmp"
        fi
        mv -f "$df_tmp" "$df_cache" 2>/dev/null || rm -f "$df_tmp" 2>/dev/null
    fi
    # Sizes come back as "20.01GB" / "512.3MB" / "0B". Count the rows we UNDERSTOOD as well as
    # totalling them, because the previous `-gt 0` test conflated two opposite answers:
    #   "docker holds nothing"      - a real reading from a fully pruned daemon, where the correction
    #                                SHOULD fire and clear an alarm the sparse file is still raising;
    #   "we could not read docker"  - where it must not fire at all.
    # Under `-gt 0` a fully pruned Docker totalled 0, the correction was skipped, and the hook nagged
    # about a disk image that had already been emptied - the exact stale alarm this block exists to
    # prevent, in its most extreme case. A row in a unit not listed here would likewise total as zero
    # and UNDER-count, inflating the corrected free figure and suppressing a real warning, so an
    # unrecognised row now disqualifies the whole reading rather than counting as nothing.
    df_totals="$(awk '
        function add(v, mult) { total += v * mult; known++ }
        { rows++ }
        /^[0-9.]+B$/  { sub(/B$/,  ""); add($0, 1/1073741824); next }
        /^[0-9.]+kB$/ { sub(/kB$/, ""); add($0, 1/1048576);    next }
        /^[0-9.]+MB$/ { sub(/MB$/, ""); add($0, 1/1024);       next }
        /^[0-9.]+GB$/ { sub(/GB$/, ""); add($0, 1);            next }
        /^[0-9.]+TB$/ { sub(/TB$/, ""); add($0, 1024);         next }
        END { printf "%d %d %d", rows + 0, known + 0, total + 0 }' "$df_cache" 2>/dev/null)"
    read -r df_rows df_known real_used_gib <<<"${df_totals:-0 0 0}"
    if [ "${df_rows:-0}" -gt 0 ] && [ "$df_rows" = "$df_known" ]; then
        vm_real_free=$((vm_total_gib - real_used_gib))
        # `docker system df` reports LOGICAL sizes, which double-count layers shared between images,
        # so the total it reports can exceed the disk it sits on and drive this negative. Clamping
        # keeps "~-16 GiB headroom" out of the message; the figure is a floor, not a measurement.
        [ "$vm_real_free" -lt 0 ] && vm_real_free=0
        # Docker is holding materially less than the file suggests: post-prune slack, not pressure.
        if [ "$vm_real_free" -ge "$VM_WARN_GIB" ]; then
            exit 0
        fi
        vm_free_gib="$vm_real_free"
        # Re-derive severity from the corrected figure - the sparse file may have claimed critical
        # for space a prune already returned.
        if [ "$vm_real_free" -lt "$VM_CRIT_GIB" ]; then band="critical"; else band="warn"; fi
    fi
fi

# --- throttle ----------------------------------------------------------------------------------
# Inside the window, speak only when the situation got WORSE. `band` is `warn` or `critical` here and
# nothing else, since `ok` exited above, so "worse" is exactly warn -> critical - and its complement,
# the condition below, is "same band again, or a downgrade from critical". A downgrade stays quiet on
# purpose: never soften the urgency of something already said until the timer expires.
#
# That is one condition. It was two `if`s testing the same window twice, which read as two rules and
# invited a change to one that contradicted the other.
stamp="$state_dir/last-warning"
if [ -f "$stamp" ]; then
    read -r last_at last_band <"$stamp" 2>/dev/null || { last_at=0; last_band="none"; }
    # THIS FILE IS INPUT, NOT OUR OWN DATA - it lives at a predictable path in a shared /tmp and is
    # read back on a later run. `last_at` reaches arithmetic below, and bash arithmetic resolves a
    # non-numeric operand as a variable NAME, recursively: `band[$(cmd)] warn` in this file EXECUTES
    # `cmd` as whoever is running the agent. Reproduced. And no attacker is needed for the other
    # half - a merely truncated `warn` aborts the script under `set -u` with a NON-ZERO status,
    # which takes away the Bash call and is the one thing this hook must never do. A torn write is
    # likeliest exactly when the disk is full, the condition this hook exists for.
    # Anything not all-digits becomes epoch 0, so the next reading SPEAKS rather than being swallowed.
    case "${last_at:-}" in "" | *[!0-9]*) last_at=0; last_band="none" ;; esac
    # A stamp from the FUTURE is the other way this file wedges the warner, and it needs no attacker
    # either: an NTP correction, a resumed VM or a laptop waking with a skewed clock leaves
    # `now - last_at` NEGATIVE, which is always inside the window, so the hook stays silent until
    # wall-clock time catches up - potentially for as long as the skew. Treated as stale, the same
    # direction as an unreadable one: speak now rather than swallow.
    [ "$last_at" -gt "$now" ] && { last_at=0; last_band="none"; }
    if [ "$((now - last_at))" -lt "$REWARN_AFTER_SECONDS" ] &&
        { [ "$band" = "$last_band" ] || [ "$last_band" = "critical" ]; }; then
        exit 0
    fi
fi
echo "$now $band" >"$stamp" 2>/dev/null || :

# --- say it --------------------------------------------------------------------------------------
# Raw stdout is discarded by the harness; the JSON envelope with `additionalContext` is the only
# channel that reaches the model (docs/agent-harness.md, verified against 2.1.223).
#
# THE FIVE SIBLING HOOKS BUILD THIS ENVELOPE WITH `python3 -c '... json.dumps(...)'`. This one
# deliberately does not, and the difference is a decision rather than an oversight, so a reviewer
# does not keep re-raising it. Those hooks already need python3 to TOKENISE a command before they can
# decide anything, so an interpreter is a precondition of their verdict. This one has no payload to
# parse and no verdict to compute: python3 would appear for the first time on the one path that
# carries the message, and a box without it would go from "warned" to "silent" - the failure mode the
# header spends four paragraphs refusing.
#
# `printf` is safe here because `reason` is closed: every interpolation is either a fixed literal or
# an integer that `printf "%d"` or shell arithmetic produced, so it cannot contain a quote, a
# backslash or a control character. The escaping below is insurance against a future field, not a
# parser - if this message ever interpolates a PATH or anything else a user controls, delete the
# `sed`/`tr` line and adopt the siblings' `json.dumps` instead of trying to extend it.
vm_clause=""
if [ -n "$vm_free_gib" ] && [ -n "$vm_total_gib" ]; then
    vm_clause=" ${vm_label}: ~${vm_free_gib} GiB headroom of ${vm_total_gib} GiB."
elif [ -n "$vm_free_gib" ]; then
    vm_clause=" ${vm_label}: ${vm_free_gib} GiB free."
fi

if [ "$band" = "critical" ]; then
    lead="DISK CRITICAL."
    advice="Stop before starting any container build. Reclaim first: \`docker image prune -f\` and \`docker volume prune -f\` are safe (dangling images, unused anonymous volumes only) - check \`docker volume ls -f dangling=true\` for NAMED volumes first, as those may hold real data. \`docker builder prune -f\` frees more but slows the next build."
else
    lead="Disk running low."
    advice="Consider reclaiming before a large container build. \`docker system df\` shows what is held; dangling images and unused anonymous volumes are the safe wins."
fi

reason="$lead Host volume: ${host_free_gib} GiB free.${vm_clause} $advice Tell the user - this is their machine, and pruning named volumes or tagged images is their call, not yours."

# Escape for JSON: backslashes first, then quotes, then any stray control characters.
escaped="$(printf '%s' "$reason" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr -d '\000-\037')"

printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow","additionalContext":"%s"}}\n' "$escaped"
exit 0
