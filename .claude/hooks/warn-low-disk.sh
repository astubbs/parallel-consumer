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
# every Bash call and buys the right to do so by being cheap: two stat-class syscalls, measured at
# 7ms, with no `docker` CLI invocation on the fast path.
#
# WHY IT CAN NEVER BLOCK. A PreToolUse hook exiting 2 takes away the tool call. A disk warner that
# blocked Bash on a full disk would remove the very commands needed to clear the disk, which is the
# outage docs/agent-harness.md describes under the misplaced-`if` trap. Every path here exits 0,
# including every internal error: it fails open and silent.
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

# Allocated size of a FILE, in 512-byte blocks, or empty when it cannot be determined.
file_blocks() {
    case "$REAL_UNAME" in
        Darwin | *BSD | DragonFly) stat -f %b "$1" 2>/dev/null ;;
        Linux | CYGWIN* | MINGW* | MSYS*) stat -c %b "$1" 2>/dev/null ;;
        *) return 0 ;;
    esac
}

# Modification time of a file as a Unix timestamp, or empty.
file_mtime() {
    case "$REAL_UNAME" in
        Darwin | *BSD | DragonFly) stat -f %m "$1" 2>/dev/null ;;
        Linux | CYGWIN* | MINGW* | MSYS*) stat -c %Y "$1" 2>/dev/null ;;
        *) return 0 ;;
    esac
}

# Free space in GiB on the filesystem holding $1, or empty. `-P` forces POSIX single-line output:
# without it, a long device name wraps onto its own line on Linux and the awk picks up the wrong
# column - a wrong number rather than no number.
fs_free_gib() {
    [ -e "$1" ] || return 0
    df -Pk "$1" 2>/dev/null | awk 'NR==2 && $4 ~ /^[0-9]+$/ {printf "%d", $4/1048576}'
}

# The device id of the filesystem holding $1, used only to tell whether two paths are on the same
# one. Any stable per-filesystem token will do, so `df`'s own device column is enough and needs no
# platform branch.
fs_id() {
    [ -e "$1" ] || return 0
    df -Pk "$1" 2>/dev/null | awk 'NR==2 {print $1}'
}

state_dir="${PC_DISK_STATE_DIR:-${TMPDIR:-/tmp}/claude-disk-warn-$(id -u 2>/dev/null || echo 0)}"
mkdir -p "$state_dir" 2>/dev/null || exit 0

# Everything below is best-effort. `exit 0` on any surprise, silently.
target="${CLAUDE_PROJECT_DIR:-$PWD}"
[ -d "$target" ] || target="$PWD"

host_free_gib="$(fs_free_gib "$target")"
# Nothing readable means nothing to say. Never assume healthy.
[ -n "${host_free_gib:-}" ] || exit 0

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
        docker_desktop_raw="${PC_DISK_DESKTOP_RAW:-$HOME/Library/Containers/com.docker.docker/Data/vms/0/data/Docker.raw}"
        if [ -n "${PC_DISK_DESKTOP_SETTINGS:-}" ]; then
            docker_desktop_settings="$PC_DISK_DESKTOP_SETTINGS"
        else
            for candidate in \
                "$HOME/Library/Group Containers/group.com.docker/settings-store.json" \
                "$HOME/Library/Group Containers/group.com.docker/settings.json"; do
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
    vm_total_gib="$(grep -o '"[Dd]iskSizeMiB"[[:space:]]*:[[:space:]]*[0-9]*' "$docker_desktop_settings" 2>/dev/null |
        head -1 | grep -o '[0-9]*$' | awk '{printf "%d", $1/1024}')"
    if [ -n "${vm_alloc_gib:-}" ] && [ -n "${vm_total_gib:-}" ] && [ "$vm_total_gib" -gt 0 ]; then
        vm_free_gib=$((vm_total_gib - vm_alloc_gib))
        vm_label="Docker VM disk"
        vm_is_high_water="yes"
    fi
elif [ "$OS_NAME" = "Linux" ]; then
    docker_root="${PC_DISK_DOCKER_ROOT:-/var/lib/docker}"
    if [ -d "$docker_root" ] && [ "$(fs_id "$docker_root")" != "$(fs_id "$target")" ]; then
        vm_free_gib="$(fs_free_gib "$docker_root")"
        [ -n "$vm_free_gib" ] && vm_label="Docker data filesystem"
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

# The high-water-mark correction. If the ONLY reason we are here is the VM figure, ask docker what
# it is really holding before saying anything - the sparse file cannot tell the difference between
# 36 GiB in use and 36 GiB that was freed by a prune an hour ago.
if [ "$host_tripped" = "no" ] && [ "$vm_tripped" = "yes" ] && [ "$vm_is_high_water" = "yes" ]; then
    df_cache="$state_dir/docker-df"
    if [ ! -f "$df_cache" ] ||
        [ "$(( $(date +%s) - $(file_mtime "$df_cache" 2>/dev/null || echo 0) ))" -gt "$DOCKER_DF_TTL_SECONDS" ]; then
        if command -v docker >/dev/null 2>&1; then
            docker system df --format '{{.Size}}' >"$df_cache" 2>/dev/null || : >"$df_cache"
        else
            : >"$df_cache"
        fi
    fi
    # Sizes come back as "20.01GB" / "512.3MB"; total them in GB, treating anything unparsed as 0.
    real_used_gib="$(awk '
        /GB$/ { sub(/GB$/, ""); t += $0 }
        /MB$/ { sub(/MB$/, ""); t += $0/1024 }
        END   { printf "%d", t }' "$df_cache" 2>/dev/null)"
    if [ -n "${real_used_gib:-}" ] && [ "$real_used_gib" -gt 0 ]; then
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
# Repeat only after REWARN_AFTER_SECONDS, or immediately if the band worsened (warn -> critical).
stamp="$state_dir/last-warning"
now="$(date +%s)"
if [ -f "$stamp" ]; then
    read -r last_at last_band <"$stamp" 2>/dev/null || { last_at=0; last_band="none"; }
    if [ "$band" = "$last_band" ] && [ "$((now - last_at))" -lt "$REWARN_AFTER_SECONDS" ]; then
        exit 0
    fi
    # Never downgrade the urgency of a repeat: critical -> warn stays quiet until the timer expires.
    if [ "$last_band" = "critical" ] && [ "$band" = "warn" ] &&
        [ "$((now - last_at))" -lt "$REWARN_AFTER_SECONDS" ]; then
        exit 0
    fi
fi
echo "$now $band" >"$stamp" 2>/dev/null || :

# --- say it --------------------------------------------------------------------------------------
# Raw stdout is discarded by the harness; the JSON envelope with `additionalContext` is the only
# channel that reaches the model (docs/agent-harness.md, verified against 2.1.223).
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
