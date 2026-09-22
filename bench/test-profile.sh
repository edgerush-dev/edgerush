#!/usr/bin/env bash
# Exercise profile orchestration without perf privileges, sockets or CPU changes.
set -euo pipefail
source <(sed '/^command=${1:-}/,$d' "$(dirname "$0")/run.sh")
OUT=$(mktemp -d)
trap 'rm -rf -- "$OUT"' EXIT
export OUT
DURATION=10 proxy_pid=$$ streamed_rate=20
FAIL_PERF= FAIL_LOAD=

sleep() { :; }
sudo() { shift; "$@"; }
chown() { :; }
perf() {
    local action=$1 destination=
    shift
    printf '%s %s\n' "$action" "$*" >>"$OUT/calls"
    [ "$action" != "$FAIL_PERF" ] || return 7
    while [ "$#" -gt 0 ]; do
        if [ "$1" = -o ]; then destination=$2; shift; fi
        shift
    done
    [ -z "$destination" ] || printf 'fixture\n' >"$destination"
    printf 'report\n'
}
streamed_request() { printf 'upload\n' >>"$OUT/load"; [ -z "$FAIL_LOAD" ]; }
streamed_answer() { printf 'answer\n' >>"$OUT/load"; [ -z "$FAIL_LOAD" ]; }

for body_direction in upload answer; do
    profile_body_runs "ours.$body_direction"
    test -s "$OUT/ours.$body_direction.streamed-$body_direction.self.txt"
    test -s "$OUT/ours.$body_direction.streamed-$body_direction.stacks.txt"
done
grep -q 'record .*cycles .*dwarf,16384' "$OUT/calls"
grep -q 'stat .*cycles:u,cycles:k,instructions:u,instructions:k' "$OUT/calls"
test "$(cat "$OUT/load")" = $'upload\nanswer'
for FAIL_PERF in record stat report; do
    if profile_body_runs "ours.failed-$FAIL_PERF"; then
        echo "failed $FAIL_PERF was reported as success" >&2
        exit 1
    fi
done
FAIL_PERF= FAIL_LOAD=yes
if profile_body_runs ours.failed-load; then
    echo 'failed load was reported as success' >&2
    exit 1
fi
# A failed generator must reach the caller rather than being hidden by the final echo.
cpu() { :; }
taskset() { shift 2; "$@"; }
GEN_CPUS=0 PROXY_CPUS=0
measured successful-command true
if measured failed-command false; then
    echo 'measured hid a generator failure' >&2
    exit 1
fi
echo 'profile orchestration tests passed'
