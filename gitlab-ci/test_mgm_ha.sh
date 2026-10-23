#!/usr/bin/env bash
# EOS MGM failover regression test for the disposable eos-on-k8s fixture.
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
set -euo pipefail

if [[ ${1:-} == --help ]]; then
  cat <<'EOF'
Usage: gitlab-ci/test_mgm_ha.sh

Run after create-all.sh -m 2 in an isolated eos-on-k8s test fixture. Requires
K8S_NAMESPACE and KUBECONFIG, and namespace label eos.cern.ch/ha-test=true.
The fixture must contain exactly eos-mgm1, eos-mgm2, and eos-cli1. This test
crashes and restarts both MGM daemons and modifies only its marked namespace.
EOF
  exit 0
fi
[[ $# == 0 ]] || { echo 'error: unexpected arguments' >&2; exit 2; }

log() { printf '[mgm-ha] %s\n' "$*"; }
fail() { log "ERROR: $*" >&2; exit 1; }
namespace=${K8S_NAMESPACE:?K8S_NAMESPACE is required}
[[ -n ${KUBECONFIG:-} ]] || fail 'KUBECONFIG is required'
# shellcheck source=gitlab-ci/utilities_func_for_tests.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/utilities_func_for_tests.sh" --type k8s "$namespace"
for executable in kubectl timeout awk sort; do
  command -v "$executable" >/dev/null || fail "missing runner tool: $executable"
done

run_id="$$-${RANDOM}"
alias_host="eos-mgm.${namespace}.svc.cluster.local"
test_path="/eos/dockertest/mgm-ha-${run_id}"
phase_timeout=180
last_error=''
leader=-1
alias_created=false
path_created=false
client_tmp=''
payload_sha=''
data_files=()
mgm_pods=()
mgm_hosts=()
mgm_ips=()
fst_pods=()
stopped=(false false)
hook_index=-1
hook_dir=''
hook_configured=false
hook_loaded=false
storage_check="$(dirname -- "${BASH_SOURCE[0]}")/check_mgm_ha_storage.py"

# Both kubectl and its command inside the pod have deadlines. A lost MGM must
# not leave an XRootD retry or an orphaned exec running past the test phase.
[[ $(k8s_cmd get namespace "$namespace" -o 'jsonpath={.metadata.labels.eos\.cern\.ch/ha-test}') == true ]] ||
  fail 'namespace must be deliberately marked eos.cern.ch/ha-test=true'
pod_command() {
  local pod=$1
  shift
  # shellcheck disable=SC2016 # Expand arguments and environment inside the pod.
  exec_cmd_argv "$pod" bash -lc '
    export PATH=/opt/eos/xrootd/bin:$PATH
    export XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/eos.keytab
    export EOS_SKIP_SSS_HOSTNAME_MATCH=1
    export XRD_REQUESTTIMEOUT=10 XRD_CONNECTIONWINDOW=5 XRD_CONNECTIONRETRY=1
    timeout -s KILL 20 "$@"
  ' mgm-ha "$@"
}
eos_command() {
  local host=$1
  shift
  pod_command "$client_pod" eos -b -r 0 0 "root://${host}:1094" "$@"
}
wait_for() {
  local description=$1 deadline=$((SECONDS + phase_timeout)) previous=''
  shift
  log "Waiting: $description (deadline ${phase_timeout}s)"
  if poll_until "$deadline" 2 report_pending "$@"; then
    log "Ready: $description"
    return 0
  fi
  fail "$description timed out: $last_error"
}
report_pending() {
  if [[ $last_error != "$previous" ]]; then
    log "Pending: $last_error"
    previous=$last_error
  fi
}

# The opaque is_master query is local and does not follow MGM write redirects.
# Only the protocol's ENOENT response denotes a follower; authentication and
# connection errors never count as evidence of the follower role.
probe_role() {
  local pod=$1 host=$2 output rc
  if output=$(pod_command "$pod" xrdfs "$host:1094" query opaquefile \
      '/?mgm.pcmd=is_master' 2>&1); then
    probe_result=leader
    return 0
  else
    rc=$?
  fi
  if [[ $output == *'[3011]'* && $output == *'No such file or directory'* ]]; then
    probe_result=follower
    return 0
  fi
  last_error="is_master probe on $host failed (rc=$rc): $output"
  return 1
}
check_status() {
  local index=$1 expected_role=$2 expected_leader=$3 output local_id role master
  local is_master master_id expected_is_master=false
  if [[ $expected_role == leader ]]; then expected_is_master=true; fi
  if ! output=$(pod_command "${mgm_pods[index]}" eos -b -r 0 0 \
      root://localhost:1094 ns stat -m 2>&1); then
    last_error="namespace status on ${mgm_hosts[index]} failed: $output"
    return 1
  fi
  if ! local_id=$(monitor_value "$output" ns.mgm.local) ||
      ! role=$(monitor_value "$output" ns.mgm.role) ||
      ! master=$(monitor_value "$output" ns.mgm.leader) ||
      ! is_master=$(monitor_value "$output" is_master) ||
      ! master_id=$(monitor_value "$output" master_id); then
    last_error="missing or duplicate namespace leadership fields on ${mgm_hosts[index]}"
    return 1
  fi
  if [[ $local_id != "${mgm_hosts[index]}:1094" || $role != "$expected_role" ||
        $master != "${mgm_hosts[expected_leader]}:1094" ||
        $is_master != "$expected_is_master" ||
        $master_id != "${mgm_hosts[expected_leader]}:1094" ]]; then
    last_error="${mgm_hosts[index]} reports local=$local_id role=$role leader=$master is_master=$is_master master_id=$master_id; expected role=$expected_role is_master=$expected_is_master leader=${mgm_hosts[expected_leader]}:1094"
    return 1
  fi
}
check_pair() {
  local expected=${1:--1} first second selected
  probe_role "${mgm_pods[0]}" localhost || return 1
  first=$probe_result
  probe_role "${mgm_pods[1]}" localhost || return 1
  second=$probe_result
  if [[ $first == leader && $second == follower ]]; then
    selected=0
  elif [[ $first == follower && $second == leader ]]; then
    selected=1
  else
    last_error="expected one leader and one follower, got $first/$second"
    return 1
  fi
  if (( expected >= 0 && selected != expected )); then
    last_error="MGM $selected leads, expected MGM $expected"
    return 1
  fi
  check_status 0 "$first" "$selected" || return 1
  check_status 1 "$second" "$selected" || return 1
  leader=$selected
}
check_active() {
  local index=$1 phase=$2 output
  probe_role "${mgm_pods[index]}" localhost || return 1
  [[ $probe_result == leader ]] || {
    last_error="${mgm_hosts[index]} has not taken leadership"; return 1;
  }
  check_status "$index" leader "$index" || return 1
  if ! output=$(eos_command "${mgm_hosts[index]}" mkdir -p \
      "$test_path/election-$phase" 2>&1); then
    last_error="new leader does not accept metadata writes: $output"
    return 1
  fi
  leader=$index
}

check_alias() {
  local index=$1 addresses
  if ! addresses=$(pod_command "$client_pod" getent ahostsv4 "$alias_host" 2>&1); then
    last_error="client DNS resolution of $alias_host failed: $addresses"
    return 1
  fi
  addresses=$(awk '{print $1}' <<< "$addresses" | sort -u)
  [[ $addresses == "${mgm_ips[index]}" ]] || {
    last_error="alias DNS returns [$addresses], expected ${mgm_ips[index]}"; return 1;
  }
  probe_role "$client_pod" "$alias_host" || return 1
  [[ $probe_result == leader ]] || {
    last_error='alias reaches a follower despite a successful DNS lookup'; return 1;
  }
}
set_alias() {
  local index=$1
  log "DNS alias $alias_host -> ${mgm_hosts[index]}"
  if [[ $alias_created == false ]]; then
    alias_created=true
    k8s_cmd create -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: eos-mgm
  namespace: $namespace
  labels:
    eos-ha-test: "$run_id"
spec:
  type: ExternalName
  externalName: ${mgm_hosts[index]}
EOF
  else
    k8s_cmd patch service eos-mgm --type merge \
      --patch "{\"spec\":{\"externalName\":\"${mgm_hosts[index]}\"}}"
  fi
  wait_for "client DNS and direct alias probe select MGM $index" check_alias "$index"
}

visible_from() {
  local index=$1 output filename
  if ! output=$(eos_command "${mgm_hosts[index]}" attr -V get user.mgm_ha "$test_path" 2>&1) ||
      [[ $output != "$run_id" ]]; then
    last_error="test metadata not visible through ${mgm_hosts[index]}: $output"
    return 1
  fi
  for filename in "${data_files[@]}"; do
    if ! output=$(eos_command "${mgm_hosts[index]}" ls -d \
        "$test_path/$filename" 2>&1); then
      last_error="payload metadata for $filename not visible through ${mgm_hosts[index]}: $output"
      return 1
    fi
  done
}
upload_payload() {
  pod_command "$client_pod" xrdcp -f "$client_tmp/payload" \
    "root://${alias_host}:1094/$test_path/$1"
}
initial_payload_ready() {
  local output rc
  if output=$(upload_payload payload-initial 2>&1); then
    return 0
  else
    rc=$?
  fi
  last_error="initial payload upload failed (rc=$rc): $output"
  # Startup can briefly precede writable replica placement on the FSTs.
  [[ $rc == 54 && $output == *'[3009]'* &&
      $output == *'Unable to get free physical space'* ]] || fail "$last_error"
  return 1
}
write_data() {
  local phase=$1 filename="payload-$1"
  local index
  for index in 0 1; do
    log "Checking actual FST storage before phase $phase (FST $((index + 1)))"
    if ! k8s_cmd exec -i "${fst_pods[index]}" -- timeout -s KILL 20 \
        python3 - "/home/data/eos$((index + 1))" < "$storage_check"; then
      fail "FST $((index + 1)) storage preflight failed before phase $phase"
    fi
  done
  log "Writing metadata and payload through alias (phase $phase)"
  eos_command "$alias_host" mkdir "$test_path/alias-$phase"
  if [[ $phase == initial ]]; then
    wait_for 'initial payload has writable replica placement' initial_payload_ready
  else
    upload_payload "$filename"
  fi
  data_files+=("$filename")
}
check_follower_writes() {
  local follower=$1 phase=$2 path="$test_path/follower-$2" output rc
  log "Checking legacy and protobuf writes through follower MGM $follower (phase $phase)"
  (( follower != leader )) && [[ ${stopped[follower]} == false ]] ||
    fail "MGM $follower is not a running follower"
  eos_command "${mgm_hosts[follower]}" mkdir "$path"
  eos_command "${mgm_hosts[leader]}" ls -d --no-globbing "$path"
  eos_command "${mgm_hosts[follower]}" rm -r --no-confirmation "$path"
  if output=$(eos_command "${mgm_hosts[leader]}" ls -d --no-globbing "$path" 2>&1); then
    fail "protobuf rm through follower left $path visible on leader"
  else
    rc=$?
  fi
  # The EOS ls response must specifically report ENOENT. A transport failure
  # or permission error cannot stand in for successful deletion.
  [[ $rc == 2 && $output == *'(errc=2)'* &&
      $output == *'No such file or directory'* ]] ||
    fail "could not verify deletion of $path on leader (rc=$rc): $output"
}
verify_data() {
  local wait_for_storage=${1:-false} filename actual output rc
  if ! output=$(eos_command "$alias_host" attr -V get user.mgm_ha "$test_path" 2>&1); then
    fail "could not read test metadata: $output"
  fi
  [[ $output == "$run_id" ]] || fail 'metadata changed across failover'
  for filename in "${data_files[@]}"; do
    log "Reading and hashing $filename through alias"
    if output=$(pod_command "$client_pod" xrdcp -f \
        "root://${alias_host}:1094/$test_path/$filename" "$client_tmp/download" 2>&1); then
      :
    else
      rc=$?
      last_error="payload read failed for $filename (rc=$rc): $output"
      # Cold boot restores namespace leadership before the FST heartbeat and
      # scheduler snapshot necessarily make replicas readable. Earlier reads
      # remain strict so this wait cannot hide a runtime failover regression.
      [[ $wait_for_storage == true && $rc == 54 && $output == *'[3014]'* &&
          $output == *'Unable to open file'* && $output == *'Network is unreachable'* ]] ||
        fail "$last_error"
      return 1
    fi
    if ! actual=$(pod_command "$client_pod" sha256sum "$client_tmp/download" 2>&1); then
      fail "could not hash downloaded payload $filename: $actual"
    fi
    [[ ${actual%% *} == "$payload_sha" ]] || fail "payload checksum mismatch: $filename"
  done
}

# Match only the MGM's xrootd argv, never the kubectl/bash exec wrapper or MQ.
mgm_control() {
  local index=$1 action=$2
  # shellcheck disable=SC2016 # This shell and its PID inspection run in the pod.
  k8s_cmd exec "${mgm_pods[index]}" -- timeout -s KILL 20 bash -lc '
    set -eo pipefail
    find_mgm() {
      local pid arguments
      for pid in $(pgrep -x xrootd || true); do
        [[ -r /proc/$pid/cmdline ]] || continue
        arguments=$(tr "\0" " " < /proc/$pid/cmdline)
        case "$arguments" in
          /opt/eos/xrootd/bin/xrootd\ -n\ mgm\ *|/usr/bin/xrootd\ -n\ mgm\ *)
            printf "%s\n" "$pid" ;;
        esac
      done
    }
    mapfile -t pids < <(find_mgm)
    if [[ $1 == stop || $1 == stop-if-running ]]; then
      [[ $1 == stop-if-running && ${#pids[@]} == 0 ]] && exit 0
      [[ ${#pids[@]} == 1 ]] || { echo "expected exactly one MGM xrootd PID" >&2; exit 1; }
      kill -KILL "${pids[0]}"
      for ((attempt=0; attempt<10; attempt++)); do
        [[ -z $(find_mgm) ]] && exit 0
        sleep 1
      done
      echo "MGM process still runs after SIGKILL" >&2
      exit 1
    fi
    [[ ${#pids[@]} == 0 ]] || { [[ ${#pids[@]} == 1 ]] && exit 0; exit 1; }
    source /etc/sysconfig/eos
    source /eos_preload.sh
    binary=/opt/eos/xrootd/bin/xrootd
    [[ -x $binary ]] || binary=/usr/bin/xrootd
    "$binary" -n mgm -c /etc/xrd.cf.mgm -l /var/log/eos/xrdlog.mgm -b -Rdaemon
  ' mgm-ha "$action"
}
stop_mgm() {
  local index=$1
  log "Hard failover: stopping MGM $index (${mgm_pods[index]})"
  stopped[index]=true
  mgm_control "$index" stop
}
start_mgm() {
  local index=$1
  log "Restarting MGM $index in its existing pod"
  mgm_control "$index" start || return $?
  stopped[index]=false
}
install_boot_hook() {
  hook_index=$1
  hook_dir="/tmp/eos-mgm-ha-hook-${run_id}"
  hook_configured=true
  # shellcheck disable=SC2016 # Install literal hook arguments inside the pod.
  pod_command "${mgm_pods[hook_index]}" bash -lc '
    set -eu
    directory=$1 owner=$2
    [[ ! -e $directory ]] || { echo "hook directory already exists" >&2; exit 1; }
    if grep -Eq "^[[:space:]]*mgmofs[.]postslavetomaster([[:space:]]|$)" /etc/xrd.cf.mgm; then
      echo "refusing to replace an existing post-promotion hook" >&2
      exit 1
    fi
    mkdir -m 0755 "$directory"
    printf "%s\n" "$owner" > "$directory/owner"
    printf "%s\n" "#!/bin/bash" "set -eu" \
      "printf \"%s\\t%s\\n\" \"\$1\" \"\$2\" >> \"$directory/invocations\"" > "$directory/hook"
    chmod 0755 "$directory/hook"
    touch "$directory/invocations"
    chown daemon:daemon "$directory/invocations"
    chmod 0600 "$directory/invocations"
    printf "\nmgmofs.postslavetomaster %s/hook\n" "$directory" >> /etc/xrd.cf.mgm
  ' mgm-ha-hook-install "$hook_dir" "$run_id"
}
check_boot_hook() {
  local output
  if ! output=$(pod_command "${mgm_pods[hook_index]}" cat "$hook_dir/invocations" 2>&1); then
    last_error="could not read the boot post-promotion hook record: $output"
    return 1
  fi
  # A fresh process starts with no remembered master identity. Require one
  # invocation with both arguments, not merely a file created during startup.
  [[ $output == $'\t'"${mgm_hosts[hook_index]}:1094" ]] || {
    last_error="boot post-promotion hook must run exactly once with the new master identity; got [$output]"
    return 1
  }
}
restore_hook_config() {
  # Remove only our exact directive, retaining any other configuration changes.
  # shellcheck disable=SC2016 # Ownership checks run inside the pod.
  pod_command "${mgm_pods[hook_index]}" bash -lc '
    set -eu
    directory=$1 owner=$2
    [[ -e $directory ]] || exit 0
    [[ $(cat "$directory/owner") == "$owner" ]] || exit 1
    awk -v owned="mgmofs.postslavetomaster $directory/hook" \
      "\$0 != owned" /etc/xrd.cf.mgm > "$directory/config-restored"
    cat "$directory/config-restored" > /etc/xrd.cf.mgm
  ' mgm-ha-hook-restore "$hook_dir" "$run_id" || return $?
  hook_configured=false
}
remove_hook_files() {
  # shellcheck disable=SC2016 # Only delete this run's owned temporary files.
  pod_command "${mgm_pods[hook_index]}" bash -lc '
    set -eu
    [[ -e $1 ]] || exit 0
    [[ $(cat "$1/owner") == "$2" ]] || exit 1
    rm -rf -- "$1"
  ' mgm-ha-hook-remove "$hook_dir" "$run_id" || return $?
  hook_dir=''
}
diagnostics() {
  log 'Failure diagnostics'
  k8s_cmd get pods,services,endpoints -o wide || true
  k8s_cmd get events --field-selector type=Warning --sort-by=.lastTimestamp || true
  local pod
  for pod in "${fst_pods[@]}"; do
    log "Actual capacity and inode counts on $pod"
    pod_command "$pod" df -hT / /var /home/data || true
    pod_command "$pod" df -i / /var /home/data || true
  done
  local index
  for index in 0 1; do
    [[ -n ${mgm_pods[index]:-} ]] || continue
    log "Direct status on ${mgm_pods[index]}"
    pod_command "${mgm_pods[index]}" xrdfs localhost:1094 query opaquefile \
      '/?mgm.pcmd=is_master' || true
    pod_command "${mgm_pods[index]}" eos -b -r 0 0 \
      root://localhost:1094 ns stat -m || true
    pod_command "${mgm_pods[index]}" eos -b -r 0 0 \
      root://localhost:1094 fs ls -m || true
  done
}
cleanup() {
  local rc=$? index owner cleanup_failed=false
  trap - EXIT
  set +e
  (( rc == 0 )) || diagnostics
  if [[ $hook_configured == true ]] && ! restore_hook_config; then
    log 'ERROR: could not restore the post-promotion hook configuration'
    cleanup_failed=true
  fi
  if [[ -n $hook_dir && $hook_configured == false ]]; then
    if [[ $hook_loaded == true ]]; then
      # A failed start may still have spawned xrootd; the bookkeeping flag is
      # insufficient proof that the process no longer has our hook loaded.
      stopped[hook_index]=true
      if mgm_control "$hook_index" stop-if-running; then
        hook_loaded=false
      else
        log 'ERROR: could not stop the MGM with the temporary hook; retaining its owned files'
        cleanup_failed=true
      fi
    fi
    if [[ $hook_loaded == false ]] && ! remove_hook_files; then cleanup_failed=true; fi
  fi
  for index in 0 1; do
    if [[ ${stopped[index]} == true ]]; then
      if ! start_mgm "$index"; then
        log "ERROR: recovery of MGM $index failed"
        cleanup_failed=true
      fi
    fi
  done
  if [[ $path_created == true ]]; then
    if ! eos_command "${mgm_hosts[0]}" rm -r --no-confirmation "$test_path" &&
        ! eos_command "${mgm_hosts[1]}" rm -r --no-confirmation "$test_path"; then
      log "ERROR: could not remove owned test path $test_path"
      cleanup_failed=true
    fi
  fi
  if [[ -n $client_tmp ]]; then
    if ! pod_command "$client_pod" rm -rf -- "$client_tmp"; then
      log "ERROR: could not remove owned client temporary directory $client_tmp"
      cleanup_failed=true
    fi
  fi
  if [[ $alias_created == true ]]; then
    owner=$(k8s_cmd get service eos-mgm -o 'jsonpath={.metadata.labels.eos-ha-test}')
    if [[ $owner == "$run_id" ]]; then
      if ! k8s_cmd delete service eos-mgm; then
        log 'ERROR: could not remove owned DNS alias'
        cleanup_failed=true
      fi
    else
      log 'ERROR: DNS alias ownership could not be verified during cleanup'
      cleanup_failed=true
    fi
  fi
  if [[ $rc == 0 && $cleanup_failed == true ]]; then rc=1; fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

log "Testing two-MGM HA in marked disposable namespace $namespace"
pod_names=$(k8s_cmd get pods -o 'jsonpath={range .items[*]}{.metadata.labels.app}{" "}{.metadata.name}{"\n"}{end}')
[[ $(awk '$1 ~ /^eos-mgm[0-9]+$/ {count++} END {print count+0}' <<< "$pod_names") == 2 ]] ||
  fail 'expected exactly two MGM pods'
for index in 0 1; do
  name="eos-mgm$((index + 1))"
  pod=$(awk -v app="$name" '$1==app {print $2}' <<< "$pod_names")
  [[ -n $pod && $pod != *$'\n'* ]] || fail "expected exactly one concrete pod for $name"
  mgm_pods+=("$pod")
  host=$(pod_command "$pod" hostname -f)
  [[ $host == "$name.$name.$namespace.svc.cluster.local" ]] ||
    fail "unexpected MGM hostname: $host"
  mgm_hosts+=("$host")
  ip=$(k8s_cmd get pod "$pod" -o 'jsonpath={.status.podIP}')
  [[ $ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "missing IPv4 pod address for $name"
  mgm_ips+=("$ip")
done
[[ $(awk '$1 ~ /^eos-fst[0-9]+$/ {count++} END {print count+0}' <<< "$pod_names") == 2 ]] ||
  fail 'expected exactly two FST pods'
for index in 0 1; do
  name="eos-fst$((index + 1))"
  pod=$(awk -v app="$name" '$1==app {print $2}' <<< "$pod_names")
  [[ -n $pod && $pod != *$'\n'* ]] || fail "expected exactly one concrete pod for $name"
  fst_pods+=("$pod")
done
client_pod=$(awk '$1=="eos-cli1" {print $2}' <<< "$pod_names")
[[ -n $client_pod && $client_pod != *$'\n'* ]] || fail 'expected exactly one eos-cli1 pod'
existing_alias=$(k8s_cmd get service eos-mgm --ignore-not-found -o name)
[[ -z $existing_alias ]] || fail 'refusing to overwrite an existing eos-mgm Service'
for pod in "${mgm_pods[@]}" "$client_pod"; do
  pod_command "$pod" test -r /etc/eos.keytab
done
wait_for 'one healthy leader, one healthy follower, and accurate local namespace status' check_pair
original_leader=$leader
other=$((1 - original_leader))
set_alias "$original_leader"
path_created=true
eos_command "$alias_host" mkdir "$test_path"
eos_command "$alias_host" attr set "user.mgm_ha=$run_id" "$test_path"
eos_command "$alias_host" attr set default=replica "$test_path"
temporary_path=$(pod_command "$client_pod" mktemp -d /tmp/eos-mgm-ha.XXXXXXXX)
[[ $temporary_path =~ ^/tmp/eos-mgm-ha\.[A-Za-z0-9]+$ ]] || fail 'unexpected client temporary path'
client_tmp=$temporary_path
pod_command "$client_pod" dd if=/dev/urandom of="$client_tmp/payload" bs=1024 count=64 status=none
checksum=$(pod_command "$client_pod" sha256sum "$client_tmp/payload")
payload_sha=${checksum%% *}
[[ $payload_sha =~ ^[a-f0-9]{64}$ ]] || fail 'could not hash the source payload'
write_data initial
check_follower_writes "$other" initial
wait_for 'shared metadata visible on follower before failover' visible_from "$other"
verify_data

stop_mgm "$original_leader"
wait_for 'the other MGM takes leadership and accepts writes' check_active "$other" first
set_alias "$other"
verify_data
write_data first
start_mgm "$original_leader"
wait_for 'the restarted MGM rejoins as a healthy follower' check_pair "$other"
wait_for 'shared metadata visible on the restored follower' visible_from "$original_leader"
check_follower_writes "$original_leader" first-rejoin

stop_mgm "$other"
wait_for 'reverse failover restores the original leader and accepts writes' \
  check_active "$original_leader" reverse
set_alias "$original_leader"
verify_data
write_data reverse
start_mgm "$other"
wait_for 'both MGMs are healthy with exactly one original leader' check_pair "$original_leader"
wait_for 'shared metadata visible on the final follower' visible_from "$other"
check_follower_writes "$other" reverse-rejoin

# Keep the peer stopped so this process must acquire leadership during its
# initial boot, rather than exercising another runtime follower promotion.
install_boot_hook "$original_leader"
stop_mgm "$other"
stop_mgm "$original_leader"
hook_loaded=true
start_mgm "$original_leader"
wait_for 'the sole restarted MGM boots as master and accepts writes' \
  check_active "$original_leader" boot
wait_for 'the boot post-promotion hook completes exactly once' check_boot_hook

# Restore the original configuration in memory as well as on disk before the
# peer rejoins. The EXIT trap repeats recovery if any preceding step fails.
restore_hook_config
stop_mgm "$original_leader"
remove_hook_files
start_mgm "$original_leader"
hook_loaded=false
wait_for 'the original leader boots with its original hook configuration' \
  check_active "$original_leader" restored
start_mgm "$other"
wait_for 'both MGMs are healthy after the boot-promotion regression' check_pair "$original_leader"
wait_for 'post-boot payload replicas are readable with matching checksums' verify_data true
log 'PASS: local leadership, DNS alias, writes, payloads, and boot post-promotion hook survived failover'
