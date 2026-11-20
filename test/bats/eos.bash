# ------------------------------------------------------------------------------
# Shared helpers of the EOS BATS suites. Each file of a suite loads it first,
# directly or from the helper file of the suite:
#
#   load ../bats/eos
#
# It sets:
#   $suite     the name of the suite directory, e.g. contention
#   $testdir   $EOS_TEST_PREFIX/$suite, the EOS directory of the suite
#   $logs      the local log directory, default ${TMPDIR:-/tmp}/eos-$suite.
#              $logs/checks gets the output of each test.
#   $url       the MGM URL for xrdcp and xrdfs, $EOS_MGM_URL or root://localhost
# Set logs before the load to change it. See README.md for a new suite.
# ------------------------------------------------------------------------------

# The bats of the submodule vendor/bats-core (eos-bats-test runs it), with
# the submodules vendor/bats-support and vendor/bats-assert: run -0,
# assert_output ...
bats_require_minimum_version 1.14.0
BATS_LIB_PATH=$(dirname "${BASH_SOURCE[0]}")/vendor${BATS_LIB_PATH:+:$BATS_LIB_PATH}
bats_load_library bats-support
bats_load_library bats-assert

suite=$(basename "$BATS_TEST_DIRNAME")
testdir=${EOS_TEST_PREFIX:+$EOS_TEST_PREFIX/$suite}
logs=${logs:-${TMPDIR:-/tmp}/eos-$suite}
mkdir -p "$logs"
export mgmlog=${mgmlog:-/var/log/eos/mgm/xrdlog.mgm}
url=${url:-${EOS_MGM_URL:-root://localhost}}
# The HTTP(S) endpoint of the MGM: $EOS_HTTP_URL, else the XrdHttp port of the
# MGM configuration on this host (default 8443)
http_port=$(sed -nE 's/^\s*xrd.protocol XrdHttp:([0-9]+).*/\1/p' /etc/xrd.cf.mgm 2>/dev/null | head -1)
http_url=${EOS_HTTP_URL:-https://$(hostname -f):${http_port:-8443}}
# A backstop for each test, in seconds. Each xrootd
# client, the console too, gives up a request after XRD_REQUESTTIMEOUT, so
# that a hung MGM does not stop the run. bats needs pkill or ps for the
# timeout; minimal images (the CI pre-commit one) have neither.
if command -v pkill >/dev/null || command -v ps >/dev/null; then
  export BATS_TEST_TIMEOUT=${BATS_TEST_TIMEOUT:-600}
fi
export XRD_REQUESTTIMEOUT=${XRD_REQUESTTIMEOUT:-60}

# ------------------------------------------------------------------------------
# Set up
# ------------------------------------------------------------------------------
# eos_setup_suite: call it first in setup_suite. Clears what the last run
# left in $logs, and fails if the MGM does not answer.
eos_setup_suite() {
  rm -rf $logs/mgm.down $logs/checks $logs/failed
  timeout 15 eos ns stat >/dev/null 2>&1 || { echo "the MGM does not answer"; return 1; }
  run_undo
  wc -l < $mgmlog > $logs/log_start 2>/dev/null || echo 0 > $logs/log_start
}

# eos_teardown_suite: call it in teardown_suite. Undoes the changes of the
# suite (see undo) and removes $testdir.
eos_teardown_suite() {
  run_undo
  remove_testdir
}

# undo <command> ...: run the command at the end of the suite, to undo a
# change of the instance. run_undo runs the commands in reverse order, once.
# eos_setup_suite also runs what an interrupted run left.
undo() {
  echo "$*" >> $logs/undo
}

run_undo() {
  [ -e $logs/undo ] || return 0
  tac $logs/undo | while read -r c; do eval "$c" >/dev/null 2>&1 || echo "undo failed: $c"; done
  rm -f $logs/undo
}

# space_set <space> <key> <value>: set space.<key> of a space, and undo it at
# the end of the suite. <key> as in "eos space status -m", e.g. policy.nstripes.
space_set() {
  local old
  old=$(space_value $1 $2)
  if [ -n "$old" ]; then
    undo eos space config $1 space.$2=$old
  else
    undo eos space config rm $1 space.$2
  fi
  eos space config $1 space.$2=$3
}

# space_value <space> <key>: one value of "eos space status -m", empty if not set
space_value() {
  eos space status $1 -m | tr ' ' '\n' | grep "^$2=" | head -1 | cut -d= -f2- || true
}

# fs_set <fsid> <key> <value>: "eos fs config <fsid> <key>=<value>", undone at
# the end of the suite, e.g. fs_set 3 configstatus drain
fs_set() {
  undo eos fs config $1 $2=$(fs_value $1 $2)
  eos fs config $1 $2=$3
}

# fs_value <fsid> <key>: one value of "eos fs ls -m", e.g. configstatus, host
fs_value() {
  eos fs ls -m | grep " id=$1 " | tr ' ' '\n' | grep "^$2=" | head -1 | cut -d= -f2- || true
}

# mgm_log: the lines of the MGM log since the start of the suite
mgm_log() {
  tail -n +$(($(cat $logs/log_start) + 1)) $mgmlog 2>/dev/null || true
}

# order_checking_on: turn on the lock order checking of the MGM, for an end
# check of mgm_log. order_checking_restore turns it off again, if
# order_checking_on turned it on.
order_checking_on() {
  eos ns mutex | grep -q "order checking is : on" && return 0
  eos ns mutex --toggleorder >/dev/null && touch $logs/order.toggled
}

order_checking_restore() {
  [ -e $logs/order.toggled ] || return 0
  eos ns mutex --toggleorder >/dev/null 2>&1
  rm -f $logs/order.toggled
}

# need <part>: skip the test if <part> is in $skip, the parts that the
# instance does not have. A setup_suite finds them and exports skip.
need() {
  [[ " ${skip-} " != *" $1 "* ]] || skip "no $1 on this instance"
}

# make_testdir: an empty $testdir that all users may write
make_testdir() {
  [ -n "$testdir" ] || { echo "set EOS_TEST_PREFIX"; return 1; }
  remove_testdir
  eos mkdir -p $testdir && eos chmod 777 $testdir >/dev/null
}

remove_testdir() {
  [ -z "$testdir" ] || eos rm -r -F --no-confirmation $testdir >/dev/null 2>&1 || true
}

# wait_writable <dir>: wait up to 5 min until an upload to <dir> succeeds. A
# new instance refuses writes until its file systems report their free space.
wait_writable() {
  local k f=$1/.writable.$$
  for k in $(seq 1 60); do
    echo x | xrdcp -f -s - ${EOS_MGM_URL:-root://localhost}/$f >/dev/null 2>&1 &&
      { eos rm -F $f >/dev/null 2>&1 || true; return 0; }
    sleep 5
  done
  echo "the instance refuses writes to $1"
  return 1
}

# wait_until <seconds> <command> ...: run the command every second until it
# succeeds, so the test goes on as soon as the state is there. <seconds> is
# only the upper limit. Use it instead of a fixed sleep before a check. At the
# end of the time it fails and prints the last output of the command.
wait_until() {
  local end=$(($(date +%s) + $1)) out
  shift
  until out=$("$@" 2>&1); do
    [ $(date +%s) -lt $end ] || { echo "not true after the wait: $*"; echo "$out" | tail -5; return 1; }
    sleep 1
  done
}

# log_has <regex>: true if the MGM log has a matching line since the start of
# the suite. wait_until 60 log_has "<regex>" waits for an event of the MGM.
log_has() {
  mgm_log | grep -qE "$1"
}

# ------------------------------------------------------------------------------
# Namespace
# ------------------------------------------------------------------------------
# md_field <path> <key>: one value of "eos fileinfo -m", empty if missing
md_field() {
  eos fileinfo $1 -m 2>/dev/null | tr ' ' '\n' | grep "^$2=" | head -1 | cut -d= -f2 || true
}

# json <jq filter> <eos args>: the JSON output of an eos command, through jq,
# e.g. json .id fileinfo $f
json() {
  eos -j "${@:2}" | jq -r "$1"
}

# attr_value <path> <key>: the value of an extended attribute, empty if not set
attr_value() {
  eos attr get $2 $1 2>/dev/null | sed -n 's/^[^=]*="\(.*\)"$/\1/p' || true
}

# replicas <path>: the fsids of the replicas of a file, one per line
replicas() {
  json '.locations[].fsid' fileinfo $1
}

# replica_file <path> <fsid>: the local path of the replica on the FST
replica_file() {
  json ".locations[] | select(.fsid == $2) | .fstpath" fileinfo $1
}

# on_fst <fsid> <command>: run a shell command on the host of a file system,
# e.g. to change a replica on the disk. On this host it runs here; on another
# host through "$EOS_FST_EXEC <host> <command>" (e.g. an ssh or kubectl
# wrapper). Without EOS_FST_EXEC the test skips.
on_fst() {
  local host
  host=$(fs_value $1 host)
  if [ "$host" = "$(hostname -f)" ] || [ "$host" = localhost ]; then
    bash -c "$2"
  elif [ -n "${EOS_FST_EXEC-}" ]; then
    $EOS_FST_EXEC $host "$2"
  else
    skip "no access to the FST host $host: set EOS_FST_EXEC"
  fi
}

# make_file <local> <size>: a local file of random bytes, <size> as for
# head -c, e.g. 16M
make_file() {
  head -c $2 /dev/urandom > $1
}

# is <expected> <command> ...: true if the command prints <expected>, e.g.
# wait_until 600 is drained fs_value 3 local.drain
is() {
  [ "$("${@:2}")" = "$1" ]
}

# eos_as <uid> <gid> <args>: the eos command with the role <uid> <gid>
eos_as() {
  eos -r "$1" "$2" "${@:3}"
}

# token_for <path> <permission> [<seconds>]: a token for <path>, for the user
# nobody, valid for <seconds> (default 600). A <path> that ends with / gives a
# token for the whole tree. Use it as "<url>?authz=<token>".
token_for() {
  local tree=
  [[ $1 != */ ]] || tree=--tree
  eos token --path $1 --permission $2 --expires $(($(date +%s) + ${3:-600})) \
    --owner nobody --group nobody $tree
}

# http <method> <path> [<curl options>]: an HTTP request to the MGM; follows
# the redirect to the FST. Fails on an HTTP error (>= 400) and prints the body.
http() {
  curl -sS -k -L --fail-with-body -m 60 -X $1 "$http_url$2" "${@:3}"
}

# upload <local> <path> [<xrdcp options>]: copy a local file to an EOS path
upload() {
  xrdcp -f -s "${@:3}" "$1" "$url/$2"
}

# download <path> <local> [<xrdcp options>]: copy an EOS file to a local path
download() {
  xrdcp -f -s "${@:3}" "$url/$1" "$2"
}

# same_file <local> <path>: true if the EOS file has the bytes of the local file
# (sha256sum: coreutils is on each image, cmp is not)
same_file() {
  download "$2" $BATS_TEST_TMPDIR/same_file &&
    [ "$(sha256sum < "$1")" = "$(sha256sum < $BATS_TEST_TMPDIR/same_file)" ]
}

# recycle_entries <path>: "<uid> <gid> <key>" of each recycle bin entry whose
# path starts with <path>. "<path> " (with a final space) gives only <path>.
recycle_entries() {
  eos recycle ls -m -n --all 2>/dev/null | grep -F "restore-path=$1" |
    sed -E 's/.* uid=([0-9]+) gid=([0-9]+) .*restore-key=([^ ]+).*/\1 \2 \3/'
}

# recycle_as_owner <"restore -f" or "purge -k"> <path>: act on the recycle bin
# entry of <path> as its owner. Only the owner may.
recycle_as_owner() {
  local uid gid key
  read -r uid gid key <<< "$(recycle_entries "$2 " | tail -1)"
  [ -n "$key" ] || { echo "no recycle bin entry of $2"; return 1; }
  eos -r $uid $gid recycle $1 $key
}

# mount_fuse <dir>: an eosxd mount of /eos at <dir>, ready when $testdir is
# in it. Without a configuration file eosxd needs a host name with a domain.
# fd 3 is closed: bats waits for each process that holds it. Fails if there
# is no mount: "mount_fuse $m || skip ..." where the instance can have none.
mount_fuse() {
  mkdir -p $1
  eosxd -ofsname=localhost.localdomain:/eos $1 > $logs/eosxd.log 2>&1 3>&- 9>&- &&
    wait_until 30 test -d "$1/${testdir#/eos/}"
}

# umount_fuse <dir>: unmount, and wait until eosxd exits (it still flushes
# writes)
umount_fuse() {
  mountpoint -q $1 2>/dev/null || return 0
  umount $1 2>/dev/null || umount -l $1 || true
  wait_until 30 none pgrep -f "eosxd.*$1"
}

# fs_ids: the ids of all file systems, one per line
fs_ids() {
  eos fs ls -m | grep -oE " id=[0-9]+" | cut -d= -f2
}

# ------------------------------------------------------------------------------
# Checks
# ------------------------------------------------------------------------------
# none <command> ...: true if the command prints nothing. Else print what it
# printed and how many lines.
none() {
  local out
  out=$("$@") || true
  [ -z "$out" ] && return 0
  echo "$out"
  echo "$(echo "$out" | wc -l) found"
  return 1
}

# together <command> ...: run the command lines all at the same time, print
# the exit code of each, one per line. A failed command is a result, not an
# error of the test.
together() {
  local pids=() c pid rc

  for c in "$@"; do
    (set +e; eval "$c") >/dev/null 2>&1 &
    pids+=($!)
  done

  for pid in ${pids[@]}; do
    rc=0
    wait $pid || rc=$?
    echo $rc
  done
}

# run_race <n> <setup> <command>: run the setup, then n copies of the command
# at the same time ($k is the number of the copy). ok is the number of
# successes, also for the next test in $logs/ok.
run_race() {
  local cmds=() k
  (eval "$2") >> $logs/checks 2>&1 || { echo "the set up failed"; return 1; }

  for k in $(seq 1 $1); do
    cmds+=("k=$k; $3")
  done

  ok=$(together "${cmds[@]}" | grep -cx 0 || true)
  echo $ok > $logs/ok
  echo "$ok of $1 succeed"
}

# run_pairs <setup> <a> <b>: 8 pairs of a and b at the same time, with the
# setup for each pair ($k is the number of the pair). both is the number of
# pairs in which both succeed.
run_pairs() {
  local cmds=() k

  for k in $(seq 1 8); do
    (eval "$1") >> $logs/checks 2>&1 || { echo "the set up failed"; return 1; }
    cmds+=("k=$k; $2" "k=$k; $3")
  done

  both=$(together "${cmds[@]}" | paste -d ' ' - - | grep -cx "0 0" || true)
  echo "both succeed in $both of 8 pairs"
}

# ------------------------------------------------------------------------------
# Logs
# ------------------------------------------------------------------------------
# dump_stacks <why>: save the MGM thread stacks in $logs/stacks/mgm.txt, once
dump_stacks() {
  mkdir $logs/stacks 2>/dev/null || return 0
  echo "$(date +%T) $*" > $logs/stacks/mgm.txt
  eu-stack -p $(pgrep -f "xrootd -n mgm" | head -1) >> $logs/stacks/mgm.txt 2>&1 || true
}

# ------------------------------------------------------------------------------
# A crashed or hung MGM: the tests after it skip at once, they do not each
# wait for the timeouts
# ------------------------------------------------------------------------------
# mgm_down <why>: mark the MGM as down for the next tests. Saves the MGM
# stacks if the process is still there (a hang).
mgm_down() {
  [ ! -e $logs/mgm.down ] || return 0

  if pgrep -f "xrootd -n mgm" >/dev/null; then
    echo "$1 (the MGM does not answer)" > $logs/mgm.down
    dump_stacks "the MGM does not answer after: $1"
  else
    echo "$1 (no MGM process)" > $logs/mgm.down
  fi

  echo "# the MGM is down after: $(< $logs/mgm.down). The next tests skip." >&3
}

# The set up of each test. A file with its own setup calls eos_setup in it.
eos_setup() {
  [ ! -e $logs/mgm.down ] || skip "the MGM is down since: $(< $logs/mgm.down)"
  wc -l < $mgmlog > $logs/test_start 2>/dev/null || echo 0 > $logs/test_start
}

# The teardown of each test: save the MGM stacks at a timeout, and add the
# output of the test to $logs/checks. BATS_OUT is the file where bats collects
# that output. After a failure or a timeout, save the MGM log lines of the
# test in $logs/failed/<n>.mgm.log and check that the MGM answers. A file with
# its own teardown calls eos_teardown in it.
eos_teardown() {
  [ -z "${BATS_TIMED_OUT-}" ] || dump_stacks "timeout of: $BATS_TEST_DESCRIPTION"
  { echo "== $BATS_TEST_DESCRIPTION"; cat "$BATS_OUT"; } >> $logs/checks 2>/dev/null
  [ -z "${BATS_TEST_COMPLETED-}${BATS_TEST_SKIPPED-}" ] || return 0

  mkdir -p $logs/failed
  { echo "== $BATS_TEST_DESCRIPTION"
    tail -n +$(($(cat $logs/test_start 2>/dev/null || echo 0) + 1)) $mgmlog 2>/dev/null
  } > $logs/failed/$BATS_SUITE_TEST_NUMBER.mgm.log

  if [ ! -e $logs/mgm.down ] && ! timeout 15 eos ns stat >/dev/null 2>&1; then
    mgm_down "$BATS_TEST_DESCRIPTION"
  fi

  return 0
}

setup() {
  eos_setup
}

teardown() {
  eos_teardown
}
