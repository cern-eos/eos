# ------------------------------------------------------------------------------
# Contention test: settings and helpers. Each file of the suite loads this file.
#
# Many clients change the same files and directories at the same time, through
# xrootd, the console, gRPC, REST and an eosxd mount. The test fails if an
# operation hangs, or if the namespace, the quota and the file systems do not
# agree at the end.
#
# Usage: eos-bats-test contention <prefix> [<bats options>]
#   CONTENTION_DURATION   the seconds of the load (default 120)
#
# Run it on the MGM host of a test instance with no other activity: some checks
# compare the whole namespace before and after the test. It works in
# <prefix>/contention, with the shared directories d0 .. d3. The files run in
# order: 1-races.bats, 2-load.bats (one file system is drained meanwhile), then
# 3-end-checks.bats.
# ------------------------------------------------------------------------------

load ../bats/eos

# The commands of the load run in their own shell: export the settings
set -a
base=$testdir
duration=${CONTENTION_DURATION:-120}
host=localhost
url=root://$host
grpc_ep=$host:50051
grpc="eos-grpc-ns --endpoint $grpc_ep"
rest=http://127.0.0.1:40054/v1/eos/rest/gateway
mnt=$logs/mnt
src=$logs/file.1k
srcb=$logs/file.1k.b
srcbig=$logs/file.8m
empty=$logs/file.0
hot=$base/hot/h
r=$base/race
mr=$mnt/${r#/eos/}
set +a
# An array does not pass to the next file: the set up exports a list
read -ra fsids <<< "${fsids_list-}"
load_kind=()
load_name=()
load_cmd=()

info() {
  echo "# $*" >&3
}

# ------------------------------------------------------------------------------
# Functions for the commands of the tests. The load runs its commands in a new
# shell: export them.
# ------------------------------------------------------------------------------
# drop_replica <file>: drop the first replica if the file has 2 or more. eos
# file drop also drops the last one, and two workers that drop one replica
# each would leave the file without data.
drop_replica() {
  local md nrep
  md=$(eos fileinfo $1 -m 2>/dev/null | tr ' ' '\n')
  nrep=$(grep -m1 "^nrep=" <<< "$md" | cut -d= -f2)
  [ "${nrep:-0}" -ge 2 ] || return 0
  eos file drop $1 $(grep -m1 "^fsid=" <<< "$md" | cut -d= -f2)
}

# rest_call <command> <path>: a request to the REST gateway
rest_call() {
  curl -fs -m 60 -X POST $rest/$1 -d "{\"md\":{\"path\":\"$(echo -n $2 | base64 -w0)\"}}"
}

export -f md_field drop_replica recycle_entries recycle_as_owner rest_call

# "<fid> <fsid>" of each file location under <dir> (default: the test tree),
# as the files say
file_locations() {
  eos find -f --fid --fs ${1-$base} 2>/dev/null |
    awk '{ fid = substr($2, 5); n = split(substr($3, 6), l, ",")
           for (k = 1; k <= n; k++) if (l[k] != "") print fid, l[k] }' | sort
}

# "<bytes> <files>" of the quota node. The MGM refreshes what quota ls shows
# at most once a minute, so wait for that first.
project_quota() {
  sleep 61
  eos quota ls -m -p $base | grep gid=project |
    sed -E 's/.*usedlogicalbytes=(-?[0-9]+) usedfiles=(-?[0-9]+).*/\1 \2/'
}

# "<tree size> <tree files> <tree containers>" of a directory. The MGM applies
# the tree updates every 5 s, so wait for that first.
tree_counters() {
  sleep 6
  echo $(md_field $1 treesize) $(md_field $1 treefiles) $(md_field $1 treecontainers)
}

# ------------------------------------------------------------------------------
# The load. Not written for errexit: 2-load.bats runs it with set +e.
# ------------------------------------------------------------------------------
# load_op <kind> <name> <command>: 2 workers of each kind repeat all lines of
# their kind, in order, until the end of the load
load_op() {
  load_kind+=("$1")
  load_name+=("$2")
  load_cmd+=("$3")
}

# Run one operation with a time limit of 60 s. Note its name, exit code and
# duration in ms, and the last line of its error output in $logs/errors.
op() {
  local t0 out rc
  t0=$(date +%s%N)
  out=$(timeout 60 bash -c "$2" 2>&1 >/dev/null)
  rc=$?
  echo "$1 $rc $((($(date +%s%N) - t0) / 1000000))" >> $logs/ops.$BASHPID

  if [ $rc != 0 ]; then
    echo "$1 rc=$rc own=$own wfile=$wfile: $2 : $(echo "$out" | tail -1)" >> $logs/errors
    [ $rc = 124 ] && dump_stacks "$1 timed out: $2"
  fi
}

# Repeat the load lines of kind $1 as worker number $2 until the end
run_worker() {
  local kind=$1 n=${#fsids[@]} k cmds=""
  export w=$2 i=0 d next own old mown wfile fsid fsid2

  for k in ${!load_kind[@]}; do
    [ ${load_kind[$k]} = $kind ] && cmds="$cmds ${load_cmd[$k]}"
  done

  while [ $(date +%s) -lt $end ]; do
    d=$base/d$((i % 4))
    next=$base/d$(((i + 1) % 4))
    own=$d/$kind${w}_$i
    old=$d/$kind${w}_$((i - 8))
    mown=$mnt/${own#/eos/}
    fsid=${fsids[$((i % n))]}
    fsid2=${fsids[$(((i + 1) % n))]}
    wfile=""

    if [[ $cmds == *'$wfile'* ]]; then
      wfile=$(eos ls $d 2>/dev/null | grep -E "^(write|big)[0-9]" | shuf -n 1)
      [ -n "$wfile" ] && wfile=$d/$wfile
    fi

    for k in ${!load_kind[@]}; do
      [ ${load_kind[$k]} = $kind ] || continue
      [[ ${load_cmd[$k]} == *'$wfile'* ]] && [ -z "$wfile" ] && continue
      [[ ${load_cmd[$k]} == *'$old'* ]] && [ $i -lt 8 ] && continue
      op ${load_name[$k]} "${load_cmd[$k]}"
    done

    i=$((i + 1))
  done
}

# Start 2 workers of each kind, a probe of the MGM and the drain. When they
# end, write the count, errors, timeouts and longest duration of each
# operation to $logs/summary.
run_load() {
  local kind n kinds
  eos rm -r -F $r >/dev/null 2>&1
  kinds=$(printf '%s\n' "${load_kind[@]}" | awk '!seen[$0]++' |
            grep -vxF -f <(printf '%s\n' $skip) | xargs)
  # For the end checks
  eos ns stat -m | tr ' ' '\n' | grep "^ns.total.files=" | cut -d= -f2 > $logs/files_before
  wc -l < $mgmlog > $logs/log_lines 2>/dev/null || echo 0 > $logs/log_lines
  end=$(($(date +%s) + duration))
  # On an interrupt, stop the workers. teardown_suite undoes the drain.
  trap 'kill $(jobs -p) 2>/dev/null; exit 2' INT TERM
  probe &
  drain &

  for n in 1 2; do
    for kind in $kinds; do
      run_worker $kind $n &
    done
  done

  wait
  undrain
  umount_fuse $mnt || true
  awk '{ n[$1]++; if ($2 == 124) tmo[$1]++; else if ($2) err[$1]++
         if ($3 > max[$1]) max[$1] = $3 }
       END { for (o in n) printf "%-24s %7d %7d %8d %7d\n", o, n[o], err[o], tmo[o], max[o] }' \
    $logs/ops.* | sort > $logs/summary
}

# The table of the load operations, for the output of a failed check
show_summary() {
  printf "%-24s %7s %7s %8s %7s\n" op count errors timeouts maxms
  cat $logs/summary
}

# Ask the MGM every 2 s if it answers
probe() {
  while [ $(date +%s) -lt $end ]; do
    timeout 15 eos ns stat >/dev/null 2>&1 ||
      { date +%T >> $logs/probe.fail; dump_stacks "the MGM did not answer"; }
    sleep 2
  done
}

# Drain a file system with files of the test during the load, so that the
# drainer moves replicas under the workers. Then set it back to rw. Not a file
# system with a replica of the append files: the MGM refuses an append
# (ENETUNREACH) while one replica is on a draining file system.
drain() {
  local fsid
  sleep $((duration / 4))
  fsid=$(file_locations | awk '{ print $2 }' | sort -u |
           grep -vxF -f <(file_locations $base/ap | awk '{ print $2 }') | head -1)
  [ -n "$fsid" ] || return
  echo $fsid > $logs/drain.fsid
  op drain "eos fs config $fsid configstatus=drain"
  sleep $((end - $(date +%s))) 2>/dev/null
  op undrain "eos fs config $fsid configstatus=rw"
  rm -f $logs/drain.fsid
}

# reset_logs: an empty $logs for the run. Call it first in setup_suite,
# before eos_setup_suite writes its files there.
reset_logs() {
  # Two runs on one instance remove each other's files and logs
  exec 9> /tmp/eos-contention.lock
  flock -n 9 || { echo "another contention test runs on this host"; exit 2; }
  # A mount left by an interrupted run must go before rm of its directory
  umount -l $mnt 2>/dev/null || true
  rm -rf --one-file-system $logs && mkdir -p $logs
}

# converter_on: turn on the converter for the convert operations of the
# load. converter_restore turns it off again, if converter_on turned it on.
converter_on() {
  eos convert config list | grep -q "status=on" && return 0
  eos convert config set status=on >/dev/null && touch $logs/converter.on
}

converter_restore() {
  [ -e $logs/converter.on ] || return 0
  eos convert config set status=off >/dev/null 2>&1
  rm -f $logs/converter.on
}

# True if a quota node covers the recycle bin
recycle_quota() {
  eos quota ls -m -p "$(eos attr get sys.recycle $base/r | cut -d '"' -f2)" >/dev/null 2>&1
}

# ------------------------------------------------------------------------------
# Clean up. These ignore their errors: they also run under errexit.
# ------------------------------------------------------------------------------
undrain() {
  if [ -s $logs/drain.fsid ]; then
    eos fs config $(cat $logs/drain.fsid) configstatus=rw >/dev/null 2>&1 || true
  fi
  rm -f $logs/drain.fsid
}

# rm -r does not remove a quota node, so remove the node first
remove_test_tree() {
  recycle_purge $base/ >/dev/null

  eos quota rmnode -p $base >/dev/null 2>&1 || true
  eos rm -r -F --no-confirmation $base >/dev/null 2>&1 || true
}

# ------------------------------------------------------------------------------
# Set up, from setup_suite. Not written for errexit. It exports what it finds
# to the tests: $skip, $recycle_ok, $first_fid, $first_cid, $qdb, $fsids_list.
# ------------------------------------------------------------------------------
contention_setup() {
  local k
  [ -n "$base" ] || { echo "set EOS_TEST_PREFIX"; exit 2; }
  info "set up $base"
  head -c 1024 /dev/urandom > $src
  head -c 1024 /dev/urandom > $srcb
  head -c 8388608 /dev/urandom > $srcbig
  : > $empty
  yes | head -c 1024 > $logs/race.1k
  remove_test_tree
  eos mkdir -p $base || exit 2
  eos chmod 777 $base >/dev/null
  eos attr set default=replica $base >/dev/null
  eos quota set -g 99 -v 1TB -i 1M $base >/dev/null || exit 2

  for k in d0 d1 d2 d3 v hot big at ap race; do
    eos mkdir $base/$k >/dev/null || exit 2
    eos chmod 777 $base/$k >/dev/null
  done

  # Writes through xrootd run as adm (3:4) with sss. A rename by adm fails with
  # EPERM in a directory of root.
  for k in d0 d1 d2 d3; do
    eos chown adm:adm $base/$k >/dev/null
  done

  eos attr set sys.versioning=2 $base/v >/dev/null
  eos attr set sys.forced.checksum=adler $r >/dev/null

  # A new instance refuses writes until its file systems report their free
  # space. The first file also marks the first id of the test: files and
  # directories with a higher id belong to the test.
  wait_writable $base || exit 2
  xrdcp -f -s $src $url/$base/watermark >/dev/null 2>&1
  first_fid=$(md_field $base/watermark fid)
  first_cid=$(md_field $base fid)
  [ -n "$first_fid" ] || { echo "the instance refuses writes"; exit 2; }
  xrdcp -f -s $src $url/$hot >/dev/null 2>&1

  # The recycle bin, gRPC, the REST gateway and eosxd can be missing: skip
  # the tests and the load kinds that need them
  skip=""
  # rm refuses to recycle when no quota node covers the recycle bin: give
  # the recycle bin a quota if it has none
  recycle_ok=0
  eos recycle config --dump 2>/dev/null | grep -qx enabled=on &&
    eos mkdir $base/r >/dev/null && eos chmod 777 $base/r >/dev/null &&
    eos recycle config --add-bin $base/r >/dev/null &&
    { recycle_quota || { eos -r 0 0 recycle config --size 100G >/dev/null &&
                         recycle_quota; }; } && recycle_ok=1
  [ $recycle_ok = 1 ] ||
    { info "skip the recycle bin parts: the recycle bin is off or has no quota"
      skip="$skip recycle"; }
  timeout 10 $grpc -p $base/g mkdir >/dev/null 2>&1 &&
    timeout 10 $grpc -p $base/g rmdir >/dev/null 2>&1 ||
    { info "skip grpc: no namespace gRPC service on $grpc_ep"; skip="$skip grpc"; }
  rest_call ls_cmd $base >/dev/null 2>&1 ||
    { info "skip rest: no REST gateway on $rest"; skip="$skip rest"; }

  mount_fuse $mnt && mkdir $mnt/${base#/eos/}/probe && rmdir $mnt/${base#/eos/}/probe ||
    { info "skip mount: no eosxd mount of /eos"; skip="$skip mount"; umount_fuse $mnt || true; }

  members=$(awk '$1 == "mgmofs.qdbcluster" { print $2 }' /etc/xrd.cf.mgm 2>/dev/null)
  pwfile=$(awk '$1 == "mgmofs.qdbpassword_file" { print $2 }' /etc/xrd.cf.mgm 2>/dev/null)
  qdb="--members $members --password-file $pwfile"
  fsids_list=$(fs_ids | xargs)
  info "logs in $logs"
}
