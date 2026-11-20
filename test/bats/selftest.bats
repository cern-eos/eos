#!/usr/bin/env bats
# ------------------------------------------------------------------------------
# Tests of the helpers in eos.bash and runtest.bash. They need no EOS
# instance: stubs of eos and pgrep stand in for it. pre-commit runs them after
# a change of these files, or run:
#
#   test/bats/vendor/bats-core/bin/bats test/bats/selftest.bats
# ------------------------------------------------------------------------------

logs=${BATS_FILE_TMPDIR:-/tmp}/eos-selftest
load eos
load runtest

# No MGM here: no skip after a failure, no MGM check
setup() {
  stub=$BATS_TEST_TMPDIR/bin
  mkdir -p $stub $logs
  export PATH=$stub:$PATH STUB=$BATS_TEST_TMPDIR
  echo off > $STUB/order
  # eos: logs its arguments, and keeps the state of the order checking
  cat > $stub/eos <<'EOF'
#!/bin/bash
echo "$*" >> $STUB/eos.calls
case "$*" in
  "ns mutex") echo "order checking is : $(< $STUB/order)" ;;
  "ns mutex --toggleorder")
    [ "$(< $STUB/order)" = on ] && echo off > $STUB/order || echo on > $STUB/order ;;
  "space status default -m") echo "a=1 policy.x=2" ;;
  "fs ls -m") echo "host=$(hostname -f) id=1 configstatus=rw"; echo "host=other.host id=2 configstatus=ro" ;;
  "-j fileinfo /f") echo '{"id":7,"locations":[{"fsid":1,"fstpath":"/d/1/a"},{"fsid":2,"fstpath":"/d/2/a"}]}' ;;
  "attr get user.x /f") echo 'user.x="v 1"' ;;
  "recycle ls -m -n --all")
    echo "recycle=ls uid=3 gid=4 restore-path=/d/f restore-key=fxid:1 size=1"
    echo "recycle=ls uid=5 gid=6 restore-path=/d/f2 restore-key=fxid:2 size=1" ;;
esac
EOF
  # xrdcp: <url>/<path> is $STUB/fs/<path>
  cat > $stub/xrdcp <<'EOF'
#!/bin/bash
src=${@: -2:1} dst=${@: -1}
cp "${src/#root:\/\/localhost/$STUB/fs}" "${dst/#root:\/\/localhost/$STUB/fs}"
EOF
  printf '#!/bin/bash\nexit 1\n' > $stub/pgrep
  printf '#!/bin/bash\necho "$*"\n' > $stub/curl
  chmod +x $stub/eos $stub/pgrep $stub/xrdcp $stub/curl
  mkdir -p $STUB/fs/d
  rm -f $logs/mgm.down $logs/order.toggled $logs/undo
}

teardown() {
  :
}

@test "none: true if the command prints nothing" {
  none true
}

@test "none: false and prints the lines and their count" {
  run none printf 'a\nb\n'
  [ "$status" = 1 ]
  [ "$output" = "$(printf 'a\nb\n2 found')" ]
}

@test "together: the exit code of each command, in order" {
  [ "$(together 'true' 'exit 3' 'sleep 1; false' | xargs)" = "0 3 1" ]
}

@test "wait_until: true at once when the command succeeds" {
  wait_until 5 true
}

@test "wait_until: false after the time" {
  run wait_until 2 false
  [ "$status" = 1 ]
  [[ $output == "not true after the wait: false" ]]
}

@test "run_race: counts the copies that succeed" {
  run_race 4 'true' '[ $k -le 2 ]'
  [ "$ok" = 2 ]
  [ "$(< $logs/ok)" = 2 ]
}

@test "run_race: fails if the set up fails" {
  run run_race 4 'false' 'true'
  [ "$status" = 1 ]
}

@test "run_pairs: counts the pairs in which both succeed" {
  run_pairs 'true' 'true' 'false'
  [ "$both" = 0 ]
  run_pairs 'true' 'true' '[ $k -le 3 ]'
  [ "$both" = 3 ]
}

@test "mgm_log: only the lines after the start of the suite" {
  export mgmlog=$BATS_TEST_TMPDIR/xrdlog.mgm
  printf 'old\nnew 1\nnew 2\n' > $mgmlog
  echo 1 > $logs/log_start
  [ "$(mgm_log | xargs)" = "new 1 new 2" ]
}

@test "order_checking_on and _restore: turn it on and back off" {
  order_checking_on
  [ "$(< $STUB/order)" = on ]
  order_checking_restore
  [ "$(< $STUB/order)" = off ]
  [ ! -e $logs/order.toggled ]
}

@test "order_checking_on and _restore: leave it on if it was on" {
  echo on > $STUB/order
  order_checking_on
  order_checking_restore
  [ "$(< $STUB/order)" = on ]
  ! grep -q toggleorder $STUB/eos.calls
}

@test "undo: run_undo runs the commands in reverse order, once" {
  undo echo 1 ">>" $STUB/out
  undo echo 2 ">>" $STUB/out
  run_undo
  run_undo
  [ "$(xargs < $STUB/out)" = "2 1" ]
}

@test "space_set: undo sets the old value back, or removes a new key" {
  space_set default policy.x 5
  space_set default policy.y 6
  run_undo
  run -0 grep "^space config" $STUB/eos.calls
  assert_output "space config default space.policy.x=5
space config default space.policy.y=6
space config rm default space.policy.y
space config default space.policy.x=2"
}

@test "eos_as: the role goes before the command" {
  eos_as 3 4 ls /x
  [ "$(tail -1 $STUB/eos.calls)" = "-r 3 4 ls /x" ]
}

@test "upload, download and same_file" {
  echo hello > $BATS_TEST_TMPDIR/a
  upload $BATS_TEST_TMPDIR/a d/f
  same_file $BATS_TEST_TMPDIR/a d/f
  echo other > $BATS_TEST_TMPDIR/b
  ! same_file $BATS_TEST_TMPDIR/b d/f
}

@test "fs_value and fs_set: the value of one file system, undone at the end" {
  [ "$(fs_value 2 configstatus)" = ro ]
  fs_set 2 configstatus drain
  run_undo
  run -0 grep "^fs config" $STUB/eos.calls
  assert_output "fs config 2 configstatus=drain
fs config 2 configstatus=ro"
}

@test "json, replicas, replica_file and attr_value" {
  [ "$(json .id fileinfo /f)" = 7 ]
  [ "$(replicas /f | xargs)" = "1 2" ]
  [ "$(replica_file /f 2)" = /d/2/a ]
  [ "$(attr_value /f user.x)" = "v 1" ]
  [ -z "$(attr_value /f user.none)" ]
}

@test "on_fst: here on this host, else through EOS_FST_EXEC" {
  [ "$(on_fst 1 'echo here')" = here ]
  [ "$(EOS_FST_EXEC='echo' on_fst 2 'ls x')" = "other.host ls x" ]
}

@test "on_fst: skips without EOS_FST_EXEC for another host" {
  skip() { echo "skip: $*"; }
  run on_fst 2 true
  assert_output --partial "no access to the FST host other.host"
}

@test "is: compares the output of a command" {
  is a echo a
  ! is a echo b
}

@test "wait_until: prints the last output at the end of the time" {
  run wait_until 1 sh -c 'echo state=busy; false'
  [ "$status" = 1 ]
  assert_output --partial "state=busy"
}

@test "log_has: a matching line since the start of the suite" {
  mgmlog=$STUB/log
  echo "old event" > $mgmlog
  echo 1 > $logs/log_start
  echo "new event" >> $mgmlog
  log_has "new event"
  ! log_has "old event"
}

@test "make_file: a file of the size" {
  make_file $BATS_TEST_TMPDIR/r 16K
  [ "$(stat -c %s $BATS_TEST_TMPDIR/r)" = 16384 ]
}

@test "recycle_entries and recycle_as_owner: entries of a prefix, one path as its owner" {
  [ "$(recycle_entries /d/ | wc -l)" = 2 ]
  [ "$(recycle_entries "/d/f ")" = "3 4 fxid:1" ]
  recycle_as_owner "restore -f" /d/f
  [ "$(tail -1 $STUB/eos.calls)" = "-r 3 4 recycle restore -f fxid:1" ]
  run recycle_as_owner "purge -k" /d/none
  assert_failure
}

@test "token_for: --tree only for a path with a final /" {
  token_for /d/ rwx
  token_for /d/f r 60
  run -0 grep "^token" $STUB/eos.calls
  assert_line --index 0 --regexp "^token --path /d/ --permission rwx --expires [0-9]+ --owner nobody --group nobody --tree$"
  assert_line --index 1 --regexp "^token --path /d/f --permission r --expires [0-9]+ --owner nobody --group nobody$"
}

@test "http: the method, the URL and the options reach curl" {
  http_url=https://h:1
  run -0 http PUT /d/f -T x
  assert_output --partial "-X PUT https://h:1/d/f -T x"
}

@test "eos_teardown: after a failure, the MGM log lines of the test" {
  mgmlog=$STUB/log
  echo "before" > $mgmlog
  eos_setup
  echo "during" >> $mgmlog
  BATS_OUT=/dev/null BATS_TEST_COMPLETED='' BATS_SUITE_TEST_NUMBER=7 eos_teardown
  run -0 cat $logs/failed/7.mgm.log
  assert_output "== $BATS_TEST_DESCRIPTION
during"
}

@test "eos_teardown: no MGM log after a success" {
  rm -rf $logs/failed
  BATS_OUT=/dev/null BATS_TEST_COMPLETED=1 eos_teardown
  [ ! -e $logs/failed ]
}

@test "mgm_down: keeps the first reason, and eos_setup then skips" {
  mgm_down "first test" 3>/dev/null
  mgm_down "second test" 3>/dev/null
  [ "$(< $logs/mgm.down)" = "first test (no MGM process)" ]
  [ "$( skip() { echo "$*"; }; eos_setup )" = \
    "the MGM is down since: first test (no MGM process)" ]
}

@test "runtest: SUCCESS, ERROR and IGNORE follow the exit code" {
  runtest "t" unix SUCCESS "" true
  run runtest "t" unix SUCCESS "" false
  [ "$status" = 1 ]
  runtest "t" unix ERROR "" false
  run runtest "t" unix ERROR "" true
  [ "$status" = 1 ]
  runtest "t" unix IGNORE "" false
}

@test "runtest: an exit code as the expected result" {
  three() { return 3; }
  runtest "t" unix 3 "" three
  run runtest "t" unix 4 "" three
  [ "$status" = 1 ]
  [[ $output == *"expected 4, got 3"* ]]
}

@test "runtest: the auth and the env reach the command" {
  show() { echo "$XrdSecPROTOCOL $FOO"; }
  run runtest "t" sss SUCCESS "FOO=bar" show
  [[ $output == *"sss bar"* ]]
}

@test "runtest: the arguments are split into words, as in eos-instance-test" {
  count() { echo "n=$#"; }
  run runtest "t" unix SUCCESS "" count "a b c"
  [[ $output == *"n=3"* ]]
}

@test "runtest: H uses the full redirector for one command" {
  rd() { echo "rd=$EOS_TEST_REDIRECTOR"; }
  EOS_TEST_REDIRECTOR=short EOS_TEST_FULL_REDIRECTOR=full
  run runtest H "t" unix SUCCESS "" rd
  [[ $output == *"rd=full"* ]]
  runtest H "t" unix SUCCESS "" true
  [ "$EOS_TEST_REDIRECTOR" = short ]
}
