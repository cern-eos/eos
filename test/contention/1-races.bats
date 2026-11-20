#!/usr/bin/env bats
# ------------------------------------------------------------------------------
# Contention test, the races: many clients change the same entries at the same
# time, and the namespace must stay right. Each race makes its own entries in
# $r, so that a filter can run it alone. A test after a race checks the state
# that the race left. The end checks (3-end-checks.bats) also find what a race
# left wrong in the quota and the namespace.
#
# How to add a test:
#   a race   run_race <n> <setup> <command>, then check $ok
#              n copies of the command at the same time
#   a pair   run_pairs <setup> <command a> <command b>, then [ $both = 0 ]
#              8 pairs at the same time, in no pair may both succeed
#   a check  plain shell code, the last command gives the result
# The commands of run_race and run_pairs are shell code in single quotes, $k
# is the number of the copy or the pair. bats runs the tests with errexit.
# ------------------------------------------------------------------------------

load contention

# The flags of a directory: 1 if it is a quota node
qflags() { eos -j fileinfo $1 2>/dev/null | sed -n 's/.*"flags" : \([0-9]*\).*/\1/p'; }
qset() { eos quota set -g 99 -v 1TB -i 1M -p $1/ >/dev/null 2>&1; }

setup_file() {
  # Copy-on-write clone: an rm or an overwrite of a file in a cloned directory
  # keeps the old version in <proc>/clone/<clone id>/<dir id>/<file id>
  proc=$(echo $base | cut -d/ -f1-3)/proc
  export proc
  export cl_changed="o rm p1 p2 p3 p4 p5 p6 p7 p8"
  [[ " $skip " == *" mount "* ]] || cl_changed="$cl_changed m1 m3"
  eos stat $proc/clone >/dev/null 2>&1 || export skip="$skip clone"

  # In $r/w the delete workflow holds rm, and the removal of a rename target,
  # for 3 s. In $r/wc the create workflow refuses each create.
  wfe=$(eos space status default -m | tr ' ' '\n' | sed -n 's/^wfe=//p')
  export wfe
  eos space config default space.wfe=on >/dev/null
  eos mkdir $r/w && eos chmod 777 $r/w >/dev/null
  eos attr set 'sys.workflow.sync::delete.default=bash:shell:slow sleep 3' $r/w >/dev/null
  eos mkdir $r/wc && eos chmod 777 $r/wc >/dev/null
  eos attr set 'sys.workflow.sync::create.default=bash:shell:fail false' $r/wc >/dev/null
}

teardown_file() {
  eos attr rm sys.workflow.sync::delete.default $r/w >/dev/null 2>&1 || true
  eos attr rm sys.workflow.sync::create.default $r/wc >/dev/null 2>&1 || true
  eos space config default space.wfe=${wfe:-off} >/dev/null
}

# Two clients append to one file at the same time
@test "2 overlapping appends succeed" {
  xrdcp -s $logs/race.1k $url/$r/append
  xrdcpappendoverlap root://123@$host/$r/append root://124@$host/$r/append
}

@test "the checksum covers both appends" {
  sleep 1
  eos fileinfo $r/append --checksum | grep -q fc46d811
}

@test "mkdir of one directory: 1 of 8 succeed" {
  run_race 8 '' 'xrdfs $url mkdir $r/dir'
  [ $ok = 1 ]
}

@test "mkdir -p of one new path: 8 of 8 succeed" {
  run_race 8 '' 'eos mkdir -p $r/deep/a/b/c/d'
  [ $ok = 8 ]
}

@test "creates of one new file: 1 of 8 succeed" {
  run_race 8 '' 'xrdcp -s $src $url/$r/new'
  [ $ok = 1 ]
}

# Not eos rm: it reports a missing file as success
@test "rm of one file: 1 of 8 succeed" {
  run_race 8 'xrdcp -s $src $url/$r/rm' 'xrdfs $url rm $r/rm'
  [ $ok = 1 ]
}

@test "the removed file is gone" {
  ! eos stat $r/rm
}

@test "overwrites of one file: 1 or more of 8 succeed" {
  run_race 8 '' 'xrdcp -f -s $src $url/$r/over'
  [ $ok -ge 1 ]
}

@test "the overwritten file has 1024 bytes and 2 replicas" {
  [ "$(md_field $r/over size) $(md_field $r/over nrep)" = "1024 2" ]
}

@test "renames of one file: 1 of 8 succeed" {
  run_race 8 'xrdcp -s $src $url/$r/mv' 'xrdfs $url mv $r/mv $r/mv.$BASHPID'
  [ $ok = 1 ]
}

@test "1 copy of the renamed file is left" {
  [ $(eos ls $r | grep -c "^mv") = 1 ]
}

@test "renames of 8 files onto one name: 1 or more of 8 succeed" {
  run_race 8 'for k in $(seq 1 8); do xrdcp -s $src $url/$r/s$k; done' \
    'xrdfs $url mv $r/s$k $r/target'
  [ $ok -ge 1 ]
}

# Each successful rename removes one source, so the sources left plus the
# target are 9 minus the successes
@test "the renames onto one name keep the files" {
  ok=$(cat $logs/ok)
  n=$(eos ls $r | grep -cE "^(s[1-8]|target)$" || true)
  echo "$n left after $ok moves"
  [ $n = $((9 - ok)) ]
}

# If both renames succeed, a and b are cut off from the namespace
@test "renames of a into b and of b into a: in no pair both succeed" {
  run_pairs 'eos mkdir -p $r/c$k/a && eos mkdir $r/c$k/b' \
    'xrdfs $url mv $r/c$k/a $r/c$k/b/a' 'xrdfs $url mv $r/c$k/b $r/c$k/a/b'
  [ $both = 0 ]
}

@test "the renamed directories stay reachable" {
  for k in $(seq 1 8); do
    [ $(eos find -d $r/c$k | wc -l) = 3 ]
  done
}

# If both succeed, x has no parent
@test "rmdir of e and mkdir of e/x: in no pair both succeed" {
  run_pairs 'eos mkdir $r/e$k' 'xrdfs $url rmdir $r/e$k' 'xrdfs $url mkdir $r/e$k/x'
  [ $both = 0 ]
}

# If both succeed, the file is in a removed directory
@test "move of a file into t and rmdir of t: in no pair both succeed" {
  run_pairs 'eos mkdir $r/t$k && xrdcp -s $src $url/$r/f$k' \
    'xrdfs $url mv $r/f$k $r/t$k/f$k' 'xrdfs $url rmdir $r/t$k'
  [ $both = 0 ]
}

@test "each moved file exists once" {
  [ $(eos find -f $r | grep -cE "/f[1-8]$") = 8 ]
}

@test "restores of one recycle bin entry: 1 of 4 succeed" {
  need recycle
  run_race 4 'xrdcp -s $src $url/$base/r/rr && eos rm $base/r/rr' \
    'recycle_as_owner "restore -f" $base/r/rr'
  [ $ok = 1 ]
}

@test "the restored file is back" {
  need recycle
  eos stat $base/r/rr
}

@test "touch of one new file: 8 of 8 succeed" {
  run_race 8 '' 'eos file touch $r/touch'
  [ $ok = 8 ]
}

@test "the touched file exists once" {
  [ $(eos ls $r | grep -cx touch) = 1 ]
}

# If both succeed, x has no parent
@test "touch of e/x and rmdir of e: in no pair both succeed" {
  run_pairs 'eos mkdir $r/te$k' 'eos file touch $r/te$k/x' 'xrdfs $url rmdir $r/te$k'
  [ $both = 0 ]
}

@test "register of one new name: 1 of 8 succeed" {
  run_race 8 '' 'eos register $r/reg size=0'
  [ $ok = 1 ]
}

@test "the registered file exists" {
  eos stat $r/reg
}

# Each overwrite commits a new version of the file
@test "overwrites of one versioned file: 1 or more of 8 succeed" {
  run_race 8 \
    'eos mkdir $r/vv && eos chmod 777 $r/vv && eos attr set sys.versioning=3 $r/vv' \
    'xrdcp -f -s $src $url/$r/vv/f'
  [ $ok -ge 1 ]
}

@test "the versioned file has 1024 bytes and at most 3 versions" {
  n=$(eos ls "$r/vv/.sys.v#.f" 2>/dev/null | wc -l)
  echo "$n versions"
  [ "$(md_field $r/vv/f size)" = 1024 ] && [ $n -le 3 ]
}

# An open of an existing file with an OwnCloud chunk id is refused, without
# a stall that the client retries until its timeout
@test "an open of an existing file with oc-chunk-uuid fails at once" {
  xrdcp -s $src $url/$r/oc
  rc=0
  timeout 20 xrdcp -f -s $src "$url/$r/oc?oc-chunk-uuid=abc" || rc=$?
  echo "rc=$rc"
  [ $rc != 0 ] && [ $rc != 124 ]
}

# The delete workflow in $r/w: what a rename changes meanwhile must not get
# lost
@test "two overwrite renames onto one file both succeed" {
  for x in x y t; do xrdcp -s $src $url/$r/w/$x; done
  eos file rename $r/w/x $r/w/t &
  sleep 1
  eos file rename $r/w/y $r/w/t
  wait $!
  eos stat $r/w/t && ! eos stat $r/w/x && ! eos stat $r/w/y
}

@test "an overwrite rename moves when its target is renamed away meanwhile" {
  xrdcp -s $src $url/$r/w/s
  xrdcp -s $src $url/$r/w/t2
  eos file rename $r/w/s $r/w/t2 &
  sleep 1
  eos file rename $r/w/t2 $r/w/u || true
  wait $!
  eos stat $r/w/t2 && eos stat $r/w/u && ! eos stat $r/w/s
}

@test "rm of a hard link target keeps the file that a rename moved meanwhile" {
  need mount
  cp $src $mr/w/a && ln $mr/w/a $mr/w/l && sync && sleep 2
  xrdfs $url rm $r/w/a &
  sleep 1
  eos file rename $r/w/a $r/w/b || true
  wait $! || true
  eos stat $r/w/b
}

# A create that the create workflow refuses removes the new file. If it
# leaves the file in the quota, the quota end checks find the leak.
@test "creates that the create workflow refuses: 0 of 8 succeed" {
  run_race 8 '' 'xrdcp -s $src $url/$r/wc/f$k'
  [ $ok = 0 ]
}

@test "the refused creates leave no file" {
  none eos ls $r/wc
}

# A hard link entry is not in the quota: the quota end checks find an rm, or
# a symlink renamed over it, that takes it out
@test "hard links through eosxd: rm of both names, a symlink moved over one" {
  need mount
  cp $src $mr/q1 && ln $mr/q1 $mr/q2 && sync && sleep 2 &&
    eos rm $r/q1 && eos rm $r/q2 &&
    cp $src $mr/q3 && ln $mr/q3 $mr/q4 && ln -s q3 $mr/q5 && sync && sleep 2 &&
    mv -f $mr/q5 $mr/q4
}

@test "rename_with_symlink and a FUSE move of one file leave one file" {
  need mount
  for k in $(seq 1 16); do
    for x in a b c; do
      eos mkdir -p $r/rs$k/$x && eos chmod 777 $r/rs$k/$x >/dev/null || return 1
    done
    xrdcp -s $src $url/$r/rs$k/a/f && ls $mr/rs$k/a >/dev/null || return 1
    together "mv $mr/rs$k/a/f $mr/rs$k/c/f" \
      "eos file rename_with_symlink $r/rs$k/a/f $r/rs$k/b" >/dev/null
  done
  sleep 1
  bad=$(for k in $(seq 1 16); do
    n=0
    for e in $(eos find -f $r/rs$k); do
      l=$(md_field $e link)
      if [ -z "$l" ]; then n=$((n + 1)); else eos stat $l >/dev/null 2>&1 || echo "$e -> $l dangles"; fi
    done
    [ $n = 1 ] || echo "rs$k has $n files"
  done)
  [ -z "$bad" ] || { echo "$bad"; return 1; }
}

@test "eos acl and a FUSE chmod of one directory keep both changes" {
  need mount
  for k in $(seq 1 16); do
    eos mkdir $r/ac$k && eos chmod 755 $r/ac$k >/dev/null && stat $mr/ac$k >/dev/null || return 1
    together "chmod 700 $mr/ac$k" "eos acl --sys u:99=rx $r/ac$k" >/dev/null
  done
  sleep 1
  bad=$(for k in $(seq 1 16); do
    m=$(md_field $r/ac$k mode); a=$(eos attr get sys.acl $r/ac$k 2>&1)
    [ "$m" = 40700 ] && [[ $a == *u:99* ]] || echo "ac$k mode=$m $a"
  done)
  [ -z "$bad" ] || { echo "$bad"; return 1; }
}

# eosxd replies before the MGM has done the change: check the namespace, not
# the exit codes. If both moves succeed, a and b are cut off.
@test "a FUSE move of a into b and a move of b into a keep both reachable" {
  need mount
  for k in $(seq 1 8); do
    eos mkdir -p $r/fm$k/a && eos mkdir $r/fm$k/b && eos chmod -r 777 $r/fm$k >/dev/null &&
      ls $mr/fm$k/a $mr/fm$k/b >/dev/null || return 1
    together "mv $mr/fm$k/a $mr/fm$k/b/a" "xrdfs $url mv $r/fm$k/b $r/fm$k/a/b" >/dev/null
  done
  sleep 2
  bad=$(for k in $(seq 1 8); do
    [ $(eos find -d $r/fm$k | wc -l) = 3 ] || echo "fm$k is cut off"
  done)
  [ -z "$bad" ] || { echo "$bad"; return 1; }
}

# If the rmdir wins over a create that succeeded, the file has no parent
@test "a FUSE rmdir of e and a create of e/f keep each created file" {
  need mount
  for k in $(seq 1 8); do
    eos mkdir $r/fe$k && eos chmod 777 $r/fe$k >/dev/null && ls $mr/fe$k >/dev/null || return 1
  done
  rcs=$(for k in $(seq 1 8); do
    together "rmdir $mr/fe$k" "xrdcp -s $src $url/$r/fe$k/f" | tail -1
  done)
  sleep 2
  k=0
  bad=$(for rc in $rcs; do
    k=$((k + 1)); [ $rc != 0 ] || eos stat $r/fe$k/f >/dev/null 2>&1 || echo "fe$k/f is lost"
  done)
  [ -z "$bad" ] || { echo "$bad"; return 1; }
}

# FUSE and console renames replace the target. A replaced file that stays in
# the quota or the namespace fails the end checks.
@test "FUSE and console overwrite renames of 8 files onto one name leave whole files" {
  need mount
  eos mkdir $r/fo && eos chmod 777 $r/fo >/dev/null
  for k in $(seq 1 8); do xrdcp -s $src $url/$r/fo/s$k; done
  ls $mr/fo >/dev/null || true
  cmds=()
  for k in 1 2 3 4; do
    cmds+=("mv -f $mr/fo/s$k $mr/fo/t" "eos file rename $r/fo/s$((k + 4)) $r/fo/t")
  done
  together "${cmds[@]}" >/dev/null
  sleep 2
  echo $(eos ls $r/fo)
  eos ls $r/fo | grep -vxE "s[1-8]|t" && return 1
  [ "$(md_field $r/fo/t size)" = 1024 ]
}

# A clone id of 0 to 9 only marks the directory. The clone id is the time, so
# wait 1 s: two clones in one second fail.
@test "overwrites of one cloned file: 1 or more of 8 succeed" {
  need clone
  run_race 8 \
    'eos mkdir $r/cl && eos chmod 777 $r/cl >/dev/null || exit
     for x in $cl_changed m2; do xrdcp -s $src $url/$r/cl/$x || exit; done
     sleep 1; eos oldfind -x sys.clone=+10 $r/cl | head -1 | cut -d" " -f2 |
       cut -d: -f2 > $logs/clone.id
     cid=$(printf %x $(md_field $r/cl fid))
     for x in $cl_changed; do
       echo $proc/clone/$(cat $logs/clone.id)/$cid/$(printf %x $(md_field $r/cl/$x fid))
     done > $logs/clone.paths' \
    'xrdcp -f -s $src $url/$r/cl/o'
  [ $ok -ge 1 ]
}

@test "the overwritten cloned file has 1024 bytes and 2 replicas" {
  need clone
  [ "$(md_field $r/cl/o size) $(md_field $r/cl/o nrep)" = "1024 2" ]
}

@test "rm of one cloned file: 1 of 8 succeed" {
  need clone
  run_race 8 '' 'xrdfs $url rm $r/cl/rm'
  [ $ok = 1 ]
}

@test "rm and overwrite of one cloned file at once leave no file or a whole one" {
  need clone
  for k in $(seq 1 8); do
    together "xrdfs $url rm $r/cl/p$k" "xrdcp -f -s $src $url/$r/cl/p$k" >/dev/null
  done
  bad=$(for k in $(seq 1 8); do
    eos stat $r/cl/p$k >/dev/null 2>&1 || continue
    [ "$(md_field $r/cl/p$k size)" = 1024 ] || echo "p$k is not whole"
  done)
  [ -z "$bad" ] || { echo "$bad"; return 1; }
}

@test "FUSE rm and mv -f over cloned files" {
  need clone
  need mount
  rm -f $mr/cl/m1 && mv -f $mr/cl/m2 $mr/cl/m3 && sync && sleep 2
  ! eos stat $r/cl/m1 && ! eos stat $r/cl/m2 && eos stat $r/cl/m3
}

@test "the clone holds the old version of each changed file" {
  need clone
  bad=$(for p in $(cat $logs/clone.paths); do
    eos stat $p >/dev/null 2>&1 || echo "$p is missing"
  done)
  [ -s $logs/clone.paths ] && [ -z "$bad" ] || { echo "$bad"; return 1; }
}

# The clone files are not in the test tree: the end checks would not find
# them, and the namespace would keep them
@test "the clean up of the clone removes its directory" {
  need clone
  id=$(cat $logs/clone.id)
  [ -n "$id" ]
  eos oldfind -x sys.clone=-$id $r/cl >/dev/null || true
  ! eos stat $proc/clone/$id
}

# Quota nodes under the test quota node $base: no namespace lock orders them
# with the writers now. Each case removes its nodes: rm -r keeps a quota node.
# The files written while their node is removed go to the parent node.
@test "quota rmnode while writers change files in the node" {
  for k in $(seq 1 8); do
    eos mkdir $r/qn$k && eos chmod 777 $r/qn$k >/dev/null && qset $r/qn$k || return 1
    for x in a b c; do xrdcp -s $src $url/$r/qn$k/$x; done
    together "for i in 1 2 3 4 5 6; do xrdcp -s $src $url/$r/qn$k/w\$i; done" \
      "for i in 1 2 3; do xrdcp -s $src $url/$r/qn$k/v\$i && xrdfs $url rm $r/qn$k/v\$i; done" \
      "sleep 0.5; eos quota rmnode -p $r/qn$k/" >/dev/null
  done
  sleep 5
  bad=$(for k in $(seq 1 8); do
    [ "$(qflags $r/qn$k)" = 0 ] || echo "qn$k is still a quota node"
    if eos quota ls -m | grep -qF "space=$r/qn$k/"; then echo "qn$k is still listed"; fi
  done)
  [ -z "$bad" ] || { echo "$bad"; return 1; }
}

# bats test_tags=slow
@test "the parent quota node counts the files of the removed nodes" {
  a=$(project_quota | cut -d " " -f2)
  eos ns recompute_quotanode $base >/dev/null
  b=$(project_quota | cut -d " " -f2)
  echo "$a, recomputed $b"
  [ -n "$a" ] && [ "$a" = "$b" ]
}

# A quota set checks that the directory exists, then creates it if it is
# missing. An rmdir in between must not leave a node on a removed directory.
@test "quota set and rmdir of one directory leave no quota node without it" {
  for k in $(seq 1 16); do
    eos mkdir $r/qs$k
    t=$(awk -v k=$k "BEGIN { print 0.005 * k }")
    together "qset $r/qs$k" "sleep $t; xrdfs $url rmdir $r/qs$k" >/dev/null
  done
  bad=$(for k in $(seq 1 16); do
    n=$(eos quota ls -m | grep -cF "space=$r/qs$k/")
    if eos stat $r/qs$k >/dev/null 2>&1; then
      [ "$(qflags $r/qs$k)" = $n ] || echo "qs$k: flags $(qflags $r/qs$k), $n nodes"
    else
      [ $n = 0 ] || echo "qs$k: quota node of a removed directory"
    fi
  done)
  for k in $(seq 1 16); do eos quota rmnode -p $r/qs$k/ >/dev/null 2>&1 || true; done
  [ -z "$bad" ] || { echo "$bad"; return 1; }
}

# eosxd answers the rmdir before the MGM refuses it: check the namespace
@test "rmdir of an empty quota node directory is refused, also through eosxd" {
  eos mkdir $r/qe && qset $r/qe
  xrdfs $url rmdir $r/qe && return 1

  if [[ " $skip " != *" mount "* ]]; then
    ls $mr >/dev/null || true
    rmdir $mr/qe || true
    sleep 2
  fi

  rc=0
  eos stat $r/qe && [ "$(qflags $r/qe)" = 1 ] || rc=$?
  eos quota rmnode -p $r/qe/ >/dev/null
  return $rc
}

# A recompute and an rmnode of one node during writes must end. A recompute
# during writes can miss a new file (also on master): recompute the parent
# after it, for the end checks.
@test "recompute and rmnode of one quota node during writes end" {
  for k in 1 2 3 4; do
    eos mkdir $r/qr$k && eos chmod 777 $r/qr$k >/dev/null && qset $r/qr$k || return 1
    for x in a b c; do xrdcp -s $src $url/$r/qr$k/$x; done
    echo "qr$k: $(together "for i in 1 2 3 4 5 6; do xrdcp -s $src $url/$r/qr$k/w\$i; done" \
      "eos ns recompute_quotanode $r/qr$k" "sleep 0.$k; eos quota rmnode -p $r/qr$k/" | xargs)"
    [ "$(qflags $r/qr$k)" = 0 ] || { echo "qr$k is still a quota node"; return 1; }
  done
  eos ns recompute_quotanode $base >/dev/null
}
