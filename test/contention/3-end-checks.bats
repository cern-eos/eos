#!/usr/bin/env bats
# ------------------------------------------------------------------------------
# Contention test, the end checks: after the races and the load, the namespace,
# the quota, the tree counters and the file systems must agree. The checks run
# in order and read the files that 2-load.bats left in $logs. The last ones rm
# the whole test tree.
# ------------------------------------------------------------------------------

load contention

# A hung MGM: the next checks skip
@test "the MGM answered every 2 s during the load" {
  if [ -s $logs/probe.fail ]; then
    mgm_down "the load"
    echo "no answer at: $(xargs < $logs/probe.fail)"
    return 1
  fi
}

@test "the workers ran operations" {
  [ -s $logs/summary ]
}

@test "no operation hung" {
  none awk '$4 > 0 { print $1 }' $logs/summary || { show_summary; return 1; }
}

@test "no operation on own entries failed" {
  none awk '$1 !~ /^(other|race)_/ && $3 > 0 { print $1 }' $logs/summary ||
    { show_summary; return 1; }
}

# Load operations that can fail at each try: a repair of a file without an
# fsck error; all hot workers queue the same conversion ("already scheduled")
never_ok="other_fsck_repair race_hot_convert"

@test "every load operation succeeded at least once" {
  awk -v never="$(echo $never_ok)" 'BEGIN { split(never, a); for (k in a) skip[a[k]] = 1 }
       $2 == $3 + $4 && !($1 in skip) { print $1 }' $logs/summary > $logs/never_ok
  none cat $logs/never_ok || { show_summary; return 1; }
}

@test "every read of the atomic upload got one whole version" {
  none grep -s . $logs/atomic.bad
}

@test "every appended file has the size of its appends" {
  sort $logs/appends 2>/dev/null | uniq -c | while read n f; do
    [ "$(md_field $f size)" = $((n * 4096)) ] || echo $f
  done > $logs/appends.bad
  none cat $logs/appends.bad
}

# Conversions and replica changes go on after the load. Wait until no
# conversion runs and the file locations stay the same for 10 s.
@test "the file system lists hold exactly the file locations" {
  for k in $(seq 1 60); do
    eos convert config list 2>&1 | grep -qE "(Running|Pending) jobs *: [1-9]" || break
    sleep 5
  done

  file_locations > $logs/locations

  for k in $(seq 1 30); do
    sleep 10
    file_locations > $logs/locations.new
    cmp -s $logs/locations $logs/locations.new && break
    mv $logs/locations.new $logs/locations
  done

  for fsid in ${fsids[@]}; do
    eos fs dumpmd $fsid -m | grep -F "&container=$base/" |
      sed -E "s/.*&id=([0-9]+)&.*/\1 $fsid/"
  done | sort > $logs/locations.fs
  diff $logs/locations $logs/locations.fs
}

@test "the FST replicas have the size and checksum of the namespace" {
  eos find -f $base 2>/dev/null | grep -v " -> " | while read f; do
    eos file check $f %size%checksum%silent >/dev/null 2>&1 || echo $f
  done > $logs/replicas.bad
  none cat $logs/replicas.bad
}

@test "the recycle bin holds no entry of the test" {
  need recycle
  none recycle_entries $base/
}

# bats test_tags=slow
@test "the quota file count matches a recompute" {
  a=$(project_quota | cut -d " " -f2)
  eos ns recompute_quotanode $base >/dev/null
  b=$(project_quota | cut -d " " -f2)
  echo "$a, recomputed $b"
  [ -n "$a" ] && [ "$a" = "$b" ]
}

@test "the tree counters match a recompute" {
  a=$(tree_counters $base)
  eos ns recompute_tree_size $base >/dev/null
  b=$(tree_counters $base)
  echo "$a, recomputed $b"
  [ -n "$a" ] && [ "$a" = "$b" ]
}

@test "the tree counters are 0 after rm of everything" {
  for k in $(eos ls $base); do
    eos rm -r -F $base/$k >/dev/null 2>&1 || true
  done

  c=$(tree_counters $base)
  echo "$c"
  [ "$c" = "0 0 0" ]
}

# bats test_tags=slow
@test "the quota is 0 after rm of everything" {
  q=$(project_quota)
  echo "$q"
  [ "$q" = "0 0" ]
}

# A removed file leaves the namespace when the FSTs have dropped its
# replicas. A record written back after its removal would stay.
@test "no file record of the test is left" {
  remove_test_tree
  before=$(cat $logs/files_before)

  for k in $(seq 1 60); do
    n=$(eos ns stat -m | tr " " "\n" | grep "^ns.total.files=" | cut -d= -f2)
    [ $n -le $before ] && return 0
    sleep 5
  done

  echo "files before $before, after $n"
  return 1
}

# In the whole namespace in QuarkDB, no file or directory with an id of the
# test may be an orphan, and no file system list may hold such a file. A file
# with parent 0 and only unlinked locations waits for the FSTs, that is fine.
@test "no file or directory of the test is an orphan" {
  # It exits 0 also when it finds orphans
  eos-ns-inspect check-orphans $qdb > $logs/orphans.all 2>&1 ||
    skip "eos-ns-inspect cannot reach QuarkDB"
  touch $logs/quarkdb.ok
  awk -v f=$first_fid -v c=$first_cid '
    /^file-id=/ { split($1, a, "="); split($2, p, "="); split($4, l, "=")
                  split($5, u, "=")
                  if (a[2] >= f && (p[2] != 0 || l[2] != "" || u[2] == "")) print }
    /^container-id=/ { split($1, a, "="); if (a[2] >= c) print }' \
    $logs/orphans.all > $logs/orphans
  none cat $logs/orphans
}

@test "no file system list holds a file of the test" {
  [ -e $logs/quarkdb.ok ] || skip "eos-ns-inspect cannot reach QuarkDB"

  for k in check-fsview-missing check-fsview-extra; do
    eos-ns-inspect $k $qdb 2>/dev/null |
      awk -v f=$first_fid '/^id=/ { split($1, a, "="); if (a[2] >= f) print }'
  done > $logs/fsview.bad
  none cat $logs/fsview.bad
}

@test "no CRIT message in the MGM log" {
  tail -n +$(($(cat $logs/log_lines) + 1)) $mgmlog > $logs/mgm.log 2>/dev/null || true
  none grep level=CRIT $logs/mgm.log
}

@test "no lock order error in the MGM log" {
  mgm_log > $logs/mgm.suite.log
  none grep "RWMutex. Order Checking Error in thread" $logs/mgm.suite.log
}
