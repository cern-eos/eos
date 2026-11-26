# bats file_tags=balance
# The balancer evens out a group that one file system in ro made unbalanced.
# It needs at least 7 online file systems (the CI instances).
load ../instance

threshold=5

# group_dev: the deviation of the filling of the group default.0
group_dev() {
  group_value default.0 dev.stat.statfs.filled
}

dev_above() {
  [ "$(echo "$(group_dev) >= $threshold" | bc)" = 1 ]
}

dev_below() {
  [ "$(echo "$(group_dev) < $threshold" | bc)" = 1 ]
}

@test "the balancer brings the deviation of a group under its threshold" {
  [ "$(eos fs ls | grep -c online)" -ge 7 ] || skip "needs at least 7 online file systems"
  local d=$testdir/balance f1=$BATS_TEST_TMPDIR/200MB f2=$BATS_TEST_TMPDIR/400MB l
  make_file $f1 200M
  make_file $f2 400M

  for l in replica raiddp raid6; do
    eos mkdir -p $d/$l
    eos attr set default=$l $d/$l
  done
  eos chmod -r 2777 $d

  # Write with one file system in ro: the group is then unbalanced
  fs_set 1 configstatus ro
  for i in 1 2 3; do
    for l in replica raiddp raid6; do
      upload $f1 $d/$l/200MB_file$i.dat
      upload $f2 $d/$l/400MB_file$i.dat
    done
  done
  eos fs config 1 configstatus=rw
  # The FSTs publish their new filling after some seconds
  wait_until 60 dev_above

  space_set default balancer.threshold $threshold
  space_set default balancer.update.interval 5
  space_set default balancer on
  wait_until 300 dev_below
}
