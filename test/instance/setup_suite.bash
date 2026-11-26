# The set up of the instance suite: the "prep" category of the old script. It
# runs for each selection of categories. teardown_suite is the "clean" one.
# Settings, undone at the end:
#   EOS_SCHEDULER_TYPE   space.scheduler.type of the default space
#   EOS_TEST_QUOTA       on or off: the quota of the default space
load instance

setup_suite() {
  eos_setup_suite
  # $testdir holds the tests of the ci area. The old categories work in
  # /eos/$EOS_TEST_INSTANCE/test.
  make_testdir
  make_pattern_files
  # The recycle tests count and restore the entries of their paths: remove what
  # an earlier run left (an interrupted run, or a failed line)
  recycle_purge /eos/$EOS_TEST_INSTANCE/test/
  [ -z "${EOS_SCHEDULER_TYPE-}" ] || space_set default scheduler.type $EOS_SCHEDULER_TYPE
  if [ -n "${EOS_TEST_QUOTA-}" ]; then
    undo eos space quota default $(space_value default quota)
    eos space quota default $EOS_TEST_QUOTA
  fi
  runtest "### Access    " unix IGNORE  "" eos "access rm stall r"
  runtest "### Access    " unix IGNORE  "" eos "access rm redirect"
  runtest "### Access    " unix IGNORE  "" eos "access unban user nobody"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/"
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/stress/"
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.forced.blockchecksum /eos/$EOS_TEST_INSTANCE/test/
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.forced.blocksize /eos/$EOS_TEST_INSTANCE/test/
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.forced.checksum /eos/$EOS_TEST_INSTANCE/test/
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.forced.layout /eos/$EOS_TEST_INSTANCE/test/
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.forced.nstripes /eos/$EOS_TEST_INSTANCE/test/
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.forced.space /eos/$EOS_TEST_INSTANCE/test/
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.forced.atomic /eos/$EOS_TEST_INSTANCE/test/
  runtest "### Cleanup   " unix IGNORE  "" eos attr rm sys.versioning /eos/$EOS_TEST_INSTANCE/test
  runtest "### Cleanup   " unix IGNORE  "" eos space config rm default space.attr.sys.versiontest
  runtest "### Cleanup   " unix IGNORE  "" eos space config rm default space.attr.sys.preservetest
  runtest "### Cleanup   " unix IGNORE  "" eos space config rm default space.attr.sys.file.obfuscate
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest "### Quota     " unix SUCCESS "" eos quota set -u ${NOBODY_UID} -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Quota     " unix ERROR   "" eos quota set -u ${NOBODY_UID} /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Quota     " unix SUCCESS "" eos quota set -u 11 -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Quota     " unix SUCCESS "" eos quota set -u 100 -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Quota     " unix SUCCESS "" eos quota set -u 2 -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Quota     " unix SUCCESS "" eos quota set -u 3 -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Quota     " unix SUCCESS "" eos quota set -u 2000 -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  # eos-user is the user of the CI images
  [ -z "$EOS_USER_UID" ] || runtest "### Quota     " unix SUCCESS "" eos quota set -u ${EOS_USER_UID} -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### NomianlSp " unix SUCCESS "" eos space config default space.nominalsize=100T
}

teardown_suite() {
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/stress"
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest"
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/rain"
  remove_testdir
  run_undo
}

# The local files of the tests, in $EOS_TEST_TESTSYS. A file stays for the
# next run.
make_pattern_files() {
  mkdir -p $EOS_TEST_TESTSYS
  [ -e $TESTSYSFILE0K ] || touch $TESTSYSFILE0K
  [ -e $TESTSYSFILE1K ] || yes | dd of=$TESTSYSFILE1K bs=1k count=1 iflag=fullblock 2>/dev/null
  [ -e $TESTSYSFILE1M ] || yes | dd of=$TESTSYSFILE1M bs=1k count=1000 iflag=fullblock 2>/dev/null
  [ -e $TESTSYSFILE50M ] || yes | dd of=$TESTSYSFILE50M bs=1k count=50000 iflag=fullblock 2>/dev/null
  [ -e $TESTFSTFILE32M ] || dd if=/dev/zero bs=1M count=32 2>/dev/null | tr '\000' '\001' > $TESTFSTFILE32M
  [ -d $EOS_TEST_RAIN_DIR ] && return 0
  local r=$EOS_TEST_RAIN_DIR
  mkdir -p $r
  dd if=/dev/urandom of=$r/file1.rain bs=1552K count=1 2>/dev/null
  dd if=/dev/urandom of=$r/file2.rain bs=18227K count=1 2>/dev/null
  dd if=/dev/urandom of=$r/file3.rain bs=1M count=34 2>/dev/null
  # The files and patterns of the IO test
  cp $r/file1.rain $r/file1.io
  printf '%s\n' "0 524288" "409600 786432" "614400 1048576" "1024000 1589248" > $r/file1.io.pattern
  cp $r/file2.rain $r/file2.io
  printf '%s\n' "0 4194304" "1048576 2097152" "3145728 7340032" "5242880 6291456" \
    "7340032 15728640" "14680064 17825792" "16777216 18664448" > $r/file2.io.pattern
  cp $r/file3.rain $r/file3.io
  printf '%s\n' "0 524288" "409600 786432" "614400 1048576" "1024000 1589248" \
    "1589248 20971520" "20971520 20973568" "20972544 20975616 " "20972544 20976640" \
    "20976640 35651584" > $r/file3.io.pattern
}
