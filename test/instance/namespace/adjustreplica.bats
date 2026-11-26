# bats file_tags=adjustreplica
load ../instance

ADJUST_TEST_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/adjustreplica"
ADJUST_TEST_FILE="${ADJUST_TEST_DIR}/file.dat"

@test "adjustreplica --exclude-fs does not use the excluded file system" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir ${ADJUST_TEST_DIR}
  runtest "### Mkdir     " unix SUCCESS "" eos attr set default=replica  ${ADJUST_TEST_DIR}
  runtest "### Upload    " unix SUCCESS "" eos cp ${TESTSYSFILE1K} ${ADJUST_TEST_FILE}
  adj_fsid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${ADJUST_TEST_FILE} | jq -r .locations[].fsid | head -n 1)
  runtest "### Drop fsid " unix SUCCESS "" eos file drop ${ADJUST_TEST_FILE} ${adj_fsid}
  # The dropped replica is only unlinked, the physical deletion on ${adj_fsid}
  # happens asynchronously - keep the new replica away from that file system so
  # that the pending deletion can not wipe it right after it was created
  runtest "### Adjust -  " unix SUCCESS "" eos file adjustreplica ${ADJUST_TEST_FILE} --exclude-fs ${adj_fsid}
  runtest "### Check     " unix SUCCESS "" wait_for 60 eosj fileinfo "${ADJUST_TEST_FILE} | jq -r .locations[].fsid | wc -l | grep -w 2"
  # Find a new file system where to over-replicate the current file - ${adj_fsid}
  # is listed as used since its pending deletion would wipe the new replica
  list_fsids=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${ADJUST_TEST_FILE} | jq -r .locations[].fsid)
  read -ra array_fsids <<< "$list_fsids $adj_fsid"
  source_fsid=${array_fsids[0]}
  target_fsid=0
  NUM_FST=$(command eos -b $EOSROLE root://$EOS_TEST_REDIRECTOR fs ls | awk '{ if (($6 ~ /booted/) || ($7 ~ /booted/)) c++} END {print c}')
  for ((i=1; i<=${NUM_FST}; i++)); do if [[ ! " ${array_fsids[*]} " == *" $i "* ]]; then target_fsid=$i; break; fi done
  runtest "### Target    " unix SUCCESS "" shell test "${target_fsid}" -ne 0
  runtest "### Replicate " unix SUCCESS "" eos file replicate ${ADJUST_TEST_FILE} ${source_fsid} ${target_fsid}
  runtest "### Check     " unix SUCCESS "" wait_for 60 eosj fileinfo "${ADJUST_TEST_FILE} | jq -r .locations[].fsid | wc -l | grep -w 3"
}

@test "With --nodrop the extra replica is kept" {
  # With --nodrop the extra replica is kept
  runtest "### Adjust nd " unix SUCCESS "" eos file adjustreplica ${ADJUST_TEST_FILE} --nodrop
  runtest "### Check     " unix SUCCESS "" wait_for 60 eosj fileinfo "${ADJUST_TEST_FILE} | jq -r .locations[].fsid | wc -l | grep -w 3"
  runtest "### Adjust +  " unix SUCCESS "" eos file adjustreplica ${ADJUST_TEST_FILE}
  runtest "### Check     " unix SUCCESS "" wait_for 60 eosj fileinfo "${ADJUST_TEST_FILE} | jq -r .locations[].fsid | wc -l | grep -w 2"
}

@test "Placement in a given space or subgroup is no longer supported" {
  # Placement in a given space or subgroup is no longer supported
  runtest "### Adjust sp " unix ERROR   "" eos file adjustreplica ${ADJUST_TEST_FILE} default
  runtest "### Adjust sg " unix ERROR   "" eos file adjustreplica ${ADJUST_TEST_FILE} default 0
  runtest "### Adjust ex " unix ERROR   "" eos file adjustreplica ${ADJUST_TEST_FILE} --exclude-fs 0
  runtest "### Rm        " unix SUCCESS "" eos rm -F ${ADJUST_TEST_FILE}
  runtest "### Rmdir     " unix SUCCESS "" eos rmdir ${ADJUST_TEST_DIR}
}
