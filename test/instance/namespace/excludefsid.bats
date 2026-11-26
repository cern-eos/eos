# bats file_tags=excludefsid
load ../instance

EXCL_TEST_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/excludefsid"
EXCL_TEST_FILE="${EXCL_TEST_DIR}/file.dat"

@test "set up of a file with two replicas" {
  # Check that eos.excludefsid is honoured when scheduling read access
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir ${EXCL_TEST_DIR}
  runtest "### Attr      " unix SUCCESS "" eos attr set default=replica ${EXCL_TEST_DIR}
  runtest "### Upload    " unix SUCCESS "" eos cp ${TESTSYSFILE1K} ${EXCL_TEST_FILE}
}

@test "eos.excludefsid makes reads use the other replica" {
  # The file has two replicas - exclude the first one and check the other is used
  excl_fsid0=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${EXCL_TEST_FILE} | jq -r .locations[0].fsid)
  excl_fsid1=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo ${EXCL_TEST_FILE} | jq -r .locations[1].fsid)
  # Download a few times so we don't end up on the surviving replica just by luck
  for i in 1 2 3; do
    download "$EOS_TEST_TESTSYS/dumpit" "${EXCL_TEST_FILE}?eos.excludefsid=${excl_fsid0}"
  done
  excl_used=0
  while read -r line; do
    # the selected file system is target[replicaindex]=(host,fsid)
    idx=$(echo "$line" | grep -oP 'replicaindex=\K[0-9]+')
    chosen_fsid=$(echo "$line" | grep -oP "target\[${idx}\]=\([^,]+,\K[0-9]+")
    [ "${chosen_fsid}" = "${excl_fsid0}" ] && excl_used=1
  done < <(grep "path=${EXCL_TEST_FILE} " /var/log/eos/mgm/xrdlog.mgm | grep replicaindex | tail -3)
  runtest "### ExclFsid  " unix SUCCESS "" shell test "${excl_used}" = "0"
  # Excluding both replicas leaves no location available - access must fail
  runtest "### ExclAll   " unix ERROR   "" download "$EOS_TEST_TESTSYS/dumpit" "${EXCL_TEST_FILE}?eos.excludefsid=${excl_fsid0},${excl_fsid1}"
  runtest "### Rm        " unix SUCCESS "" eos rm -F ${EXCL_TEST_FILE}
  runtest "### Rmdir     " unix SUCCESS "" eos rmdir ${EXCL_TEST_DIR}
  rm -rf "$EOS_TEST_TESTSYS/dumpit"
}
