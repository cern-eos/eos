# bats file_tags=tpc
load ../instance

@test "third party copy" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc"
  runtest "### Upload    " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.1"
  if [ "${XRD_VERS_MAJOR}" -eq 5 ]; then
  runtest "### TPC 1     " unix SUCCESS "" tpc1 "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.1" "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.2"
  elif [ "${XRD_VERS_MAJOR}" -eq 4 ]; then
  runtest "### TPC 1     " unix SUCCESS "" tpc1 "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.1" "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.2"
  else
  runtest "### TPC 1     " unix SUCCESS "" tpc2 "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.1" "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.2"
  fi
  runtest "### TPC 2     " unix SUCCESS "" tpc2 "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.1" "/eos/$EOS_TEST_INSTANCE/test/instancetest/tpc/file.tpc.3"
  runtest "### Rm        " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/tpc
}
