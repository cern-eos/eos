# bats file_tags=race
load ../instance

@test "overlapping appends keep a correct checksum" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/race"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 /eos/$EOS_TEST_INSTANCE/test/instancetest/race/
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=adler /eos/$EOS_TEST_INSTANCE/test/instancetest/race/
  runtest "### Upload    " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/race/file.1k" ""
  runtest "### Checksum  " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/race/file.1k --checksum | grep 14c30610"
  runtest "### AppendOL  " unix SUCCESS "" overlap "/eos/$EOS_TEST_INSTANCE/test/instancetest/race/file.1k"
  runtest "### Checksum  " unix SUCCESS "" wait_for 30 eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/race/file.1k --checksum | grep fc46d811"
  runtest "### Rm        " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/race/
}
