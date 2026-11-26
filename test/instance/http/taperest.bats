# bats file_tags=taperest
load ../instance

@test "WLCG Tape REST API through xrdfs" {
  runtest "### Tape REST " unix SUCCESS "" eos_tape_rest_xrootd_client "/eos/$EOS_TEST_INSTANCE/test/instancetest"
}
