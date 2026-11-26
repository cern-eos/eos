# bats file_tags=token
load ../instance

@test "eos-token-test" {
  runtest "### eos-token  " unix SUCCESS "" eos_token "/eos/$EOS_TEST_INSTANCE/test/instancetest/token.dir/"
}
