# bats file_tags=lru
load ../instance

@test "eos-lru-test" {
  runtest "### eos-lru  " unix SUCCESS "" eos_lru "/eos/$EOS_TEST_INSTANCE/test/instancetest/lru_test"
}
