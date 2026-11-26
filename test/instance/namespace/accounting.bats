# bats file_tags=accounting
load ../instance

@test "eos-accounting-test" {
  runtest "### eos-accounting  " unix SUCCESS "" eos_accounting "/eos/$EOS_TEST_INSTANCE/test/instancetest/accounting_test"
}
