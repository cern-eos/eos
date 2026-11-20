# Once for the whole run: an empty test directory, and its removal at the end,
# also after a failure or an interrupt.

load template

setup_suite() {
  eos_setup_suite
  make_testdir
  wait_writable $testdir
}

teardown_suite() {
  eos_teardown_suite
}
