# Set up the test tree and probe the instance once for all files of the suite.
# Clean up at the end, also after a failure or an interrupt.

load contention

setup_suite() {
  reset_logs
  eos_setup_suite
  order_checking_on
  converter_on
  # The set up is not written for errexit. Export what it finds to the tests.
  set +e -a
  contention_setup
  set -e +a
}

teardown_suite() {
  set +e
  order_checking_restore
  converter_restore
  undrain
  umount_fuse $mnt
  remove_test_tree
  echo "# logs in $logs" >&3
}
