# bats file_tags=rclone
load ../instance

@test "eos-rclone-test" {
  runtest "### rclone    " unix SUCCESS "" eos_rclone "/var/tmp/eos-instance-test/" "$EOS_TEST_INSTANCE/test/instancetest/rclone/"
}
