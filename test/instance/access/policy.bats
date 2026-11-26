# bats file_tags=policy
load ../instance

@test "a file without policy gets the plain layout" {
  runtest "### Cd        " unix SUCCESS "" eos cd /eos/$EOS_TEST_INSTANCE/test
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir -p /eos/$EOS_TEST_INSTANCE/test/instancetest/policies/
  runtest "### Attr      " unix IGNORE  "" eos attr rm sys.forced.layout /eos/$EOS_TEST_INSTANCE/test/instancetest/policies/
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p1" "-ODeos.space=default"
  runtest "### Grep      " unix SUCCESS "" eos file info "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p1"
}

@test "the space and directory policies give the layout" {
  runtest "### Grep      " unix SUCCESS "" eos file info "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p1| grep plain | wc -l | grep -w 1"
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.layout=replica
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.nstripes=2
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.layout | wc -l | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.nstripes | wc -l | grep -w 1"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p2" "-ODeos.space=default"
  runtest "### Grep      " unix SUCCESS "" eos file info "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p2| grep replica | wc -l | grep -w 1"
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.checksum=adler
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.checksum | wc -l | grep -w 1"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p3" "-ODeos.space=default"
  runtest "### Grep      " unix SUCCESS "" eos file info "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p3| grep adler | wc -l | grep -w 1"
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.blocksize=1M
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.blocksize | wc -l | grep -w 1"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p4" "-ODeos.space=default"
  runtest "### Grep      " unix SUCCESS "" eos file info "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p4| grep 1M | wc -l | grep -w 1"
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.blockchecksum=crc32c
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.blockchecksum | wc -l | grep -w 1"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/policies/p5" "-ODeos.space=default"
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.layout=remove
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.nstripes=remove
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.checksum=remove
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.blockchecksum=remove
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.blocksize=remove
  runtest "### Policy    " unix ERROR "" eos space config default space.policy.blocksize=remove
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.layout | wc -l | grep -w 0"
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.nstripes | wc -l | grep -w 0"
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.checksum | wc -l | grep -w 0"
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.blockchecksum | wc -l | grep -w 0"
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.blocksize | wc -l | grep -w 0"
  runtest "### Policy    " unix SUCCESS "" eos space config default space.policy.iopriority=be:6
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep policy.iopriority | wc -l | grep -w 1"
  runtest "### Policy    " unix SUCCESS "" eos space config rm default space.policy.iopriority
  runtest "### Rm        " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/policies
}
