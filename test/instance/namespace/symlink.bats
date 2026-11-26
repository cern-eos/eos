# bats file_tags=symlink
load ../instance

@test "absolute and relative symlinks" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/ref/
  runtest "### Touch     " unix SUCCESS "" eos touch /eos/$EOS_TEST_INSTANCE/test/instancetest/ref/touch
  runtest "### Symlink-d " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/symdir /eos/$EOS_TEST_INSTANCE/test/instancetest/ref
  runtest "### Symlink-r " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/symrel2 ../../test/instancetest/ref/
  runtest "### Symlink   " unix SUCCESS "" eos stat /eos/$EOS_TEST_INSTANCE/test/instancetest/symrel2/touch
  runtest "### Symlink-r " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/symrel1 ../instancetest/ref/
  runtest "### Symlink   " unix SUCCESS "" eos stat /eos/$EOS_TEST_INSTANCE/test/instancetest/symrel1/touch
  runtest "### Symlink-f " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/whoami /eos/$EOS_TEST_INSTANCE/test/instancetest/ref/touch
  runtest "### Symlink-E " unix ERROR "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/whoami /eos/$EOS_TEST_INSTANCE/proc/whoami
  runtest "### Symlink-E " unix ERROR "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/symdir /eos/$EOS_TEST_INSTANCE/test/instancetest/ref
  runtest "### Symlink   " unix SUCCESS "" eos ls /eos/$EOS_TEST_INSTANCE/test/instancetest/symdir/touch
  runtest "### Symlink   " unix SUCCESS "" eos stat /eos/$EOS_TEST_INSTANCE/test/instancetest/whoami
  runtest "### Symlink   " unix SUCCESS "" eos ls /eos/$EOS_TEST_INSTANCE/test/instancetest/whoami
  runtest "### Symlink-L " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/fwd-loop /eos/$EOS_TEST_INSTANCE/bwd-loop
  runtest "### Symlink-L " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/bwd-loop /eos/$EOS_TEST_INSTANCE/fwd-loop
  runtest "### Symlink-L " unix ERROR "" eos ls /eos/$EOS_TEST_INSTANCE/test/instancetest/bwd-loop
  runtest "### Symlink-L " unix ERROR "" eos ls /eos/$EOS_TEST_INSTANCE/test/instancetest/fwd-loop
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer/
  runtest "### Chown     " unix SUCCESS "" eos chown ${NOBODY_UID}:${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r 0 0'" eos cp /etc/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/passwd
  runtest "### Chmod     " unix SUCCESS "" eos chmod 700 /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/
  runtest "### Symlink-S " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/passwd
  runtest "### Chown     " unix SUCCESS "" eos chown -h ${NOBODY_UID}:${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer/passwd"
  runtest "### Download  " unix SUCCESS "EOSROLE='-r 0 0'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer/passwd" -
  runtest "### Download  " unix SUCCESS "EOSROLE='-r 0 0'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/passwd" -
  runtest "### Download  " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer/passwd" -
  runtest "### Download  " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/passwd" -
  runtest "### Rm        " unix SUCCESS "" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer/passwd
  runtest "### Rm        " unix SUCCESS "" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/passwd
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/dir
  runtest "### Symlink-S " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/link-dir /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/dir
  runtest "### Touch     " unix SUCCESS "" eos touch /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/file
  runtest "### Symlink-S " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/link-file /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/file
  runtest "### Ownership " unix SUCCESS "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/ | grep -w root | wc -l | grep -w 4"
  runtest "### Ownership " unix SUCCESS "" eos chown -h bin "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/link-file"
  runtest "### Ownership " unix SUCCESS "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/ | grep -w bin | wc -l | grep -w 1"
  runtest "### Ownership " unix SUCCESS "" eos chown -h root "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/link-file"
  runtest "### Ownership " unix SUCCESS "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/ | grep -w root | wc -l | grep -w 4"
  runtest "### Ownership " unix SUCCESS "" eos chown -r bin "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/"
  runtest "### Ownership " unix SUCCESS "" eos ls -la "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/ | grep -w bin | wc -l | grep -w 3"
  runtest "### Ownership " unix SUCCESS "" eos chown -r -h bin "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/"
  runtest "### Ownership " unix SUCCESS "" eos ls -la "/eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/ | grep -w bin | wc -l | grep -w 5"
  runtest "### Rmdir     " unix SUCCESS "" eos rm -rf /eos/$EOS_TEST_INSTANCE/test/instancetest/link-source/
  runtest "### Rmdir     " unix SUCCESS "" eos rm -rf /eos/$EOS_TEST_INSTANCE/test/instancetest/link-sniffer/
}
