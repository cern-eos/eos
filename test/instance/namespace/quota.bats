# bats file_tags=quota
load ../instance

@test "user and project quota" {
  command eos -b space status default | grep quota | grep -q on || skip "the quota of the default space is off"
  # if quota is enabled
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest"
  runtest "### Quota     " unix SUCCESS "" eos quota set -u ${NOBODY_UID} -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.quota" "-ODeos.ruid=${NOBODY_UID}" "-DITransactionTimeout 30"
  runtest "### Quota     " unix SUCCESS "" eos quota set -u ${NOBODY_UID} -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.quota" "-ODeos.ruid=${NOBODY_UID}" "-DITransactionTimeout 30"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.quota"
  runtest "### Quota     " unix SUCCESS "" eos quota set -u ${NOBODY_UID} -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Quota     " unix ERROR "" eos quota set -u ${NOBODY_UID} -p /doesnotexist -v 1T -i 1M
  runtest "### Quota     " unix SUCCESS "" eos quota set -u 3 -p /eos/$EOS_TEST_INSTANCE/test -v 1T -i 1M
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/1k" "-ODeos.ruid=3" "-DITransactionTimeout 30"
  runtest "### QUpdate   " unix SUCCESS "" eos_quota "/eos/$EOS_TEST_INSTANCE/test/" "uid=adm"
  runtest "### Rem       " unix IGNORE  "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/1k"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/testproj
  runtest "### Quota     " unix SUCCESS "" eos quota set -g 99 -p /eos/$EOS_TEST_INSTANCE/test/testproj -v 1T -i 1M
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/testproj/1k" "-ODeos.ruid=3"
  runtest "### QUpdate   " unix SUCCESS "" eos quota ls -m "/eos/$EOS_TEST_INSTANCE/test/testproj" "| grep \"gid=project space=/eos/$EOS_TEST_INSTANCE/test/testproj\""
  runtest "### Rem       " unix IGNORE  "" eos rm  "/eos/$EOS_TEST_INSTANCE/test/testproj/1k"
  runtest "### RmdQuota  " unix SUCCESS "" eos quota rmnode -p "/eos/$EOS_TEST_INSTANCE/test/testproj/"
  runtest "### QUpdate   " unix ERROR   "" eos_quota_node_exists "/eos/$EOS_TEST_INSTANCE/test/testproj/"
  runtest "### Rmdir     " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/testproj/"
}
