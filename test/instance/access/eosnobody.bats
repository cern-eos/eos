# bats file_tags=eosnobody
load ../instance

teardown() {
  rm -f /tmp/eos.nobody.keytab
  instance_teardown
}

@test "the eosnobody sss key may read only .sqsh files" {
  grep eosnobody $SERVERKEY > /tmp/eos.nobody.keytab || skip "no eosnobody key in $SERVERKEY"
  chmod 400 /tmp/eos.nobody.keytab
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir -p /eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/p1"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/.p1.sqsh"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "700" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/"
  runtest "### Ls        " unix ERROR "XrdSecSSSKT='/tmp/eos.nobody.keytab'" eosr ls -la /eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/
  runtest "### Ls        " sss ERROR "XrdSecSSSKT='/tmp/eos.nobody.keytab'" eosr ls -la /eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/
  runtest "### Download  " sss ERROR "XrdSecSSSKT='/tmp/eos.nobody.keytab'" downloadh  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/p1"
  runtest "### Download  " sss SUCCESS "XrdSecSSSKT='/tmp/eos.nobody.keytab'" eos_squash /eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/.p1.sqsh
  runtest "### Rm        " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/eosnobody/
}
