# bats file_tags=eoscp
load ../instance

@test "eos cp copies between local, EOS and symlinks" {
  runtest "### Setup     " unix SUCCESS "" shell ln -s /etc/passwd /tmp/passwd.ln
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp -d /etc/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp -
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp.1
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp root://$EOS_TEST_REDIRECTOR//eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp.1 root://$EOS_TEST_REDIRECTOR//eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp.2
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp root://$EOS_TEST_REDIRECTOR//eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp.2 /tmp/
  runtest "### eoscp     " unix ERROR "EOSROLE='-r 0 0'" eos cp /etc/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/a/b/c/
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp -p /etc/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/a/b/c/
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp -p /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/* /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp2
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp -r /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp3
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp /tmp/passwd.ln /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp3
  runtest "### Symlink-S " unix SUCCESS "" eos ln /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp.ln /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp3
  runtest "### eoscp     " unix SUCCESS "EOSROLE='-r 0 0'" eos cp /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp/passwd.eoscp.ln /tmp/
  runtest "### Cleanup   " unix SUCCESS "" shell rm /tmp/passwd.ln /tmp/passwd.eoscp.2 /tmp/passwd.eoscp.ln
  runtest "### Cleanup   " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp
  runtest "### Cleanup   " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp2
  runtest "### Cleanup   " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/eoscp3
}
