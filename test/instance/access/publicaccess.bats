# bats file_tags=publicaccess
load ../instance

@test "publicaccesslevel limits the depth that nobody can reach" {
  runtest "### Mkdir    " unix SUCCESS "" eos mkdir -p /eos/$EOS_TEST_INSTANCE/test/instancetest/public/
  runtest "### Chmod    " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/public/"
  runtest "### Upload   " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/public/p1" "-ODeos.space=default"
  runtest "### Vid      " unix SUCCESS "" eos vid publicaccesslevel 2
  runtest "### Rm       " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/public/p1"
  runtest "### Rm       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/public/p1"
  runtest "### Rmdir    " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rmdir /eos/$EOS_TEST_INSTANCE/test/instancetest/public
  runtest "### Ls       " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/"
  runtest "### Ls       " unix SUCCESS "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test/"
  runtest "### Find     " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos find "/eos/$EOS_TEST_INSTANCE/test/"
  runtest "### Vid      " unix SUCCESS "" eos vid publicaccesslevel 1024
  runtest "### Ls       " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/"
  runtest "### Find     " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos find "/eos/$EOS_TEST_INSTANCE/test/"
  runtest "### Rmdir    " unix SUCCESS "" eos rmdir /eos/$EOS_TEST_INSTANCE/test/instancetest/public
}

@test "nobody reaches files deeper than publicaccesslevel with a token" {
  # eos token
  runtest "### Token    " sss SUCCESS "" eos_token /eos/$EOS_TEST_INSTANCE/test/instancetest/tokens/
}
