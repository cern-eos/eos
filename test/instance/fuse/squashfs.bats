# bats file_tags=squashfs
load ../instance

# An eosxd mount of /eos at /eos, without a sss keytab. krb5 and x509 are off.
setup_file() {
  export X509_USER_PROXY=/tmp/invalid
  export KRB5CCNAME=/tmp/invalid
  ! test -d /eos/ || umount -fl /eos/ 2>/dev/null || true
  mkdir -p /eos/
  ! test -e /etc/eos/fuse.sss.keytab || mv -f /etc/eos/fuse.sss.keytab /etc/eos/fuse.sss.keytab.save
  eosxd -ofsname=localhost.localdomain:/eos /eos/ 3>&- || true
  wait_until 30 test -e "/eos/$EOS_TEST_INSTANCE" || true
}

teardown_file() {
  ! test -d /eos/ || umount -fl /eos/ 2>/dev/null || true
  rmdir /eos/ 2>/dev/null || true
  ! test -e /etc/eos/fuse.sss.keytab.save || mv -f /etc/eos/fuse.sss.keytab.save /etc/eos/fuse.sss.keytab
}

@test "squash packages and releases" {
  [ -e "/eos/$EOS_TEST_INSTANCE" ] || skip "no eosxd mount at /eos"
  # simple sqash packages
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash"
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --add-bin /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash"
  runtest "### New SQFS  " unix SUCCESS "" eos squash new "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1
  runtest "### Pack SQFS " unix SUCCESS "" eos squash pack "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1"
  runtest "### Pack SQFS " unix ERROR "" eos squash pack "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1"
  runtest "### Pack SQFS " unix ERROR "" eos squash pack "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package2"
  runtest "### info SQFS " unix SUCCESS "" eos squash info "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1"
  runtest "### rm label  " unix SUCCESS "" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1
  runtest "### label SQFS" unix SUCCESS "" eos squash relabel /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1
  runtest "### label SQFS" unix ERROR "" eos squash relabel /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package2
  runtest "### rm SQFS   " unix SUCCESS "" eos squash rm /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1
  runtest "### stat SQFS " unix ERROR "" eos stat /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/package1
  runtest "### stat SQFS " unix ERROR "" eos stat /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/.package1.sqsh

  # squash releases
  runtest "### New SQFSR " unix SUCCESS "" eos squash new-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1/next
  runtest "### Pack SQFSR" unix SUCCESS "" eos squash pack-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1"
  # A release is named by its time in seconds: the next one needs another second
  runtest "### Delay       " unix SUCCESS "" delay 1
  runtest "### New SQFSR " unix SUCCESS "" eos squash new-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1 1.0.1"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1/next
  runtest "### Pack SQFSR" unix SUCCESS "" eos squash pack-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1"
  runtest "### info SQFSR" unix SUCCESS "" eos squash info-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1"
  runtest "### info SQFSR" unix ERROR "" eos squash info-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release2"
  runtest "### trim SQFSR" unix SUCCESS "" eos squash trim-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1 1 1 "
  runtest "### trim SQFSR" unix ERROR "" eos squash trim-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1 0 0 "
  runtest "### rm SQFSR"   unix SUCCESS "" eos squash rm-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1"
  runtest "### rm SQFSR"   unix ERROR "" eos squash rm-release "/eos/$EOS_TEST_INSTANCE/test/instancetest/squash/release1"
  runtest "### Rm        " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/squash/
}
