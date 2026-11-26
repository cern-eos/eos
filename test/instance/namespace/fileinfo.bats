# bats file_tags=fileinfo
load ../instance

@test "set up of the fileinfo tree" {
  runtest "### Mkdir      " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/"
  runtest "### Rm_recycle " unix 0 "" eos recycle config --remove-bin "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo"
  runtest "### Mkdir      " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/"
  runtest "### Mkdir      " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirB/"
  runtest "### Chmod      " unix SUCCESS "" eos chmod -r "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/"
  runtest "### Attr Set   " unix SUCCESS "" eos attr set default=replica "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/"
  runtest "### Attr Set   " unix SUCCESS "" eos attr set user.attr.key='value' "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirB/"
  runtest "### Upload     " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file\&symbol"
  runtest "### Upload     " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA"
}

@test "fileinfo of files and directories, also in JSON" {
  # Test basic functionality (including JSON)
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/ -m | grep container=2 | grep files=1"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirB/ -m | grep 'xattrn=user.attr.key' | grep xattrv=value"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA -m | grep nrep=2"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA | grep RBytes:"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA | grep Rbc:"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA | grep UBytes:"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA | grep Ubc:"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA -m | grep total-rbytes=0"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA -m | grep total-rbc=0"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA -m | grep total-ubytes=0"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA -m | grep total-ubc=0"
  runtest "### Account   " unix SUCCESS "" eos space config "default space.account.reads=true"
  runtest "### Account   " unix SUCCESS "" eos space config "default space.account.reads=1"
  runtest "### Account   " unix SUCCESS "" eos space config "default space.account.reads=0"
  runtest "### Account   " unix SUCCESS "" eos space config "default space.account.reads=false"
  runtest "### Account   " unix ERROR "" eos space config "default space.account.reads=on"
  runtest "### MkSpace   " unix SUCCESS "" eos "space define accountreads"
  runtest "### Account   " unix ERROR "" eos space config "accountreads space.account.reads=1"
  runtest "### RmSpace   " unix SUCCESS "" eos "space rm accountreads"
  runtest "### Account   " unix SUCCESS "" eos space config "rm default space.account.reads"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file\&symbol | jq -r '.name' | grep -w 'file\&symbol'"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirB/ | jq -r '.xattr' | grep -w 'user.attr.key' | grep -w value"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA -m | jq -r '.nstripes' | grep -w 2"
}

@test "fileinfo accepts fid, fxid, pid and pxid" {
  # Test fid|fxid|pid|pxid identifiers
  pid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/" | jq -r .id)
  pxid=$(printf "%x" $pid)
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file&symbol" | jq -r .id)
  fxid=$(printf "%x" $fid)
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "pid:${pid} | jq -r '.name' | grep -w fileinfo"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "pxid:${pxid} | jq -r '.name' | grep -w fileinfo"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "fid:${fid} | jq -r '.name' | grep -w 'file\&symbol'"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "fxid:${fxid} | jq -r '.name' | grep -w 'file\&symbol'"
  runtest "### Rm         " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file\&symbol"
  runtest "### Rm         " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/fileA"
  runtest "### Rmdir      " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirA/"
  runtest "### Rmdir      " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/dirB/"
  # Test detached flag
  runtest "### Upload     " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file1"
  runtest "### Upload     " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file2"
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file1 -m | grep -w detached=0"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file1 | jq -r '.detached' | grep false"
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file1" | jq -r .id)
  runtest "### Rm         " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file1"
  # The complete deletion of the file could happen by the time we issue the next command
  # so the file info command will file with "found deletion tombstone" error. The test is
  # still considered successful in this case.
  runtest "### Fileinfo   " unix SUCCESS "" eos fileinfo "fid:${fid} -m 2>&1 | grep -w 'detached=1\|tombstone'"
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file2" | jq -r .id)
  runtest "### Rm         " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/file2"
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "fid:${fid} | jq -r '.detached' | grep true"
}

@test "clean up of the fileinfo tree" {
  runtest "### Rmdir      " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/fileinfo/"
}
