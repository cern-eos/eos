# bats file_tags=stress
load ../instance

@test "xrdstress writes and reads 100 files" {
  runtest "### Chmod      " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/"
  runtest "### Attr Set   " unix SUCCESS "" eos attr set default=replica "/eos/$EOS_TEST_INSTANCE/test"
  runtest "### Stress Wpp " unix SUCCESS "" stress "-d root://localhost//eos/$EOS_TEST_INSTANCE/test -o wr -f 50 -s 1KB -n stress -j 10 -c -p"
  runtest "### Stress Wp  " unix SUCCESS "" stress "-d root://localhost//eos/$EOS_TEST_INSTANCE/test -o wr -f 50 -s 1KB -n stress -j 10 -c "
  runtest "### Find       " unix SUCCESS "" eos find "-f --count /eos/$EOS_TEST_INSTANCE/test/ | grep nfiles=100"
  runtest "### Stress Rp  " unix SUCCESS "" stress "-d root://localhost//eos/$EOS_TEST_INSTANCE/test -o rd -f 50 -s 1KB -n stress -j 10 -c"
  runtest "### Stress Rpp " unix SUCCESS "" stress "-d root://localhost//eos/$EOS_TEST_INSTANCE/test -o rd -f 50 -s 1KB -n stress -j 10 -c -p"
  runtest "### Cleanup    " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/stress/*"
  runtest "### Ls         " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test | grep [a-z] | wc -l | grep -w 2"
  runtest "### Ls         " unix SUCCESS "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test | grep ^d| wc -l | grep -w 2"
  runtest "### Ls         " unix SUCCESS "" eos ls -la "/eos/$EOS_TEST_INSTANCE/test | grep ^d| wc -l | grep -w 4"
  runtest "### Find       " unix SUCCESS "" eos find -d "/eos/$EOS_TEST_INSTANCE/test | grep eos | wc -l | grep -w 3"
}
