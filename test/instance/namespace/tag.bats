# bats file_tags=tag
load ../instance

@test "file tag adds and removes a location" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/
  runtest "### Touch     " unix SUCCESS "" eos touch -n /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch
  runtest "### Tag       " unix SUCCESS "" eos file tag /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch +123456
  runtest "### Tag       " unix ERROR "" eos file tag /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch +123456
  runtest "### Tag       " unix SUCCESS "" eos file tag /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch ~123456
  runtest "### Tag       " unix SUCCESS "" eos file tag /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch ~123456
  runtest "### Tag       " unix SUCCESS "" eos file tag /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch -123456
  runtest "### Tag       " unix ERROR "" eos file tag /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch -123456
  tag_fxid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch | jq -r .fxid)
  runtest "### Tag       " unix SUCCESS "" eos file tag fxid:${tag_fxid} +98765
  runtest "### Tag       " unix ERROR "" eos file tag fxid:${tag_fxid} +98765
  runtest "### Tag       " unix SUCCESS "" eos file tag fxid:${tag_fxid} -98765
  runtest "### Tag       " unix ERROR "" eos file tag fxid:${tag_fxid} -98765
  runtest "### Rm        " unix SUCCESS "" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/touch
  runtest "### Rmdir     " unix SUCCESS "" eos rmdir /eos/$EOS_TEST_INSTANCE/test/instancetest/tag/
}
