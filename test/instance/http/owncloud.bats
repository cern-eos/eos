# bats file_tags=owncloud
load ../instance

@test "set up of the owncloud directory" {
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set default=replica "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc"
}

@test "upload with chunking" {
  # upload with chunking
  runtest "### OcChunk   " unix SUCCESS "" shell eos-oc-test chunkedupload chunked /eos/$EOS_TEST_INSTANCE/test/instancetest/oc/
}

@test "check it exists" {
  # check it exists
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc/chunked"
  runtest "### OcChunk   " unix ERROR "" shell eos-oc-test chunkedupload chunked
}

@test "upload again" {
  # upload again
  runtest "### OcChunk   " unix SUCCESS "" shell eos-oc-test chunkedupload chunked /eos/$EOS_TEST_INSTANCE/test/instancetest/oc/
}

@test "check that there is no version" {
  # check that there is no version
  runtest "### Stat      " unix ERROR "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc/.sys.v#.chunked/"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc/chunked"
}

@test "enable versioning" {
  # enable versioning
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.versioning=2 /eos/$EOS_TEST_INSTANCE/test/instancetest/oc/
  runtest "### OcChunk   " unix SUCCESS "" shell eos-oc-test chunkedupload chunked /eos/$EOS_TEST_INSTANCE/test/instancetest/oc/
  runtest "### OcChunk   " unix SUCCESS "" shell eos-oc-test chunkedupload chunked /eos/$EOS_TEST_INSTANCE/test/instancetest/oc/
}

@test "check that versions are kept as expected" {
  # check that versions are kept as expected
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc/.sys.v#.chunked/"
  runtest "### Find      " unix SUCCESS "" eos find "-f --count /eos/$EOS_TEST_INSTANCE/test/instancetest/oc | grep nfiles=3"
}

@test "Clean up the files created in owncloud tests" {
  # Clean up the files created in owncloud tests
  runtest "### Cleanup   " unix IGNORE  "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/oc"
}
