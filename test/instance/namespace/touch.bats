# bats file_tags=touch
load ../instance

touchTestDir="/eos/$EOS_TEST_INSTANCE/test/instancetest/touch_test"

@test "touch of a directory and of files" {
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir -p "$touchTestDir"
  runtest "### Stat        " unix 14 "" eos stat "$touchTestDir/dir1/" # touch directory does not exist yet
  runtest "### Touch       " unix 21 "" eos touch "$touchTestDir/dir1/"
  runtest "### Mkdir       " unix SUCCESS "" eos mkdir "$touchTestDir/dir1/"
  runtest "### Stat        " unix SUCCESS "" eos stat "$touchTestDir/dir1/"
}

@test "touch on the dir and file uri should return in the same error" {
  # touch on the dir and file uri should return in the same error
  runtest "### Touch       " unix 21 "" eos touch "$touchTestDir/dir1"
  runtest "### Touch       " unix 21 "" eos touch "$touchTestDir/dir1/"
}

@test "touch a file in a directory that does not exist" {
  # touch a file in a directory that does not exist
  runtest "### Touch       " unix 2 "" eos touch "$touchTestDir/dir2/file2"
}

@test "touch a file that does not previously exist" {
  # touch a file that does not previously exist
  runtest "### Stat       " unix 14 "" eos stat "$touchTestDir/dir1/file1"
  runtest "### Touch      " unix SUCCESS "" eos touch "$touchTestDir/dir1/file1"
  runtest "### Stat       " unix SUCCESS "" eos stat "$touchTestDir/dir1/file1"
}

@test "touch a file that already exists" {
  # touch a file that already exists
  runtest "### Touch      " unix SUCCESS "" eos touch "$touchTestDir/dir1/file1"
}

@test "clean up of the touch tests" {
  # cleanup
  runtest "### Cleanup     " unix SUCCESS "" eos rm -rF "$touchTestDir"
}
