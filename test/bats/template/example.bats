#!/usr/bin/env bats
# bats file_tags=example
# ------------------------------------------------------------------------------
# <Suite>, <part>: what these tests check. Rename the file to <part>.bats and the
# tag to <part>. Number the files (1-<part>.bats) only when one needs the state
# of an earlier one.
# ------------------------------------------------------------------------------

load template

@test "an upload reads back the same bytes" {
  echo "hello" > $BATS_TEST_TMPDIR/hello
  upload $BATS_TEST_TMPDIR/hello $testdir/hello
  same_file $BATS_TEST_TMPDIR/hello $testdir/hello
}

@test "nobody may not write in a directory of root with mode 755" {
  eos mkdir $testdir/closed
  eos chmod 755 $testdir/closed
  run eos_as 99 99 touch $testdir/closed/f
  assert_failure
}

@test "a space setting is undone at the end of the suite" {
  space_set default attr.sys.bats.template 1
  run -0 space_value default attr.sys.bats.template
  assert_output 1
}

@test "a file that is not there cannot be read" {
  ! xrdcp -s $url/$testdir/missing - 2>/dev/null
}
