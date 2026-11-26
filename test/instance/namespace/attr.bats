# bats file_tags=attr
load ../instance

@test "attr set, ls, get and rm" {
  runtest "### Attr Set  " unix SUCCESS "" eos attr set default=replica "/eos/$EOS_TEST_INSTANCE/test"
  runtest "### Attr Ls   " unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test | grep sys | wc -l | grep -w 6"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get "sys.forced.space /eos/$EOS_TEST_INSTANCE/test  | grep default"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm  "sys.forced.blocksize /eos/$EOS_TEST_INSTANCE/test"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm  "sys.forced.checksum /eos/$EOS_TEST_INSTANCE/test"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm  "sys.forced.layout /eos/$EOS_TEST_INSTANCE/test"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm  "sys.forced.nstripes /eos/$EOS_TEST_INSTANCE/test"
  runtest "### File Touch" unix SUCCESS "" eos file touch "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.forced.atomic=1 "/eos/$EOS_TEST_INSTANCE/test"
  runtest "### File Touch" unix SUCCESS "" eos file touch "/eos/$EOS_TEST_INSTANCE/test/file.atomic"
  runtest "### Attr Set  " unix SUCCESS "" eos attr rm sys.forced.atomic "/eos/$EOS_TEST_INSTANCE/test"
  runtest "### Attr Set  " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/file.atomic"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.test=foo "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.attr.test "/eos/$EOS_TEST_INSTANCE/test/file | grep foo"
  runtest "### Attr Get  " unix ERROR "" eos attr get user.attr.bar "/eos/$EOS_TEST_INSTANCE/test/file | grep foo"
  runtest "### Attr Ls   " unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/file | grep foo"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.test "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Rm   " unix ERROR "" eos attr rm user.attr.test "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### SpaceAttr " unix SUCCESS "" eos space config default space.attr.sys.foo=bar
  runtest "### SpaceAttr " unix SUCCESS "" eos attr get sys.foo "/eos/$EOS_TEST_INSTANCE/test/  | grep bar"
  runtest "### SpaceAttr " unix SUCCESS "" eos space config rm default space.attr.sys.foo
  runtest "### SpaceAttr " unix ERROR "" eos attr get sys.foo "/eos/$EOS_TEST_INSTANCE/test/  | grep bar"
}

@test "attr ls and get in JSON" {
  # Test JSON output for attributes
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.json='value' "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.json2='2ndvalue' "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.json3='3' "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.json4='value\"quote' "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.json5='value1\&value2\&value3' "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.json6='value\\\"quote' "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Get  " unix SUCCESS "" eosj attr get user.attr.json "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.get[].user.attr.json' | grep -w 'value'"
  runtest "### Attr Get  " unix SUCCESS "" eosj attr get user.attr.json2 "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.get[].user.attr.json2' | grep -w '2ndvalue'"
  runtest "### Attr Get  " unix SUCCESS "" eosj attr get user.attr.json3 "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.get[].user.attr.json3' | grep -w '3'"
}

@test "literal quotes and backslash-quote values stay apart" {
  # Preserve literal quotes independently from literal backslash-plus-quote values.
  runtest "### Attr Get  " unix SUCCESS "" eosj attr get user.attr.json4 "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.get[].user.attr.json4' | grep -Fqx 'value\"quote'"
  runtest "### Attr Get  " unix SUCCESS "" eosj attr get user.attr.json6 "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.get[].user.attr.json6' | grep -Fqx 'value\\\"quote'"
  runtest "### Attr Ls   " unix SUCCESS "" eosj attr ls "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.ls[].user.attr.json4 // empty' | grep -Fqx 'value\"quote'"
  runtest "### Attr Ls   " unix SUCCESS "" eosj attr ls "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.ls[].user.attr.json6 // empty' | grep -Fqx 'value\\\"quote'"
  runtest "### Attr Get  " unix SUCCESS "" eosj attr get user.attr.json5 "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.get[].user.attr.json5' | grep -w 'value1\&value2\&value3'"
  runtest "### Attr Ls   " unix ERROR "" eosj attr ls "/eos/$EOS_TEST_INSTANCE/test/file | jq -r '.attr.ls[].user.attr.json5' | grep -w 'nullvalues'"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.json "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.json2 "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.json3 "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.json4 "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.json5 "/eos/$EOS_TEST_INSTANCE/test/file"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.json6 "/eos/$EOS_TEST_INSTANCE/test/file"
}

@test "attr accepts fid, fxid, pid and pxid" {
  # Test fid|fxid|pid|pxid identifiers for attributes
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/file" | jq -r .id)
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.attr.key='value' "fid:${fid}"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.attr.key "fid:${fid} | grep -w 'value'"
  runtest "### Attr Get  " unix SUCCESS "" eosj attr get user.attr.key "fid:${fid} | jq -r '.attr.get[].user.attr.key' | grep -w 'value'"
  runtest "### Attr Rm   " unix SUCCESS "" eos attr rm user.attr.key "fid:${fid}"
  runtest "### Attr Ls   " unix SUCCESS "" eos attr ls "fid:${fid} | wc -l | grep -w 4"
  runtest "### Attr Get  " unix ERROR "" eos attr get user.attr.key "fid:invalid"
  runtest "### Attr Get  " unix ERROR "" eos attr get user.attr.key "fxid:123456"
  runtest "### Attr Get  " unix ERROR "" eos attr get user.attr.key "pxid:"
  runtest "### Rm        " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/file"
}

@test "Xattr locks" {
  # Xattr locks
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock"
  runtest "### File Lck  " unix SUCCESS "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### File Lck  " unix SUCCESS "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### File Lck  " unix ERROR "EOSAPP='-a app2' EOSROLE='-r 2 2'" eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### eoscp     " unix ERROR  "EOSROLE='-r 2 2'" eos cp /eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1 /dev/null
  runtest "### eoscp     " unix SUCCESS  "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" /dev/null
  runtest "### eoscp     " unix ERROR  "EOSAPP='-a app2' EOSROLE='-r 2 2'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" /dev/null
  runtest "### File ULck " unix ERROR "EOSAPP='-a app2' EOSROLE='-r 2 2'" eos file touch -u "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### File ULck " unix SUCCESS "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos file touch -u "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### File Lck  " unix SUCCESS "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" 0
  # The lock of app1 expires: app2 gets it
  runtest "### File Lck  " unix SUCCESS "EOSAPP='-a app2' EOSROLE='-r 2 2'" wait_for 10 eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### File Lck  " unix ERROR "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### File Lck  " unix SUCCESS "EOSAPP='-a app2' EOSROLE='-r 2 2'" eos file touch -u "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
}

@test "user audience" {
  # user audience
  runtest "### File Lck  " unix SUCCESS "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1 1000 user"
  runtest "### eoscp     " unix SUCCESS  "EOSAPP='-a app2' EOSROLE='-r 2 2'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" /dev/null
  runtest "### eoscp     " unix ERROR  "EOSAPP='-a app2' EOSROLE='-r 3 3'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" /dev/null
}

@test "app audience" {
  # app audience
  runtest "### File Lck  " unix SUCCESS "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos file touch -l "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1 1000 app"
  runtest "### eoscp     " unix ERROR  "EOSAPP='-a app2' EOSROLE='-r 2 2'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" /dev/null
  runtest "### eoscp     " unix SUCCESS  "EOSAPP='-a app1' EOSROLE='-r 2 2'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" /dev/null
  runtest "### eoscp     " unix SUCCESS  "EOSAPP='-a app1' EOSROLE='-r 3 3'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1" /dev/null
  runtest "### Rm        " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock/lock1"
  runtest "### Rmdir     " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/xattr-lock"
}
