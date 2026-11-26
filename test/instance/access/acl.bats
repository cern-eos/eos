# bats file_tags=acl
load ../instance

@test "eos acl sets, lists and updates the ACLs of a tree" {
  runtest "### Mkdir   " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Mkdir   " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/acl/dir1"
  runtest "### Mkdir   " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/acl/dir1/dir2"
  runtest "### Acl set " unix SUCCESS "" eos acl --recursive u:daemon=rwx "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl set " unix SUCCESS "" eos acl --recursive u:${NOBODY_UID}=rwx "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl ls  " unix SUCCESS "" "eos attr -r ls \"/eos/$EOS_TEST_INSTANCE/test/acl\" | grep -o -c 'sys.acl=\"u:2:rwx,u:65534:rwx\"' | grep '3'"
  runtest "### Acl ls  " unix SUCCESS "" eos attr -r rm sys.acl "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### RmDir   " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/acl/dir1/dir2"
  runtest "### RmDir   " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/acl/dir1/"
  runtest "### Acl set " unix ERROR "" eos acl u:daemon=\!x /eos/does_not_exist
  runtest "### Acl set " unix ERROR "" eos acl --recursive u:daemon=\!x /eos/does_not_exist
  runtest "### Acl ls  " unix ERROR "" eos acl -l "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl set " unix SUCCESS "" eos acl u:daemon=rwx "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl ls  " unix SUCCESS "" eos acl -l "/eos/$EOS_TEST_INSTANCE/test/acl" "| grep \"u:$(id -u daemon):rwx\""
  runtest "### Acl upd " unix SUCCESS "" eos acl u:daemon:+\!d+\!u+q+c "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl ls  " unix SUCCESS "" eos acl -l "/eos/$EOS_TEST_INSTANCE/test/acl" "| grep \"u:$(id -u daemon):rwx!d!uqc\""
  runtest "### Acl upd " unix SUCCESS "" eos acl u:daemon:-x-\!d-\!u-c "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl ls  " unix SUCCESS "" eos acl -l "/eos/$EOS_TEST_INSTANCE/test/acl" "| grep \"u:$(id -u daemon):rwq\""
  runtest "### Acl set " unix SUCCESS "" eos acl u:daemon=rw+d+u "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl ls  " unix SUCCESS "" eos acl -l "/eos/$EOS_TEST_INSTANCE/test/acl" "| grep \"u:$(id -u daemon):rw+d+u\""
  runtest "### Acl upd " unix SUCCESS "" eos acl u:daemon:-r-w-+d-+u "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl ls  " unix ERROR "" eos acl -l "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### dir ls  " unix SUCCESS "EOSROLE='-r daemon daemon'" eos ls "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl deny ls" unix SUCCESS "" eos acl u:daemon=\!x "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### dir ls  " unix ERROR "EOSROLE='-r daemon daemon'" eos ls "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Acl set " unix SUCCESS "" eos acl u:daemon=rwx "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### dir ls  " unix SUCCESS "EOSROLE='-r daemon daemon'" eos ls "/eos/$EOS_TEST_INSTANCE/test/acl"
}

@test "directory and file sys.acl both apply" {
  # Test the fact that both directory and file level sys.acl are taken into account
  runtest "### Rmdir   " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Mkdir   " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Chown   " unix SUCCESS "" eos chown 2:2   "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Chmod   " unix SUCCESS "" eos chmod "700" "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Upload  " unix SUCCESS "EOSROLE='-r 2 2'"   eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat"
  runtest "### Download" unix SUCCESS "EOSROLE='-r 2 2'"   eos cp "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat" -
  runtest "### Download" unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat" -
  runtest "### Acl set " unix SUCCESS "" eos acl u:${NOBODY_UID}=rw "/eos/$EOS_TEST_INSTANCE/test/acl"
  runtest "### Download" unix SUCCESS "EOSROLE='-r  ${NOBODY_UID}  ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat" -
  runtest "### Download" unix ERROR "EOSROLE='-r 985 979'" eos cp "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat" -
  runtest "### Acl set " unix SUCCESS "" eos acl u:985=rw "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat"
  runtest "### Download" unix SUCCESS "EOSROLE='-r 985 970'" eos cp "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat" -
  runtest "### Download" unix SUCCESS "EOSROLE='-r  ${NOBODY_UID}  ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/acl/file.dat" -
}

@test "the !d ACL denies a rename" {
  # Test the file rename ACL !d
  runtest "### Mkdir   " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src"
  runtest "### Mkdir   " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_dst"
  runtest "### Acl set " sss 0 "" eos acl u:2=rwx!d "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src"
  runtest "### Acl set " sss 0 "" eos acl u:2=rwx!d "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_dst"
  runtest "### Upload  " unix SUCCESS "EOSROLE='-r 2 2'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/uid2.dat"
  runtest "### Upload  " unix SUCCESS "EOSROLE='-r 2 2'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/nobody.dat"
  runtest "### Chown   " unix SUCCESS  "" eos chown "${NOBODY_UID}:${NOBODY_GID}" "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/nobody.dat"
}

@test "the owner may move its file despite !d" {
  # uid 2 should be able to move their file despite the !d permission as it is their file
  runtest "### Move    " unix SUCCESS "EOSROLE='-r 2 2'" eos file rename "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/uid2.dat" "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_dst/uid2.dat"
}

@test "uid 2 may not move a file of nobody" {
  # uid 2 should not be able to move the NOBODY file as they don't own it
  runtest "### Move    " unix ERROR "EOSROLE='-r 2 2'" eos file rename "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/nobody.dat" "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_dst/nobody.dat"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default space.attr.sys.acl=u:1000:rwx
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep u:1000:rwx"
  runtest "### SpaceACL" unix SUCCESS "" eos attr get sys.acl "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep u:1000:rwx"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default "space.attr.sys.acl=\|u:1000:rwx"
  runtest "### SpaceACL" unix ERROR "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep u:1000:rwx"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default "space.attr.sys.acl=\<u:1000:rwx"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep -F 'u:1000:rwx,u:65534:rw,u:2:rwx!d'"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default "space.attr.sys.acl=\>u:1000:rwx"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep -F 'u:65534:rw,u:2:rwx!d,u:1000:rwx'"
  runtest "### SpaceACL" unix SUCCESS "" eos space config rm default "space.attr.sys.acl"
}

@test "a space ACL that the directory has already is not duplicated" {
  # Test that if directory already has the space acl then we don't see duplicates
  runtest "### SpaceACL" sss SUCCESS "" eos acl u:1000=rx "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep -F 'u:65534:rw,u:2:rwx!d,u:1000:rx'"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default space.attr.sys.acl=u:1000:rx
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep u:1000:rx | grep -v u:65534:rw"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default "space.attr.sys.acl=\|u:1000:rx"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep u:1000:rx | grep u:65534:rw"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default "space.attr.sys.acl=\<u:1000:rx"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep -F 'u:1000:rx,u:65534:rw,u:2:rwx!d'"
  runtest "### SpaceACL" unix SUCCESS "" eos space config default "space.attr.sys.acl=\>u:1000:rx"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep -F 'u:65534:rw,u:2:rwx!d,u:1000:rx'"
  runtest "### SpaceACL" unix SUCCESS "" eos space config rm default "space.attr.sys.acl"
  runtest "### SpaceACL" unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/acl/nobody_src/ | grep u:1000:rx"
  runtest "### Rmdir   " unix SUCCESS "" eos rm -rf "/eos/$EOS_TEST_INSTANCE/test/acl"
}

@test "concurrent recursive ACL changes" {
  # Test concurrent ACL recursive modifications
  runtest "### Mkdir   " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/acl_concurrent"
  runtest "### Acl rec " unix SUCCESS "" eos_acl_concurrent "/eos/$EOS_TEST_INSTANCE/test/acl_concurrent"
}
