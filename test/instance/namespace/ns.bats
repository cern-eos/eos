# bats file_tags=ns
load ../instance

@test "cd and ls of a missing path fail" {
  runtest "### Cd        " unix ERROR "" eos cd /__dont_exists
  runtest "### Cd        " unix SUCCESS "" eos cd /eos/$EOS_TEST_INSTANCE/test
  runtest "### Ls        " unix ERROR "" eos ls /__dont_exist
}

@test "mode bits and sys.acl give access" {
  runtest "### Chmod     " unix SUCCESS "" eos chmod 700 /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Ls        " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Ls        " unix SUCCESS "" eos ls /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Attr      " unix ERROR "" eos attr set sys.acl=illegal /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:nobody:rx /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Upload    " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:nobody:rwox /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Upload    " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Upload    " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Rm        " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:nobody:rwx,u:100:rwx /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Upload    " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Upload    " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.100.1k" "-ODeos.ruid=100\&eos.rgid=100"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:nobody:rwox /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Rm        " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:nobody:rwx!d /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Rm        " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.100.1k
  runtest "### Rm        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:100:rwx+d,g:nobody:rwx!d /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Rm        " unix SUCCESS "EOSROLE='-r 100 ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.100.1k
  runtest "### Attr      " unix SUCCESS "" eos attr rm sys.acl /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:nobody:rwx!u /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest "### Upload    " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Update    " unix ERROR "" append    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 755 /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Attr      " unix SUCCESS "" eos attr rm sys.acl /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.eval.useracl=1 /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/new
  runtest "### Mkdir     " unix ERROR "" eos mkdir /eos/$EOS_TEST_INSTANCE/new
}

@test "mode bits decide when no ACL applies (sss, not sudoer)" {
  # "runtest H" with sss-auth secures "not sudoer" and group list contains ${NOBODY_UID} where needed... without H the former fails, with unix-auth the latter (gids=[2] hence hasAcl=0)
  # ACL impact: no ACL or user neither granted nor denied -> dir mode bits
  # ACL denials: override except for u/d for file owner
  runtest   "### Attr      " unix SUCCESS "" eos debug debug '\*'
  runtest   "### Chown     " unix SUCCESS "" eos chown daemon /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest   "### Chown     " unix SUCCESS "" eos chown daemon /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k
}

@test "most frequent case, file/dir without ACL mode bits decide" {
  # most frequent case, file/dir without ACL mode bits decide
  runtest   "### Chmod     " unix SUCCESS "" eos chmod 777 /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss SUCCESS "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # no ACL, dir mode bits allow (o=rw)
  runtest   "### Chmod     " unix SUCCESS "" eos chmod 755 /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss ERROR "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # fail: no ACL, dir mode bits deny
  runtest   "### Chmod     " unix SUCCESS "" eos chmod 775 /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss ERROR "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # fail: no ACL, dir mode bits deny (g=rw but not group)
  runtest   "### Chown     " unix SUCCESS "" eos chown 2:${NOBODY_GID} /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss SUCCESS "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # no ACL, dir mode bits allow (g=rw same group)
  runtest   "### Chown     " unix SUCCESS "" eos chown ${NOBODY_UID}:2 /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss SUCCESS "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # no ACL, dir mode bits allow (u=rw same user)
}

@test "user.acl !d denies delete in a directory that is not owned" {
  runtest   "### Chown     " unix SUCCESS "" eos chown 2:2 /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Rm        " sss ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k     # fail: dir not owned, no ACL
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:nobody:rwx!d /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Rm        " sss ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k     # fail: dir not owned, ACL denies delete
  runtest H "### Attr      " unix SUCCESS "" eos whoami         # running as user 2:2
  runtest   "### Mkdir     " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl
  runtest   "### Chown     " unix SUCCESS "" eos chown 1:1 /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:2:rwxc /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl       # the 'c'-right should be ignored on user.acl
  runtest H "### Chown     " sss ERROR "" eos chown ${NOBODY_UID} /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl                         # fail: 'c' must only effective in sys.acl
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:2:c /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl
  runtest H "### Chown     " sss SUCCESS "" eos chown ${NOBODY_UID} /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl                         # should work with 'c' in sys.acl
  runtest   "### Chown     " unix SUCCESS "" eos chown root /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:${NOBODY_UID}:rwx /eos/$EOS_TEST_INSTANCE/test/instancetest               # the 'c'-right should be ignored on user.acl
}

@test "in a directory not owned, the ACL allows the write" {
  # dir not owned
  runtest H "### Rm        " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k             # dir not owned, but ACL allows write
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"  # ACL allows write, new file owned
  runtest H "### Update    " sss SUCCESS "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # dir not owned, ACL allows write
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:nobody:rx /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss ERROR "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # fail: dir not owned, ACL does not grant write
}

@test "user.acl !u denies the update of a file that is not owned" {
  runtest   "### Chown     " unix SUCCESS "" eos chown root /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k
  runtest H "### Update    " sss ERROR "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # fail: not owner, ACL allows rx only
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:nobody:rwx\!u /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss ERROR "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # fail: not owner, ACL allows RW but denies update
}

@test "a user not in the ACL cannot update" {
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:1:rwx /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss ERROR "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # fail: not owner, not in ACL
}

@test "+d works in sys.acl, not in user.acl" {
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:${NOBODY_UID}:rwx\!d /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Rm        " sss ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k             # fail: dir not owned, ACL denies delete
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:${NOBODY_UID}:rwx\!d,g:${NOBODY_GID}:rwx+d /eos/$EOS_TEST_INSTANCE/test/instancetest # +d for group ${NOBODY_GID}, but should be ignored in user.acl
  runtest H "### Rm        " sss ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k             # fail: dir not owned, ACL denies delete
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl=g:${NOBODY_GID}:rwx+d /eos/$EOS_TEST_INSTANCE/test/instancetest              # +d for nobody, this should work in sys.acl
  runtest H "### Rm        " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k             # ACL allows delete
  runtest   "### Attr      " unix SUCCESS "" eos attr rm sys.acl /eos/$EOS_TEST_INSTANCE/test/instancetest
}

@test "in an owned directory, the owner may write" {
  # dir owned
  runtest   "### Chown     " unix SUCCESS "" eos chown ${NOBODY_UID}:${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest"
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"  # ACL allows write, new file owned
  runtest H "### Update    " sss SUCCESS "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"   # dir owned
  runtest H "### Upload    " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp -a "$TESTSYSFILE1K" /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k # dir owned
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:nobody:r!wx /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Update    " sss ERROR "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k\?eos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"     # writes are denied, and updates are note allowed explicitly
  runtest H "### Upload    " sss ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp -a "$TESTSYSFILE1K" /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k  # writes are denied, and updates are not allowed explicitly
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=u:${NOBODY_UID}:rwx!d /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest   "### Chown     " unix SUCCESS "" eos chown ${NOBODY_UID}:${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k"                    # make sure file owned
  runtest H "### Rm        " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k             # ACL denies delete but file owned
}

@test "chown on file" {
  # chown on file
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"  # owned by ${NOBODY_GID}
  runtest   "### Chmod     " unix SUCCESS "" eos chmod 775 /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=g:${NOBODY_GID}:rwxc /eos/$EOS_TEST_INSTANCE/test/instancetest              # 'c' (chown) in user.acl
  runtest H "### Chown     " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos chown 98 /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k       # fail: user.acl cannot grant this chown but ${NOBODY_UID} is the owner
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl=g:${NOBODY_GID}:c /eos/$EOS_TEST_INSTANCE/test/instancetest                  # 'c' (chown) in sys.ascl
  runtest H "### Chown     " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos chown 98 /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k
  runtest   "### Attr      " unix SUCCESS "" eos attr rm sys.acl /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest H "### Chown     " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos chown ${NOBODY_UID} /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k       # chown to self should work in user.acl
}

@test "chown of a directory uses its own ACL" {
  # chown on directory - the ACL that applies is that of the directory itself
  runtest   "### Chown     " unix SUCCESS "" eos chown ${NOBODY_UID}:${NOBODY_GID} /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl                      # same tests with a directory
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=g:${NOBODY_GID}:rwxc /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl      # 'c' (chown) in user.acl
  runtest H "### Chown     " sss ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos chown 98 /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl       # fail: user.acl cannot grant this chown
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl=g:${NOBODY_GID}:c /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl          # 'c' (chown) in sys.ascl
  runtest H "### Chown     " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos chown 98 /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl= /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl                # rm sys.acl, user.acl remains
  runtest H "### Chown     " sss SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos chown ${NOBODY_UID} /eos/$EOS_TEST_INSTANCE/test/instancetest/testAcl       # chown to self should work in user.acl
}

@test "test inheritance of file acls" {
  # test inheritance of file acls
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl=g:${NOBODY_GID}:rwx /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"  # should have now sys.acl
  runtest   "### Attr      " unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1 | grep sys.acl | wc -l | grep -w 0 "
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.2" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=g:${NOBODY_GID}:rwx /eos/$EOS_TEST_INSTANCE/test/instancetest/file.2
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.2" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"  # should have no user.acl
  runtest   "### Attr      " unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.2 | grep user.acl | wc -l | grep -w 0 "
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.3" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest   "### Attr      " unix SUCCESS "" eos attr set user.acl=g:${NOBODY_GID}:rwx /eos/$EOS_TEST_INSTANCE/test/instancetest/file.3
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.eval.useracl=1 /eos/$EOS_TEST_INSTANCE/test/instancetest/file.3
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.3" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"  # should have user.acl and sys.eval.useracl
  runtest   "### Attr      " unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.3 | grep user.acl | wc -l | grep -w 1 "
  runtest   "### Attr      " unix SUCCESS "" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.3 | grep sys.eval.useracl | wc -l | grep -w 1 "
}

@test "test sys file acl" {
  # test sys file acl
  runtest   "### Attr      " unix SUCCESS "" eos attr rm user.acl /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest   "### Chmod     " sss SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest   "### Chmod     " sss SUCCESS "" eos chown root:root "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
}

@test "a file sys.acl gives read access" {
  runtest H "### Upload    " sss SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4"
  runtest   "### Chmod     " sss SUCCESS "" eos chmod 700 "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest   "### Ls        " sss SUCCESS "" eos ls -la "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest   "### Download  " unix SUCCESS "EOSROLE='-r 0 0'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4" $EOS_TEST_TESTSYS/null.txt
  runtest   "### Download  " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4" $EOS_TEST_TESTSYS/null.txt
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:${NOBODY_UID}:rx "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4"
  runtest   "### Download  " unix SUCCESS "EOSROLE='-r 0 0'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4" $EOS_TEST_TESTSYS/null.txt
  runtest   "### Download  " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4" $EOS_TEST_TESTSYS/null.txt
  runtest   "### Chmod     " sss SUCCESS "" eos chmod 700 "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest   "### Attr      " unix SUCCESS "" eos attr set sys.acl=u:${NOBODY_UID}:w "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4"
  runtest   "### Download  " unix SUCCESS "EOSROLE='-r 0 0'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4" $EOS_TEST_TESTSYS/null.txt
  runtest   "### Download  " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.4" $EOS_TEST_TESTSYS/null.txt
  runtest   "### Chmod     " sss SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
}

@test "user.acl rwx lets nobody upload, then the clean up" {
  # date +%s.%N;exit
  #cleanup
  runtest "### Attr      " unix SUCCESS "" eos debug info '\*'
  runtest "### Rm        " unix IGNORE  "" eos rm /eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k
  runtest "### Attr      " unix SUCCESS "" eos attr set user.acl=u:nobody:rwx /eos/$EOS_TEST_INSTANCE/test/instancetest
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.1k" "-ODeos.ruid=${NOBODY_UID}\&eos.rgid=${NOBODY_GID}"
  runtest "### Attr      " unix SUCCESS "" eos attr rm user.acl /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### Chown     " unix SUCCESS "" eos chown 0:0 "/eos/$EOS_TEST_INSTANCE/test/instancetest"
}

@test "ns map" {
  # Ns map functionality
  runtest "### Map ls    " unix SUCCESS "" eos map ls
  runtest "### Map mkdir " unix SUCCESS "" eos mkdir /eos/$EOS_TEST_INSTANCE/test/map_test/
  runtest "### Touch     " unix SUCCESS "" eos touch /eos/$EOS_TEST_INSTANCE/test/map_test/file1.dat
  runtest "### Stat      " unix SUCCESS "" eos stat /eos/$EOS_TEST_INSTANCE/test/map_test/file1.dat
  runtest "### Ls        " unix SUCCESS "" eos ls /eos/$EOS_TEST_INSTANCE/test/map_test/file1.dat
  runtest "### Map link  " unix SUCCESS "" eos map link /store/ /eos/$EOS_TEST_INSTANCE/test/map_test/
  runtest "### Stat      " unix SUCCESS "" eos stat /store/file1.dat
  runtest "### Ls        " unix SUCCESS "" eos ls /store/file1.dat
  runtest "### Rm        " unix SUCCESS "" eos rm /store/file1.dat
  runtest "### Map unlink" unix SUCCESS "" eos map unlink /store/
  runtest "### Rmdir     " unix SUCCESS "" eos rm -rf /eos/$EOS_TEST_INSTANCE/test/map_test/
}
