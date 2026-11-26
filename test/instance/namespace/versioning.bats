# bats file_tags=versioning
load ../instance

VERSIONING_DURING="/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/during"
VERSIONING_TAKEN="/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/taken"
VERSIONING_SKIP="/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/duringskip"

@test "versioning keeps the configured number of versions" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning"
  runtest "### Mkdir     " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.bla"
  runtest "### Attr Ls   " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos attr ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.bla | wc -l | grep -w 1"
  runtest "### Rmdir     " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.bla"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.versioning=3 /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  sleep 1.1 # force the first file to fall in a different tv_sec interval
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd | wc -l | grep -w 3"
  runtest "### File Vers." unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos file version /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd 99
  runtest "### File Vers." unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos file version /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd 99
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd | wc -l | grep -w 5"
  runtest "### File Vers." unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos file purge /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd 4
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd | wc -l | grep -w 4"
  runtest "### File Vers." unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos file purge /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd -1
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd | wc -l | grep -w 3"
  runtest "### File Vers." unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos file purge /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd 0
  runtest "### Stat      " unix ERROR "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd"
  runtest "### Rm        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Stat      " unix ERROR "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  sleep 1.1 # force the first file to fall in a different tv_sec interval
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Find Purge" unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos find --purge -1 /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd | wc -l | grep -w 3"
  runtest "### Find Purge" unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos find --purge 1 /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd | wc -l | grep -w 1"
  runtest "### Find Purge" unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos find --purge 0 /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/
  runtest "### Stat      " unix ERROR "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.passwd"
}

@test "a new version does not store the space attributes" {
  # Space attributes are only merged into listings, a new version must not store
  # them as they would stick to the file after the space configuration changes
  runtest "### SpaceAttr " unix SUCCESS "" eos space config default space.attr.sys.versiontest=space
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "--atomic /etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd.atomic"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "--atomic /etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd.atomic"
  runtest "### SpaceAttr " unix SUCCESS "" eos space config rm default space.attr.sys.versiontest
  runtest "### Attr Get  " unix ERROR "" eos attr get sys.versiontest "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd | grep space"
  runtest "### Attr Get  " unix ERROR "" eos attr get sys.versiontest "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd.atomic | grep space"
}

@test "a new version does not take the tape metadata" {
  # the tape metadata of the previous version is not carried over, CTA gives the
  # new file its own - at open and at the commit of an atomic upload
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.archive.file_id=4242 "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Attr Get  " unix ERROR "" eos attr get sys.archive.file_id "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.archive.file_id=4242 "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd.atomic"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "--atomic /etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd.atomic"
  runtest "### Attr Get  " unix ERROR "" eos attr get sys.archive.file_id "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/passwd.atomic"
}

@test "an atomic commit keeps a forced ETag, a new version not" {
  # the commit of an atomic upload keeps a forced ETag, a new version must not
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/etag" "-ODeos.etag=versiontag\&eos.atomic=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.tmp.etag "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/etag | grep versiontag"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/etag" "-ODeos.atomic=1"
  runtest "### Attr Get  " unix ERROR "" eos attr get sys.tmp.etag "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/etag"
}

@test "changes during a versioned atomic upload are kept" {
  # what changes on the file while a versioned atomic upload is open - its
  # application lock refreshed or released the way the wopiserver does - is
  # neither lost nor brought back, xattrs given with eos.xattr take precedence
  VERSIONING_LOCK="expires:$(( $(date +%s) + 3600 )),type:shared,owner:*:wopi_test"
  VERSIONING_REFRESH=$(( $(date +%s) + 7200 ))
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$VERSIONING_DURING"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set "'sys.app.lock=${VERSIONING_LOCK}'" "$VERSIONING_DURING"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.iop.lock=before "$VERSIONING_DURING"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.changed=before "$VERSIONING_DURING"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.given=before "$VERSIONING_DURING"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.removed=before "$VERSIONING_DURING"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$VERSIONING_DURING" "eos.app=wopi_test&eos.xattr=user.given=fromcgi" xattr_change "$VERSIONING_REFRESH"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.during | wc -l | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/versioning.during" "$VERSIONING_DURING"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/versioning.during"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.app.lock "$VERSIONING_DURING | grep -F 'expires:${VERSIONING_REFRESH},'"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.iop.lock "$VERSIONING_DURING | grep after"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.changed "$VERSIONING_DURING | grep after"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.added "$VERSIONING_DURING | grep after"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.removed "$VERSIONING_DURING"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.given "$VERSIONING_DURING | grep fromcgi"
  # a lock released while the upload is open no longer guards the new version
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$VERSIONING_DURING" "eos.app=wopi_test" lock_release
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.app.lock "$VERSIONING_DURING"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.iop.lock "$VERSIONING_DURING"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.changed "$VERSIONING_DURING | grep after"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$VERSIONING_DURING" "-ODeos.app=other_app"
  # a file removed while the upload is open gives no xattrs to the new one
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$VERSIONING_DURING" "eos.app=wopi_test" file_remove
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/versioning.removed" "$VERSIONING_DURING"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/versioning.removed"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.changed "$VERSIONING_DURING"
  # a file ACL changed or removed while the upload is open is neither lost nor
  # brought back, a user.acl given with eos.xattr does not replace it, and a file
  # removed meanwhile passes no ACL to the new one
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.eval.useracl=1 "$VERSIONING_DURING"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.acl=u:2:r "$VERSIONING_DURING"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$VERSIONING_DURING" "eos.app=wopi_test&eos.xattr=user.acl=u:4:r" acl_change
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.acl "$VERSIONING_DURING | grep -F 'u:3:r'"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$VERSIONING_DURING | grep -F 'u:4:r'"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$VERSIONING_DURING" "eos.app=wopi_test" acl_remove
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$VERSIONING_DURING"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.eval.useracl "$VERSIONING_DURING | grep 1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.acl=u:2:r "$VERSIONING_DURING"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$VERSIONING_DURING" "eos.app=wopi_test" file_remove
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$VERSIONING_DURING"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.eval.useracl "$VERSIONING_DURING"
  # a commit which finds no free name for the version discards the upload: the
  # client gets an error, the file keeps its contents, ACL and versions, and the
  # temporary upload doesn't stay behind. The folder allows more versions than the
  # ones taken, the upload purges it at open.
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "$VERSIONING_TAKEN"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "$VERSIONING_TAKEN"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.versioning=16 "$VERSIONING_TAKEN"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$VERSIONING_TAKEN/file"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$VERSIONING_TAKEN/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.eval.useracl=1 "$VERSIONING_TAKEN/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.acl=u:2:r "$VERSIONING_TAKEN/file"
  runtest "### Taken     " unix SUCCESS "" version_names_take "$VERSIONING_TAKEN/file"
  runtest "### Atomic    " unix SUCCESS "" upload_discarded "/etc/passwd" "$VERSIONING_TAKEN/file" "-ODeos.atomic=1"
  runtest "### Kept      " unix SUCCESS "" version_names_file_kept "$VERSIONING_TAKEN/file"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.acl "$VERSIONING_TAKEN/file | grep -F 'u:2:r'"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.eval.useracl "$VERSIONING_TAKEN/file | grep 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/versioning.taken.file" "$VERSIONING_TAKEN/file"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/versioning.taken.file"
  runtest "### Ls        " unix SUCCESS "" eos ls "$VERSIONING_TAKEN/.sys.v#.file | wc -l | grep -w 7"
  runtest "### Ls        " unix SUCCESS "" eos ls "-a $VERSIONING_TAKEN | grep -cF '.sys.a#.' | grep -w 0"
  runtest "### Rm        " unix SUCCESS "" eos rm -r "$VERSIONING_TAKEN"
  # an application lock, set the way the wopiserver does, is carried over onto a
  # new version and keeps guarding it - at open and at the commit of an atomic upload
  WOPILOCK_EXPIRES=$(( $(date +%s) + 3600 ))
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set "'sys.app.lock=expires:${WOPILOCK_EXPIRES},type:shared,owner:*:wopi_test'" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.iop.lock=wopilockid "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=other_app"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=wopi_test"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.wopilock | wc -l | grep -w 1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.app.lock "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock | grep -F 'expires:${WOPILOCK_EXPIRES},type:shared,owner:*:wopi_test'"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.iop.lock "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock | grep wopilockid"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=other_app"
  # a FUSE state saying the file is open lets any application past the lock, a
  # new version must not inherit it
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.fusex.state=CU "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=wopi_test"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.fusex.state "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=other_app"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=other_app\&eos.atomic=1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=wopi_test\&eos.atomic=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.app.lock "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock | grep -F 'expires:${WOPILOCK_EXPIRES},type:shared,owner:*:wopi_test'"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.iop.lock "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock | grep wopilockid"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=other_app"
  # eos.versioning=0, which the wopiserver sends within its noversiongraceperiod,
  # only skips the new version: the previous versions, the attributes of their
  # folder and the xattrs of the file are kept - the lock included
  WOPILOCK_VERSIONS=$(command eos -b root://$EOS_TEST_REDIRECTOR ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.wopilock" 2>/dev/null | wc -l)
  runtest "### Version   " unix SUCCESS "" shell test "$WOPILOCK_VERSIONS" "-gt" "0"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=wopi_test\&eos.versioning=0"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.wopilock"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.wopilock | wc -l | grep -w $WOPILOCK_VERSIONS"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.app.lock "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock | grep -F 'expires:${WOPILOCK_EXPIRES},type:shared,owner:*:wopi_test'"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.iop.lock "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock | grep wopilockid"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=other_app"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/wopilock" "-ODeos.app=other_app\&eos.versioning=0"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversion"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversion"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversion | wc -l | grep -w 1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.keep=kept "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversion"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.vfolder=kept "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversion"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversion" "-ODeos.versioning=0"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversion"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversion | wc -l | grep -w 1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.vfolder "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversion | grep kept"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversion | grep kept"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/versioning.noversion" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversion"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/versioning.noversion"
  # also for an atomic upload
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversiona"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversiona"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversiona | wc -l | grep -w 1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.keep=kept "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversiona"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.vfolder=kept "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversiona"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversiona" "-ODeos.versioning=0\&eos.atomic=1"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversiona"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversiona | wc -l | grep -w 1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.vfolder "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.noversiona | grep kept"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversiona | grep kept"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/versioning.noversiona" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/noversiona"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/versioning.noversiona"
  # and what changes on the file while the atomic upload is open is neither lost
  # nor brought back, as for a versioned one
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$VERSIONING_SKIP"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/group" "$VERSIONING_SKIP"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set "'sys.app.lock=${VERSIONING_LOCK}'" "$VERSIONING_SKIP"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.iop.lock=before "$VERSIONING_SKIP"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.changed=before "$VERSIONING_SKIP"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.given=before "$VERSIONING_SKIP"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.removed=before "$VERSIONING_SKIP"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$VERSIONING_SKIP" "eos.app=wopi_test&eos.versioning=0&eos.xattr=user.given=fromcgi" xattr_change "$VERSIONING_REFRESH"
  runtest "### Ls        " unix SUCCESS "" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/.sys.v#.duringskip | wc -l | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/versioning.duringskip" "$VERSIONING_SKIP"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/versioning.duringskip"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.app.lock "$VERSIONING_SKIP | grep -F 'expires:${VERSIONING_REFRESH},'"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.iop.lock "$VERSIONING_SKIP | grep after"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.changed "$VERSIONING_SKIP | grep after"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.added "$VERSIONING_SKIP | grep after"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.removed "$VERSIONING_SKIP"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.given "$VERSIONING_SKIP | grep fromcgi"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$VERSIONING_SKIP" "eos.app=wopi_test&eos.versioning=0" lock_release
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.app.lock "$VERSIONING_SKIP"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.iop.lock "$VERSIONING_SKIP"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.changed "$VERSIONING_SKIP | grep after"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$VERSIONING_SKIP" "-ODeos.app=other_app\&eos.versioning=0"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$VERSIONING_SKIP" "eos.app=wopi_test&eos.versioning=0" file_remove
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/versioning.removedskip" "$VERSIONING_SKIP"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/versioning.removedskip"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.changed "$VERSIONING_SKIP"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.eval.useracl=1 "$VERSIONING_SKIP"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.acl=u:2:r "$VERSIONING_SKIP"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$VERSIONING_SKIP" "eos.versioning=0&eos.xattr=user.acl=u:4:r" acl_change
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.acl "$VERSIONING_SKIP | grep -F 'u:3:r'"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$VERSIONING_SKIP | grep -F 'u:4:r'"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$VERSIONING_SKIP" "eos.versioning=0" acl_remove
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$VERSIONING_SKIP"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.eval.useracl "$VERSIONING_SKIP | grep 1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.acl=u:2:r "$VERSIONING_SKIP"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$VERSIONING_SKIP" "eos.versioning=0" file_remove
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$VERSIONING_SKIP"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.eval.useracl "$VERSIONING_SKIP"
  runtest "### Cleanup   " unix IGNORE  "" shell rm "-f" "$EOS_TEST_TESTSYS/versioning.*"
}

@test "versioning when ACLs give the write permission" {
  # Versioning test when acls control the permission for the user on the folder and
  # implicitly also for the creation of the versions directory.
  runtest "### Mkdir     " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/acl_versions/"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "2550" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/acl_versions/"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.acl="u:${NOBODY_UID}:rwx" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/acl_versions/"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/acl_versions/file.dat"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/acl_versions/file.dat"
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls "/eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/acl_versions/.sys.v#.file.dat | wc -l | grep -w 1"
  runtest "### Cleanup"    unix IGNORE  "" eos rm  "-r /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning/acl_versions/"
  runtest "### Cleanup"    unix IGNORE  "" eos rm  "-r /eos/$EOS_TEST_INSTANCE/test/instancetest/versioning"
}
