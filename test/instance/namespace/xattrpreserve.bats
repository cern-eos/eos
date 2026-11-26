# bats file_tags=xattrpreserve
load ../instance

XATTRPRESERVE_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/xattrpreserve"

@test "set up of the xattr preserve directory" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "$XATTRPRESERVE_DIR"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "$XATTRPRESERVE_DIR"
  runtest "### Version   " unix ERROR   "" eos attr get sys.versioning "$XATTRPRESERVE_DIR"
}

@test "without the CGI an overwrite drops the xattrs of the file it replaces" {
  # without the CGI an overwrite drops the xattrs of the file it replaces
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.preserve=kept "$XATTRPRESERVE_DIR/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.preserve=kept "$XATTRPRESERVE_DIR/file"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/file"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.preserve "$XATTRPRESERVE_DIR/file"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.preserve "$XATTRPRESERVE_DIR/file"
}

@test "with it they are copied onto the new file, which is still a new file" {
  # with it they are copied onto the new file, which is still a new file
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.preserve=kept "$XATTRPRESERVE_DIR/file"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.preserve=kept "$XATTRPRESERVE_DIR/file"
  XATTRPRESERVE_FID=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "$XATTRPRESERVE_DIR/file" 2>/dev/null | jq -r .id)
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/file" "-ODeos.xattr.preserve=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.preserve "$XATTRPRESERVE_DIR/file | grep kept"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.preserve "$XATTRPRESERVE_DIR/file | grep kept"
  XATTRPRESERVE_NEWFID=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "$XATTRPRESERVE_DIR/file" 2>/dev/null | jq -r .id)
  runtest "### Fid       " unix SUCCESS "" shell test "-n" "\"$XATTRPRESERVE_FID\""
  runtest "### Fid       " unix SUCCESS "" shell test "\"$XATTRPRESERVE_FID\"" "!=" "null"
  runtest "### Fid       " unix SUCCESS "" shell test "-n" "\"$XATTRPRESERVE_NEWFID\""
  runtest "### Fid       " unix SUCCESS "" shell test "\"$XATTRPRESERVE_NEWFID\"" "!=" "null"
  runtest "### Fid       " unix SUCCESS "" shell test "\"$XATTRPRESERVE_FID\"" "!=" "\"$XATTRPRESERVE_NEWFID\""
}

@test "xattrs given with eos.xattr take precedence over the copied ones" {
  # xattrs given with eos.xattr take precedence over the copied ones
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/file" "-ODeos.xattr.preserve=1\&eos.xattr=user.preserve=new"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.preserve "$XATTRPRESERVE_DIR/file | grep new"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.preserve "$XATTRPRESERVE_DIR/file | grep kept"
}

@test "an atomic upload copies them when it is committed" {
  # an atomic upload copies them when it is committed
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/file" "-ODeos.xattr.preserve=1\&eos.atomic=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.preserve "$XATTRPRESERVE_DIR/file | grep new"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.preserve "$XATTRPRESERVE_DIR/file | grep kept"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/xattrpreserve.atomic" "$XATTRPRESERVE_DIR/file"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/xattrpreserve.atomic"
}

@test "of two overlapping atomic uploads the last opened wins" {
  # of two overlapping atomic uploads the last opened one wins, also when it is
  # closed first: the new file must not get the sys.tmp.atomic tag of the file
  # it replaces, or the first upload would replace it
  runtest "### Atomic    " unix SUCCESS "" atomic_preserve_overlap "$XATTRPRESERVE_DIR/file"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/xattrpreserve.overlap" "$XATTRPRESERVE_DIR/file"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/xattrpreserve.overlap"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.preserve "$XATTRPRESERVE_DIR/file | grep new"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.preserve "$XATTRPRESERVE_DIR/file | grep kept"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.tmp.atomic "$XATTRPRESERVE_DIR/file"
  runtest "### Ls        " unix SUCCESS "" eos ls "-a $XATTRPRESERVE_DIR | grep -cF '.sys.a#.' | grep -w 0"
}

@test "changes during an atomic upload are kept" {
  # what changes on the file while an atomic upload replacing it is open - its
  # application lock refreshed or released the way the wopiserver does - is
  # neither lost nor brought back, xattrs given with eos.xattr take precedence
  XATTRPRESERVE_LOCK="expires:$(( $(date +%s) + 3600 )),type:shared,owner:*:wopi_test"
  XATTRPRESERVE_REFRESH=$(( $(date +%s) + 7200 ))
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set "'sys.app.lock=${XATTRPRESERVE_LOCK}'" "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.iop.lock=before "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.changed=before "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.given=before "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.removed=before "$XATTRPRESERVE_DIR/during"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$XATTRPRESERVE_DIR/during" "eos.app=wopi_test&eos.xattr.preserve=1&eos.xattr=user.given=fromcgi" xattr_change "$XATTRPRESERVE_REFRESH"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/xattrpreserve.during" "$XATTRPRESERVE_DIR/during"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/xattrpreserve.during"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.app.lock "$XATTRPRESERVE_DIR/during | grep -F 'expires:${XATTRPRESERVE_REFRESH},'"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.iop.lock "$XATTRPRESERVE_DIR/during | grep after"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.changed "$XATTRPRESERVE_DIR/during | grep after"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.added "$XATTRPRESERVE_DIR/during | grep after"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.removed "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.given "$XATTRPRESERVE_DIR/during | grep fromcgi"
}

@test "a lock released while the upload is open no longer guards the new file" {
  # a lock released while the upload is open no longer guards the new file
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$XATTRPRESERVE_DIR/during" "eos.app=wopi_test&eos.xattr.preserve=1" lock_release
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.app.lock "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.iop.lock "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.changed "$XATTRPRESERVE_DIR/during | grep after"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/during" "-ODeos.app=other_app\&eos.xattr.preserve=1"
}

@test "a file removed while the upload is open gives no xattrs to the new one" {
  # a file removed while the upload is open gives no xattrs to the new one
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$XATTRPRESERVE_DIR/during" "eos.app=wopi_test&eos.xattr.preserve=1" file_remove
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/xattrpreserve.removed" "$XATTRPRESERVE_DIR/during"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/xattrpreserve.removed"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.changed "$XATTRPRESERVE_DIR/during"
}

@test "a file ACL changed during the upload is kept" {
  # a file ACL changed or removed while the upload is open is neither lost nor
  # brought back, a user.acl given with eos.xattr does not replace it, and a file
  # removed meanwhile passes no ACL to the new one
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.eval.useracl=1 "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.acl=u:2:r "$XATTRPRESERVE_DIR/during"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$XATTRPRESERVE_DIR/during" "eos.xattr.preserve=1&eos.xattr=user.acl=u:4:r" acl_change
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.acl "$XATTRPRESERVE_DIR/during | grep -F 'u:3:r'"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$XATTRPRESERVE_DIR/during | grep -F 'u:4:r'"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/passwd" "$XATTRPRESERVE_DIR/during" "eos.xattr.preserve=1" acl_remove
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.eval.useracl "$XATTRPRESERVE_DIR/during | grep 1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.acl=u:2:r "$XATTRPRESERVE_DIR/during"
  runtest "### Atomic    " unix SUCCESS "" atomic_upload_during "/etc/group" "$XATTRPRESERVE_DIR/during" "eos.xattr.preserve=1" file_remove
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.acl "$XATTRPRESERVE_DIR/during"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.eval.useracl "$XATTRPRESERVE_DIR/during"
  runtest "### Ls        " unix SUCCESS "" eos ls "-a $XATTRPRESERVE_DIR | grep -cF '.sys.a#.' | grep -w 0"
}

@test "an atomic commit keeps a forced ETag, the next upload not" {
  # the commit of an atomic upload keeps a forced ETag, the next upload must not
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/etag" "-ODeos.etag=preservetag\&eos.atomic=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.tmp.etag "$XATTRPRESERVE_DIR/etag | grep preservetag"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/etag" "-ODeos.xattr.preserve=1\&eos.atomic=1"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.tmp.etag "$XATTRPRESERVE_DIR/etag"
}

@test "space attributes are not stored on the file" {
  # space attributes are only merged into listings, they must not be stored
  runtest "### SpaceAttr " unix SUCCESS "" eos space config default space.attr.sys.preservetest=space
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.preservetest "$XATTRPRESERVE_DIR/file | grep space"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/file" "-ODeos.xattr.preserve=1"
  runtest "### SpaceAttr " unix SUCCESS "" eos space config rm default space.attr.sys.preservetest
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.preservetest "$XATTRPRESERVE_DIR/file | grep space"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.preserve "$XATTRPRESERVE_DIR/file | grep kept"
}

@test "only eos.xattr.preserve=1 asks for them" {
  # only eos.xattr.preserve=1 asks for them
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/value"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.preserve=kept "$XATTRPRESERVE_DIR/value"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/value" "-ODeos.xattr.preserve=0"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.preserve "$XATTRPRESERVE_DIR/value"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.preserve=kept "$XATTRPRESERVE_DIR/value"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/value" "-ODeos.xattr.preserve=true"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.preserve "$XATTRPRESERVE_DIR/value"
}

@test "and eos.versioning=0 changes nothing in a directory without versioning" {
  # and eos.versioning=0 changes nothing in a directory without versioning
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.preserve=kept "$XATTRPRESERVE_DIR/value"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/value" "-ODeos.versioning=0"
  runtest "### Attr Get  " unix ERROR   "" eos attr get user.preserve "$XATTRPRESERVE_DIR/value"
  runtest "### Stat      " unix ERROR   "" meta stat "$XATTRPRESERVE_DIR/.sys.v#.value"
}

@test "ownership and mode are not copied" {
  # ownership and mode are not copied, the new file belongs to the overwriter -
  # root here, a plain upload is not mapped to root on every instance
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/owner" "-ODeos.ruid=${NOBODY_UID}"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "600" "$XATTRPRESERVE_DIR/owner"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.preserve=kept "$XATTRPRESERVE_DIR/owner"
  runtest "### Fileinfo  " unix SUCCESS "" eos fileinfo "$XATTRPRESERVE_DIR/owner -m | grep -w uid=${NOBODY_UID} | grep -w mode=0600"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/owner" "-ODeos.xattr.preserve=1\&eos.ruid=0"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.preserve "$XATTRPRESERVE_DIR/owner | grep kept"
  runtest "### Fileinfo  " unix SUCCESS "" eos fileinfo "$XATTRPRESERVE_DIR/owner -m | grep -w uid=0"
  runtest "### Fileinfo  " unix ERROR   "" eos fileinfo "$XATTRPRESERVE_DIR/owner -m | grep -w mode=0600"
}

@test "the tape metadata is not copied" {
  # the tape metadata belongs to the previous contents, CTA gives the new file
  # its own archive file id when it is created
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$XATTRPRESERVE_DIR/tape"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.preserve=kept "$XATTRPRESERVE_DIR/tape"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.archive.file_id=4242 "$XATTRPRESERVE_DIR/tape"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.cta.objectstore.id=preservetest "$XATTRPRESERVE_DIR/tape"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.retrieve.req_id=preservetest "$XATTRPRESERVE_DIR/tape"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$XATTRPRESERVE_DIR/tape" "-ODeos.xattr.preserve=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.preserve "$XATTRPRESERVE_DIR/tape | grep kept"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.archive.file_id "$XATTRPRESERVE_DIR/tape"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.cta.objectstore.id "$XATTRPRESERVE_DIR/tape"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.retrieve.req_id "$XATTRPRESERVE_DIR/tape"
  runtest "### Cleanup   " unix IGNORE  "" shell rm "-f" "$EOS_TEST_TESTSYS/xattrpreserve.*"
  runtest "### Cleanup   " unix SUCCESS "" eos rm "-r" "$XATTRPRESERVE_DIR"
}
