# bats file_tags=fusex
load ../instance

HLPRESERVE_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve"
HLCLONE_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlclone"
HLCLONE_FUSE="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlclone"

# An eosxd mount of /eos at /test-fusex, with the eosnobody key of the MGM as
# its sss keytab. krb5 and x509 are off.
setup_file() {
  export X509_USER_PROXY=/tmp/invalid
  export KRB5CCNAME=/tmp/invalid
  ! test -d /test-fusex/ || umount -fl /test-fusex/ 2>/dev/null || true
  mkdir -p /test-fusex/
  ! test -e /etc/eos/fuse.sss.keytab || mv -f /etc/eos/fuse.sss.keytab /etc/eos/fuse.sss.keytab.save
  grep eosnobody $SERVERKEY > /etc/eos/fuse.sss.keytab
  chmod 444 /etc/eos/fuse.sss.keytab
  # eosxd makes /var/run/eos again each time it starts
  rm -rf /var/run/eos
  eosxd -ofsname=localhost.localdomain:/eos /test-fusex/ 3>&- || true
  wait_until 30 test -e "/test-fusex/$EOS_TEST_INSTANCE" || true
}

teardown_file() {
  ! test -d /test-fusex/ || umount -fl /test-fusex/ 2>/dev/null || true
  rmdir /test-fusex/ 2>/dev/null || true
  rm -f /etc/eos/fuse.sss.keytab
  ! test -e /etc/eos/fuse.sss.keytab.save || mv -f /etc/eos/fuse.sss.keytab.save /etc/eos/fuse.sss.keytab
}

@test "eosxd makes /var/run/eos/credentials for all users" {
  runtest "### Stat /run/eos " unix SUCCESS "" test "$(stat -c '%a' /var/run/eos/credentials)" = 1777 -a "$(stat -c '%a' /var/run/eos/credentials/store)" = 1777
}

@test "file operations through the eosxd mount" {
  [ -e "/test-fusex/$EOS_TEST_INSTANCE" ] || skip "no eosxd mount at /test-fusex"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.fusex.rename.version=1 /eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.versioning=3 /eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/
  runtest "### Imm Mkdir " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/"
  runtest "### Imm Chmod " unix SUCCESS "" eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/"
  runtest "### Imm Warm  " unix SUCCESS "" shell runuser nobody -s /bin/bash -c "'touch /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/warmup && rm /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/warmup'"
  runtest "### Imm Attr  " unix SUCCESS "" eos attr set sys.eval.useracl=1 "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/"
  runtest "### Imm Attr  " unix SUCCESS "" eos attr set user.acl=z:i "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/"
  runtest "### Imm Touch " unix ERROR "" shell runuser nobody -s /bin/bash -c "'touch /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/thisshouldnotwork'"
  runtest "### Imm Leak  " unix ERROR "" eos stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/thisshouldnotwork"
  runtest "### Imm Clean " unix IGNORE "" eos attr rm user.acl "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/"
  runtest "### Imm Clean " unix IGNORE "" eos attr rm sys.eval.useracl "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/"
  runtest "### Imm Clean " unix IGNORE "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/immutable/"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f2"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f2"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f2"
  runtest "### Fusex-Stat" unix SUCCESS "" shell stat "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex"
  runtest "### Fusex-Stat" unix SUCCESS "" shell stat "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/f1"
  runtest "### Fusex-Stat" unix SUCCESS "" shell stat "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/f2"
  runtest "### Versions  " unix SUCCESS "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f1 | wc -l | grep -w 1"
  runtest "### Versions  " unix SUCCESS "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f2 | wc -l | grep -w 2"
  runtest "### Rename    " unix SUCCESS "" shell mv -f /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/f1 /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/f2
  runtest "### Versions  " unix ERROR "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f1 | wc -l | grep -w 1"
  runtest "### Versions  " unix SUCCESS "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/f2 | wc -l | grep -w 1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/f1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/f1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/f1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/f2"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f2"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r1"
  runtest "### Upload    " unix SUCCESS "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos cp "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r1"
  # version overwriting
  runtest "### Versions  " unix SUCCESS "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/f1 | wc -l | grep -w 2"
  runtest "### Versions  " unix SUCCESS "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f1 | wc -l | grep -w 1"
  runtest "### Rename    " unix SUCCESS "" shell mv -f /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/f1 /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f1
  runtest "### Rename    " unix SUCCESS "" shell mv -f /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/f2 /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f2
  runtest "### Versions  " unix SUCCESS "" wait_for 30 eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f2 | wc -l | grep -w 1"
  runtest "### Versions  " unix SUCCESS "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/f1 | wc -l | grep -w 2"
  # rename including versions
  runtest "### Versions  " unix SUCCESS "" eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r1 | wc -l | grep -w 2"
  runtest "### Rename    " unix SUCCESS "" shell mv -f /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r1 /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r2
  runtest "### Versions  " unix SUCCESS "" wait_for 30 eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r2 | wc -l | grep -w 2"
  # move including versions
  runtest "### Rename    " unix SUCCESS "" shell mv -f /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir1/r2 /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/r1
  runtest "### Versions  " unix SUCCESS "" wait_for 30 eos file versions "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/dir2/r1 | wc -l | grep -w 2"
  #
  runtest "### Upload    " unix SUCCESS "" shell cp /etc/group "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### Chmod     " unix SUCCESS "" shell chmod 444 "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### test -x   " unix ERROR "" shell test -x "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### Chmod     " unix SUCCESS "" shell chmod 744 "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### test -x   " unix SUCCESS "" shell test -x "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### Chmod     " unix SUCCESS "" shell chmod 474 "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### test -x   " unix SUCCESS "" shell test -x "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### Chmod     " unix SUCCESS "" shell chmod 447 "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### test -x   " unix SUCCESS "" shell test -x "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/perm"
  runtest "### symlink   " unix SUCCESS "" shell ln -s "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/target" "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.1"
  runtest "### symlink   " unix SUCCESS "" shell ln -s "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/target" "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.2"
  runtest "### syj-mkd   " unix SUCCESS "" shell mkdir "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/symdir/"
  # eosxd sends the new entries to the MGM after some time
  runtest "### stat l1   " unix SUCCESS "" wait_for 30 eos stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.1"
  runtest "### stat l2   " unix SUCCESS "" wait_for 30 eos stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.2"
  runtest "### sym-mv    " unix SUCCESS "" shell mv -f "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.1" "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.3"
  runtest "### sym-mv    " unix SUCCESS "" shell mv -f "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.2" "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/symdir/symlink.4"
  runtest "### stat l1   " unix SUCCESS "" wait_for 30 eos_path_absent "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.1"
  runtest "### stat l2   " unix SUCCESS "" wait_for 30 eos_path_absent "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.2"
  runtest "### stat l1   " unix SUCCESS "" eos stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/symlink.3"
  runtest "### stat l2   " unix SUCCESS "" eos stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/symdir/symlink.4"
  #hardlinks
  runtest "### Recycle   " unix SUCCESS "" eos recycle config --add-bin /eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink
  runtest "### mkdir     " unix SUCCESS "" shell mkdir -p "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/group "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/file"
  runtest "### link      " unix SUCCESS "" shell cd "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/ && ln file file.hardlink"
  dd of="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/file.hardlink" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/newfile"
  runtest "### cp        " unix SUCCESS "" shell mv -f "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/newfile" "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/file"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink/file.hardlink"
  runtest "### Rm        " unix SUCCESS "" eos rm -r /eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/hardlink
  # a file overwritten with eos.xattr.preserve must not inherit the link count of
  # a hard link target, otherwise its removal renames it to ...eos.ino... instead
  # of deleting it - needs a directory without versioning and recycle bin. The
  # directory is created through FUSE, which writes into it first, and is only
  # changed through the MGM once the sync has flushed it.
  runtest "### mkdir     " unix SUCCESS "" shell mkdir -p "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve/"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve/file"
  runtest "### link      " unix SUCCESS "" shell cd "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve/ && ln file file.hardlink"
  runtest "### Sync      " unix SUCCESS "" shell dd "of=/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve/file.hardlink" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### chmod     " unix SUCCESS "" eos chmod 777 "$HLPRESERVE_DIR"
  runtest "### Version   " unix IGNORE  "" eos attr rm sys.versioning "$HLPRESERVE_DIR"
  runtest "### Version   " unix ERROR   "" eos attr get sys.versioning "$HLPRESERVE_DIR"
  runtest "### Recycle   " unix ERROR   "" eos attr get sys.recycle "$HLPRESERVE_DIR"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.eos.nlink "$HLPRESERVE_DIR/file"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$HLPRESERVE_DIR/file" "-ODeos.xattr.preserve=1"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.eos.nlink "$HLPRESERVE_DIR/file"
  # only the previous file, still referenced by the hard link, stays hidden
  runtest "### Rm        " unix SUCCESS "" eos rm "$HLPRESERVE_DIR/file"
  runtest "### Ls        " unix SUCCESS "" eos ls "-a $HLPRESERVE_DIR | grep -c 'eos.ino' | grep -w 1"
  # nor must a new version of a hard link target - only its owner or root may
  # version it, and a plain upload is not mapped to the account FUSE created it as
  runtest "### Version   " unix SUCCESS "" eos attr set sys.versioning=2 "$HLPRESERVE_DIR"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve/vfile"
  runtest "### link      " unix SUCCESS "" shell cd "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve/ && ln vfile vfile.hardlink"
  runtest "### Sync      " unix SUCCESS "" shell dd "of=/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/hlpreserve/vfile.hardlink" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Attr Get  " unix SUCCESS "" eos attr get sys.eos.nlink "$HLPRESERVE_DIR/vfile"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$HLPRESERVE_DIR/vfile" "-ODeos.ruid=0"
  runtest "### Attr Get  " unix ERROR   "" eos attr get sys.eos.nlink "$HLPRESERVE_DIR/vfile"
  # the hard link now refers to the version: it stays hidden in the version
  # folder once the file is removed, and goes with the hard link
  runtest "### Rm        " unix SUCCESS "" eos rm "$HLPRESERVE_DIR/vfile"
  runtest "### Ls        " unix SUCCESS "" eos ls "-a $HLPRESERVE_DIR/.sys.v#.vfile/ | grep -c 'eos.ino' | grep -w 1"
  runtest "### Rm        " unix SUCCESS "" eos rm "$HLPRESERVE_DIR/vfile.hardlink"
  runtest "### Ls        " unix SUCCESS "" eos ls "-a $HLPRESERVE_DIR/.sys.v#.vfile/ | grep -c 'eos.ino' | grep -w 0"
  runtest "### Rm        " unix SUCCESS "" eos rm -r "$HLPRESERVE_DIR"
  # the last hard link of a target in a clone goes while the directory of the
  # target can't be found: the clone can't be created, and the target must keep
  # its contents - for an rm of the hard link and for a rename over it
  runtest "### mkdir     " unix SUCCESS "" shell mkdir -p "$HLCLONE_FUSE/rm" "$HLCLONE_FUSE/mv"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd "$HLCLONE_FUSE/rm/file"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/passwd "$HLCLONE_FUSE/mv/file"
  runtest "### Sync      " unix SUCCESS "" shell dd "of=$HLCLONE_FUSE/rm/file" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Sync      " unix SUCCESS "" shell dd "of=$HLCLONE_FUSE/mv/file" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Clone     " unix SUCCESS "" hl_clone_tag "$HLCLONE_DIR"
  runtest "### link      " unix SUCCESS "" shell cd "$HLCLONE_FUSE/rm/ && ln file file.hardlink"
  runtest "### link      " unix SUCCESS "" shell cd "$HLCLONE_FUSE/mv/ && ln file file.hardlink"
  runtest "### rm        " unix SUCCESS "" shell rm "$HLCLONE_FUSE/rm/file"
  runtest "### rm        " unix SUCCESS "" shell rm "$HLCLONE_FUSE/mv/file"
  runtest "### Hidden    " unix SUCCESS "" wait_for 30 hl_target_hidden "$HLCLONE_DIR/rm"
  runtest "### Hidden    " unix SUCCESS "" wait_for 30 hl_target_hidden "$HLCLONE_DIR/mv"
  runtest "### cp        " unix SUCCESS "" shell cp /etc/group "$HLCLONE_FUSE/mv/other"
  runtest "### Sync      " unix SUCCESS "" shell dd "of=$HLCLONE_FUSE/mv/other" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Detach    " unix SUCCESS "" hl_target_detach "$HLCLONE_DIR/rm"
  runtest "### Detach    " unix SUCCESS "" hl_target_detach "$HLCLONE_DIR/mv"
  runtest "### rm        " unix SUCCESS "" shell rm "$HLCLONE_FUSE/rm/file.hardlink"
  runtest "### mv        " unix SUCCESS "" shell mv "$HLCLONE_FUSE/mv/other" "$HLCLONE_FUSE/mv/file.hardlink"
  runtest "### Gone      " unix SUCCESS "" wait_for 30 eos_path_absent "$HLCLONE_DIR/rm/file.hardlink"
  runtest "### Gone      " unix SUCCESS "" wait_for 30 eos_path_absent "$HLCLONE_DIR/mv/other"
  runtest "### Attach    " unix SUCCESS "" hl_target_attach "$HLCLONE_DIR/rm"
  runtest "### Attach    " unix SUCCESS "" hl_target_attach "$HLCLONE_DIR/mv"
  runtest "### Holds     " unix SUCCESS "" hl_target_holds "$HLCLONE_DIR/rm" /etc/passwd
  runtest "### Holds     " unix SUCCESS "" hl_target_holds "$HLCLONE_DIR/mv" /etc/passwd
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/hl.renamed" "$HLCLONE_DIR/mv/file.hardlink"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/hl.renamed"
  runtest "### Clone     " unix SUCCESS "" hl_clone_purge "$HLCLONE_DIR"
  runtest "### Rm        " unix SUCCESS "" eos rm -r "$HLCLONE_DIR"
  runtest "### Cleanup   " unix IGNORE  "" shell rm "-f" "$EOS_TEST_TESTSYS/hl.*"

  #obfuscation
  runtest "### Obfuscate " unix SUCCESS "" shell mkdir "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/"
  runtest "### Obfuscate " unix SUCCESS "" shell eos chmod 777 "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/"
  runtest "### Obfuscate " unix SUCCESS "" shell eos attr set sys.file.obfuscate=1 "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/"
  runtest "### Upload    " unix SUCCESS "" shell cp /etc/group "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/group"
  runtest "### Upload    " unix SUCCESS "" shell cp /etc/group "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/group"
  runtest "### Download  " unix SUCCESS "" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/group?eos.obfuscate=0" "/var/tmp/obfuscate.raw"
  runtest "### Download  " unix SUCCESS "" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/group" "/var/tmp/obfuscate"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group  "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/group"
  runtest "### Diff      " unix ERROR "" shell diff /etc/group  "/var/tmp/obfuscate.raw"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group  "/var/tmp/obfuscate"
  runtest "### Diff      " unix SUCCESS "" shell rm  "/var/tmp/obfuscate"
  runtest "### Cleanup"    unix SUCCESS "" eos rm  "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/group"
  #encryption
  runtest "### Id        " unix SUCCESS "" shell env EOS_FUSE_SECRET=bla eosxd get eos.reconnect "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/"
  runtest "### Upload    " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos_bash cp /etc/group "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted"
  runtest "### Download  " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos cp /etc/group "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted.cp"
  runtest "### Dowload   " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted /var/tmp/encrypted.ok"
  runtest "### Dowload   " unix SUCCESS "EOS_FUSE_SECRET=false" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted /var/tmp/encrypted.false"
  runtest "### Diff      " unix SUCCESS "" eos_bash diff /etc/group  /var/tmp/encrypted.ok
  runtest "### Diff      " unix ERROR "" eos_bash diff /etc/group  /var/tmp/encrypted.false

  # drop from buffer cache; without the key the open fails, as in the old script
  dd of="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Diff      " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos_bash diff /etc/group  "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted"
  runtest "### Diff      " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos_bash diff /etc/group  "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted.cp"
  dd of="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Diff      " unix ERROR "EOS_FUSE_SECRET=false" eos_bash diff /etc/group  "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted"
  dd of="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Diff      " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos_bash diff /etc/group  "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted"
  dd of="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Diff      " unix ERROR "" eos_bash diff /etc/group  "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted"
  # an upload which does not bring the key of a file encrypted through the mount
  # is refused - the file keeps its contents, readable with the key remotely and
  # through the mount. The directory inherits versioning and a plain upload is
  # not mapped to the account the mount created the file as, upload as root so
  # that only the key can be the reason for the refusal.
  runtest "### Overwrite " unix ERROR   "" upload "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" "-ODeos.ruid=0"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.encrypted.fp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted"
  runtest "### Dowload   " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted /var/tmp/encrypted.kept"
  runtest "### Diff      " unix SUCCESS "" eos_bash diff /etc/group /var/tmp/encrypted.kept
  dd of="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Diff      " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos_bash diff /etc/group  "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted"
  runtest "### Cleanup   " unix IGNORE  "" shell rm "-f" /var/tmp/encrypted.kept
  # eosxd applies the secret itself and the MGM never gets it: an update through
  # the mount works, while a remote update without the key is refused and leaves
  # the file as it is
  runtest "### Expected  " unix SUCCESS "" shell "cat /etc/group /etc/group > /var/tmp/encrypted.expected"
  runtest "### Update    " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos_bash dd if=/etc/group "of=/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" oflag=append conv=notrunc
  dd of="/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted" oflag=nocache conv=notrunc,fdatasync count=0 || true
  runtest "### Dowload   " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted /var/tmp/encrypted.updated"
  runtest "### Diff      " unix SUCCESS "" eos_bash diff /var/tmp/encrypted.expected /var/tmp/encrypted.updated
  runtest "### Update    " unix ERROR   "" append "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted\?eos.ruid=0"
  runtest "### Dowload   " unix SUCCESS "EOS_FUSE_SECRET=12e629ee-e68a-4a5b-9743-33ba8f794e07" eos cp "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/obfuscate/encrypted /var/tmp/encrypted.updated"
  runtest "### Diff      " unix SUCCESS "" eos_bash diff /var/tmp/encrypted.expected /var/tmp/encrypted.updated
  runtest "### Cleanup   " unix IGNORE  "" shell rm "-f" /var/tmp/encrypted.expected /var/tmp/encrypted.updated
  runtest "### defaultcc " unix SUCCESS "" eos_defaultcc "/test-fusex/"
  runtest "### sycntime  " unix SUCCESS "" eos_synctime "/test-fusex/" "$EOS_TEST_INSTANCE/test/instancetest/fusex/"

  # sys.owner.auth FuseServer tests
  #
  # Verify that sys.owner.auth is honored by Server::FillContainerCAP():
  #   - without the attribute a non-owner FUSE client is denied on a 0700
  #     root-owned directory (baseline);
  #   - with sys.owner.auth=<prot>:<user> matching the FUSE connection the
  #     client gains write access AND new entries are owned by the directory
  #     owner (i.e. the cap MODE is now computed as the owner, not just the
  #     cap.uid/gid stamp);
  #   - sys.owner.auth=<prot>:<user>-no-such-user (a superstring of the real
  #     ownerkey) must NOT grant access -- regression test for the comma-anchor
  #     fix in attr::checkDirOwner.
  runtest "### OwnerAuth " unix SUCCESS "" eos mkdir "-p" "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"
  runtest "### OwnerAuth " unix SUCCESS "" eos chown root:root "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"
  runtest "### OwnerAuth " unix SUCCESS "" eos chmod 700 "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"

  # Discover the FUSE-side authentication protocol (sss for our keytab); the
  # keytab maps to the local 'nobody' user, so the ownerkey is "<prot>:nobody".
  OWNERAUTH_WHOAMI=$(cat "/test-fusex/$EOS_TEST_INSTANCE/proc/whoami" 2>/dev/null) || true
  echo "OwnerAuth: whoami='$OWNERAUTH_WHOAMI'" >> $EOSCERTLOG 2>&1
  OWNERAUTH_PROT=$(echo "$OWNERAUTH_WHOAMI" | sed -nE 's/.*\[authz:([^]]+)\].*/\1/p')
  [ -z "$OWNERAUTH_PROT" ] && OWNERAUTH_PROT=sss
  OWNERAUTH_KEY="${OWNERAUTH_PROT}:nobody"
  echo "OwnerAuth: ownerkey='${OWNERAUTH_KEY}'" >> $EOSCERTLOG 2>&1

  # Baseline: 0700 root:root, no sys.owner.auth -> the FUSE client (nobody)
  # must be denied for mkdir. We only test directory operations because they
  # go through the FuseServer cap path (Server::FillContainerCAP) which is
  # what this fix is about; xrootd-open-based file creation in eosxd may use
  # the local process credentials and is therefore not a reliable indicator.
  runtest "### OwnerNeg  " unix ERROR   "" shell mkdir "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth/sub.baseline"

  # Grant ownership via sys.owner.auth=<prot>:<user> matching the FUSE client.
  runtest "### OwnerAuth " unix SUCCESS "" eos attr set sys.owner.auth=${OWNERAUTH_KEY} "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"
  runtest "### OwnerAuth " unix SUCCESS "" shell eosxd set system.eos.dropcap - "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"

  # With sys.owner.auth matching, mkdir via FUSE must now succeed.
  # Before the FillContainerCAP fix the cap MODE was still 0 and this would
  # fail despite cap.uid being stamped as the directory owner. eosxd drops the
  # cap after some time: retry until then (a failed mkdir changes nothing).
  runtest "### OwnerPos  " unix SUCCESS "" wait_for 30 shell mkdir "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth/sub.granted"
  # The new directory must be owned by the directory owner (root, uid 0),
  # not by the impersonating FUSE user.
  runtest "### OwnerUid  " unix SUCCESS "" test "$(stat -c '%u' /test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth/sub.granted)" = 0

  # Substring-anchor regression: a superstring like '<prot>:nobody-no-such-user'
  # must NOT match the ownerkey '<prot>:nobody'. Previously a plain substring
  # search in attr::checkDirOwner would falsely grant access.
  runtest "### OwnerAuth " unix SUCCESS "" eos attr set sys.owner.auth=${OWNERAUTH_KEY}-no-such-user "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"
  runtest "### OwnerAuth " unix SUCCESS "" shell eosxd set system.eos.dropcap - "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"
  # A fixed wait for the cap drop: a retry of this mkdir, which must fail, can
  # succeed with the old cap and then fail as "exists"
  runtest "### Delay     " unix SUCCESS "" delay 1
  runtest "### OwnerAnch " unix ERROR   "" shell mkdir "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth/sub.anchored"

  # Cleanup
  runtest "### OwnerAuth " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/ownerauth"

  # enable sss now
  chmod 400 /etc/eos/fuse.sss.keytab
  runtest "### EnableTok " unix SUCCESS "" eos space config default space.token.generation=1
  EXPIRE=$(date +%s); LATER=$((EXPIRE + 9000))
  EOSTOKENR=$(/usr/bin/eos token --path /eos/ --permission rx --tree --owner nobody --group nobody --expires $LATER)
  EOSTOKENW=$(/usr/bin/eos token --path /eos/ --permission rwx --tree --owner nobody --group nobody --expires $LATER)
  export EXPIRE EOSTOKENR EOSTOKENW
  runtest "### Id        " unix SUCCESS "" shell eosxd get eos.reconnect "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/"
  # tokensudo arg tests
  runtest "### tokensudo " unix SUCCESS "" eos vid tokensudo always
  runtest "### tokensudo " unix SUCCESS "" eos vid tokensudo encrypted
  runtest "### tokensudo " unix SUCCESS "" eos vid tokensudo strong
  runtest "### tokensudo " unix SUCCESS "" eos vid tokensudo never
  runtest "### tokensudo " unix ERROR "" eos vid tokensudo bla
  runtest "### tokensudo " unix SUCCESS "" eos vid tokensudo always

  # token via fusex tests
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/token"
  runtest "### Chown     " unix SUCCESS "" eos chown root:root "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/token"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 700 "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/token"
  runtest "### LsFuse    " unix ERROR "" shell ls -la "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/token/"
  runtest "### LsRoot    " unix SUCCESS "" eos ls -la "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/token/"
  runtest "### LsNobody  " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls -la "/eos/$EOS_TEST_INSTANCE/test/instancetest/fusex/token/"
  runtest "### EosToken  " unix SUCCESS "XrdSecsssENDORSEMENT=$EOSTOKENR" shell eosxd get eos.reconnect "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/token"
  runtest "### WhoToken  " unix SUCCESS "XrdSecsssENDORSEMENT=$EOSTOKENR" shell cat "/test-fusex/$EOS_TEST_INSTANCE/proc/whoami"
  runtest "### LsToken   " unix SUCCESS "XrdSecsssENDORSEMENT=$EOSTOKENR" shell ls -la "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/token/"
  runtest "### MkToken   " unix ERROR "XrdSecsssENDORSEMENT=$EOSTOKENR" shell mkdir "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/token/forbidden"
  runtest "### ConToken  " unix SUCCESS "XrdSecsssENDORSEMENT=$EOSTOKENW" shell eosxd get eos.reconnect "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/token"
  runtest "### WhoToken  " unix SUCCESS "XrdSecsssENDORSEMENT=$EOSTOKENR" shell cat "/test-fusex/$EOS_TEST_INSTANCE/proc/whoami"
  runtest "### MkToken   " unix SUCCESS "XrdSecsssENDORSEMENT=$EOSTOKENW" shell mkdir "/test-fusex/$EOS_TEST_INSTANCE/test/instancetest/fusex/token/allowed"
}
