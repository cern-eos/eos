# bats file_tags=core
load ../instance

@test "upload, download, stat and rename of a file" {
  runtest "### Rm        " unix SUCCESS "" eos rm "-r" "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "755" "/eos/$EOS_TEST_INSTANCE/test/instancetest/"
  runtest "### Upload    " unix ERROR   "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd" "-ODeos.space=default"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd" "-ODeos.space=default"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Stat      " unix ERROR   "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.1"
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Part.Dwld " unix SUCCESS "" partial   "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Download  " unix ERROR   "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.1"
  runtest "### Dowl Lfn  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/enoent" "-OSeos.lfn=/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Dowl Lfn  " unix ERROR   "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/enoent" "-OSeos.lfn=/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.1"
  runtest "### Dowl Pfx  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/test/instancetest/passwd" "-OSeos.prefix=/eos/$EOS_TEST_INSTANCE/"
  runtest "### Dowl Pfx  " unix ERROR   "" download  "$EOS_TEST_TESTSYS/dumpit" "/test/instancetest/passwd" "-OSeos.prefix=/eos/faulty/"
  runtest "### Find      " unix SUCCESS "" eos find "-f --count /eos/$EOS_TEST_INSTANCE/test/ | grep nfiles=1"
  runtest "### Rem       " unix ERROR   "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.1"
  runtest "### FileInfo  " unix ERROR   "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.1"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Whoami    " unix SUCCESS "" eos "whoami | grep authz:unix | grep uid=0"
  runtest "### Who       " unix SUCCESS "" eos "who | grep root"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Rename    " unix ERROR   "" eos "file rename /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.rename"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd" "-ODeos.space=default"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.meta4" "-ODeos.space=default"
  runtest "### Upload    " unix SUCCESS "XRD_METALINKPROCESSING=0" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.meta4"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/file.meta4"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "755" "/eos/$EOS_TEST_INSTANCE/test/instancetest"
  runtest "### Rename    " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos file rename /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.rename
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "/eos/$EOS_TEST_INSTANCE/test/instancetest"
  runtest "### Rename    " unix SUCCESS "" eos file rename /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.rename
}

@test "rename of a directory" {
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/rename.dir"
  runtest "### Rename    " unix ERROR   "" eos "file rename /eos/$EOS_TEST_INSTANCE/test/instancetest/rename.dir /eos/$EOS_TEST_INSTANCE/test/instancetest/rename.dir/myself"
  runtest "### Rename    " unix ERROR   "" eos "file rename /eos/$EOS_TEST_INSTANCE/test/instancetest/rename.dir /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.rename"
  runtest "### Rename    " unix ERROR   "" eos "file rename /eos/$EOS_TEST_INSTANCE/test/instancetest/rename.dir.missing /eos/$EOS_TEST_INSTANCE/test/instancetest/rename"
  runtest "### Rename    " unix SUCCESS "" eos "file rename /eos/$EOS_TEST_INSTANCE/test/instancetest/rename.dir /eos/$EOS_TEST_INSTANCE/test/instancetest/rename"
  runtest "### Rem       " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd.rename"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd" "-ODeos.space=default"
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Stat      " unix ERROR "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd/"
  runtest "### Upload    " unix ERROR "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd/" "-ODeos.space=default"
  runtest "### Upload    " unix ERROR "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd/" "-f"
  runtest "### Rmdir     " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/rename"
}

@test "File/Directory Rename by Identifier (fxid/cxid)" {
  # File/Directory Rename by Identifier (fxid/cxid)
  runtest "### Upload     " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_for_fxid.txt" "-ODeos.space=default"
  runtest "### Mkdir      " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/dir_for_cxid"
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_for_fxid.txt" | jq -r .id)
  fxid=$(printf "fxid:%08x" $fid)
  runtest "### RenameFxid " unix SUCCESS "" eos file rename ${fxid} /eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_by_fxid.txt
  runtest "### StatRenam  " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_by_fxid.txt"
  runtest "### StatOld    " unix ERROR "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_for_fxid.txt"
  runtest "### Upload     " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_for_fid.txt" "-ODeos.space=default"
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_for_fid.txt" | jq -r .id)
  runtest "### RenameFid  " unix SUCCESS "" eos file rename fid:${fid} /eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_by_fid.txt
  runtest "### StatRenam  " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_by_fid.txt"
  cid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/dir_for_cxid" | jq -r .id)
  cxid=$(printf "cxid:%08x" $cid)
  runtest "### RenameCxid " unix SUCCESS "" eos file rename ${cxid} /eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_dir_by_cxid
  runtest "### StatRenam  " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_dir_by_cxid"
  runtest "### StatOld    " unix ERROR "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/dir_for_cxid"
  runtest "### Upload     " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_for_symlink.txt" "-ODeos.space=default"
  runtest "### Mkdir      " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/symlink_archive"
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/file_for_symlink.txt" | jq -r .id)
  fxid=$(printf "fxid:%08x" $fid)
  cid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/symlink_archive" | jq -r .id)
  cxid=$(printf "cxid:%08x" $cid)
  runtest "### RenSymFxid " unix SUCCESS "" eos file rename_with_symlink ${fxid} ${cxid}
  runtest "### ChkSymlink " unix SUCCESS "" eos "ls -l /eos/$EOS_TEST_INSTANCE/test/instancetest/ | grep \"file_for_symlink.txt ->\""
  runtest "### StatTarget " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/symlink_archive/file_for_symlink.txt"
  runtest "### Upload     " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/src_file_for_identifier_symlink.txt" "-ODeos.space=default"
  fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/src_file_for_identifier_symlink.txt" | jq -r .id)
  fxid=$(printf "fxid:%08x" $fid)
  runtest "### SymlinkFx  " unix SUCCESS "" eos file symlink /eos/$EOS_TEST_INSTANCE/test/instancetest/identifier_symlink_test.txt ${fxid}
  runtest "### ChkLink    " unix SUCCESS "" eos "ls -l /eos/$EOS_TEST_INSTANCE/test/instancetest/ | grep \"identifier_symlink_test.txt ->\""
  runtest "### StatLnkTgt " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/identifier_symlink_test.txt"
}

@test "rename by an invalid identifier fails" {
  runtest "### RenInvFxid " unix ERROR "" eos "file rename fxid:invalid /eos/$EOS_TEST_INSTANCE/test/instancetest/test.txt"
  runtest "### RenInvCxid " unix ERROR "" eos "file rename cxid:999999999 /eos/$EOS_TEST_INSTANCE/test/instancetest/test"
  runtest "### RenInvFid  " unix ERROR "" eos "file rename fid:abc /eos/$EOS_TEST_INSTANCE/test/instancetest/test.txt"
}

@test "clean up of the rename tests" {
  runtest "### Cleanup    " unix SUCCESS "" eos rm -rf "/eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_dir_by_cxid"
  runtest "### Cleanup    " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_by_fxid.txt"
  runtest "### Cleanup    " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/renamed_by_fid.txt"
  runtest "### Cleanup    " unix SUCCESS "" eos rm -rf "/eos/$EOS_TEST_INSTANCE/test/instancetest/symlink_archive"
  runtest "### Cleanup    " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/src_file_for_identifier_symlink.txt"
  runtest "### Cleanup    " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/identifier_symlink_test.txt"
}

@test "a directory with & in its name" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/a\&b"
  runtest "### Rmdir     " unix SUCCESS "" eos rmdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/a\&b"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/a\&b"
  runtest "### Rem       " unix SUCCESS "" eos rm -rf "/eos/$EOS_TEST_INSTANCE/test/instancetest/a\&b"
}

@test "rename with a symlink" {
  # Tests for rename_with_symlink functionality
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/archive"
  runtest "### Upload    " unix SUCCESS "" upload    "/etc/passwd" "/eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/file.dat" "-ODeos.space=default"
}

@test "Fail if destination directory matches source directory" {
  # Fail if destination directory matches source directory
  runtest "### RenameSym " unix ERROR "" eos "file rename_with_symlink /eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/file.dat /eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink"
  runtest "### RenameSym " unix SUCCESS "" eos "file rename_with_symlink /eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/file.dat /eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/archive"
  runtest "### RenameSym " unix SUCCESS "" eos "ls -l /eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/ | grep \"file.dat -> /eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/archive/file.dat\""
  runtest "### Stat      " unix SUCCESS "" meta stat "/eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink/archive/file.dat"
  runtest "### Rem       " unix SUCCESS "" eos rm -rf "/eos/$EOS_TEST_INSTANCE/test/instancetest/mv_with_symlink"
}

@test "space define, set and rm" {
  runtest "### MkSpace   " unix SUCCESS "" eos "space define __test__"
  runtest "### SetSpace  " unix SUCCESS "" eos "space set __test__ on "
  runtest "### RmSpace   " unix SUCCESS "" eos "space rm __test__"
}

@test "group set and rm" {
  runtest "### MkGroup   " unix SUCCESS "" eos "group set __test__ on"
  runtest "### RmGroup   " unix SUCCESS "" eos "group rm __test__"
}

@test "node set and rm, and eos-rename-test" {
  runtest "### MkNode    " unix SUCCESS "" eos "node set __test__ on"
  runtest "### RmNode    " unix SUCCESS "" eos "node rm __test__"
  runtest "### eos-rename" unix SUCCESS "" eos_rename "/eos/$EOS_TEST_INSTANCE/test/instancetest/rename-test"
  runtest "### Rm        " unix SUCCESS "" eos rm -rf "/eos/$EOS_TEST_INSTANCE/test/instancetest/rename-test"
}

@test "a booking size must fit on the file systems" {
  # The FST reserves the booking size on the disk. When the file systems share
  # the disk of /var, this fills /var and the FSTs set all of them to ro.
  local fs_path
  fs_path=$(command eos -b root://$EOS_TEST_REDIRECTOR fs ls -m | sed -n 's/.* path=\([^ ]*\).*/\1/p' | head -1)
  if [ -d "$fs_path" ] && [ "$(stat -c %d "$fs_path")" = "$(stat -c %d /var)" ]; then
    skip "the file systems share the disk of /var"
  fi
  # Get the the minimum amount of free space on any of the file systems
  fs_min_size=$(command eos -b $EOSROLE root://$EOS_TEST_REDIRECTOR geosched show -m snapshot default.0 plct |
  grep fsid |
  gawk -M -v RS=" |\n" -F= '/^(fsid|totW)=/{ if($1=="fsid") f=$2; else print $2, f }' |
  sort -rn |
  tail -1 |
  awk '{print $1;}')

  fs_max_size=$(command eos -b $EOSROLE root://$EOS_TEST_REDIRECTOR geosched show -m snapshot default.0 plct |
  grep fsid |
  gawk -M -v RS=" |\n" -F= '/^(fsid|totW)=/{ if($1=="fsid") f=$2; else print $2, f }' |
  sort -rn |
  head -1 |
  awk '{print $1;}')

  # Tests for the replica layout
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/replica"
  runtest "### Attr      " unix SUCCESS "" eos "attr set default=replica /eos/$EOS_TEST_INSTANCE/test/instancetest/replica"
  runtest "### UploadMax+" unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/replica/file.dat" "-ODeos.bookingsize=$((fs_max_size + 1024*1024*1024))"
  runtest "### UploadMax-" unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/replica/file.dat" "-ODeos.bookingsize=$((fs_min_size - 1024*1024*1024))"

  # Tests for the rain layout
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/rain"
  runtest "### Attr      " unix SUCCESS "" eos "attr set default=raid6 /eos/$EOS_TEST_INSTANCE/test/instancetest/rain"
  runtest "### UploadMax+" unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/rain/file.dat" "-ODeos.bookingsize=$((fs_max_size + 1024*1024*1024))"
  runtest "### UploadMax-" unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/rain/file.dat" "-ODeos.bookingsize=$((fs_max_size * 7))"

  # Cleanup
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/replica"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/rain"
}

@test "Constrain minimum file size" {
  # Constrain minimum file size
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/minsize"
  runtest "### Attr      " unix SUCCESS "" eos "attr set sys.forced.minsize=1000000 /eos/$EOS_TEST_INSTANCE/test/instancetest/minsize"
  runtest "### Upload    " unix ERROR   "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/minsize/f1"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1M" "/eos/$EOS_TEST_INSTANCE/test/instancetest/minsize/f1"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/minsize"
}

@test "Constrain maximum file size" {
  # Constrain maximum file size
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/maxsize"
  runtest "### Attr      " unix SUCCESS "" eos "attr set sys.forced.maxsize=1000000 /eos/$EOS_TEST_INSTANCE/test/instancetest/maxsize"
  runtest "### Upload    " unix ERROR   "" upload "$TESTSYSFILE1M" "/eos/$EOS_TEST_INSTANCE/test/instancetest/maxsize/f1"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/maxsize/f1"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/maxsize"
}

@test "Control-C" {
  # Control-C
  runtest "### Abort     " unix ERROR "" abort /eos/$EOS_TEST_INSTANCE/test/instancetest/abort
  runtest "### FileInfo  " unix ERROR "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/abort"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/abort"
}

@test "Extend a file via truncate" {
  # Extend a file via truncate
  runtest "### Extend    " unix SUCCESS "" extend /eos/$EOS_TEST_INSTANCE/test/instancetest/extend
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/extend"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/extend"
}

@test "Shrink a file via truncate" {
  # Shrink a file via truncate
  runtest "### Shrink    " unix SUCCESS "" shrink /eos/$EOS_TEST_INSTANCE/test/instancetest/shrink
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/shrink"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/shrink"
}

@test "Write file in random order" {
  # Write file in random order
  runtest "### Random    " unix SUCCESS "" random /eos/$EOS_TEST_INSTANCE/test/instancetest/random
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/random"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/random"
}

@test "Extend file and rewrite the beginning" {
  # Extend file and rewrite the beginning
  runtest "### Truncate  " unix SUCCESS "" truncate /eos/$EOS_TEST_INSTANCE/test/instancetest/truncate
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/truncate"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/truncate"
}

@test "Expand file several times" {
  # Expand file several times
  runtest "### holes     " unix SUCCESS "" holes /eos/$EOS_TEST_INSTANCE/test/instancetest/holes
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/holes"
  runtest "### backward  " unix SUCCESS "" backward  /eos/$EOS_TEST_INSTANCE/test/instancetest/holes
  runtest "### downrnd   " unix SUCCESS "" downloadrandom  /eos/$EOS_TEST_INSTANCE/test/instancetest/holes
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/holes"
}

@test "an upload with a wrong checksum fails" {
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=adler /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### UploadCks " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=$CKS1K"
  runtest "### UploadCks " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum="
  runtest "### UploadCks " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=$CKS1K\&eos.checksumtype=adler"
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=bliblablo\&eos.checksumtype=adler"
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=$CKS1K\&eos.checksumtype=does_not_exist"
}

@test "a checksum without a type is checked against the layout" {
  # legacy: a checksum value without a type is verified against the layout checksum
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=bliblablo"
}

@test "requested checksum type does not match the layout checksum type" {
  # requested checksum type does not match the layout checksum type
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=$CKS1K\&eos.checksumtype=sha-256"
}

@test "cks.value alone is verified like eos.checksum alone" {
  # cks.value alone is verified like eos.checksum alone, no cks.type needed
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODcks.value=bliblablo"
  runtest "### UploadCks " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODcks.value=$CKS1K"
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODcks.value=bliblablo\&cks.type=adler"
}

@test "restore a good replica after the failed uploads above" {
  # restore a good replica after the failed uploads above
  runtest "### UploadCks " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=$CKS1K"
}

@test "a bogus checksum type on a read does not fail the download" {
  # a bogus checksum type on a read must neither fail the download nor touch the file
  runtest "### DownlCks  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-OSeos.checksum=deadbeef\&eos.checksumtype=does_not_exist"
  runtest "### FileInfo  " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k -m | grep nrep=1"
  runtest "### UploadCks " unix SUCCESS "" upload    "$TESTSYSFILE0K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/good-checksum0k" "-ODeos.checksum=00000001"
  runtest "### Rem       " unix SUCCESS "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/good-checksum0k"
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=00000000"
  runtest "### FileInfo  " unix ERROR "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k -m | grep nrep=0"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k"
  runtest "### UploadCks " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k" "-ODeos.checksum=00000000"
  runtest "### FileInfo  " unix ERROR "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k -m | grep nrep=0"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-checksum1k"
  runtest "### Attr      " unix SUCCESS "" eos attr rm sys.forced.checksum /eos/$EOS_TEST_INSTANCE/test/instancetest/
  runtest "### UploadSz  " unix SUCCESS "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-size1k" "-ODeos.targetsize=1024"
  runtest "### FileInfo  " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-size1k -m | grep nrep=1"
  runtest "### UploadSz  " unix ERROR "" upload    "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-size1k" "-ODeos.targetsize=1000"
  runtest "### FileInfo  " unix ERROR "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-size1k -m | grep nrep=0"
  runtest "### Rem       " unix IGNORE  "" eos rm    "/eos/$EOS_TEST_INSTANCE/test/instancetest/faulty-size1k"
}

@test "a space without file systems falls back to its altspaces" {
  # Alternative Space test
  runtest "### MkSpace   " unix SUCCESS "" eos "space define nothing"
  runtest "### SetSpace  " unix SUCCESS "" eos "space set nothing on "
  runtest "### AltSpace  " unix SUCCESS "" eos "space config nothing space.policy.altspaces=default"
  runtest "### AltSpace  " unix SUCCESS "" eos "space config default space.policy.space=nothing"
  runtest "### AltSpace  " unix SUCCESS "" eos "space config default space.policy.nstripes=1"
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/forced-altspace"
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/default-altspace"
  runtest "### Force     " unix SUCCESS "" eos "attr set sys.forced.space=nothing /eos/$EOS_TEST_INSTANCE/test/instancetest/forced-altspace"
  runtest "### Upload    " unix SUCCESS "" upload   "$TESTSYSFILE1M" "/eos/$EOS_TEST_INSTANCE/test/instancetest/forced-altspace/1M"
  runtest "### Upload    " unix SUCCESS "" upload   "$TESTSYSFILE1M" "/eos/$EOS_TEST_INSTANCE/test/instancetest/default-altspace/1M"
  runtest "### RmSpace   " unix SUCCESS "" eos "space rm nothing"
  runtest "### AltSpace  " unix SUCCESS "" eos "space config default space.policy.space=default"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/forced-altspace"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/default-altspace"
}

@test "sys.owner.auth gives the owner rights" {
  # Verify sys.owner.auth behaviour
  runtest "### Mkdir     " unix SUCCESS "" eos "mkdir /eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/"
  runtest "### Attr      " unix SUCCESS "" eos "attr set sys.owner.auth=* /eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth"
  runtest "### Chown     " unix SUCCESS "" eos chown 2000:2001 "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth"
  runtest "### Upload-L3 " unix SUCCESS "" upload   "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/l1/l2/l3/f1"
  runtest "### Vfy-Uid   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/l1/l2/l3/f1 -m | grep uid=2000"
  runtest "### Vfy-Gid   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/l1/l2/l3/f1 -m | grep gid=2001"
  runtest "### Upload-L1 " unix SUCCESS "" upload   "/etc/group" "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/f1"
  runtest "### Vfy-Uid   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/f1 -m | grep uid=2000"
  runtest "### Vfy-Gid   " unix SUCCESS "" eos fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/f1 -m | grep gid=2001"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/owner-auth/"
}

@test "permission checks apply higher up in the hierarchy" {
  # Make sure permission checks are also applied higher up in the hierarchy when
  # the direct parent of the path does not exist
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/auth/"
  runtest "### Chmod     " unix SUCCESS "" eos chmod 700 "/eos/$EOS_TEST_INSTANCE/test/instancetest/auth/"
  runtest "### Chown     " unix SUCCESS "" eos chown daemon:daemon "/eos/$EOS_TEST_INSTANCE/test/instancetest/auth/"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.owner.auth="*" "/eos/$EOS_TEST_INSTANCE/test/instancetest/auth"
  runtest "### Mkdir     " unix ERROR "EOSROLE='-r 3 4'" eos mkdir -p "/eos/$EOS_TEST_INSTANCE/test/instancetest/auth/d1/d2"
  runtest "### Rem       " unix SUCCESS "" eos rm -r "/eos/$EOS_TEST_INSTANCE/test/instancetest/auth/"
}

@test "eos version shows the instance and client version" {
  # VERSION
  runtest "### Version   " unix SUCCESS "" eos "version | grep EOS_INSTANCE"
  runtest "### Version   " unix SUCCESS "" eos "version | grep EOS_CLIENT_VERSION"
  runtest "### Version   " unix SUCCESS "" eos "version | grep EOS_SERVER_VERSION"
  runtest "### Version   " unix SUCCESS "" eos "version -m | grep xrootd.version"
  runtest "### Tui Ver   " unix SUCCESS "" eos "tui --version"
}

@test "whoami --comment writes the comment to the logbook" {
  # COMMENT
  runtest "### Comment         " unix SUCCESS "" eos "whoami --comment \"Hello World\""
  runtest "### Comment         " unix SUCCESS "" shell grep $EOSLOGBOOK -e \"Hello World\"
  runtest "### Comment Proto   " unix SUCCESS "" eos "fs ls --comment \"Hello Proto World\""
  runtest "### Comment Proto   " unix SUCCESS "" shell grep $EOSLOGBOOK -e \"Hello Proto World\"
}

@test "access ban of a user" {
  # ACCESS
  # Access ban
  runtest "### Access    " unix SUCCESS "" eos "access ban user nobody"
}

@test "access unban removes the ban of nobody" {
  # takes long
  #runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  #runtest "### Download  " unix ERROR "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd" "-OSeos.ruid=99" "-DITransactionTimeout 1"
  runtest "### Access    " unix SUCCESS "" eos "access ls | grep nobody"
  runtest "### Access    " unix SUCCESS "" eos "access unban user nobody"
  runtest "### Access    " unix ERROR "" eos "access ls | grep nobody"
}

@test "Access stall" {
  # Access stall
  runtest "### Access    " unix SUCCESS "" eos "access set stall 1 r"
  runtest "### Download  " unix SUCCESS "" download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
  runtest "### Download  " unix ERROR "" eosssh-timeout -t 3 download  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd" "-OSeos.ruid=${NOBODY_UID}"
  runtest "### Access    " unix SUCCESS "" eos "access ls | grep \"r:\* => 1\""
  runtest "### Access    " unix SUCCESS "" eos "access rm stall r"
  runtest "### Access    " unix ERROR "" eos "access ls | grep \"r:\* => 1\""
}

@test "Access redirect" {
  # Access redirect
  runtest "### Access    " unix SUCCESS "" eos "access set redirect foo.senf"
  runtest "### Download  " unix ERROR "" downloadh  "$EOS_TEST_TESTSYS/dumpit" "/eos/$EOS_TEST_INSTANCE/test/instancetest/passwd" "-DIFirstConnectTimeout 2 -DIConnectTimeout 2 -DIReconnectWait 1 -DIMaxRedirectCnt 1"
  runtest "### Access    " unix SUCCESS "" eos "access ls | grep foo"
  runtest "### Access    " unix SUCCESS "" eos "access rm redirect"
  runtest "### Access    " unix ERROR "" eos "access ls | grep foo"
  runtest "### Rm        " unix SUCCESS "" eos "rm /eos/$EOS_TEST_INSTANCE/test/instancetest/passwd"
}

@test "Access whitelist domain" {
  # Access whitelist domain
  runtest "### Access    " unix SUCCESS "" eos "access allow user bin"
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r 1 1'" eos ls /eos/$EOS_TEST_INSTANCE/
  runtest "### Ls        " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls /eos/$EOS_TEST_INSTANCE/
  runtest "### Access    " unix SUCCESS "" eos "access ls | grep bin"
  runtest "### Access    " unix SUCCESS "" eos "access unallow user bin"
  runtest "### Ls        " unix SUCCESS "EOSROLE='-r 1 1'" eos ls /eos/$EOS_TEST_INSTANCE/test
  runtest "### Access    " unix SUCCESS "" eos "access allow user bin"
  runtest "### Ls        " unix ERROR "EOSROLE='-r ${NOBODY_UID} ${NOBODY_GID}'" eos ls /eos/$EOS_TEST_INSTANCE/
  runtest "### Access    " unix SUCCESS "" eos "access allow domain nobody@localdomain"
  runtest "### Ls        " unix ERROR shell runuser -u nobody eos ls /eos/$EOS_TEST_INSTANCE/
  runtest "### Access    " unix SUCCESS "" eos "access allow domain -"
  runtest "### Ls        " unix SUCCESS shell runuser -u nobody eos ls /eos/$EOS_TEST_INSTANCE/
  runtest "### Access    " unix SUCCESS "" eos "access ls"
  runtest "### Access    " unix SUCCESS "" eos "access unallow user bin"
  runtest "### Access    " unix SUCCESS "" eos "access unallow domain -"
  runtest "### Access    " unix SUCCESS "" eos "access unallow domain nobody@localdomain"
}

@test "root, admins and sudoers only can use eos access" {
  # root, admins and sudoers only can `eos access`
  runtest "### Access    " unix SUCCESS "" eos "access allow user nobody"
  runtest "### Access    " unix ERROR shell runuser -u nobody eos access ls
  runtest "### Access    " unix SUCCESS "" eos "access unallow user nobody"
}
