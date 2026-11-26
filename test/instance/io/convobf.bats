# bats file_tags=convobf
load ../instance

CONVOBF_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/convobf"
CONVOBF_KEY="1b1be459-bf35-424c-bf69-368336e3d3d8"

@test "set up of the convert directory" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "$CONVOBF_DIR"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "$CONVOBF_DIR"
}

@test "a conversion keeps this checksum unless it asks for another one" {
  # a conversion keeps this checksum unless it asks for another one
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.checksum=adler "$CONVOBF_DIR"
}

@test "a conversion to plain keeps the checksum" {
  # the files are converted from replica to plain, which only needs one file
  # system besides the one holding the file - replica:2 needs two in one group
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=replica "$CONVOBF_DIR"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.nstripes=1 "$CONVOBF_DIR"
}

@test "without a space key new files are only obfuscated" {
  # without a space key new files are only obfuscated
  runtest "### KeyRm     " unix IGNORE  "" eos space config default space.encryptionkey=remove --no-confirmation
}

@test "the files are written before the space obfuscates new files" {
  # the files are written before the space obfuscates new files
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/plainctl"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/plain"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/plainxs"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/plainkey"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/obfctl" "-ODeos.obfuscate=1"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/obf" "-ODeos.obfuscate=1"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/obfxs" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/plain | grep -c 'Layout: replica' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/plain | grep -c 'Crypt:' | grep -w 0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/obf | grep -c 'Crypt: obfuscated' | grep -w 1"
}

@test "a conversion to another checksum copies without a check" {
  # a conversion to another checksum copies the file without verifying it - it
  # works while the space does not obfuscate new files
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/plainctl plain:1 default scattered md5"
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/obfctl plain:1 default scattered md5"
  runtest "### Convert   " unix SUCCESS "" wait_for 120 layout_is plain "$CONVOBF_DIR/plainctl" "$CONVOBF_DIR/obfctl"
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/plainctl | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/plainctl | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.plainctl" "$CONVOBF_DIR/plainctl"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.plainctl"
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/obfctl | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/obfctl | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.obfctl" "$CONVOBF_DIR/obfctl"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.obfctl"
}

@test "the space obfuscates every new file, proc/conversion included" {
  # the space obfuscates every new file, proc/conversion included
  runtest "### SpaceAttr " unix SUCCESS "" eos space config default space.attr.sys.file.obfuscate=1
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/new"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/new | grep -c 'Crypt: obfuscated' | grep -w 1"
}

@test "conversions keeping the checksum, then changing it" {
  # conversions keeping the checksum, then changing it
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/plain plain:1 default"
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/obf plain:1 default"
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/plainxs plain:1 default scattered md5"
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/obfxs plain:1 default scattered md5"
  runtest "### Convert   " unix SUCCESS "" wait_for 120 layout_is plain "$CONVOBF_DIR/plain" "$CONVOBF_DIR/obf" "$CONVOBF_DIR/plainxs" "$CONVOBF_DIR/obfxs"
}

@test "plain stays plain, obfuscated keeps its key, both read back" {
  # a plain file stays plain and an obfuscated one keeps its key, both read back
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/plain | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/plain | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.plain" "$CONVOBF_DIR/plain"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.plain"
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/obf | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/obf | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.obf" "$CONVOBF_DIR/obf"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.obf"
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/plainxs | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/plainxs | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.plainxs" "$CONVOBF_DIR/plainxs"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.plainxs"
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/obfxs | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/obfxs | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.obfxs" "$CONVOBF_DIR/obfxs"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.obfxs"
}

@test "with a key on the space, the copy is not encrypted with it either" {
  # with a key on the space, the copy is not encrypted with it either
  runtest "### SpaceKey  " unix SUCCESS "" eos space config default "space.encryptionkey=$CONVOBF_KEY" --no-confirmation
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$CONVOBF_DIR/enc"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/enc | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/plainkey plain:1 default scattered md5"
  runtest "### Convert   " unix SUCCESS "" eos file convert "$CONVOBF_DIR/enc plain:1 default scattered md5"
  runtest "### Convert   " unix SUCCESS "" wait_for 120 layout_is plain "$CONVOBF_DIR/plainkey" "$CONVOBF_DIR/enc"
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/plainkey | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/plainkey | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.plainkey" "$CONVOBF_DIR/plainkey"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.plainkey"
  runtest "### Convert   " unix SUCCESS "" eos file info "$CONVOBF_DIR/enc | grep -c 'Layout: plain' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$CONVOBF_DIR/enc | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/convobf.enc" "$CONVOBF_DIR/enc"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/convobf.enc"
  runtest "### KeyRm     " unix SUCCESS "" eos space config default space.encryptionkey=remove --no-confirmation
  runtest "### SpaceAttr " unix SUCCESS "" eos space config rm default space.attr.sys.file.obfuscate
}

@test "clean up of the convert tests" {
  runtest "### Cleanup   " unix IGNORE  "" shell rm "-f" "$EOS_TEST_TESTSYS/convobf.*"
  runtest "### Cleanup   " unix SUCCESS "" eos rm "-r" "$CONVOBF_DIR"
}
