# bats file_tags=spaceenc
load ../instance

SPACEENC_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/spaceenc"
SPACEENC_CGI_DIR="/eos/$EOS_TEST_INSTANCE/test/instancetest/spaceenc-cgi"
SPACEENC_KEY1="82e9f4b4-518a-4ba1-b557-39ea36f7bdde"
SPACEENC_KEY2="1dd23915-782a-4301-8cfd-37f99ce37a58"
SPACEENC_USERKEY="039b9858-5bb3-4dcc-85c2-a5a333eb78b0"

@test "set up of the space encryption directory" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "$SPACEENC_DIR"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "$SPACEENC_DIR"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.file.obfuscate=1 "$SPACEENC_DIR"
}

@test "set up of the conversion from replica to plain" {
  # the conversion below goes from replica to plain, which only needs one file
  # system besides the one holding the file - replica:2 needs two in one group
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=replica "$SPACEENC_DIR"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.nstripes=1 "$SPACEENC_DIR"
}

@test "make sure we start without a key configured on the space" {
  # make sure we start without a key configured on the space
  runtest "### KeyRm     " unix IGNORE  "" eos space config default space.encryptionkey=remove --no-confirmation
}

@test "without a space key files are only obfuscated" {
  # without a space key files are only obfuscated
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_DIR/obfuscated"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/obfuscated | grep -c 'Crypt: obfuscated' | grep -w 1"
}

@test "eos.obfuscate=0 does not turn off the policy of the directory" {
  # eos.obfuscate=0 does not turn the policy of the directory off, the file is
  # obfuscated all the same and reads back
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_DIR/obfuscated0" "-ODeos.obfuscate=0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/obfuscated0 | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.obf0" "$SPACEENC_DIR/obfuscated0"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.obf0"
}

@test "an update of an obfuscated file needs no key from the client" {
  # an update of an obfuscated file needs no key from the client
  runtest "### Expected  " unix SUCCESS "" append_expected /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.obf0.expected"
  runtest "### Update    " unix SUCCESS "" append "$SPACEENC_DIR/obfuscated0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.obf0.updated" "$SPACEENC_DIR/obfuscated0"
  runtest "### Diff      " unix SUCCESS "" shell diff "$EOS_TEST_TESTSYS/spaceenc.obf0.expected" "$EOS_TEST_TESTSYS/spaceenc.obf0.updated"
}

@test "keys that would break the opaque info of a capability are refused" {
  # keys which would break the opaque info of a capability are refused
  # note the embedded single quotes - runtest eval's its arguments, without them
  # the '&' would background the command instead of reaching the MGM
  runtest "### BadKey    " unix ERROR   "" eos space config default "'space.encryptionkey=bad&key'" --no-confirmation
  runtest "### BadKey    " unix ERROR   "" eos space config default "'space.encryptionkey=bad key'" --no-confirmation
  runtest "### BadKey    " unix ERROR   "" eos space config default "'space.encryptionkey=bad=key'" --no-confirmation
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep -c encryptionkey | grep -w 0"
}

@test "the short form encryptionkey= works too" {
  # the ticket documents the short form without the 'space.' prefix - both work
  runtest "### ShortForm " unix SUCCESS "" eos space config default "encryptionkey=$SPACEENC_KEY1" --no-confirmation
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep encryptionkey | grep -c hidden | grep -w 1"
  runtest "### KeyRm     " unix SUCCESS "" eos space config default encryptionkey=remove --no-confirmation
}

@test "configure an instance encryption key for the default space" {
  # configure an instance encryption key for the default space
  runtest "### SpaceKey  " unix SUCCESS "" eos space config default "space.encryptionkey=$SPACEENC_KEY1" --no-confirmation
}

@test "without --no-confirmation the change is aborted" {
  # without --no-confirmation the console asks for a typed confirmation code -
  # with no terminal to answer it the operation is aborted and nothing changes
  runtest "### Confirm   " unix ERROR   "" shell "eos -b space config default space.encryptionkey=$SPACEENC_KEY2 < /dev/null"
  runtest "### Confirm   " unix ERROR   "" shell "eos -b space config default space.encryptionkey=remove < /dev/null"
  runtest "### Confirm   " unix ERROR   "" shell "eos -b space config rm default space.encryptionkey < /dev/null"
}

@test "the configured key stays after the aborted changes" {
  # ... the originally configured key has to be untouched after those aborts
  runtest "### Confirm   " unix SUCCESS "" eos space status "default | grep encryptionkey | grep -c hidden | grep -w 1"
}

@test "the key itself must never be printed, only a fingerprint of it" {
  # the key itself must never be printed, only a fingerprint of it
  runtest "### Hidden    " unix SUCCESS "" eos space status "default | grep encryptionkey | grep -c hidden | grep -w 1"
  runtest "### Hidden    " unix SUCCESS "" eos space status "default | grep -c $SPACEENC_KEY1 | grep -w 0"
  runtest "### Hidden    " unix SUCCESS "" eos config dump "| grep -c $SPACEENC_KEY1 | grep -w 0"
  runtest "### Hidden    " unix SUCCESS "" eos config changelog "| grep -c $SPACEENC_KEY1 | grep -w 0"
  runtest "### Hidden    " unix SUCCESS "" eos config changelog "| grep encryptionkey | grep -c hidden | grep -v -w 0"
}

@test "new files get the key of their space" {
  # new files are now encrypted with the key of the space they are created in
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_DIR/encrypted"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/encrypted | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
}

@test "the key is never exposed through the file attributes either" {
  # the key is never exposed through the file attributes either
  runtest "### Hidden    " unix SUCCESS "" eos attr ls "$SPACEENC_DIR/encrypted | grep -c $SPACEENC_KEY1 | grep -w 0"
}

@test "the contents read back without a key at the client" {
  # and the contents come back unchanged without the client knowing any key
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.dec" "$SPACEENC_DIR/encrypted"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.dec"
}

@test "removing the fingerprint does not bypass the key check" {
  # the fingerprint is a user.* attribute - removing it must not bypass the key
  # check, the file is refused until the fingerprint is restored
  SPACEENC_FP=$(command eos -b root://$EOS_TEST_REDIRECTOR attr get user.encrypted.fp "$SPACEENC_DIR/encrypted" 2>/dev/null | cut -d'"' -f2)
  runtest "### Fp        " unix SUCCESS "" shell test "-n" "\"$SPACEENC_FP\""
  runtest "### Fp        " unix SUCCESS "" eos attr rm user.encrypted.fp "$SPACEENC_DIR/encrypted"
  runtest "### Download  " unix ERROR   "" download "$EOS_TEST_TESTSYS/spaceenc.nofp" "$SPACEENC_DIR/encrypted"
  runtest "### Fp        " unix SUCCESS "" eos attr set "user.encrypted.fp=$SPACEENC_FP" "$SPACEENC_DIR/encrypted"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.fp" "$SPACEENC_DIR/encrypted"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.fp"
}

@test "a new space key makes the old contents unreadable" {
  # rotating the key of the space destroys the contents cryptographically
  runtest "### Rotate    " unix SUCCESS "" eos space config default "space.encryptionkey=$SPACEENC_KEY2" --no-confirmation
  runtest "### Download  " unix ERROR   "" download "$EOS_TEST_TESTSYS/spaceenc.lost" "$SPACEENC_DIR/encrypted"
}

@test "metadata stays accessible, only the contents are gone" {
  # metadata stays accessible, only the contents are gone
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/encrypted | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
}

@test "putting the original key back makes the file readable again" {
  # putting the original key back makes the file readable again
  runtest "### Restore   " unix SUCCESS "" eos space config default "space.encryptionkey=$SPACEENC_KEY1" --no-confirmation
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.dec2" "$SPACEENC_DIR/encrypted"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.dec2"
}

@test "an overwrite after a key change uses the current key" {
  # a file whose key was rotated away can still be overwritten, the new contents
  # are encrypted with the current key of the space
  runtest "### Rotate    " unix SUCCESS "" eos space config default "space.encryptionkey=$SPACEENC_KEY2" --no-confirmation
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_DIR/encrypted"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.new" "$SPACEENC_DIR/encrypted"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.new"
}

@test "versioning keeps the encryption markers right" {
  # versioning copies the extended attributes of the previous file onto its
  # successor - make sure the encryption markers stay consistent with the data
  runtest "### Version   " unix SUCCESS "" eos attr set sys.versioning=2 "$SPACEENC_DIR"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_DIR/versioned"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/group" "$SPACEENC_DIR/versioned"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/versioned | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.v2" "$SPACEENC_DIR/versioned"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.v2"
}

@test "the archived version keeps its own key and reads back" {
  # the archived version keeps its own key and stays readable as well - version
  # files are named <timestamp>.<fid>, so look the name up instead of guessing it
  # 'command' bypasses the eos() wrapper of this script, which sends stdout to the log
  SPACEENC_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_DIR/versioned" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.v1" "$SPACEENC_DIR/.sys.v#.versioned/$SPACEENC_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.v1"
  runtest "### Version   " unix SUCCESS "" eos attr rm sys.versioning "$SPACEENC_DIR"
}

@test "an overwrite without eos.obfuscate keeps the file encrypted" {
  # an overwrite which does not ask for obfuscation again keeps an encrypted file
  # encrypted, with a key of its own and the current key of its space - reusing
  # the keys of the file it replaces, for writing or on a version, would make the
  # new contents read back as garbage without any error
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "-p" "$SPACEENC_CGI_DIR"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "$SPACEENC_CGI_DIR"
  runtest "### Attr      " unix ERROR   "" eos attr get sys.file.obfuscate "$SPACEENC_CGI_DIR"
}

@test "plain overwrite" {
  # plain overwrite
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/plain" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/plain | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/plain"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/plain | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.plain" "$SPACEENC_CGI_DIR/plain"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.plain"
}

@test "atomic overwrite" {
  # atomic overwrite
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/atomic" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/atomic | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/atomic" "-ODeos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/atomic | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.atomic" "$SPACEENC_CGI_DIR/atomic"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.atomic"
}

@test "versioned overwrite, the version keeps its own key" {
  # versioned overwrite, the version keeps its own key
  runtest "### Version   " unix SUCCESS "" eos attr set sys.versioning=2 "$SPACEENC_CGI_DIR"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/versioned" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/versioned | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/versioned"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/versioned | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.versioned" "$SPACEENC_CGI_DIR/versioned"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.versioned"
  SPACEENC_CGI_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/versioned" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.v1" "$SPACEENC_CGI_DIR/.sys.v#.versioned/$SPACEENC_CGI_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.v1"
  # also through the commit of an atomic upload
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/versioneda" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/versioneda | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/versioneda" "-ODeos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/versioneda | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.versioneda" "$SPACEENC_CGI_DIR/versioneda"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.versioneda"
  SPACEENC_CGI_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/versioneda" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.versioneda1" "$SPACEENC_CGI_DIR/.sys.v#.versioneda/$SPACEENC_CGI_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.versioneda1"
}

@test "eos.obfuscate=0 asks for plain contents" {
  # eos.obfuscate=0 asks for plain contents
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/versioned0" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/versioned0 | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/versioned0" "-ODeos.obfuscate=0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/versioned0 | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.versioned0" "$SPACEENC_CGI_DIR/versioned0"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.versioned0"
}

@test "eos.versioning=0 keeps the file encrypted and its versions" {
  # a save which skips the new version with eos.versioning=0, as the wopiserver
  # does, keeps the file encrypted and keeps its versions
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/encnv" "-ODeos.obfuscate=1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/encnv" "-ODeos.obfuscate=1"
  runtest "### Ls        " unix SUCCESS "" eos ls "$SPACEENC_CGI_DIR/.sys.v#.encnv | wc -l | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/encnv" "-ODeos.versioning=0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/encnv | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Stat      " unix SUCCESS "" meta stat "$SPACEENC_CGI_DIR/.sys.v#.encnv"
  runtest "### Ls        " unix SUCCESS "" eos ls "$SPACEENC_CGI_DIR/.sys.v#.encnv | wc -l | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.encnv" "$SPACEENC_CGI_DIR/encnv"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.encnv"
}

@test "an atomic versioned overwrite does not reuse the old keys" {
  # atomic overwrite of a versioned file asking for obfuscation again - the
  # commit must not put the keys of the previous version back
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/atomicv" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/atomicv | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/atomicv" "-ODeos.obfuscate=1\&eos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/atomicv | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.atomicv" "$SPACEENC_CGI_DIR/atomicv"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.atomicv"
}

@test "a restored version keeps its own keys" {
  # a restored version keeps the keys it is encrypted with instead of taking the
  # ones of the current file, which becomes a version keeping its own
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/restore" "-ODeos.obfuscate=1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/restore" "-ODeos.obfuscate=1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.keep=kept "$SPACEENC_CGI_DIR/restore"
  SPACEENC_CGI_RESTORE_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/restore" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_RESTORE_VERSION\""
  runtest "### Restore   " unix SUCCESS "" eos file versions "$SPACEENC_CGI_DIR/restore $SPACEENC_CGI_RESTORE_VERSION"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "$SPACEENC_CGI_DIR/restore | grep kept"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/restore | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.restore" "$SPACEENC_CGI_DIR/restore"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.restore"
  SPACEENC_CGI_RESTORE_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/restore" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_RESTORE_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.restore.v" "$SPACEENC_CGI_DIR/.sys.v#.restore/$SPACEENC_CGI_RESTORE_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.restore.v"
}

@test "a plain version stays plain when the current file is encrypted" {
  # a plain version stays plain when the current file is encrypted
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/restoreplain"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/restoreplain" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/restoreplain | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  SPACEENC_CGI_RESTORE_PLAIN=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/restoreplain" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_RESTORE_PLAIN\""
  runtest "### Restore   " unix SUCCESS "" eos file versions "$SPACEENC_CGI_DIR/restoreplain $SPACEENC_CGI_RESTORE_PLAIN"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/restoreplain | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.restoreplain" "$SPACEENC_CGI_DIR/restoreplain"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.restoreplain"
  runtest "### Version   " unix SUCCESS "" eos attr rm sys.versioning "$SPACEENC_CGI_DIR"
}

@test "a client key takes precedence over the space key" {
  # a key provided by the client takes precedence over the key of the space
  runtest "### UserKey   " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_DIR/userkey" "-ODeos.key=$SPACEENC_USERKEY"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/userkey | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/userkey | grep -c 'space:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.user" "$SPACEENC_DIR/userkey" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.user"
}

@test "a file with a client key needs that key for an overwrite" {
  # a file encrypted with a key of the client can only be overwritten with that
  # key: the MGM can't encrypt the new contents with it, and contents stored plain
  # or with another key decode to garbage for a reader passing the key. The
  # upload is refused before the file is touched, the file keeps its contents.
  runtest "### UserKey   " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/userkey" "-ODeos.obfuscate=1\&eos.key=$SPACEENC_USERKEY"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkey | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "$SPACEENC_CGI_DIR/userkey"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkey | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.user" "$SPACEENC_CGI_DIR/userkey" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.user"
}

@test "the key alone is enough to encrypt the new contents with it" {
  # the key alone is enough to encrypt the new contents with it
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/userkey" "-ODeos.key=$SPACEENC_USERKEY"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkey | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkey | grep -c 'space:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.user.key" "$SPACEENC_CGI_DIR/userkey" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.user.key"
}

@test "an update without the client key is refused" {
  # an update without the key is refused like an overwrite: the new range would
  # not decode with the key. eos.obfuscate=0 doesn't help, the file keeps its keys.
  runtest "### Update    " unix ERROR   "" append "$SPACEENC_CGI_DIR/userkey"
  runtest "### Update    " unix ERROR   "" append "$SPACEENC_CGI_DIR/userkey\?eos.obfuscate=0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.user.refused" "$SPACEENC_CGI_DIR/userkey" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.user.refused"
}

@test "with the key the update works" {
  # with the key the update works
  runtest "### Expected  " unix SUCCESS "" append_expected /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.update.expected"
  runtest "### Update    " unix SUCCESS "" append "$SPACEENC_CGI_DIR/userkey\?eos.key=$SPACEENC_USERKEY"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.user.updated" "$SPACEENC_CGI_DIR/userkey" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff "$EOS_TEST_TESTSYS/spaceenc.cgi.update.expected" "$EOS_TEST_TESTSYS/spaceenc.cgi.user.updated"
}

@test "the MGM applies the space key, the client needs none" {
  # the key of a space is applied by the MGM, the client doesn't need it
  runtest "### Upload    " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/spaceupdate" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/spaceupdate | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Update    " unix SUCCESS "" append "$SPACEENC_CGI_DIR/spaceupdate"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.space.updated" "$SPACEENC_CGI_DIR/spaceupdate"
  runtest "### Diff      " unix SUCCESS "" shell diff "$EOS_TEST_TESTSYS/spaceenc.cgi.update.expected" "$EOS_TEST_TESTSYS/spaceenc.cgi.space.updated"
}

@test "a recall from tape stores the bytes of a file it has no key for" {
  # a recall from tape stores the bytes of a file it has no key for
  runtest "### Touch     " unix SUCCESS "" eos touch "$SPACEENC_CGI_DIR/recall"
  runtest "### Attr      " unix SUCCESS "" eos attr set user.encrypted=1 "$SPACEENC_CGI_DIR/recall"
  runtest "### Recall    " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/recall" "-ODeos.ruid=0\&eos.rgid=0\&eos.injection=1"
}

@test "and eos.obfuscate=0 stores them plain on purpose" {
  # and eos.obfuscate=0 stores them plain on purpose
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/userkey" "-ODeos.obfuscate=0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkey | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.user.plain" "$SPACEENC_CGI_DIR/userkey"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.user.plain"
}

@test "eos.obfuscate=0 in an obfuscating directory still encrypts" {
  # in a directory obfuscating its files eos.obfuscate=0 does not turn the policy
  # off: following the advice of the refusal, the new contents are encrypted with
  # the key of the space and read back
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "$SPACEENC_DIR/userkey"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_DIR/userkey" "-ODeos.obfuscate=0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/userkey | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.user0" "$SPACEENC_DIR/userkey"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.user0"
}

@test "an atomic upload is refused the same way" {
  # an atomic upload is refused the same way
  runtest "### UserKey   " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/userkeya" "-ODeos.obfuscate=1\&eos.key=$SPACEENC_USERKEY"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkeya | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "$SPACEENC_CGI_DIR/userkeya" "-ODeos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkeya | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.usera" "$SPACEENC_CGI_DIR/userkeya" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.usera"
}

@test "and a refused upload creates no version" {
  # and a refused upload creates no version
  runtest "### Version   " unix SUCCESS "" eos attr set sys.versioning=2 "$SPACEENC_CGI_DIR"
  runtest "### UserKey   " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/userkeyv" "-ODeos.obfuscate=1\&eos.key=$SPACEENC_USERKEY"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkeyv | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Overwrite " unix ERROR   "" upload "/etc/group" "$SPACEENC_CGI_DIR/userkeyv"
  runtest "### Stat      " unix ERROR   "" meta stat "$SPACEENC_CGI_DIR/.sys.v#.userkeyv"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.userv" "$SPACEENC_CGI_DIR/userkeyv" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.userv"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/userkeyv" "-ODeos.key=$SPACEENC_USERKEY"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkeyv | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.userv2" "$SPACEENC_CGI_DIR/userkeyv" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.userv2"
  SPACEENC_CGI_USER_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/userkeyv" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_USER_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.userv1" "$SPACEENC_CGI_DIR/.sys.v#.userkeyv/$SPACEENC_CGI_USER_VERSION" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.userv1"
  runtest "### UserKey   " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/userkeyav" "-ODeos.obfuscate=1\&eos.key=$SPACEENC_USERKEY"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkeyav | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/userkeyav" "-ODeos.obfuscate=1\&eos.key=$SPACEENC_USERKEY\&eos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/userkeyav | grep -c 'Crypt: encrypted' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.userav" "$SPACEENC_CGI_DIR/userkeyav" "-OSeos.key=$SPACEENC_USERKEY"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.userav"
  runtest "### Version   " unix SUCCESS "" eos attr rm sys.versioning "$SPACEENC_CGI_DIR"
}

@test "eos.xattr.preserve keeps the xattrs but not the keys" {
  # eos.xattr.preserve keeps the xattrs of an encrypted file but never its keys,
  # the new file is encrypted with keys of its own
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/preserve" "-ODeos.obfuscate=1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.keep=kept "$SPACEENC_CGI_DIR/preserve"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/preserve" "-ODeos.xattr.preserve=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "$SPACEENC_CGI_DIR/preserve | grep kept"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/preserve | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.preserve" "$SPACEENC_CGI_DIR/preserve"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.preserve"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/preserve" "-ODeos.xattr.preserve=1\&eos.obfuscate=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "$SPACEENC_CGI_DIR/preserve | grep kept"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/preserve | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.preserve.enc" "$SPACEENC_CGI_DIR/preserve"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.preserve.enc"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/preserve" "-ODeos.xattr.preserve=1\&eos.atomic=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "$SPACEENC_CGI_DIR/preserve | grep kept"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/preserve | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.preserve.atomic" "$SPACEENC_CGI_DIR/preserve"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.preserve.atomic"
}

@test "set up of files for the removal of the space key" {
  # encrypted files which get overwritten once the key of their space is gone,
  # see below
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/keyrm" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/keyrm | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/keyrmv" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/keyrmv | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
}

@test "a conversion copies the encrypted bytes as they are" {
  # a conversion has to copy the stored bytes as they are - decoding them on the
  # way out would store the file in clear and fail the checksum of the original
  # the bare space lets the scheduler pick the group - the group of the source
  # replica may have no other file system to take the new copies
  runtest "### Convert   " unix SUCCESS "" eos file info "$SPACEENC_DIR/encrypted | grep -c 'Layout: replica' | grep -w 1"
  runtest "### Convert   " unix SUCCESS "" eos file convert "$SPACEENC_DIR/encrypted plain:1 default"
  runtest "### Convert   " unix SUCCESS "" wait_for 120 layout_is plain "$SPACEENC_DIR/encrypted"
  runtest "### Convert   " unix SUCCESS "" eos file info "$SPACEENC_DIR/encrypted | grep -c 'Layout: plain' | grep -w 1"
}

@test "after the conversion the file is encrypted and reads back" {
  # still encrypted, and the contents still decode with the key of the space
  runtest "### Convert   " unix SUCCESS "" eos file info "$SPACEENC_DIR/encrypted | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.conv" "$SPACEENC_DIR/encrypted"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.conv"
}

@test "removing the space key makes its files unreadable" {
  # removing the key of a space leaves the files encrypted with it unreadable and
  # the next ones not encrypted, the operator is told both
  runtest "### KeyRm     " unix SUCCESS "" eos space config default "space.encryptionkey=remove --no-confirmation | grep -c -e 'not readable anymore' -e 'not encrypted anymore' | grep -w 2"
  runtest "### KeyRm     " unix ERROR   "" eos space config default space.encryptionkey=remove --no-confirmation
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep -c encryptionkey | grep -w 0"
  runtest "### Download  " unix ERROR   "" download "$EOS_TEST_TESTSYS/spaceenc.gone" "$SPACEENC_DIR/encrypted"
}

@test "'space config rm' removes it as well and warns the same way" {
  # 'space config rm' removes it as well and warns the same way
  runtest "### KeyRm     " unix SUCCESS "" eos space config default "space.encryptionkey=$SPACEENC_KEY1" --no-confirmation
  runtest "### KeyRm     " unix SUCCESS "" eos space config rm default "space.encryptionkey --no-confirmation | grep -c -e 'not readable anymore' -e 'not encrypted anymore' | grep -w 2"
  runtest "### Grep      " unix SUCCESS "" eos space status "default | grep -c encryptionkey | grep -w 0"
}

@test "and new files fall back to plain obfuscation" {
  # and new files fall back to plain obfuscation
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_DIR/plain"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_DIR/plain | grep -c 'Crypt: obfuscated' | grep -w 1"
}

@test "an overwrite without the space key stays obfuscated" {
  # an encrypted file overwritten while the key of its space is gone stays
  # obfuscated and readable, it can't be encrypted any more
  runtest "### Download  " unix ERROR   "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrm.gone" "$SPACEENC_CGI_DIR/keyrm"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/keyrm"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/keyrm | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrm" "$SPACEENC_CGI_DIR/keyrm"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrm"
}

@test "an overwrite of an obfuscated file stays obfuscated" {
  # an obfuscated file overwritten without asking for obfuscation again stays
  # obfuscated, with a key of its own, with or without a new version
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obf" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obf | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obf"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obf | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obf" "$SPACEENC_CGI_DIR/obf"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obf"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfa" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfa | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfa" "-ODeos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfa | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfa" "$SPACEENC_CGI_DIR/obfa"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfa"
}

@test "a new version of an obfuscated file stays obfuscated" {
  # a new version of a file which is only obfuscated stays obfuscated, as it did
  # when it reused the previous key, but with a key of its own
  runtest "### Version   " unix SUCCESS "" eos attr set sys.versioning=2 "$SPACEENC_CGI_DIR"
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfv" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfv | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfv"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfv | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfv" "$SPACEENC_CGI_DIR/obfv"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfv"
  SPACEENC_CGI_OBF_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/obfv" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_OBF_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfv1" "$SPACEENC_CGI_DIR/.sys.v#.obfv/$SPACEENC_CGI_OBF_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.obfv1"
  # also through the commit of an atomic upload
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfva" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfva | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfva" "-ODeos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfva | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfva" "$SPACEENC_CGI_DIR/obfva"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfva"
  SPACEENC_CGI_OBF_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/obfva" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_OBF_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfva1" "$SPACEENC_CGI_DIR/.sys.v#.obfva/$SPACEENC_CGI_OBF_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.obfva1"
  # eos.obfuscate=0 asks for plain contents
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfv0" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfv0 | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfv0" "-ODeos.obfuscate=0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfv0 | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfv0" "$SPACEENC_CGI_DIR/obfv0"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfv0"
  # an encrypted file which gets a new version while the key of its space is gone
  # stays obfuscated and readable - the previous version keeps its contents and
  # its key, it is readable again once that key is configured again
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/keyrmv"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/keyrmv | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrmv" "$SPACEENC_CGI_DIR/keyrmv"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrmv"
  SPACEENC_CGI_KEYRM_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/keyrmv" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_KEYRM_VERSION\""
  runtest "### Download  " unix ERROR   "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrmv.gone" "$SPACEENC_CGI_DIR/.sys.v#.keyrmv/$SPACEENC_CGI_KEYRM_VERSION"
  runtest "### SpaceKey  " unix SUCCESS "" eos space config default "space.encryptionkey=$SPACEENC_KEY2" --no-confirmation
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrmv1" "$SPACEENC_CGI_DIR/.sys.v#.keyrmv/$SPACEENC_CGI_KEYRM_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrmv1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrmv2" "$SPACEENC_CGI_DIR/keyrmv"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.keyrmv2"
  runtest "### KeyRm     " unix SUCCESS "" eos space config default space.encryptionkey=remove --no-confirmation
  # a save which skips the new version with eos.versioning=0, as the wopiserver
  # does, keeps the file obfuscated and keeps its versions
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfnv" "-ODeos.obfuscate=1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfnv" "-ODeos.obfuscate=1"
  runtest "### Ls        " unix SUCCESS "" eos ls "$SPACEENC_CGI_DIR/.sys.v#.obfnv | wc -l | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfnv" "-ODeos.versioning=0"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfnv | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Stat      " unix SUCCESS "" meta stat "$SPACEENC_CGI_DIR/.sys.v#.obfnv"
  runtest "### Ls        " unix SUCCESS "" eos ls "$SPACEENC_CGI_DIR/.sys.v#.obfnv | wc -l | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfnv" "$SPACEENC_CGI_DIR/obfnv"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.obfnv"
  # in a space which has a key by now, the new version is encrypted with it
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfvk" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfvk | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### SpaceKey  " unix SUCCESS "" eos space config default "space.encryptionkey=$SPACEENC_KEY1" --no-confirmation
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfvk"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfvk | grep -c 'Crypt: encrypted (space:default)' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfvk" "$SPACEENC_CGI_DIR/obfvk"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfvk"
  SPACEENC_CGI_OBF_VERSION=$(command eos -b root://$EOS_TEST_REDIRECTOR file versions "$SPACEENC_CGI_DIR/obfvk" 2>/dev/null | awk 'NF{print $NF}' | tail -1)
  runtest "### Version   " unix SUCCESS "" shell test "-n" "\"$SPACEENC_CGI_OBF_VERSION\""
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfvk1" "$SPACEENC_CGI_DIR/.sys.v#.obfvk/$SPACEENC_CGI_OBF_VERSION"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.obfvk1"
  runtest "### KeyRm     " unix SUCCESS "" eos space config default space.encryptionkey=remove --no-confirmation
}

@test "an atomic overwrite keeps the key of the new contents" {
  # the commit of an atomic overwrite must keep the key of the new contents
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfav" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfav | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfav" "-ODeos.obfuscate=1\&eos.atomic=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfav | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfav" "$SPACEENC_CGI_DIR/obfav"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfav"
  runtest "### Version   " unix SUCCESS "" eos attr rm sys.versioning "$SPACEENC_CGI_DIR"
}

@test "eos.xattr.preserve keeps an obfuscated file obfuscated" {
  # eos.xattr.preserve keeps a file which is only obfuscated obfuscated, like a
  # new version does, unless the upload asks for eos.obfuscate=0
  runtest "### Upload    " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfp" "-ODeos.obfuscate=1"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfp | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set user.keep=kept "$SPACEENC_CGI_DIR/obfp"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfp" "-ODeos.xattr.preserve=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "$SPACEENC_CGI_DIR/obfp | grep kept"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfp | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfp" "$SPACEENC_CGI_DIR/obfp"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfp"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/passwd" "$SPACEENC_CGI_DIR/obfp" "-ODeos.xattr.preserve=1\&eos.atomic=1"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "$SPACEENC_CGI_DIR/obfp | grep kept"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfp | grep -c 'Crypt: obfuscated' | grep -w 1"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfp.atomic" "$SPACEENC_CGI_DIR/obfp"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/spaceenc.cgi.obfp.atomic"
  runtest "### Overwrite " unix SUCCESS "" upload "/etc/group" "$SPACEENC_CGI_DIR/obfp" "-ODeos.xattr.preserve=1\&eos.obfuscate=0"
  runtest "### Attr Get  " unix SUCCESS "" eos attr get user.keep "$SPACEENC_CGI_DIR/obfp | grep kept"
  runtest "### Grep      " unix SUCCESS "" eos file info "$SPACEENC_CGI_DIR/obfp | grep -c 'Crypt:' | grep -w 0"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/spaceenc.cgi.obfp.plain" "$SPACEENC_CGI_DIR/obfp"
  runtest "### Diff      " unix SUCCESS "" shell diff /etc/group "$EOS_TEST_TESTSYS/spaceenc.cgi.obfp.plain"
}

@test "clean up of the space encryption tests" {
  runtest "### Cleanup   " unix IGNORE  "" shell rm "-f" "$EOS_TEST_TESTSYS/spaceenc.*"
  runtest "### Cleanup   " unix SUCCESS "" eos rm "-r" "$SPACEENC_DIR"
  runtest "### Cleanup   " unix SUCCESS "" eos rm "-r" "$SPACEENC_CGI_DIR"
}

@test "limits of path and name length" {
  # OTHER TESTS
  runtest "### Ls max input " unix ERROR "" eos ls /file1/file2/file3/file4/file5/file6/file7/file8/file9/file10/file11/file12/file13/file14/file15/file16/file17/file18/file19/file20/file21/file22/file23/file24/file25/file26/file27/file28/file29/file30/file31/file32/file33/file34/file35/file36/file37/file38/file39/file40/file41/file42/file43/file44/file45/file46/file47/file48/file49/file50/file51/file52/file53/file54/file55/file56/file57/file58/file59/file60/file61/file62/file63/file64/file65/file66/file67/file68/file69/file70/file71/file72/file73/file74/file75/file76/file77/file78/file79/file80/file81/file82/file83/file84/file85/file86/file87/file88/file89/file90/file91/file92/file93/file94/file95/file96/file97/file98/file99/file100/file101/file102/file103/file104/file105/file106/file107/file108/file109/file110/file111/file112/file113/file114/file115/file116/file117/file118/file119/file120/file121/file122/file123/file124/file125/file126/file127/file128/file129/file130/file131/file132/file133/file134/file135/file136/file137/file138/file139/file140/file141/file142/file143/file144/file145/file146/file147/file148/file149/file150/file151/file152/file153/file154/file155/file156/file157/file158/file159/file160/file161/file162/file163/file164/file165/file166/file167/file168/file169/file170/file171/file172/file173/file174/file175/file176/file177/file178/file179/file180/file181/file182/file183/file184/file185/file186/file187/file188/file189/file190/file191/file192/file193/file194/file195/file196/file197/file198/file199/file200/file201/file202/file203/file204/file205/file206/file207/file208/file209/file210/file211/file212/file213/file214/file215/file216/file217/file218/file219/file220/file221/file222/file223/file224/file225/file226/file227/file228/file229/file230/file231/file232/file233/file234/file235/file236/file237/file238/file239/file240/file241/file242/file243/file244/file245/file246/file247/file248/file249/file250/file251/file252/file253/file254/file255/file256/file257/file258/file259/file260/file261/file262/file263/file264/file265/file266/file267/file268/file269/file270/file271/file272/file273/file274/file275/file276/file277/file278/file279/file280/file281/file282/file283/file284/file285/file286/file287/file288/file289/file290/file291/file292/file293/file294/file295/file296/file297/file298/file299/file300
  LONG_NAME="$(head -c 4500 < /dev/zero | tr '\0' '\141')"
  export LONG_NAME
  runtest "### Touch long name  " unix SUCCESS "" eos file touch "/eos/$EOS_TEST_INSTANCE/test/${LONG_NAME}"
  runtest "### Ls long name     " unix SUCCESS "" eos ls -l "/eos/$EOS_TEST_INSTANCE/test/"
  runtest "### Remove long name " unix SUCCESS "" eos rm -F "/eos/$EOS_TEST_INSTANCE/test/${LONG_NAME}"
  unset LONG_NAME
}
