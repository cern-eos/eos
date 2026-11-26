# bats file_tags=find
load ../instance

@test "find by name, type, owner and attribute" {
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/b"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c"
  runtest "### Mkdir     " unix SUCCESS "" eos touch "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a.txt"
  runtest "### Mkdir     " unix SUCCESS "" eos touch "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/b.txt"
  runtest "### Mkdir     " unix SUCCESS "" eos touch "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c.txt"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "750" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c"
  runtest "### Chown     " unix SUCCESS "" eos chown 0:0 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest"
  runtest "### Chown     " unix SUCCESS "" eos chown 0:0 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a"
  runtest "### Chown     " unix SUCCESS "" eos chown 0:0 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/b"
  runtest "### Chown     " unix SUCCESS "" eos chown ${NOBODY_UID}:${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c"
  runtest "### Chown     " unix SUCCESS "" eos chown 0:0 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a.txt"
  runtest "### Chown     " unix SUCCESS "" eos chown 0:0 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/b.txt"
  runtest "### Chown     " unix SUCCESS "" eos chown ${NOBODY_UID}:${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c.txt"
  runtest "### Find      " unix SUCCESS "" eos find -d -flag 750 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Find      " unix SUCCESS "" eos find -d -nflag 750 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 3"
  runtest "### Find      " unix SUCCESS "" eos find -d -uid ${NOBODY_UID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Find      " unix SUCCESS "" eos find -d -nuid ${NOBODY_UID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 3"
  runtest "### Find      " unix SUCCESS "" eos find -d -gid ${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Find      " unix SUCCESS "" eos find -d -ngid ${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 3"
  runtest "### Find      " unix SUCCESS "" eos find -f -uid ${NOBODY_UID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Find      " unix SUCCESS "" eos find -f -nuid ${NOBODY_UID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 2"
  runtest "### Find      " unix SUCCESS "" eos find -f -gid ${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Find      " unix SUCCESS "" eos find -f -ngid ${NOBODY_GID} "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 2"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.test=abc "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a.txt"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.test=def "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/b.txt"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.test=def "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c.txt"
  runtest "### Find      " unix SUCCESS "" eos find -f -x sys.test=abc "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/links/"
  runtest "### Symlink   " unix SUCCESS "" eos file symlink "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/links/a.ln" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a.txt"
  runtest "### Symlink   " unix SUCCESS "" eos file symlink -f "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/links/a.ln" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a.txt"
  runtest "### Find      " unix SUCCESS "" eos find "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/links/ | grep -w \"/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/links/a.ln -> /eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a.txt\""
  runtest "### Rm        " unix SUCCESS "" eos rm "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/links/a.ln"
  runtest "### Upload    " unix SUCCESS "" upload   "$TESTSYSFILE1M" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/f1" "-ODeos.ruid=0"
  # The tree size is updated after the upload, not at once
  runtest "### Du -s     " unix SUCCESS "" wait_for 60 eos du -s "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/ | grep 1024000"
  runtest "### Du -s     " unix SUCCESS "" eos du -s -h "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/ | grep 1.02 | grep MB"
  runtest "### Du -s     " unix SUCCESS "" eos du -s -h --si "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/ | grep 1000.00 | grep kiB"
  runtest "### Rm        " unix SUCCESS "" eos rm -rF "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest"
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest"
  runtest "### Attr Set  " unix SUCCESS "" eos attr set sys.versioning=3 "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/a.txt"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/b.txt"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c.txt"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c.txt"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c.txt"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/c.txt"
  runtest "### Find      " unix SUCCESS "" eos find -d "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 2"
  runtest "### Find      " unix SUCCESS "" eos find -d --skip-version-dirs "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Find      " unix SUCCESS "" eos find -d --cache --skip-version-dirs "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 1"
  runtest "### Find      " unix SUCCESS "" eos find -f "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 6"
  runtest "### Find      " unix SUCCESS "" eos find -f --skip-version-dirs "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 3"
  runtest "### Find      " unix SUCCESS "" eos find -f --cache --skip-version-dirs "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 3"
  runtest "### Find      " unix SUCCESS "" eos find "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 8"
  runtest "### Find      " unix SUCCESS "" eos find --skip-version-dirs "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 4"
  runtest "### Find      " unix SUCCESS "" eos find --cache --skip-version-dirs "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 4"
}

@test "find -j --fileinfo gives one JSON array of all entries" {
  # Test the JSON fileinfo output of find - 'eos -j find --fileinfo'. The entries
  # form a single JSON array, each of them serialized exactly the way a plain
  # 'fileinfo -j' would, so the whole output has to parse as one JSON array of
  # as many objects as the plain find reports entries.
  runtest "### Find json " unix SUCCESS "" eosj find --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq -r 'type' | grep -w array"
  runtest "### Find json " unix SUCCESS "" eosj find --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq 'length' | grep -w 8"
  runtest "### Find json " unix SUCCESS "" eosj find --skip-version-dirs --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq 'length' | grep -w 4"
  runtest "### Find json " unix SUCCESS "" eosj find -f --skip-version-dirs --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq -r '.[].path' | grep -c 'findtest/[abc].txt' | grep -w 3"
  runtest "### Find json " unix SUCCESS "" eosj find -f --skip-version-dirs --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq -r '.[].size' | sort -u | grep -w 1024"
  runtest "### Find json " unix SUCCESS "" eosj find -d --skip-version-dirs --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq -r '.[].nfiles' | grep -w 3"
}

@test "a directory entry does not carry its children" {
  # A directory entry must not carry the fileinfo of its children, find already
  # reports each of them as an entry of its own
  runtest "### Find json " unix SUCCESS "" eosj find -d --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq -r '.[] | has(\"children\")' | sort -u | grep -w false"
}

@test "... whereas plain fileinfo keeps expanding them" {
  # ... whereas plain fileinfo keeps expanding them
  runtest "### Fileinfo   " unix SUCCESS "" eosj fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/ | jq -r 'has(\"children\")' | grep -w true"
}

@test "Without -j, or without --fileinfo, the output must not change" {
  # Without -j, or without --fileinfo, the output must not change
  runtest "### Find      " unix SUCCESS "" eos find -f --skip-version-dirs --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | grep -c 'keylength.file=' | grep -w 3"
  runtest "### Find      " unix SUCCESS "" eosj find -f --skip-version-dirs "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | wc -l | grep -w 3"
}

@test "names with & or #and# come back unchanged" {
  # The fileinfo of an entry is collected directly rather than through the opaque
  # CGI interface of the fileinfo command, which seals '&' into '#and#' with no
  # lossless way back. Both a name holding a '&' and one holding the seal itself
  # have to come back untouched.
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/find\&symbol"
  runtest "### Find json " unix SUCCESS "" eosj find -f --skip-version-dirs --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq -r '.[].name' | grep -w 'find\&symbol'"
  runtest "### Upload    " unix SUCCESS "" upload "$TESTSYSFILE1K" "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest/find#and#seal"
  runtest "### Find json " unix SUCCESS "" eosj find -f --skip-version-dirs --fileinfo "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest | jq -r '.[].name' | grep -w 'find#and#seal'"
  runtest "### Rm        " unix SUCCESS "" eos rm -rF "/eos/$EOS_TEST_INSTANCE/test/instancetest/findtest"
}
