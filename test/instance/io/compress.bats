# bats file_tags=compress
load ../instance

COMPRESS_DIR=/eos/$EOS_TEST_INSTANCE/test/instancetest/compress
COMPRESS_PATH=$COMPRESS_DIR/plain

@test "file convert compresses and decompresses at rest" {
  runtest "### Payload   " unix SUCCESS "" make_compress_src
  runtest "### Mkdir     " unix SUCCESS "" eos mkdir "$COMPRESS_DIR"
  runtest "### Chmod     " unix SUCCESS "" eos chmod "777" "$COMPRESS_DIR"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.layout=plain "$COMPRESS_DIR"
  runtest "### Attr      " unix SUCCESS "" eos attr set sys.forced.nstripes=1 "$COMPRESS_DIR"
  runtest "### Upload    " unix SUCCESS "" upload "$EOS_TEST_TESTSYS/compress.src" "$COMPRESS_PATH"
  runtest "### Reject    " unix ERROR   "" eos file convert --compress --uncompress "$COMPRESS_PATH"
  runtest "### Plain     " unix SUCCESS "" eos file convert --uncompress "$COMPRESS_PATH | grep -c 'already marked as uncompressed' | grep -w 1"
  runtest "### Compress  " unix SUCCESS "" eos file convert --compress "$COMPRESS_PATH | grep -c ':zstd' | grep -w 1"
  runtest "### Compress  " unix SUCCESS "" eos file convert --compress "$COMPRESS_PATH | grep -c 'already marked for compression' | grep -w 1"
  runtest "### Wait      " unix SUCCESS "" wait_for 90 compress_csize "$COMPRESS_PATH" set
  runtest "### Ratio     " unix SUCCESS "" eosj fileinfo "$COMPRESS_PATH | jq -e '.size == 1048576 and .csize > 0 and (.ratio | startswith(\"1:\"))'"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/compress.out" "$COMPRESS_PATH"
  runtest "### Diff      " unix SUCCESS "" shell diff "$EOS_TEST_TESTSYS/compress.src" "$EOS_TEST_TESTSYS/compress.out"
  runtest "### Decompress" unix SUCCESS "" eos file convert --uncompress "$COMPRESS_PATH | grep -c 'success: marked for decompression' | grep -w 1"
  runtest "### Reject    " unix ERROR   "" eos file convert --uncompress "$COMPRESS_PATH" replica:2
  runtest "### Wait      " unix SUCCESS "" wait_for 90 compress_csize "$COMPRESS_PATH" clear
  runtest "### Cleared   " unix SUCCESS "" eosj fileinfo "$COMPRESS_PATH | jq -e '.size == 1048576 and (has(\"csize\") | not)'"
  runtest "### Download  " unix SUCCESS "" download "$EOS_TEST_TESTSYS/compress.plain" "$COMPRESS_PATH"
  runtest "### Diff      " unix SUCCESS "" shell diff "$EOS_TEST_TESTSYS/compress.src" "$EOS_TEST_TESTSYS/compress.plain"
  runtest "### Plain     " unix SUCCESS "" eos file convert --uncompress "$COMPRESS_PATH | grep -c 'already marked as uncompressed' | grep -w 1"
  runtest "### Cleanup   " unix IGNORE  "" shell rm -f "$EOS_TEST_TESTSYS/compress.src" "$EOS_TEST_TESTSYS/compress.probe" "$EOS_TEST_TESTSYS/compress.out" "$EOS_TEST_TESTSYS/compress.plain"
  runtest "### Cleanup   " unix SUCCESS "" eos rm "-r" "$COMPRESS_DIR"
}
