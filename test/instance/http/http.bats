# bats file_tags=http
load ../instance

cacert="/etc/grid-security/certificates/rootCA.pem"
cert="/root/.globus/usercert.pem"
key="/root/.globus/userkey.pem"
HTTPDIR=eos/$EOS_TEST_INSTANCE/test/instancetest/http.dir

@test "HTTPS PUT, GET and xattr preserve" {
  capath="$(dirname "${cacert}")"
  runtest "### Mkdir        " unix SUCCESS "" eos mkdir $HTTPDIR
  runtest "### Chmod        " unix SUCCESS "" eos chmod 777 $HTTPDIR
  runtest "### Http PUT     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --upload-file $TESTSYSFILE50M https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m
  runtest "### Http GET     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m --output $EOS_TEST_TESTSYS/dumpit
  # a PUT replaces the file and drops its xattrs, unless its query asks for
  # eos.xattr.preserve
  runtest "### Http PUT     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --upload-file /etc/passwd https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/preserve
  runtest "### Attr Set     " unix SUCCESS "" eos attr set user.preserve=kept /$HTTPDIR/preserve
  runtest "### Http PUT     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --upload-file /etc/group https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/preserve
  runtest "### Attr Get     " unix ERROR   "" eos attr get user.preserve /$HTTPDIR/preserve
  runtest "### Attr Set     " unix SUCCESS "" eos attr set user.preserve=kept /$HTTPDIR/preserve
  HTTPPRESERVE_FID=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/$HTTPDIR/preserve" 2>/dev/null | jq -r .id)
  runtest "### Http PUT     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --upload-file /etc/passwd https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/preserve?eos.xattr.preserve=1
  runtest "### Attr Get     " unix SUCCESS "" eos attr get user.preserve "/$HTTPDIR/preserve | grep kept"
  HTTPPRESERVE_NEWFID=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "/$HTTPDIR/preserve" 2>/dev/null | jq -r .id)
  runtest "### Fid          " unix SUCCESS "" shell test "-n" "\"$HTTPPRESERVE_FID\""
  runtest "### Fid          " unix SUCCESS "" shell test "\"$HTTPPRESERVE_FID\"" "!=" "null"
  runtest "### Fid          " unix SUCCESS "" shell test "-n" "\"$HTTPPRESERVE_NEWFID\""
  runtest "### Fid          " unix SUCCESS "" shell test "\"$HTTPPRESERVE_NEWFID\"" "!=" "null"
  runtest "### Fid          " unix SUCCESS "" shell test "\"$HTTPPRESERVE_FID\"" "!=" "\"$HTTPPRESERVE_NEWFID\""
  runtest "### Http GET     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/preserve --output $EOS_TEST_TESTSYS/http.preserve
  runtest "### Diff         " unix SUCCESS "" shell diff /etc/passwd "$EOS_TEST_TESTSYS/http.preserve"
  runtest "### Chmod        " unix SUCCESS "" eos chmod 644 $HTTPDIR
  runtest "### Http PUT     " unix ERROR "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --upload-file $TESTSYSFILE50M https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m
  runtest "### Http GET     " unix ERROR "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m --output $EOS_TEST_TESTSYS/dumpit
  runtest "### Chmod        " unix SUCCESS "" eos chmod 777 $HTTPDIR
  # Range requests
  runtest "### Http GET     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --range 0-499 https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m --output $EOS_TEST_TESTSYS/dumpit
  runtest "### Http GET     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --range 0-499,500-999 https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m --output $EOS_TEST_TESTSYS/dumpit
  runtest "### Http GET     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --range 0-499,1000-1499 https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m --output $EOS_TEST_TESTSYS/dumpit
  runtest "### Http GET     " unix SUCCESS "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --range 50000000- https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m --output $EOS_TEST_TESTSYS/dumpit
  runtest "### Http GET     " unix ERROR "" curl -v --capath "${capath}" --cert "${cert}" --cacert "${cacert}" --key "${key}" --location --fail --silent --range 51200000-512000000 https://$EOS_TEST_FULL_REDIRECTOR:${EOS_HTTPS_PORT}/$HTTPDIR/file.50m --output $EOS_TEST_TESTSYS/dumpit
  runtest "### HttpXUpload  " unix SUCCESS "" eos attr set sys.forced.checksum=adler $HTTPDIR
  runtest "### HttpXUpload  " unix SUCCESS "" shell eos-http-upload-test rangeupload multipartfile /$HTTPDIR/
  runtest "### Cleanup      " unix SUCCESS "" eos rm "-r" "/$HTTPDIR"
}
