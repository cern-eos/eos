# bats file_tags=https
# HTTPS transfers with x509, macaroons, EOS tokens, TPC and the "authz=" CGI.
# Needs the user certificate of the CI images (/root/.globus), whose DN the MGM
# maps to eos-user, and HTTPS on the MGM ($http_url of eos.bash).
load ../instance

d=$testdir/https
# The CA and the user certificate, see also instance.bash
ca=(--capath "$capath" --cacert "$cacert")
user=(--cert "$cert" --key "$key")
# The proxy is not in the default place (/tmp/x509up_u<uid>): each xrootd
# client of the host would present it, also the plain eos calls of the suite.
proxy=/tmp/eos-bats-https-proxy-u$(id -u)

setup_file() {
  [ -r $cert ] || skip "no user certificate $cert"
  id eos-user >/dev/null 2>&1 || skip "no eos-user"
  eos mkdir -p $d
  eos chown eos-user:eos-user $d
  space_set default token.generation 1
  # The credentials of the tests. They carry the identity of $cert, which the
  # MGM maps to the owner of $d. A failure shows in the tests that use them.
  X509_USER_CERT=$cert X509_USER_KEY=$key X509_USER_PROXY=$proxy voms-proxy-init >/dev/null 2>&1 || true
  export MACAROON EOS_RW_TOKEN EOS_RO_TOKEN later=$(($(date +%s) + 600))
  MACAROON=$(X509_USER_PROXY=$proxy eos-macaroon-init $http_url/$d/ --activity UPLOAD,DOWNLOAD,MANAGE --validity 60 |
    jq -r .macaroon) || true
  EOS_RW_TOKEN=$(token_gsi $d/ rwx --tree) || true
  EOS_RO_TOKEN=$(token_gsi $d/ rx --tree) || true
}

teardown_file() {
  X509_USER_PROXY=$proxy voms-proxy-destroy >/dev/null 2>&1 || true
}

# token_gsi <path> <permission> [--tree]: an EOS token, asked for with the proxy
token_gsi() {
  XrdSecPROTOCOL=gsi X509_USER_PROXY=$proxy command eos root://$(hostname -f)/ token \
    --path $1 --permission $2 --expires $later ${3-}
}

# statuses <curl options>: the HTTP status lines of a HEAD request and its
# redirects, joined with +
statuses() {
  curl -I -sS -L "$@" | grep HTTP | tr -d '\r' | paste -sd+
}

up_ok="HTTP/1.1 307 TEMPORARY_REDIRECT+HTTP/1.1 201 CREATED"
down_ok="HTTP/1.1 200 OK"

@test "x509: upload and download" {
  is "$up_ok" statuses "${ca[@]}" "${user[@]}" $http_url/$d/helloworld.txt --upload-file <(echo ola)
  is "$down_ok" statuses "${ca[@]}" "${user[@]}" $http_url/$d/helloworld.txt
}

# EOS-6643: a GET of a range across the 1 MiB read buffer of the FST
@test "range GETs give the right bytes" {
  local p=$BATS_TEST_TMPDIR/payload f
  python3 -c "import sys; sys.stdout.buffer.write(bytes(range(256)) * 4096 + b'final-range-bytes!')" > $p
  : > $BATS_TEST_TMPDIR/empty
  for f in payload empty; do
    curl --fail -sS -L -m 60 "${ca[@]}" "${user[@]}" --upload-file $BATS_TEST_TMPDIR/$f $http_url/$d/range-$f.dat >/dev/null
  done
  eos-http-range-test $http_url/$d/range-payload.dat $p --empty-url $http_url/$d/range-empty.dat \
    --capath $capath --cacert $cacert --cert $cert --key $key
}

@test "macaroon: the issuer refuses a random caveat" {
  # The URL comes first: --caveat takes all values that follow
  run ! env X509_USER_PROXY=$proxy eos-macaroon-init $http_url/$d/ --activity UPLOAD,DOWNLOAD,MANAGE \
    --validity 60 --caveat dummy:random
}

@test "macaroon: upload and download" {
  # Without a macaroon, the requests would go as a client without authentication
  [[ -n $MACAROON && $MACAROON != null ]] || { echo "the issuer gave no macaroon"; return 1; }
  is "$up_ok" statuses -H "Authorization: Bearer $MACAROON" $http_url$d/helloworld-macaroon.txt --upload-file <(echo ola)
  is "$down_ok" statuses -H "Authorization: Bearer $MACAROON" $http_url$d/helloworld-macaroon.txt
}

@test "EOS token: rw and ro upload and download" {
  [[ $EOS_RW_TOKEN == zteos64:* && $EOS_RO_TOKEN == zteos64:* ]] || { echo "no EOS tokens"; return 1; }
  # Only eos-user may access the directory
  eos chmod 700 $d
  is "$up_ok" statuses -H "Authorization: Bearer $EOS_RW_TOKEN" $http_url$d/helloworld-eostoken.txt --upload-file <(echo ola-token)
  is "$down_ok" statuses -H "Authorization: Bearer $EOS_RW_TOKEN" $http_url$d/helloworld-eostoken.txt
  is "$down_ok" statuses -H "Authorization: Bearer $EOS_RO_TOKEN" $http_url$d/helloworld-eostoken.txt
  # In a read-only directory, the ro token cannot upload
  eos chmod 500 $d
  is "HTTP/1.1 403 FORBIDDEN" statuses -H "Authorization: Bearer $EOS_RO_TOKEN" $http_url$d/helloworld-eostoken-ro.txt --upload-file <(echo ola-token)
  # The authz tests need 750: it denies all others
  eos chmod 750 $d
}

# The token alone identifies and authorizes the file: no certificate, no header
@test "EOS token as path: GET, range and HEAD of its file" {
  local token body=$BATS_TEST_TMPDIR/body opts=(-q -s -m 60 "${ca[@]}")
  token=$(token_gsi $d/helloworld-eostoken.txt r)
  [[ $token == zteos64:* && $token != *[[:space:]]* ]]
  run -0 curl "${opts[@]}" -L -o $body -w '%{http_code} %{num_redirects}' $http_url/$token
  [[ $output =~ ^200\ [1-9][0-9]*$ ]]
  [ "$(cat $body)" = ola-token ]
  is 206 curl "${opts[@]}" -L --range 0-2 -o $body -w '%{http_code}' $http_url/$token
  [ "$(cat $body)" = ola ]
  is 200 curl "${opts[@]}" -I -o /dev/null -w '%{http_code}' $http_url/$token
  is 403 curl "${opts[@]}" -o /dev/null -w '%{http_code}' $http_url/zteos64:invalid
}

# expect_status "<codes>" <method> <url> [<curl options>]: the HTTP status is one of <codes>
expect_status() {
  local m=(--request "$2") status
  [ $2 != HEAD ] || m=(--head)
  status=$(curl -sS -L -m 60 "${ca[@]}" -o /dev/null -w '%{http_code}' "${m[@]}" "${@:4}" $3)
  echo "$2 $3 -> $status, expected $1"
  [[ " $1 " == *" $status "* ]]
}

@test "HTTP DELETE of files, directories and trees" {
  local base=$http_url$d/delete-$$ s suffix q e mac t
  for s in plain slash; do
    suffix=
    [ $s = plain ] || suffix=/
    for q in "" '?eos.app=delete-test&label=a%20b'; do
      e=$base-empty-$s
      expect_status 201 MKCOL $e/ "${user[@]}"
      mac=$(curl --fail -sS -m 60 "${ca[@]}" "${user[@]}" -H 'Content-Type: application/macaroon-request' \
        --data '{"caveats":["activity:DELETE"],"validity":"PT1M"}' "$e/$q" | jq -er '.macaroon | select(length > 0)')
      # The macaroon of one directory does not delete another one
      expect_status 201 MKCOL $e-sibling/ "${user[@]}"
      expect_status '403 404' DELETE $e-sibling/ -H "Authorization: Bearer $mac"
      expect_status 200 HEAD $e-sibling/ "${user[@]}"
      expect_status 204 DELETE $e-sibling/ "${user[@]}"
      expect_status 404 HEAD $e-sibling/ "${user[@]}"
      expect_status 204 DELETE $e$suffix -H "Authorization: Bearer $mac"
      expect_status 404 HEAD $e/ "${user[@]}"
    done
    t=$base-tree-$s
    expect_status 201 MKCOL $t/ "${user[@]}"
    expect_status 201 MKCOL $t/child/ "${user[@]}"
    expect_status 201 PUT $t/child/file.txt "${user[@]}" --data-binary 'HTTP DELETE regression'
    expect_status 204 DELETE $t$suffix "${user[@]}"
    expect_status 404 HEAD $t/ "${user[@]}"
    expect_status 404 HEAD $t/child/ "${user[@]}"
    expect_status 404 HEAD $t/child/file.txt "${user[@]}"
  done
  expect_status 201 PUT $base-file.txt "${user[@]}" --data-binary 'HTTP DELETE regression'
  expect_status 204 DELETE $base-file.txt "${user[@]}"
  expect_status 404 HEAD $base-file.txt "${user[@]}"
  # A read-only token cannot delete
  [[ $EOS_RO_TOKEN == zteos64:* ]]
  expect_status 201 MKCOL $base-readonly/ "${user[@]}"
  expect_status 403 DELETE $base-readonly/ -H "Authorization: Bearer $EOS_RO_TOKEN"
  expect_status 200 HEAD $base-readonly/ "${user[@]}"
  expect_status 204 DELETE $base-readonly/ "${user[@]}"
  expect_status 404 HEAD $base-readonly/ "${user[@]}"
}

# macaroon_for <url> <activities>: a macaroon of the user certificate
macaroon_for() {
  curl -sS -4 "${ca[@]}" "${user[@]}" -X POST -H 'Content-Type: application/macaroon-request' \
    -d "{\"caveats\": [\"activity:$2\"], \"validity\": \"PT3000M\"}" $1 | jq -r .macaroon
}

@test "TPC: pull and push with macaroons" {
  local src=$http_url$d/helloworld.txt pull=$http_url$d/helloworld-tcp-pull.txt push=$http_url$d/helloworld-tcp-push.txt
  local tsrc tpull tpush ok="HTTP/1.1 307 Temporary Redirect+HTTP/1.1 201 Created"
  local tpc=(-4 --capath "$capath" -X COPY -H 'Secure-Redirection: 1' -H 'X-No-Delegate: 1' -H 'Credentials: none' -H "TransferHeaderTest: Test")
  tsrc=$(macaroon_for $src DOWNLOAD)
  tpull=$(macaroon_for $pull UPLOAD,DELETE,LIST,MANAGE)
  tpush=$(macaroon_for $push UPLOAD,DELETE,LIST,MANAGE)
  is "$ok" statuses "${tpc[@]}" -H "Authorization: Bearer $tpull" -H "TransferHeaderAuthorization: Bearer $tsrc" -H "Source: $src" $pull
  is "$ok" statuses "${tpc[@]}" -H "Authorization: Bearer $tsrc" -H "TransferHeaderAuthorization: Bearer $tpush" -H "Destination: $push" $src
}

# The authorization can also be the CGI authz= instead of a header. XrdHttp
# gives the MGM the query already decoded, so each accepted spelling of the
# bearer prefix must map the same identity (EOS-6614). The directory is 750
# (see the EOS token test): only the owner gets access.
@test "authz CGI: each spelling of a macaroon and a token maps the owner" {
  [[ -n $MACAROON && -n $EOS_RW_TOKEN ]] || { echo "needs a macaroon and an EOS token"; return 1; }
  local u=$http_url$d
  is "$up_ok" statuses "$u/helloworld-authz.txt?authz=Bearer%20$MACAROON" --upload-file <(echo ola-authz)
  is "$down_ok" statuses "$u/helloworld-authz.txt?authz=Bearer%20$MACAROON"
  # Double encoded: the work-around from before EOS-6614 still works
  is "$down_ok" statuses "$u/helloworld-authz.txt?authz=Bearer%2520$MACAROON"
  is "$down_ok" statuses "$u/helloworld-eostoken.txt?authz=Bearer%20$EOS_RW_TOKEN"
  # Without the bearer prefix, as over xroot
  is "$down_ok" statuses "$u/helloworld-eostoken.txt?authz=$EOS_RW_TOKEN"
  # An invalid token maps nobody, whom the 750 mode denies
  is "HTTP/1.1 403 FORBIDDEN" statuses "$u/helloworld-authz.txt?authz=Bearer%20invalidtoken"
  # The CGI and the header together: the MGM refuses
  run -0 statuses -H "Authorization: Bearer $MACAROON" "$u/helloworld-authz.txt?authz=Bearer%20$MACAROON"
  [[ $output == *" 500"* ]]
}
