# bats file_tags=grpc_gateway
# The HTTP/JSON REST API of the eos-grpc-gateway, with plain curl. The MGM
# needs EOS_MGM_ENABLE_REST_API=1. Setting: EOS_GRPC_GATEWAY_URL, default
# http://127.0.0.1:40054.
load ../instance

gw=${EOS_GRPC_GATEWAY_URL:-http://127.0.0.1:40054}
d=$testdir/grpcgateway

setup_file() {
  curl -sS --max-time 10 -o /dev/null $gw/ ||
    { echo "no gateway at $gw: is EOS_MGM_ENABLE_REST_API=1 set on the MGM?"; return 1; }
  eos mkdir -p $d
  eos chmod 777 $d
}

b64() { printf '%s' "$1" | base64 -w0; }
md() { echo "\"md\":{\"path\":\"$(b64 "$1")\"}"; }

# gw <endpoint> <json body> [<retc>|nonzero] [<jq filter of the reply>]: one
# request; the "retc" of the reply must be <retc> (default 0). A streamed
# command wraps its reply: filter .result. A streamed reply is one JSON object
# per gRPC message, and only the last one has the status (GrpcStreamReply.hh).
# Sets $out, the stdOut of all messages, and $messages, their number.
gw() {
  local resp retc sel=${4:-.}
  [ "$sel" != . ] || sel=
  resp=$(curl -sS --max-time 30 -X POST $gw/v1/eos/rest/gateway/$1 -H 'Content-Type: application/json' -d "$2")
  jq -e "${4:-.}" >/dev/null 2>&1 <<< "$resp" || { echo "not a JSON reply: ${resp:0:400}"; return 1; }
  retc=$(jq -s -r ".[-1]$sel.retc" <<< "$resp")
  messages=$(jq -s length <<< "$resp")
  out=$(jq -s -r "[.[]$sel.stdOut] | add" <<< "$resp")
  case ${3:-0} in
    nonzero) [ "$retc" != 0 ] && [ "$retc" != null ] ;;
    *) [ "$retc" = "${3:-0}" ] ;;
  esac || { echo "retc=$retc, expected ${3:-0}: ${resp:0:400}"; return 1; }
}

@test "whoami and version answer" {
  gw whoami_cmd '{"whoami":true}'
  gw version_cmd '{}'
}

@test "mkdir, attr set, attr get and chmod of a directory" {
  gw mkdir_cmd "{$(md $d/sub/),\"parents\":true}"
  gw attr_cmd "{\"cmd\":\"ATTR_SET\",$(md $d/sub/),\"key\":\"user.gwtest\",\"value\":\"$(b64 hello)\"}"
  gw attr_cmd "{\"cmd\":\"ATTR_GET\",$(md $d/sub/),\"key\":\"user.gwtest\"}"
  [[ $out == *'user.gwtest="hello"'* ]]
  gw chmod_cmd "{$(md $d/sub/),\"mode\":755}"
}

@test "touch, ls, fileinfo and find of a file" {
  gw touch_cmd "{$(md $d/sub/f1)}"
  gw ls_cmd "{$(md $d/sub/)}" 0 .result
  [[ $out == *f1* ]]
  gw fileinfo_cmd "{$(md $d/sub/f1),\"path\":true}"
  gw find_cmd "{\"Path\":\"$d/sub/\",\"Files\":true}" 0 .result
  [[ $out == *$d/sub/f1* ]]
}

@test "mv, rm and rmdir, and the errors after them" {
  gw mv_cmd "{$(md $d/sub/f1),\"target\":\"$(b64 $d/sub/f2)\"}"
  gw rm_cmd "{\"Path\":\"$d/sub/f2\"}"
  gw rmdir_cmd "{$(md $d/sub/)}"
  gw fileinfo_cmd "{$(md $d/sub/f2),\"path\":true}" nonzero
  gw ls_cmd "{$(md $d/sub/)}" nonzero .result
}

# Long names under a deep parent give ~1.2 kB of output per file: 900 files
# take the reply past the 1 MB chunk size
@test "a reply bigger than one gRPC message comes back in several" {
  local n=900 long deep
  long=$(printf 'd%0240d' 1)
  deep=$d/chunk/$long/$long/$long/$long
  eos mkdir -p $deep
  # One console session for all files: an eos touch for each is much slower
  for i in $(seq 1 $n); do echo "touch $deep/$(printf 'f%0240d' $i)"; done | command eos >/dev/null 2>&1
  gw find_cmd "{\"Path\":\"$deep/\",\"Files\":true}" 0 .result
  [ "$messages" -ge 2 ]
  [ "$(grep -c . <<< "$out")" = $n ]
}
