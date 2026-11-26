# ------------------------------------------------------------------------------
# Settings and helpers of the instance suite, the BATS form of the old
# eos-instance-test. Each category is one file, tagged with its name:
#
#   eos-bats-test instance /eos/dev                         all categories
#   eos-bats-test instance /eos/dev --filter-tags core      one category
#   eos-bats-test instance /eos/dev --filter-tags core --filter-tags acl
#
# The files are in directories by area, and bats runs them in name order. Each
# category must work alone, after the set up of setup_suite.bash.
#
# The tests keep their runtest lines (see ../bats/runtest.bash) and the paths
# of the old script: they work in /eos/$EOS_TEST_INSTANCE/test. The prefix
# gives the instance: /eos/dev -> EOS_TEST_INSTANCE=dev.
#
# The modes of runtest (eos, meta, upload, download ...) are the functions
# below, as in the old script. upload and download keep the old order of
# arguments: download <local> <eos path>. Do not use same_file of eos.bash.
# ------------------------------------------------------------------------------
# The files are one level down, in the directories of the areas
suite=instance
here=$(dirname "${BASH_SOURCE[0]}")
load "$here/../bats/eos"
load "$here/../bats/runtest"

export EOS_TEST_INSTANCE=${EOS_TEST_INSTANCE:-$(basename "${EOS_TEST_PREFIX:-/eos/dev}")}
export EOS_TEST_REDIRECTOR=${EOS_TEST_REDIRECTOR:-localhost}
export EOS_TEST_FULL_REDIRECTOR=${EOS_TEST_FULL_REDIRECTOR:-$(hostname -f)}
export EOS_TEST_TESTSYS=${EOS_TEST_TESTSYS:-/tmp/eos-instance-test}
export EOS_HTTPS_PORT=${EOS_HTTPS_PORT:-$(grep -oP 'XrdHttp:\K(\d+)' /etc/xrd.cf.mgm 2>/dev/null)}
export EOS_TEST_RAIN_DIR=$EOS_TEST_TESTSYS/rain
export TESTSYSFILE0K=$EOS_TEST_TESTSYS/file.0K
export TESTSYSFILE1K=$EOS_TEST_TESTSYS/file.1K
export TESTSYSFILE1M=$EOS_TEST_TESTSYS/file.1M
export TESTSYSFILE50M=$EOS_TEST_TESTSYS/file.50M
export TESTFSTFILE32M=$EOS_TEST_TESTSYS/file.32M
export EOSLOGBOOK=/var/log/eos/mgm/logbook.log
NOBODY_UID=$(id -u nobody)
NOBODY_GID=$(id -g nobody)
EOS_USER_UID=$(id -u eos-user 2>/dev/null || true)
SERVERKEY=$(grep "sec.protocol sss" /etc/xrd.cf.mgm 2>/dev/null | grep -o '\-s.*' | cut -d' ' -f2)
XRD_VERS_MAJOR=$(xrootd -v 2>&1 | grep -oEm1 '[0-9]+\.[0-9]+' | head -n1 | cut -d. -f1)
XRD_VERS_MAJOR=${XRD_VERS_MAJOR:-0}
# The certificates of the HTTP tests
cacert=/etc/grid-security/certificates/rootCA.pem
capath=$(dirname $cacert)
cert=/root/.globus/usercert.pem
key=/root/.globus/userkey.pem

# As in the old script, a failed runtest line does not stop the test: the
# next lines run, also the clean up at its end, and the next tests start from
# the state that they expect. The line is printed, and teardown fails the test.
eval "$(declare -f runtest | sed '1s/^runtest/runtest_line/')"
# In setup_suite and teardown_suite (no test), the line is only printed.
runtest() {
  runtest_line "$@" && return 0
  echo "# FAILED: $*"
  [ -z "${BATS_TEST_TMPDIR-}" ] || touch $BATS_TEST_TMPDIR/runtest.failed
}

instance_teardown() {
  if [ -e $BATS_TEST_TMPDIR/runtest.failed ]; then
    # eos_teardown saves the MGM log of a test that did not complete
    unset BATS_TEST_COMPLETED
    eos_teardown
    echo "a runtest line failed, see FAILED above"
    return 1
  fi
  eos_teardown
}

teardown() {
  instance_teardown
}

# The old script wrote the command lines and their output to these logs. Here
# the output goes to the test output: bats shows it when a test fails.
EOSLASTLOG=/dev/null
EOSCERTLOG=/dev/stderr

# The checksums of the pattern files that setup_suite makes
CKS1K=$(local_checksum adler $TESTSYSFILE1K 2>/dev/null)
CKS1M=$(local_checksum adler $TESTSYSFILE1M 2>/dev/null)
CKS50M=$(local_checksum adler $TESTSYSFILE50M 2>/dev/null)

# ------------------------------------------------------------------------------
# The modes of runtest and the helpers of the old script
# ------------------------------------------------------------------------------
upload () {
    src=$1
    dst=$2
    shift
    shift
    echo xrdcp -np -v -f $* $src root://$EOS_TEST_REDIRECTOR/$dst >& $EOSLASTLOG ;eval command xrdcp -f $* $src root://$EOS_TEST_REDIRECTOR/$dst >> $EOSCERTLOG 2>&1
}
download () {
    src=$1
    dst=$2
    shift
    shift
    echo xrdcp -np -v -f $* root://$EOS_TEST_REDIRECTOR/$dst $src  >& $EOSLASTLOG ;eval command xrdcp -f $* root://$EOS_TEST_REDIRECTOR/$dst $src >> $EOSCERTLOG 2>&1
}
downloadh () {
    src=$1
    dst=$2
    shift
    shift
    echo xrdcp -np -v -f $* root://$EOS_TEST_FULL_REDIRECTOR/$dst $src  >& $EOSLASTLOG ;eval command xrdcp -f $* root://$EOS_TEST_FULL_REDIRECTOR/$dst $src >> $EOSCERTLOG 2>&1
}
# wait until <count> atomic uploads of <dir>/<name>, versioned or not, are open
wait_atomic_uploads () {
    local dir=$1 name=$2 count=$3 i
    for i in $(seq 1 300); do
      if [ "$(command eos -b root://$EOS_TEST_REDIRECTOR ls -a "$dir" 2>/dev/null | grep -cF -e ".sys.a#.$name." -e ".sys.a#.v#$name.")" = "$count" ]; then
        return 0
      fi
      sleep 0.1
    done
    return 1
}
# two overlapping atomic uploads with eos.xattr.preserve=1 of <path>: /etc/passwd
# is opened first, /etc/group second and closed first. Each upload streams from
# stdin and is kept open until its flag file exists. Succeeds if the upload of
# /etc/group is committed and the one of /etc/passwd is discarded.
atomic_preserve_overlap () {
    local path=$1
    local url="root://$EOS_TEST_REDIRECTOR/$path?eos.atomic=1&eos.xattr.preserve=1"
    local flag="$EOS_TEST_TESTSYS/atomic.overlap"
    local pid_first pid_second ret_first ret_second ok=0
    echo atomic_preserve_overlap $path >& $EOSLASTLOG
    rm -f "$flag.first" "$flag.second"
    ( cat /etc/passwd; while [ ! -e "$flag.first" ]; do sleep 0.1; done ) |
      command xrdcp -f - "$url" >> $EOSCERTLOG 2>&1 &
    pid_first=$!
    if wait_atomic_uploads "$(dirname "$path")" "$(basename "$path")" 1; then
      ( cat /etc/group; while [ ! -e "$flag.second" ]; do sleep 0.1; done ) |
        command xrdcp -f - "$url" >> $EOSCERTLOG 2>&1 &
      pid_second=$!
      wait_atomic_uploads "$(dirname "$path")" "$(basename "$path")" 2 && ok=1
      touch "$flag.second"
      wait $pid_second; ret_second=$?
    fi
    touch "$flag.first"
    wait $pid_first; ret_first=$?
    rm -f "$flag.first" "$flag.second"
    echo "overlap opened=$ok first=$ret_first second=$ret_second" >> $EOSCERTLOG 2>&1
    test "$ok" = 1 && test "$ret_second" = 0 && test "$ret_first" != 0
}
# atomic upload of <src> to <path> with the CGI <cgi>, kept open while
# <command> [<args>] <path> changes the file it replaces. Succeeds if the
# command succeeds while the upload is open and the upload is committed.
atomic_upload_during () {
    local src=$1 path=$2 cgi=$3
    shift 3
    local url="root://$EOS_TEST_REDIRECTOR/$path?eos.atomic=1&$cgi"
    local flag="$EOS_TEST_TESTSYS/atomic.during"
    local pid ret ok=0
    echo atomic_upload_during $src $path $cgi $* >& $EOSLASTLOG
    rm -f "$flag"
    ( cat "$src"; while [ ! -e "$flag" ]; do sleep 0.1; done ) |
      command xrdcp -f - "$url" >> $EOSCERTLOG 2>&1 &
    pid=$!
    wait_atomic_uploads "$(dirname "$path")" "$(basename "$path")" 1 &&
      "$@" "$path" >> $EOSCERTLOG 2>&1 && ok=1
    touch "$flag"
    wait $pid; ret=$?
    rm -f "$flag"
    echo "during=$ok upload=$ret" >> $EOSCERTLOG 2>&1
    test "$ok" = 1 && test "$ret" = 0
}
# refresh the application lock of <path> the way the wopiserver does, to expire
# at <expires>, and change its xattrs: user.changed, user.given and user.added
# are set to "after", user.removed is removed
xattr_change () {
    local expires=$1 path=$2
    command eos -b -a wopi_test root://$EOS_TEST_REDIRECTOR attr set "sys.app.lock=expires:$expires,type:shared,owner:*:wopi_test" "$path" &&
      command eos -b -a wopi_test root://$EOS_TEST_REDIRECTOR attr set user.iop.lock=after "$path" &&
      command eos -b root://$EOS_TEST_REDIRECTOR attr set user.changed=after "$path" &&
      command eos -b root://$EOS_TEST_REDIRECTOR attr set user.given=after "$path" &&
      command eos -b root://$EOS_TEST_REDIRECTOR attr set user.added=after "$path" &&
      command eos -b root://$EOS_TEST_REDIRECTOR attr rm user.removed "$path"
}
# release the application lock of <path> the way the wopiserver does
lock_release () {
    command eos -b -a wopi_test root://$EOS_TEST_REDIRECTOR attr rm sys.app.lock "$1" &&
      command eos -b -a wopi_test root://$EOS_TEST_REDIRECTOR attr rm user.iop.lock "$1"
}
# share <path>, which has the file ACL user.acl=u:2:r, with uid 3 as well
acl_change () {
    command eos -b root://$EOS_TEST_REDIRECTOR attr set user.acl=u:2:r,u:3:r "$1"
}
# remove the file ACL of <path>
acl_remove () {
    command eos -b root://$EOS_TEST_REDIRECTOR attr rm user.acl "$1"
}
# remove <path>
file_remove () {
    command eos -b root://$EOS_TEST_REDIRECTOR rm "$1"
}
# take the names the commit of an atomic upload can give the version of <path>:
# the one derived from the file and the five it falls back to. Remembers the
# file, which the upload must then leave in place.
version_names_take () {
    local path=$1 info ctime fxid i
    local vdir
    vdir="$(dirname "$path")/.sys.v#.$(basename "$path")"
    info=$(command eos -b root://$EOS_TEST_REDIRECTOR fileinfo "$path" -m) || return 1
    ctime=$(echo "$info" | tr ' ' '\n' | sed -n 's/^ctime=\([0-9]*\)\..*/\1/p')
    fxid=$(echo "$info" | tr ' ' '\n' | sed -n 's/^fxid=//p')
    echo "version_names_take $path ctime=$ctime fxid=$fxid" >> $EOSCERTLOG
    test -n "$ctime" && test -n "$fxid" || return 1
    echo "$fxid" > "$EOS_TEST_TESTSYS/versioning.taken"
    for i in $(seq 0 5); do
      command eos -b root://$EOS_TEST_REDIRECTOR file touch "$vdir/$((ctime + i)).$fxid" >> $EOSCERTLOG 2>&1 || return 1
    done
}
# succeeds while <path> is still the file version_names_take looked at
version_names_file_kept () {
    local fxid info
    fxid=$(cat "$EOS_TEST_TESTSYS/versioning.taken")
    info=$(command eos -b root://$EOS_TEST_REDIRECTOR fileinfo "$1" -m 2>> $EOSCERTLOG)
    echo "version_names_file_kept $1 taken=$fxid $info" >> $EOSCERTLOG
    echo "$info" | tr ' ' '\n' | grep -qx "fxid=$fxid"
}
# upload <src> to <dst> with the xrdcp options that follow, succeeds if the
# commit discards it as an overlapping atomic upload
upload_discarded () {
    local src=$1 dst=$2 out
    shift 2
    out=$(command xrdcp -f "$@" "$src" "root://$EOS_TEST_REDIRECTOR/$dst" 2>&1)
    local rc=$?
    echo "upload_discarded rc=$rc $out" >> $EOSCERTLOG
    test $rc -ne 0 && echo "$out" | grep -qF "overlapping atomic upload"
}
# write the contents of <src> followed by the buffer xrdcpappend appends to <dst>
append_expected () {
    local i byte
    {
      cat "$1"
      for ((i = 0; i < 4096; ++i)); do
        printf -v byte '\\%03o' $((i % 255))
        printf "$byte"
      done
    } > "$2"
}
# A hard link target whose directory can't be found. As in
# eos-file-cont-detached-test the parent is changed behind the back of the MGM,
# eos-ns-inspect also invalidates the file in its cache. The clone has to be tagged before the hard link exists and the target has to
# be hidden through FUSE: eos rm clones a tagged file, and a tagged hard link
# would be cloned instead of being removed.
# Usage: hl_clone_tag <dir>          - make <dir> part of a new clone
#        hl_clone_purge <dir>        - remove that clone again
#        hl_target_hidden <dir>      - <dir> holds a hidden hard link target
#        hl_target_detach <dir>      - give the target a parent which doesn't exist
#        hl_target_attach <dir>      - give it <dir> back as its parent
#        hl_target_holds <dir> <src> - the target has the contents of <src>
#        eos_path_absent <path>      - <path> does not exist
hl_clone_tag () {
    local id
    id=$(command eos -b root://$EOS_TEST_REDIRECTOR oldfind -j -x sys.clone=+10 "$1" | head -1 | jq -r .c)
    echo "hl_clone_tag $1 id=$id" >> $EOSCERTLOG
    test "$id" -gt 10 2>/dev/null && echo "$id" > "$EOS_TEST_TESTSYS/hl.clone"
}

hl_clone_purge () {
    command eos -b root://$EOS_TEST_REDIRECTOR oldfind -f -x "sys.clone=-$(cat "$EOS_TEST_TESTSYS/hl.clone")" "$1" >> $EOSCERTLOG 2>&1
}

hl_target_name () {
    command eos -b root://$EOS_TEST_REDIRECTOR ls -a "$1" | grep -F '...eos.ino...'
}

hl_target_hidden () {
    test -n "$(hl_target_name "$1")"
}

hl_target_parent () {
    local fid=$1 parent=$2
    local QDB_CLUSTER QDB_PWDFILE
    QDB_CLUSTER=$(cat /etc/xrd.cf.mgm | grep "^mgmofs.qdbcluster" | awk '{print $2;}')
    QDB_PWDFILE=$(cat /etc/xrd.cf.mgm | grep "^mgmofs.qdbpassword_file" | awk '{print $2;}')
    eos-ns-inspect change-fid --password-file "$QDB_PWDFILE" --members "$QDB_CLUSTER" --fid "$fid" --new-parent "$parent" --no-dry-run >> $EOSCERTLOG 2>&1
}

hl_target_detach () {
    local dir=$1 fid
    fid=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "$dir/$(hl_target_name "$dir")" | jq -r .id)
    echo "hl_target_detach $dir fid=$fid" >> $EOSCERTLOG
    test "$fid" -gt 0 2>/dev/null || return 1
    echo "$fid" > "$EOS_TEST_TESTSYS/hl.target.$(basename "$dir")"
    hl_target_parent "$fid" 20000000
}

hl_target_attach () {
    local dir=$1 parent
    parent=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "$dir" | jq -r .id)
    hl_target_parent "$(cat "$EOS_TEST_TESTSYS/hl.target.$(basename "$dir")")" "$parent"
}

hl_target_holds () {
    local dir=$1 src=$2 name info
    local local_copy
    local_copy="$EOS_TEST_TESTSYS/hl.holds.$(basename "$dir")"
    name=$(hl_target_name "$dir")
    # a read alone proves nothing: a file without locations gets placed on a file
    # system, and reads back until the FST has deleted its contents
    info=$(command eos -b root://$EOS_TEST_REDIRECTOR fileinfo "$dir/$name" -m) || return 1
    echo "$info" >> $EOSCERTLOG
    echo "$info" | grep -q -w 'nrep=0' && return 1
    command xrdcp -f "root://$EOS_TEST_REDIRECTOR/$dir/$name" "$local_copy" >> $EOSCERTLOG 2>&1 &&
      diff "$src" "$local_copy" >> $EOSCERTLOG 2>&1
}

# layout_is <layout> <path> ...: each path has the layout, e.g. plain: a
# conversion is done
layout_is() {
    local layout=$1 f
    shift
    for f; do
        command eos -b root://$EOS_TEST_REDIRECTOR file info "$f" 2>/dev/null | grep -q "Layout: $layout" || return 1
    done
}

# all_fs_ready: each file system is rw and online again, e.g. after
# eos-rain-test switched some of them off
all_fs_ready() {
    local fs
    fs=$(command eos -b root://$EOS_TEST_REDIRECTOR fs ls -m) || return 1
    ! grep -v ' configstatus=rw ' <<< "$fs" | grep -q . && ! grep -v ' stat.active=online ' <<< "$fs" | grep -q .
}

eos_path_absent () {
    ! command eos -b root://$EOS_TEST_REDIRECTOR fileinfo "$1" > /dev/null 2>&1
}
meta() {
    eval command xrdfs $EOS_TEST_REDIRECTOR $*
}

# eos <args>: the console with $EOSROLE and $EOSAPP. As in the old script the
# arguments go through eval: eos "ls $d | grep x" runs the pipe. The output
# goes to stdout, for the helpers of eos.bash (json, attr_value ...).
eos() {
    local -x EOS_MGM_URL=root://$EOS_TEST_REDIRECTOR
    eval command eos -b $EOSROLE $EOSAPP $*
}

# eosr <args>: eos through the full host name, not localhost: the MGM does not
# trust the client as local
eosr() {
    local -x EOS_MGM_URL=root://$EOS_TEST_FULL_REDIRECTOR
    eval command eos -b $EOSROLE $*
}

eosj() {
    echo eos -j -b $EOSROLE $EOSAPP root://$EOS_TEST_REDIRECTOR $* >& $EOSLASTLOG; eval command eos -j -b $EOSROLE $EOSAPP root://$EOS_TEST_REDIRECTOR $*
}

recycle_config_path_encoding() (
    local operation=$1
    local base="/eos/$EOS_TEST_INSTANCE/test/instancetest/recycle-path-${operation#--}-$$"
    local expected="/eos/$EOS_TEST_INSTANCE/proc/recycle/"
    local sentinel="recycle-path-neighbour"
    local suffix path target neighbour actual attributes request response operand identifier idtype index=0
    local targets=()
    # EXIT must also see these arrays after a failed function unwinds its locals.
    recycle_path_created=()
    recycle_path_created_files=()

    # Keep literal ampersands out of the eval-based eos() wrapper.
    recycle_path_eos() {
        command eos -b -r 0 0 "root://$EOS_TEST_REDIRECTOR" "$@"
    }

    recycle_path_value() {
        actual=$(recycle_path_eos attr -V get sys.recycle "$1" 2>> "$EOSCERTLOG") || return 1
        if [ "$actual" != "$2" ]; then
            echo "error: sys.recycle on '$1': expected '$2', got '$actual'" >> "$EOSCERTLOG"
            return 1
        fi
    }

    recycle_path_cleanup() {
        local result=$? i
        trap - EXIT
        # These are metadata-only files created by this test. Disable recycling
        # before removing them so cleanup does not populate the recycle bin.
        for path in "${recycle_path_created_files[@]}"; do
            if attributes=$(recycle_path_eos attr ls "$path" 2>> "$EOSCERTLOG"); then
                if printf '%s\n' "$attributes" | grep -q '^sys[.]recycle='; then
                    recycle_path_eos attr rm sys.recycle "$path" >> "$EOSCERTLOG" 2>&1 || result=1
                fi
            else
                result=1
            fi
            recycle_path_eos rm -F "$path" >> "$EOSCERTLOG" 2>&1 || result=1
        done
        # Remove only the empty directories created by this test, deepest first.
        for ((i=${#recycle_path_created[@]}-1; i>=0; --i)); do
            path=${recycle_path_created[i]}
            if attributes=$(recycle_path_eos attr ls "$path" 2>> "$EOSCERTLOG"); then
                if printf '%s\n' "$attributes" | grep -q '^sys[.]recycle='; then
                    recycle_path_eos attr rm sys.recycle "$path" >> "$EOSCERTLOG" 2>&1 || result=1
                fi
            else
                result=1
            fi
            recycle_path_eos rmdir "$path" >> "$EOSCERTLOG" 2>&1 || result=1
        done
        exit "$result"
    }

    echo "recycle config $operation: literal subtree paths, identifiers and untouched neighbours" > "$EOSLASTLOG"
    trap recycle_path_cleanup EXIT
    recycle_path_eos mkdir "$base" >> "$EOSCERTLOG" 2>&1 || exit 1
    recycle_path_created+=("$base")

    for suffix in '-bin' AND '&' '&mgm.fixture=value' '&eos.fixture=value' '&xrd.fixture=value' '&callback=value' '&authz=value' '%26mgm.fixture=value' '&mgm.fixture=value&eos.fixture=other' cid cxid pid pxid fid fxid cid-injected legacy; do
        index=$((index+1))
        neighbour="$base/case-$index"
        case "$suffix" in
            cid|cxid|pid|pxid|fid|fxid|cid-injected) target="$neighbour-bin" ;;
            AND|'&') target="$base/$suffix" ;;
            legacy) target="$neighbour&legacy=value" ;;
            *) target="$neighbour$suffix" ;;
        esac
        # Seed removal fixtures directly, independently of the add-bin test.
        for path in "$neighbour" "$neighbour/child"; do
            recycle_path_eos mkdir "$path" >> "$EOSCERTLOG" 2>&1 || exit 1
            recycle_path_created+=("$path")
            recycle_path_eos attr set "sys.recycle=$sentinel" "$path" >> "$EOSCERTLOG" 2>&1 || exit 1
        done
        targets=("$target")
        case "$suffix" in
            fid|fxid)
                recycle_path_eos touch -n "$target" >> "$EOSCERTLOG" 2>&1 || exit 1
                recycle_path_created_files+=("$target")
                ;;
            *)
                targets+=("$target/child")
                for path in "${targets[@]}"; do
                    recycle_path_eos mkdir "$path" >> "$EOSCERTLOG" 2>&1 || exit 1
                    recycle_path_created+=("$path")
                done
                ;;
        esac
        for path in "${targets[@]}"; do
            recycle_path_eos attr set "sys.recycle=$sentinel" "$path" >> "$EOSCERTLOG" 2>&1 || exit 1
            if [ "$suffix" = cid-injected ]; then
                recycle_path_eos attr set "user.fixture=$sentinel" "$path" >> "$EOSCERTLOG" 2>&1 || exit 1
            fi
        done
        operand=$target
        case "$suffix" in
            cid|cxid|pid|pxid|fid|fxid|cid-injected)
                attributes=$(command eos -b -j -r 0 0 "root://$EOS_TEST_REDIRECTOR" fileinfo "$target" 2>> "$EOSCERTLOG") || exit 1
                identifier=$(printf '%s\n' "$attributes" | jq -er '.id | tostring | select(test("^[1-9][0-9]*$"))' 2>> "$EOSCERTLOG") || exit 1
                idtype=${suffix%-injected}
                case "$idtype" in
                    cxid|pxid|fxid) printf -v operand '%s:%x' "$idtype" "$identifier" ;;
                    *) operand="$idtype:$identifier" ;;
                esac
                # The existing identifier resolver permits a suffix. It must not
                # become another CGI field while crossing the recycle boundary.
                if [ "$suffix" = cid-injected ]; then
                    operand="$operand&mgm.attr.key=user.fixture"
                fi
                ;;
            legacy) operand=${target//&/#AND#} ;;
        esac
        echo "recycle config $operation: '$operand' resolves to '$target'" >> "$EOSCERTLOG"

        # The console's legacy tokenizer seals ampersands. Send a raw protobuf
        # subtree so this reaches the MGM encoding boundary without that help.
        request=$(command eos-recycle-config-request "$operation" "$operand" 2>> "$EOSCERTLOG") || exit 1
        response=$(command xrdcp -np "root://$EOS_TEST_REDIRECTOR//proc/user/?mgm.cmd.proto=$request&eos.ruid=0&eos.rgid=0" - 2>> "$EOSCERTLOG") || exit 1
        printf '%s\n' "$response" >> "$EOSCERTLOG"
        case "$response" in
            *'&mgm.proc.retc=0') ;;
            *) echo "error: recycle config $operation failed for '$operand'" >> "$EOSCERTLOG"; exit 1;;
        esac
        for path in "${targets[@]}"; do
            if [ "$suffix" = cid-injected ]; then
                actual=$(recycle_path_eos attr -V get user.fixture "$path" 2>> "$EOSCERTLOG") || exit 1
                if [ "$actual" != "$sentinel" ]; then
                    echo "error: identifier suffix changed user.fixture on '$path'" >> "$EOSCERTLOG"
                    exit 1
                fi
            fi
            if [ "$operation" = --add-bin ]; then
                recycle_path_value "$path" "$expected" || exit 1
            else
                # A failed attr request must not masquerade as an absent key.
                attributes=$(recycle_path_eos attr ls "$path" 2>> "$EOSCERTLOG") || exit 1
                if printf '%s\n' "$attributes" | grep -q '^sys[.]recycle='; then
                    echo "error: sys.recycle remains on '$path' after --remove-bin" >> "$EOSCERTLOG"
                    exit 1
                fi
            fi
        done
        recycle_path_value "$neighbour" "$sentinel" || exit 1
        recycle_path_value "$neighbour/child" "$sentinel" || exit 1
    done
)

tpc1() {
    src=$1
    dst=$2
    echo xrdcp --tpc only root://$EOS_TEST_REDIRECTOR/$src root://$EOS_TEST_REDIRECTOR/$dst >& $EOSLASTLOG; eval command xrdcp -f --tpc only root://$EOS_TEST_REDIRECTOR/$src root://$EOS_TEST_REDIRECTOR/$dst >> $EOSCERTLOG 2>&1
}

tpc2() {
    src=$1
    dst=$2
    echo xrdcopy --tpc only root://$EOS_TEST_REDIRECTOR/$src root://$EOS_TEST_REDIRECTOR/$dst >& $EOSLASTLOG; eval command xrdcopy --tpc only root://$EOS_TEST_REDIRECTOR/$src root://$EOS_TEST_REDIRECTOR/$dst >> $EOSCERTLOG 2>&1
}

tgrep() {
        a=$1
        shift
        echo "eval $* | grep $a" >& $EOSLASTLOG; eval $* | grep $a >> $EOSCERTLOG 2>&1
}

stress() {
    echo xrdstress $* >& $EOSLASTLOG; eval XRD_RUNFORKHANDLER=1 xrdstress $* >> $EOSCERTLOG 2>&1
}

append() {
    echo xrdcpappend root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpappend root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

overlap() {
    echo xrdcpappendoverlap root://123@$EOS_TEST_REDIRECTOR/$1 root://124@$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpappendoverlap root://123@$EOS_TEST_REDIRECTOR/$1 root://124@$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

abort() {
    echo xrdcpabort root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpabort root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

extend() {
    echo xrdcpextend root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpextend root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

delay() {
    echo sleep $1 >& $EOSLASTLOG ; eval sleep $1 >> $EOSCERTLOG 2>&1
}

shrink() {
    echo xrdcpshrink root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpshrink root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

random() {
    echo xrdcprandom root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcprandom root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

truncate() {
    echo xrdcptruncate root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcptruncate root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

cp_nonstreaming() {
    echo xrdcpnonstreaming $1 root://$EOS_TEST_REDIRECTOR/$2 >& $EOSLASTLOG; eval xrdcpnonstreaming $1 root://$EOS_TEST_REDIRECTOR/$2 >> $EOSCERTLOG 2>&1
}

holes() {
    echo xrdcpholesroot://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpholes root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

backward() {
    echo xrdcpbackward root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpbackward root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

partial() {
    echo xrdcppartial root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcppartial root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

downloadrandom() {
    echo xrdcpdownloadrandom root://$EOS_TEST_REDIRECTOR/$1 >& $EOSLASTLOG; eval xrdcpdownloadrandom root://$EOS_TEST_REDIRECTOR/$1 >> $EOSCERTLOG 2>&1
}

eos_rain() {
    echo eos-rain-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >& $EOSLASTLOG; eval eos-rain-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >> $EOSCERTLOG 2>&1
}

eos_ec_parallel() {
    echo eos-ec-parallel-write-test $EOS_TEST_REDIRECTOR $1 >& $EOSLASTLOG; eval eos-ec-parallel-write-test $EOS_TEST_REDIRECTOR $1 >> $EOSCERTLOG 2>&1
}

eos_quota() {
    echo eos-quota-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >& $EOSLASTLOG; eval eos-quota-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >> $EOSCERTLOG 2>&1
}

# Succeeds only if a quota node is defined for exactly the given path
eos_quota_node_exists() {
    echo eos quota ls -m $1 >& $EOSLASTLOG; eval command eos -b $EOSROLE $EOSAPP root://$EOS_TEST_REDIRECTOR quota ls -m $1 2>> $EOSCERTLOG | grep "space=${1%/}" >> $EOSCERTLOG 2>&1
}

eos_defaultcc() {
    echo eos-defaultcc-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >& $EOSLASTLOG; eval eos-defaultcc-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >> $EOSCERTLOG 2>&1
}

eos_bash() {
    echo env EOS_FUSE_SECRET=${EOS_FUSE_SECRET} eos-bash $* >& $EOSLASTLOG; env EOS_FUSE_SECRET=${EOS_FUSE_SECRET} eos-bash $* >> $EOSCERTLOG 2>&1
}

eos_synctime() {
    echo eos-synctime-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >& $EOSLASTLOG; eval eos-synctime-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >> $EOSCERTLOG 2>&1
}

eos_rclone() {
    echo eos-rclone-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >& $EOSLASTLOG; eval eos-rclone-test $1 $2 root://$EOS_TEST_REDIRECTOR/$3 >> $EOSCERTLOG 2>&1
}

eoscp_rain() {
    echo eoscp-rain-test $1 root://$EOS_TEST_REDIRECTOR/$2 >& $EOSLASTLOG; eval eoscp-rain-test $1 root://$EOS_TEST_REDIRECTOR/$2 >> $EOSCERTLOG 2>&1
}

eos_io() {
   echo eos-io-test root://$EOS_TEST_REDIRECTOR/$1 $2 >& $EOSLASTLOG; eval eos-io-test root://$EOS_TEST_REDIRECTOR/$1 $2 >> $EOSCERTLOG 2>&1
}

eos_rename() {
   echo eos-rename-test $1 $EOS_TEST_REDIRECTOR; eval eos-rename-test $1 $EOS_TEST_REDIRECTOR >> $EOSCERTLOG 2>&1
}

eos_acl_concurrent() {
   echo eos-acl-concurrent $1; eval eos-acl-concurrent $1 >> $EOSCERTLOG 2>&1
}

eos_grpc() {
   echo eos-grpc-test $1 $2; eval eos-grpc-test $1 $2 >> $EOSCERTLOG 2>&1
}

eos_grpc_file() {
   echo eos-grpc-file-test $1 $2; eval eos-grpc-file-test $1 $2 >> $EOSCERTLOG 2>&1
}

eos_file_dual_protocol() {
   echo eos-file-dual-protocol-test $1; eval eos-file-dual-protocol-test $1 >> $EOSCERTLOG 2>&1
}

eos_token() {
   echo eos-token-test $1 ; eval eos-token-test $1 ${EOS_TEST_FULL_REDIRECTOR} >> $EOSCERTLOG 2>&1
}

eos_squash() {
   echo eos-squash-test $1 ; eval eos-squash-test $1 ${EOS_TEST_FULL_REDIRECTOR} >> $EOSCERTLOG 2>&1
}

eos_accounting() {
  echo eos-accounting-test "$1" ; eval eos-accounting-test "$1" ${EOS_TEST_REDIRECTOR} >> $EOSCERTLOG 2>&1
}

eos_file_cont_detached() {
  local QDB_CLUSTER QDB_PWDFILE
  QDB_CLUSTER=$(cat /etc/xrd.cf.mgm | grep "^mgmofs.qdbcluster" | awk '{print $2;}')
  QDB_PWDFILE=$(cat /etc/xrd.cf.mgm | grep "^mgmofs.qdbpassword_file" | awk '{print $2;}')
  echo eos-file-cont-detached-test "$1"; eval eos-file-cont-detached-test "$1" "$QDB_CLUSTER" "$QDB_PWDFILE" >> "$EOSCERTLOG" 2>&1
}

eos_lru() {
  echo eos-lru-test $1 ; eval eos-lru-test $1 ${EOS_TEST_REDIRECTOR} >> $EOSCERTLOG 2>&1
}

# FST-local SyntheticIO bench. The CLI exits 0 even when every transfer fails,
# so require a zero error count and a non-zero write and read total.
eos_mirage() {
  echo "eos -b -r 0 0 root://$EOS_TEST_REDIRECTOR bench --syntheticio --layout replica:1 10 1" >& "$EOSLASTLOG"
  local out rc
  out=$(command eos -b -r 0 0 "root://$EOS_TEST_REDIRECTOR" bench --syntheticio --layout replica:1 10 1 2>&1)
  rc=$?
  printf '%s\n' "$out" | tee -a "$EOSCERTLOG"
  if [ "$rc" -ne 0 ]; then
    return "$rc"
  fi
  printf '%s\n' "$out" | grep -q 'errors: 0$' || return 1
  printf '%s\n' "$out" | awk '
    /^write:/ { w = $2 + 0 }
    /^read:/  { r = $2 + 0 }
    END { if (w <= 0 || r <= 0) exit 1 }
  '
}

eos_tape_rest_xrootd_client() {
  (
    set -e -o pipefail
    local prefix=${1%/} origin="https://${EOS_TEST_FULL_REDIRECTOR}:${EOS_HTTPS_PORT}"
    local rooturl="root://${EOS_TEST_REDIRECTOR}" output value dir="" payload="" request="" created=0
    if [[ ! "$prefix" =~ ^/[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$ ]]; then
      echo "Invalid Tape REST test prefix"; exit 1
    fi
    case "$prefix/" in */../*|*/./*) echo "Invalid Tape REST test prefix"; exit 1;; esac

    tape_status() {
      # FD 3 keeps the CI console visible while detailed output goes to the log.
      printf '%s\n' "$1"
      printf '%s\n' "$1" >&3
    }

    # NsCmd omits tapeenabled on disk-only instances; validate monitoring first.
    tape_value() {
      awk -v key="$2" -v optional="${3:-0}" '
        { for (i=1; i<=NF; i++) if (index($i, key "=")==1) { value=substr($i,length(key)+2); count++ } }
        END { if (count>1 || (count && value=="") || (!count && !optional)) exit 1; print value }' <<< "$1"
    }
    output=$(command eos -b "$rooturl" ns stat -m)
    for value in ns.total.files ns.total.directories; do
      [[ $(tape_value "$output" "$value") =~ ^[0-9]+$ ]]
    done
    value=$(tape_value "$output" ns.tapeenabled 1)
    case "$value" in
      true) ;;
      false|"")
        if [[ ${EOS_TAPE_REST_REQUIRE:-0} == 1 ]]; then
          echo "Tape REST requires a tape-enabled MGM"; exit 1
        fi
        tape_status "SKIP: MGM reports tape disabled"; exit 0;;
      *) echo "Invalid ns.tapeenabled monitoring value"; exit 1;;
    esac
    output=$(command eos -b "$rooturl" space status default -m)
    [[ $(tape_value "$output" taperestapi.status) == on && $(tape_value "$output" taperestapi.stage) == on ]]

    # Reuse the HTTP fixture credentials only when no authentication was supplied.
    if [[ -z "${BEARER_TOKEN:-}${BEARER_TOKEN_FILE:-}${XRD_BEARERTOKEN:-}${XRD_BEARERTOKENFILE:-}${XRD_HTTPHEADERS:-}${X509_USER_PROXY:-}${X509_USER_CERT:-}${X509_USER_KEY:-}${XRD_HTTPCLIENTCERTFILE:-}${XRD_HTTPCLIENTKEYFILE:-}${XRD_HTTPDISABLEX509:-}" && ! -f /tmp/x509up_u$(id -u) ]]; then
      export XRD_HTTPCLIENTCERTFILE="$cert" XRD_HTTPCLIENTKEYFILE="$key"
    fi
    [[ -z ${X509_USER_CERT:-} || -n ${XRD_HTTPCLIENTCERTFILE:-} ]] || export XRD_HTTPCLIENTCERTFILE="$X509_USER_CERT"
    [[ -z ${X509_USER_KEY:-} || -n ${XRD_HTTPCLIENTKEYFILE:-} ]] || export XRD_HTTPCLIENTKEYFILE="$X509_USER_KEY"
    if [[ -z ${XRD_HTTPCERTFILE:-}${X509_CERT_FILE:-}${XRD_HTTPCERTDIR:-}${X509_CERT_DIR:-} ]]; then
      export XRD_HTTPCERTFILE="$cacert" X509_CERT_DIR="$capath"
    fi

    tape_xrdfs() {
      # eos-xrootd installs the HTTP client library without a plugin configuration.
      XRD_PLUGIN=libXrdClHttp.so xrdfs "$origin" "$@"
    }

    tape_xrdfs query opaque tape.discover | jq -e '
      .version == "v1" and (.uri | test("^https://[^/?#@]+/[^?#]*v1/?$"))
      and (.sitename | type == "string" and length > 0)'

    tape_cleanup() {
      local result=$? cleanup_output cleanup_result
      trap - EXIT
      if [[ -n "$request" ]]; then
        if cleanup_output=$(tape_xrdfs query opaque $'tape.stage_delete\n'"$request" 2>&1); then :; else
          cleanup_result=$?
          if [[ $cleanup_result != 54 || "$cleanup_output" != *"[3011]"* ]]; then
            printf '%s\n' "$cleanup_output"
            echo "Failed to clean up Tape REST request $request"; [[ $result != 0 ]] || result=1
          fi
        fi
      fi
      if (( created )) && ! command eos -b "$rooturl" rm -r "$dir"; then
        echo "Failed to clean up Tape REST directory $dir"; [[ $result != 0 ]] || result=1
      fi
      if [[ -n "$payload" ]] && ! rm -f "$payload"; then [[ $result != 0 ]] || result=1; fi
      if [[ $result == 0 ]]; then tape_status "PASS: WLCG Tape REST via xrdfs"; fi
      exit "$result"
    }
    trap tape_cleanup EXIT
    dir="$prefix/tape-rest-xrootd-$(cat /proc/sys/kernel/random/uuid)"
    local path="$dir/payload.dat" missing="$dir/missing.dat"
    payload=$(mktemp)
    printf 'EOS WLCG Tape REST client test\n' > "$payload"
    command eos -b "$rooturl" mkdir "$dir"
    created=1
    # HTTPS uses the fixture user; prepare requires an explicit ACL even for disk files.
    command eos -b "$rooturl" attr set sys.acl=z:p "$dir"
    xrdcp -np "$payload" "$rooturl/$path"

    tape_xrdfs query opaque $'tape.archiveinfo\n'"$origin$path"$'\n'"$origin$missing" |
      jq -e --arg path "$path" --arg missing "$missing" '
        length == 2 and ([.[] | select(.path == $path and .locality == "DISK" and (.error // "") == "")] | length == 1)
        and ([.[] | select(.path == $missing and .error == "USER ERROR: file does not exist or is not accessible to you")] | length == 1)'
    request=$(tape_xrdfs prepare -s "$path")
    [[ -n "$request" && "$request" != *$'\n'* && "$request" != *$'\r'* ]]
    tape_xrdfs query prepare "$request" |
      jq -e --arg request "$request" --arg path "$path" --arg workflow "No prepare workflow set on the directory $dir/" '
        .id == $request and (.files | length == 1)
        and .files[0].path == $path and .files[0].onDisk == true
        and ((.files[0].error // "") == "" or .files[0].error == $workflow)'
    tape_xrdfs prepare -e "$request" "$path"
    tape_xrdfs prepare -a "$request" "$path"
    tape_xrdfs query opaque $'tape.stage_delete\n'"$request"
    local result
    if output=$(tape_xrdfs query prepare "$request" 2>&1); then
      echo "Deleted Tape REST request is still present"; exit 1
    else
      result=$?
      printf '%s\n' "$output"
      [[ $result == 54 && "$output" == *"[3011]"* ]]
    fi
    request=""
  ) 3>&1 >> "$EOSCERTLOG" 2>&1
}

shell() {
    echo $* >& $EOSLASTLOG; eval $* >> $EOSCERTLOG 2>&1
}

file_is_deleted () {
    local fxid=$1
    local output
    output=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "fxid:${fxid}" 2>&1)
    local rc=$?
    if [ $rc -ne 0 ] || echo "$output" | grep -q "pending_deletion\|tombstone\|does not exist"; then
        return 0
    fi
    return 1
}
# Retry a condition until it holds or the given timeout expires. Any function
# of this script can be used as condition, it is re-evaluated once per second.
# Use this instead of a fixed "delay" whenever waiting for an asynchronous
# operation - a fixed sleep is either too short under load or wasted time.
# Usage: wait_for <timeout_sec> <condition> [args ...]
wait_for () {
    wait_until "$@"
}

# At-rest compression. file convert queues the FST worker directly; csize
# shows up in the namespace only after that rewrite commits.
make_compress_src() {
    mkdir -p "$EOS_TEST_TESTSYS" || return 1
    dd if=/dev/zero bs=1048576 count=1 2>/dev/null | tr $'\0' 'C' > "$EOS_TEST_TESTSYS/compress.src" || return 1
    [ "$(wc -c < "$EOS_TEST_TESTSYS/compress.src")" -eq 1048576 ]
}

compress_csize() {
    local path=$1
    local mode=$2
    download "$EOS_TEST_TESTSYS/compress.probe" "$path" || true
    local info
    info=$(command eos -j -b root://$EOS_TEST_REDIRECTOR fileinfo "$path" 2>>"$EOSCERTLOG") || return 1

    if [ "$mode" = set ]; then
        printf '%s\n' "$info" | jq -e '.csize > 0' >/dev/null
    else
        printf '%s\n' "$info" | jq -e 'has("csize") | not' >/dev/null
    fi
}
# WFE helpers. Async workflow entries are stored as
# /eos/<instance>/proc/workflow/<day>/<queue>/<workflow>/<when>:<fxid>:<event>
# Usage: wfe_trigger <dir> <workflow> <event> <n>  - event on <n> new files
#        wfe_count <queue> <workflow> <n>          - queue holds exactly <n>
#        wfe_rm <queue> <workflow>                 - remove the queue entries
wfe_entries () {
    command eos -b $EOSROLE $EOSAPP root://$EOS_TEST_REDIRECTOR find -f "/eos/$EOS_TEST_INSTANCE/proc/workflow/" 2>/dev/null | grep "/$1/$2/"
}

wfe_trigger () {
    local i
    for i in $(seq 1 $4); do
        command eos -b $EOSROLE $EOSAPP root://$EOS_TEST_REDIRECTOR touch "$1/$2.$i" &&
        command eos -b $EOSROLE $EOSAPP root://$EOS_TEST_REDIRECTOR file workflow "$1/$2.$i" $2 $3 || return 1
    done >> $EOSCERTLOG 2>&1
}

wfe_count () {
    [ "$(wfe_entries $1 $2 | wc -l)" -eq "$3" ]
}

wfe_rm () {
    local entries entry
    entries=$(wfe_entries $1 $2)
    [ -n "$entries" ] || return 1
    for entry in $entries; do
        command eos -b $EOSROLE $EOSAPP root://$EOS_TEST_REDIRECTOR rm "$entry" || return 1
    done >> $EOSCERTLOG 2>&1
}
