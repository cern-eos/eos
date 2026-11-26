# bats file_tags=rain_pio
# RAIN parallel IO (PIO) writes of the XrdCl RAIN plugin (libEosRainClient.so).
# The plugin asks the MGM for the stripe locations, writes the data and parity
# stripes directly to the FSTs and commits the file to the MGM by itself.
# Obfuscated files and non-RAIN layouts fall back to a gateway access.
#
# Settings: EOS_RAIN_PLUGIN (default /usr/lib64/libEosRainClient.so),
# EOS_PIO_UNTRUSTED_HOST (default the FQDN of this host): the MGM host for the
# checks of a client that is not trusted. It needs 6 file systems.
load ../instance

plugin=${EOS_RAIN_PLUGIN:-/usr/lib64/libEosRainClient.so}
untrusted=${EOS_PIO_UNTRUSTED_HOST:-$(hostname -f)}
nstripes=6
# Empty, below one block, one block, just above one raid6 group (4 data blocks
# of 1M) and above one raiddp group (16 data blocks of 1M)
sizes="0 1 4095 1048576 4194321 21000000"

setup_file() {
  [ -f $plugin ] || skip "no RAIN plugin $plugin"
  [ "$(fs_ids | wc -l)" -ge $nstripes ] || skip "needs $nstripes file systems"
  export ref=$BATS_FILE_TMPDIR d=$testdir/rain_pio
  for sz in $sizes; do
    head -c $sz /dev/urandom > $ref/ref_$sz.dat
  done
  head -c 3000000 /dev/urandom > $ref/other.dat
  export big=$ref/ref_21000000.dat other=$ref/other.dat
}

# with_plugin <command>: run the command with the plugin. $BATS_TEST_TMPDIR/plugin.log
# gets what XrdCl and the plugin log at info level: printed after a failure.
with_plugin() {
  rm -f $BATS_TEST_TMPDIR/plugin.log
  XRD_PLUGIN=$plugin XRD_LOGLEVEL=Info XRD_LOGFILE=$BATS_TEST_TMPDIR/plugin.log "$@"
}

teardown() {
  [ -n "${BATS_TEST_COMPLETED-}" ] || cat $BATS_TEST_TMPDIR/plugin.log 2>/dev/null || true
  eos_teardown
}

log_has_line() {
  grep -q "$1" $BATS_TEST_TMPDIR/plugin.log
}

# pio_write_done: the last plugin run wrote in PIO mode, without a fall back
pio_write_done() {
  log_has_line 'msg="PIO commit done"' || { echo "no PIO commit"; return 1; }
  if log_has_line 'fall back to gateway write'; then echo "fell back to a gateway write"; return 1; fi
}

# fell_back <read|write>: the last plugin run fell back to a gateway access
fell_back() {
  log_has_line "fall back to gateway $1" || { echo "no fall back to a gateway $1"; return 1; }
}

# check_file <local> <path> [<stripes>|-]: size, checksum and stripes at the
# MGM, and the same bytes in a gateway read and in a PIO read
check_file() {
  [ "$(json .size fileinfo $2)" = "$(stat -c %s $1)" ]
  [ "$(json .checksumvalue fileinfo $2)" = "$(local_checksum adler $1)" ]
  [ "${3:-$nstripes}" = - ] || [ "$(json '.locations | length' fileinfo $2)" = "${3:-$nstripes}" ]
  xrdcp -f -s $url/$2 $BATS_TEST_TMPDIR/read.dat
  cmp $1 $BATS_TEST_TMPDIR/read.dat
  with_plugin xrdcp -f -s $url/$2 $BATS_TEST_TMPDIR/read.dat
  cmp $1 $BATS_TEST_TMPDIR/read.dat
}

# check_obfuscated <local> <path>: obfuscated, and the same bytes in a gateway
# read and in a plugin read, which falls back to a gateway read
check_obfuscated() {
  xrdcp -f -s $url/$2 $BATS_TEST_TMPDIR/read.dat
  cmp $1 $BATS_TEST_TMPDIR/read.dat
  with_plugin xrdcp -f -s $url/$2 $BATS_TEST_TMPDIR/read.dat
  cmp $1 $BATS_TEST_TMPDIR/read.dat
  fell_back read
  eos file info $2 | grep -Eq 'Crypt: (obfuscated|encrypted)'
}

# make_dir <dir> <layout> <stripes>: a directory with this layout and adler
make_dir() {
  eos mkdir -p $1
  eos chmod 777 $1
  eos attr set sys.forced.checksum=adler $1
  eos attr set sys.forced.layout=$2 $1
  eos attr set sys.forced.nstripes=$3 $1
  [ $2 = replica ] && return 0
  eos attr set sys.forced.blockchecksum=crc32c $1
  eos attr set sys.forced.blocksize=1M $1
}

# ------------------------------------------------------------------------------
# The tests of one RAIN layout, for raid6 and raiddp
# ------------------------------------------------------------------------------
sizes_test() {
  make_dir $d/$1 $1 $nstripes
  for sz in $sizes; do
    with_plugin xrdcp -s $ref/ref_$sz.dat $url/$d/$1/file_$sz.dat
    pio_write_done
    check_file $ref/ref_$sz.dat $d/$1/file_$sz.dat
  done
}

cksum_test() {
  with_plugin xrdcp -s --cksum adler32 $big $url/$d/$1/file_cksum.dat
  pio_write_done
  check_file $big $d/$1/file_cksum.dat
}

overwrite_test() {
  local f=$d/$1/file_1048576.dat
  with_plugin xrdcp -f -s $other $url/$f
  pio_write_done
  check_file $other $f
  run ! with_plugin xrdcp -s $big $url/$f
  check_file $other $f
}

preserve_test() {
  local f=$d/$1/file_preserve.dat
  with_plugin xrdcp -s $other $url/$f
  eos attr set user.preserve=kept $f
  with_plugin xrdcp -f -s $big "$url/$f?eos.atomic=1&eos.xattr.preserve=1"
  pio_write_done
  check_file $big $f
  [ "$(attr_value $f user.preserve)" = kept ]
  none eval "eos ls -a $d/$1 | grep -F '.sys.a#.'"
}

# No FST applies the key to the stripes that a PIO client accesses, so the
# plugin falls back to a gateway access. The MGM refuses the PIO overwrite
# before it makes a version: one version stays.
obfuscated_test() {
  local o=$d/$1/obfuscated v=$d/$1/obfuscated_versioned p=$d/$1/obfuscating f
  make_dir $o $1 $nstripes
  make_dir $v $1 $nstripes
  eos attr set sys.versioning=5 $v
  for f in $o/file.dat $v/file.dat; do
    xrdcp -s $ref/ref_4194321.dat "$url/$f?eos.obfuscate=1"
    check_obfuscated $ref/ref_4194321.dat $f
    with_plugin xrdcp -f -s $other $url/$f
    fell_back write
    check_obfuscated $other $f
  done
  [ "$(eos ls "$v/.sys.v#.file.dat" | wc -l)" = 1 ]

  make_dir $p $1 $nstripes
  eos attr set sys.file.obfuscate=1 $p
  with_plugin xrdcp -s $other $url/$p/file.dat
  fell_back write
  check_obfuscated $other $p/file.dat
}

nonstreaming_test() {
  local f=$d/$1/file_nonstreaming.dat
  with_plugin xrdcpnonstreaming $big $url/$f
  pio_write_done
  log_has_line 'msg="rescanned checksum"'
  check_file $big $f
}

# The plugin registers the stripes in their logical order: the first two
# locations hold data, and a read without them needs the parity
parity_test() {
  local drop f idx
  for drop in "0 1" "2 3"; do
    f=$d/$1/file_parity_${drop// /}.dat
    with_plugin xrdcp -s $big $url/$f
    pio_write_done
    for idx in $drop; do
      eos file drop $f $(json ".locations[$idx].fsid" fileinfo $f) -f
    done
    # Not the number of stripes: fsck can repair them already
    check_file $big $f -
  done
}

for layout in raid6 raiddp; do
  bats_test_function --description "$layout: PIO writes of files of all sizes" -- sizes_test $layout
  bats_test_function --description "$layout: xrdcp --cksum verifies the committed checksum" -- cksum_test $layout
  bats_test_function --description "$layout: overwrite works, an exclusive create of a file fails" -- overwrite_test $layout
  bats_test_function --description "$layout: an atomic overwrite keeps the xattrs" -- preserve_test $layout
  bats_test_function --description "$layout: obfuscated files fall back to a gateway access" -- obfuscated_test $layout
  bats_test_function --description "$layout: non-sequential writes give the right checksum" -- nonstreaming_test $layout
  bats_test_function --description "$layout: a read without two stripes uses the parity" -- parity_test $layout
done

# ------------------------------------------------------------------------------
@test "replica: a PIO write falls back to a gateway write" {
  make_dir $d/replica replica 2
  with_plugin xrdcp -s $other $url/$d/replica/file_fallback.dat
  fell_back write
  run ! log_has_line 'msg="PIO commit done"'
  check_file $other $d/replica/file_fallback.dat 2
}

# what_left <path>: size and number of locations, to see that a refused
# request did not change the file
what_left() {
  echo "$(json .size fileinfo $1) $(json '.locations | length' fileinfo $1)"
}

@test "a trusted client needs a PIO write capability too" {
  local f=$d/raid6/file_21000000.dat before
  before=$(what_left $f)
  run ! xrdfs ${url#root://} query opaquefile "/?mgm.pcmd=commit&mgm.pio.commit=1&mgm.size=1&mgm.mtime=1&mgm.mtime_ns=0"
  run ! xrdfs ${url#root://} query opaquefile "/?mgm.pcmd=drop&mgm.pio.drop=1"
  [ "$(what_left $f)" = "$before" ]
}

@test "a client that is not trusted needs a PIO write capability" {
  local f=$d/raid6/file_21000000.dat before fxid fsid who
  # unix authentication through the FQDN: the MGM sees neither sss nor localhost
  who=$(XrdSecPROTOCOL=unix command eos root://$untrusted whoami 2>&1 || true)
  [[ $who == *authz:unix* && $who != *host=localhost* ]] ||
    skip "no access to the MGM as a unix client that is not trusted, through $untrusted: $who"
  before=$(what_left $f)
  fxid=$(json .fxid fileinfo $f)
  fsid=$(json '.locations[0].fsid' fileinfo $f)
  export XrdSecPROTOCOL=unix
  run ! xrdfs $untrusted query opaquefile "/?mgm.pcmd=commit&mgm.path=$f&mgm.fid=$fxid&mgm.add.fsid=$fsid&mgm.size=1&mgm.commit.size=1&mgm.mtime=1&mgm.mtime_ns=0"
  run ! xrdfs $untrusted query opaquefile "/?mgm.pcmd=drop&mgm.fid=$fxid&mgm.fsid=0&mgm.dropall=1"
  run ! xrdfs $untrusted query opaquefile "/?mgm.pcmd=commit&mgm.pio.commit=1&mgm.size=1&mgm.mtime=1&mgm.mtime_ns=0"
  run ! xrdfs $untrusted query opaquefile "/?mgm.pcmd=drop&mgm.pio.drop=1"
  unset XrdSecPROTOCOL
  [ "$(what_left $f)" = "$before" ]
}
