# bats file_tags=archive
# eos archive create writes the .archive.init file of a tree. Compared with a
# reference, except the timestamp, src and dst of the header. Needs a running
# eosarchived and /etc/eos.client.keytab. Setting: EOS_ARCHIVE_REF_INIT,
# default /var/eos/test/eos.archive.init.ref (the eos-test package).
load ../instance

ref_init=${EOS_ARCHIVE_REF_INIT:-/var/eos/test/eos.archive.init.ref}
# All times of the tree: 2026-06-14 00:00:00 UTC
epoch=$(date -u -d "2026-06-14 00:00:00" +%s)

# set_times <path>: ctime and mtime to $epoch, through the utimes fsctl
set_times() {
  local out
  out=$(xrdfs ${url#root://} query opaquefile \
    "$1?mgm.pcmd=utimes&eos.ruid=0&eos.rgid=0&tv1_sec=$epoch&tv1_nsec=0&tv2_sec=$epoch&tv2_nsec=0")
  [[ $out == *retc=0* ]] || { echo "utimes of $1: $out"; return 1; }
}

# without_header <file>: the file without the fields of the header that change
without_header() {
  sed -E '1s/"timestamp":[[:space:]]*[0-9]*/"timestamp": <IGNORED>/;1s/"(src|dst)":[[:space:]]*"[^"]*"/"\1": <IGNORED>/g' $1
}

@test "eos archive create writes the .archive.init file of the reference" {
  pgrep -f eosarchived.py >/dev/null || skip "no eosarchived running"
  [ -r /etc/eos.client.keytab ] || skip "no /etc/eos.client.keytab"
  [ -r $ref_init ] || skip "no reference $ref_init"
  export XrdSecPROTOCOL=sss,unix XrdSecSSSKT=/etc/eos.client.keytab
  local a=$testdir/archive uid gid pattern=$BATS_TEST_TMPDIR/pattern f
  uid=$(id -u daemon)
  gid=$(id -g daemon)

  eos_as 0 0 mkdir -p $a
  eos_as 0 0 chown daemon:daemon $a
  eos_as 0 0 attr set default=replica $a
  eos_as $uid $gid chmod 775 $a
  eos_as $uid $gid mkdir -p $a/dir1
  eos_as $uid $gid mkdir -p $a/dir2
  # 2048 bytes of a fixed pattern
  for i in $(seq 1 128); do printf 0123456789ABCDEF; done > $pattern
  for f in dir1/file11.dat dir1/file12.dat dir2/file21.dat dir2/file22.dat; do
    upload $pattern "$a/$f?eos.ruid=$uid&eos.rgid=$gid"
    set_times $a/$f
  done
  for f in $a/dir1 $a/dir2 $a; do
    set_times $f
    eos_as 0 0 attr set sys.eos.btime=$epoch.0 $f
  done

  eos_as 0 0 archive create $a
  xrdcp -f -s "$url/$a/.archive.init?eos.ruid=0&eos.rgid=0" $BATS_TEST_TMPDIR/init
  diff -u <(without_header $ref_init) <(without_header $BATS_TEST_TMPDIR/init)
}
