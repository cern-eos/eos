# bats file_tags=altxs
# Alternative checksums: computed on upload, or later by the scan and the sync
# of the FSTs, after a change of sys.altxs in the namespace.
load ../instance

setup_file() {
  export f16=$BATS_FILE_TMPDIR/16M f64=$BATS_FILE_TMPDIR/64M
  make_file $f16 16M
  make_file $f64 64M
  # Scan the disks often, and restore the interval at the end
  for_each_fs fs_set {} scan_disk_interval 5
}

teardown_file() {
  scan_off
  sync_off
}

setup() {
  eos_setup
  d=$testdir/altxs
  scan_off
  sync_off
}

# for_each_fs <command with {} for the fsid>
for_each_fs() {
  local fs
  for fs in $(fs_ids); do
    eval "${*//\{\}/$fs}"
  done
}

scan_on() { for_each_fs eos fs config {} scan_altxs_interval=5; }
scan_off() { for_each_fs eos fs config {} scan_altxs_interval=0; }
sync_on() { for_each_fs eos fs config {} altxs_sync=1 '&&' eos fs config {} altxs_sync_interval=5; }
sync_off() { for_each_fs eos fs config {} altxs_sync=0 '&&' eos fs config {} altxs_sync_interval=0; }

# upload_altxs on|off: compute the alternative checksums on upload, in each space
upload_altxs() {
  local s
  for s in $(json '.result[].name' space ls); do
    space_set $s altxs $1
  done
}

# has_altxs <eos file> <local file> "<types>": the file has exactly these
# alternative checksums, with the values of the local file
has_altxs() {
  local xs t want
  read -ra want <<< "$3"
  xs=$(json .altchecksums file info $1)
  [ "$(jq length <<< "$xs")" = ${#want[@]} ] || { echo "has: $xs, want: $3"; return 1; }
  for t in "${want[@]}"; do
    [ "$(jq -r ".[] | select(.type == \"$t\") | .value" <<< "$xs")" = "$(local_checksum $t $2)" ] ||
      { echo "$t differs: $xs"; return 1; }
  done
}

@test "with altxs on, the upload computes the checksums of sys.altxs" {
  eos mkdir -p $d/enabled
  eos attr set sys.altxs=md5,sha1 $d/enabled
  upload_altxs on
  upload $f16 $d/enabled/altxs.dat
  # The FST commits the checksums after the close
  wait_until 30 has_altxs $d/enabled/altxs.dat $f16 "md5 sha"
}

@test "scan and sync compute the checksums of a later sys.altxs" {
  eos mkdir -p $d/disabled
  upload $f16 $d/disabled/altxs.dat
  eos attr set sys.altxs=md5,sha1,sha256 $d/disabled
  sync_on
  scan_on
  wait_until 300 has_altxs $d/disabled/altxs.dat $f16 "md5 sha sha256"
}

@test "an overwrite updates the checksums" {
  eos mkdir -p $d/updated
  eos attr set sys.altxs=sha1,sha256 $d/updated
  upload_altxs on
  upload $f16 $d/updated/altxs.dat
  upload $f64 $d/updated/altxs.dat
  wait_until 30 has_altxs $d/updated/altxs.dat $f64 "sha sha256"
}

@test "file verify -commitchecksum computes and commits the checksums" {
  eos mkdir -p $d/triggered
  upload_altxs on
  upload $f16 $d/triggered/altxs.dat
  eos attr set sys.altxs=md5,sha1,sha256 $d/triggered
  eos file verify $d/triggered/altxs.dat -checksum -commitchecksum
  wait_until 300 has_altxs $d/triggered/altxs.dat $f16 "md5 sha sha256"
}

@test "types added to sys.altxs are computed by the scan and the sync" {
  eos mkdir -p $d/added
  eos attr set sys.altxs=md5 $d/added
  upload_altxs on
  upload $f16 $d/added/altxs.dat
  eos attr set sys.altxs=md5,sha1,sha256 $d/added
  sync_on
  scan_on
  wait_until 300 has_altxs $d/added/altxs.dat $f16 "md5 sha sha256"
}

@test "types removed from sys.altxs are removed from the namespace" {
  eos mkdir -p $d/removed
  eos attr set sys.altxs=md5,sha1 $d/removed
  upload_altxs on
  upload $f16 $d/removed/altxs.dat
  eos attr -r set sys.altxs=sha1 $d/removed
  sync_on
  scan_on
  wait_until 300 has_altxs $d/removed/altxs.dat $f16 sha
}

@test "with altxs off, the upload computes no checksums" {
  eos mkdir -p $d/disabled_on_upload
  eos attr set sys.altxs=md5 $d/disabled_on_upload
  upload_altxs off
  upload $f16 $d/disabled_on_upload/altxs.dat
  has_altxs $d/disabled_on_upload/altxs.dat $f16 ""
}
