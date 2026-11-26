#!/usr/bin/env bats
# bats file_tags=load
# ------------------------------------------------------------------------------
# Contention test, the load: 2 workers of each kind for $CONTENTION_DURATION s,
# while one file system is drained and a probe asks the MGM every 2 s if it
# answers. 3-end-checks.bats checks the results.
#
# How to add a load operation:
#   load_op <kind> <name> <command>
#     2 workers of each kind repeat all lines of their kind, in order, until
#     the end of the load. A new kind needs only its lines.
#
# The commands are shell code in single quotes. They see the settings of
# contention.bash ($url, $base, $src ...) and:
#   $w $i         the worker number and the step number
#   $d $next      the shared directory of the step and the next one
#   $own          a new name for each step: $d/<kind><w>_<i>
#   $old          the $own of 8 steps ago (the line waits for step 8)
#   $mown         $own through the eosxd mount
#   $wfile        a file of the write and big workers in $d (the line is
#                 skipped if there is none)
#   $fsid $fsid2  two file system ids
#
# An operation of a worker on its own entries must succeed. Operations named
# other_* use entries of other workers, and race_* entries that several
# workers share: these can fail, but they must not hang. At the first hang
# the test saves the MGM thread stacks in the log directory.
# ------------------------------------------------------------------------------

load contention

# The load, the drain and the last operations (60 s at most)
BATS_TEST_TIMEOUT=$((duration + 300))

# Files that the other workers use as $wfile, read back and removed 8 steps
# later. A conversion can move the replicas under the read.
#       kind    operation            command
load_op write   write                'xrdcp -f -s $src $url/$own'
load_op write   other_read           'xrdcp -f -s $url/$old /dev/null'
load_op write   rm                   'eos rm -F $old'
load_op big     write                'xrdcp -f -s $srcbig $url/$own'
load_op big     other_read           'xrdcp -f -s $url/$old /dev/null'
load_op big     rm                   'eos rm -F $old'

# Uploads that stop without a close: xrdcpabort kills itself (rc 137) after
# its write, and the MGM must drop the file (rm finds none, rc 2)
load_op abort   abort                'xrdcpabort $url/$own; [ $? = 137 ]'
load_op abort   abort_atomic         'xrdcpabort "$url/$own.a?eos.atomic=1"; [ $? = 137 ]'
load_op abort   abort_gone           'eos rm -F $own; [ $? = 2 ]'

# A file, and a directory with a file in it, move from one shared directory
# to the next
load_op rename  write                '[ $i != 0 ] || xrdcp -s $src $url/$d/n$w'
load_op rename  mkdir                '[ $i != 0 ] || eos mkdir $d/nd$w'
load_op rename  write                '[ $i != 0 ] || xrdcp -s $src $url/$d/nd$w/f'
load_op rename  rename               'xrdfs $url mv $d/n$w $next/n$w'
load_op rename  rename_dir           'xrdfs $url mv $d/nd$w $next/nd$w'

load_op dir     mkdir                'xrdfs $url mkdir $own'
load_op dir     rmdir                'xrdfs $url rmdir $own'
load_op dir     symlink              'eos ln $own.l $d'
load_op dir     rm_link              'eos rm -F $own.l'

load_op meta    attr_dir             'eos attr set user.contention=$i $d'
load_op meta    chmod_dir            'eos chmod 777 $d'
load_op meta    chown_dir            'eos chown adm:adm $d'
load_op meta    other_attr           'eos attr set user.contention=$i $wfile'
load_op meta    other_chmod          'xrdfs $url chmod $wfile rw-r--r--'
load_op meta    other_chown          'eos chown adm:adm $wfile'

# Lookups through xrootd, the console, the proc calls of the FUSE clients
# and the REST gateway
load_op read    stat                 'xrdfs $url stat $d'
load_op read    ls                   'eos ls -l $d'
load_op read    fileinfo_dir         'eos -j fileinfo $d'
load_op read    other_fileinfo       'eos -j fileinfo $wfile'
load_op read    fuse_ls              'xrdcp -f -s "$url//proc/user/?mgm.cmd=fuse&mgm.statentries=1&mgm.path=$d" /dev/null'
load_op read    fusex_get            'xrdcp -f -s "$url//proc/user/?mgm.cmd=fuseX&mgm.op=GET&mgm.path=$d" /dev/null'
load_op read    other_fusex_get      'xrdcp -f -s "$url//proc/user/?mgm.cmd=fuseX&mgm.op=GET&mgm.path=$wfile" /dev/null'
# Opens by file id and by inode, and the FUSE caps listing
load_op read    other_open_fxid      'xrdcp -f -s "$url//.fxid:$(md_field $wfile fxid)" /dev/null'
load_op read    other_open_ino       'xrdcp -f -s "$url//eos/x?eos.lfn=ino:$(printf %x $(md_field $wfile ino))" /dev/null'
load_op read    caps_ls              'eos fusex caps -p'
load_op rest    rest_ls              'rest_call ls_cmd $d'
load_op rest    other_rest_fileinfo  'rest_call fileinfo_cmd $wfile'

# Walks over the file system lists and the test tree. Not fs dropdeletion:
# it leaves the records of the dropped files, and an end check counts them.
load_op walk    fs_status            'eos fs status $fsid'
load_op walk    fs_dumpmd            'eos fs dumpmd $fsid --path'
load_op walk    fs_compare           'eos fs compare $fsid $fsid2'
load_op walk    fs_dropghosts        'eos fs dropghosts $fsid'
load_op walk    fsck_report          'eos fsck report'
load_op walk    find                 'eos find --count $base'
load_op walk    find_fs              'eos find -f --fid --fs --checksum $base'
load_op walk    oldfind              'eos oldfind -f $base'
load_op walk    quota_ls             'eos quota ls -m -p $base'
# Prints the tracked entries: the scan runs also with the tracker off
load_op walk    tracker_scan         'eos space tracker'

# Write, rm, rename, mkdir and rmdir of the same 3 names
load_op clash   race_write           'xrdcp -f -s $src $url/$base/d0/c$((RANDOM % 3))'
load_op clash   race_rm              'eos rm -F $base/d0/c$((RANDOM % 3))'
load_op clash   race_rename          'xrdfs $url mv $base/d0/c$((RANDOM % 3)) $base/d1/c$((RANDOM % 3))'
load_op clash   race_mkdir           'xrdfs $url mkdir $base/d2/c$((RANDOM % 3))'
load_op clash   race_rmdir           'xrdfs $url rmdir $base/d2/c$((RANDOM % 3))'
load_op clash   race_attr            'eos attr set user.contention=$i $base/d1/c$((RANDOM % 3))'

# The cache drops make the next lookup read QuarkDB again
load_op replica other_drop           'drop_replica $wfile'
load_op replica other_adjustreplica  'eos file adjustreplica $wfile'
load_op replica other_verify         'eos file verify $wfile -checksum -commitchecksum -commitsize'
load_op replica other_convert        'eos convert file $wfile replica:2'
load_op replica other_fsck_repair    'eos fsck repair --fxid $(md_field $wfile fxid)'
load_op replica race_cache_file      'eos ns cache drop-single-file $(md_field $wfile fid)'
load_op replica race_cache_dir       'eos ns cache drop-single-container $(md_field $d fid)'

load_op version version              'xrdcp -f -s $src $url/$base/v/f$w'
load_op version other_versions       'eos file versions $base/v/f$w'
load_op version other_purge          'eos file purge $base/v/f$w 1'

# Clone a directory, overwrite and rm its file, then clean up the clone. Two
# clones in one second fail: the clone id is the time.
load_op clone   race_clone           'eos mkdir $own && xrdcp -s $src $url/$own/f && sleep 1 && id=$(eos oldfind -x sys.clone=+10 $own | head -1 | cut -d" " -f2 | cut -d: -f2) && xrdcp -f -s $src $url/$own/f && xrdfs $url rm $own/f; [ -z "$id" ] || eos oldfind -x sys.clone=-$id $own >/dev/null; eos rm -r -F $own'

load_op admin   write                'xrdcp -f -s $src $url/$own'
load_op admin   touch                'eos file touch $own.t'
load_op admin   register             'eos register $own.n size=0'
load_op admin   symlink              'eos file symlink $own.l $own'
load_op admin   utimes               'xrdfs $url query opaquefile "$own?mgm.pcmd=utimes&tv1_sec=$i&tv1_nsec=0&tv2_sec=$i&tv2_nsec=0"'
load_op admin   checksum             'xrdfs $url query checksum $own'
load_op admin   verify               'eos file verify $own -checksum -commitchecksum -commitsize'
load_op admin   layout               'eos file layout $own -stripes 1'
load_op admin   adjustreplica        'eos file adjustreplica $own'
load_op admin   layout               'eos file layout $own -stripes 2'
load_op admin   other_convert        'eos convert file $own replica:2'
load_op admin   file_rename          'eos file rename $own $own.r'
load_op admin   rm                   'eos rm -F $own.l'
load_op admin   rm                   'eos rm -F $own.r'
load_op admin   rm                   'eos rm -F $own.t'
load_op admin   rm                   'eos rm -F $own.n'

load_op tree    mkdir                'eos mkdir -p $own/a/b'
load_op tree    write                'for x in f a/f a/b/f; do xrdcp -s $src $url/$own/$x || exit; done'
load_op tree    chmod_r              'eos chmod -r 777 $own'
load_op tree    chown_r              'eos chown -r adm:adm $own'
load_op tree    attr_r               'eos attr -r set user.tree=$i $own'
load_op tree    attr_link            'eos attr link $next $own'
load_op tree    find_tree            'eos find $own'
load_op tree    rm_r                 'eos rm -r -F $own'

# A tree goes to the recycle bin, comes back, goes again and is purged
load_op recycle mkdir                'eos mkdir -p $base/r/$w.$i/a'
load_op recycle write                'xrdcp -s $src $url/$base/r/$w.$i/a/f'
load_op recycle rm_recycle           'eos rm -r $base/r/$w.$i'
load_op recycle recycle_restore      'recycle_as_owner "restore -f" $base/r/$w.$i'
load_op recycle rm_recycle           'eos rm -r $base/r/$w.$i'
load_op recycle recycle_purge        'recycle_as_owner "purge -k" $base/r/$w.$i'

load_op grpc    grpc_mkdir           '$grpc -p $own mkdir'
load_op grpc    grpc_touch           '$grpc -p $own/f touch'
load_op grpc    grpc_xattr           '$grpc --xattr user.grpc=$i -p $own/f setxattr'
load_op grpc    grpc_chmod           '$grpc --mode 644 -p $own/f chmod'
# The gRPC client is nobody: the MGM looks up the file, then refuses the chown
load_op grpc    grpc_chown_eperm     '$grpc --owner-uid 3 --owner-gid 4 -p $own/f chown 2>&1 | grep -q "Operation not permitted"'
load_op grpc    grpc_acl             '$grpc --acl u:3=rwx -p $own acl'
load_op grpc    grpc_rename          '$grpc --target $own/f2 -p $own/f rename'
load_op grpc    grpc_symlink         '$grpc --target $own/f2 -p $own/l symlink'
load_op grpc    grpc_version         '$grpc --max-version 2 -p $own/f2 create-version'
load_op grpc    grpc_version         '$grpc -p $own/f2 list-version'
load_op grpc    grpc_version         '$grpc --max-version 1 -p $own/f2 purge-version'
load_op grpc    grpc_md              'eos-grpc-md --endpoint $grpc_ep -l $own'
load_op grpc    grpc_md_big          'eos-grpc-md --endpoint $grpc_ep -l $base/big'
load_op grpc    grpc_find            'eos-grpc-find --endpoint $grpc_ep $d'
load_op grpc    grpc_rm              '$grpc -p $own/l rm'
# The gRPC rmdir has no recursive mode (-r is not sent): it must refuse $own,
# which is not empty
load_op grpc    grpc_rmdir_notempty  '$grpc -p $own rmdir 2>&1 | grep -q "Directory not empty"'
load_op grpc    grpc_mkdir           '$grpc -p $own/e mkdir'
load_op grpc    grpc_rmdir           '$grpc -p $own/e rmdir'
load_op grpc    rm_r                 'eos rm -r -F $own'

# All hot workers change one file and its directory at once. The file can
# be renamed away or dropped meanwhile, so all operations are races. The
# writes create the file again.
load_op hot     race_hot_write       'xrdcp -f -s $src $url/$hot'
load_op hot     race_hot_append      'xrdcpappend $url/$hot'
load_op hot     race_hot_read        'xrdcp -f -s $url/$hot /dev/null'
load_op hot     race_hot_pgread      'xrdcppgread $url/$hot 0 1024'
load_op hot     race_hot_attr        'eos attr set user.hot=$i $hot'
load_op hot     race_hot_chmod       'eos chmod 640 $hot'
load_op hot     race_hot_utimes      'xrdfs $url query opaquefile "$hot?mgm.pcmd=utimes&tv1_sec=$i&tv1_nsec=0&tv2_sec=$i&tv2_nsec=0"'
load_op hot     race_hot_verify      'eos file verify $hot -checksum -commitchecksum -commitsize'
load_op hot     race_hot_drop        'drop_replica $hot'
load_op hot     race_hot_adjustreplica 'eos file adjustreplica $hot'
load_op hot     race_hot_rename      'xrdfs $url mv $hot $hot.$w && xrdfs $url mv $hot.$w $hot'
load_op hot     race_hot_convert     'eos convert file $hot replica:2'
load_op hot     race_hot_dir_attr    'eos attr set user.hot=$i $base/hot'
load_op hot     race_hot_stat        'xrdfs $url stat $hot'
load_op hot     race_hot_lock        'eos file touch -l $hot 60 && eos file touch -u $hot'

# One directory grows by 16 entries and shrinks again while it is listed. Each
# touch starts a console, so more entries do not end within the 60 s timeout
load_op bigdir  bigdir_touch         'for k in $(seq 1 16); do eos file touch -n $base/big/$w.$i.$k || exit; done'
load_op bigdir  bigdir_ls            'eos ls $base/big'
load_op bigdir  bigdir_fuse_ls       'xrdcp -f -s "$url//proc/user/?mgm.cmd=fuse&mgm.statentries=1&mgm.path=$base/big" /dev/null'
load_op bigdir  bigdir_rm            'eos rm -F "$base/big/$w.$i.*"'

# Atomic uploads of two versions of one file. Each read must get one of
# them whole.
load_op atomic  race_atomic_write    'xrdcp -f -s $src "$url/$base/at/a?eos.atomic=1"'
load_op atomic  race_atomic_read     'xrdcp -f -s $url/$base/at/a $logs/at.$w && { cmp -s $logs/at.$w $src || cmp -s $logs/at.$w $srcb || echo "step $i of worker $w" >> $logs/atomic.bad; }'
load_op atomic  race_atomic_write    'xrdcp -f -s $srcb "$url/$base/at/a?eos.atomic=1"'
load_op atomic  race_atomic_read     'xrdcp -f -s $url/$base/at/a $logs/at.$w && { cmp -s $logs/at.$w $src || cmp -s $logs/at.$w $srcb || echo "step $i of worker $w" >> $logs/atomic.bad; }'

# Each append adds 4096 bytes: the sizes that the FSTs commit must add up
load_op append  write                '[ $i != 0 ] || xrdcp -s $empty $url/$base/ap/a$w'
load_op append  append               'xrdcpappend $url/$base/ap/a$w && echo $base/ap/a$w >> $logs/appends'

# xrdstress: 8 threads write and read 64 files
load_op storm   mkdir                'eos mkdir $own'
load_op storm   race_storm           'xrdstress -d $url/$own/ -o rdwr -j 8 -f 8 -s 4KB -c'
load_op storm   rm_r                 'eos rm -r -F $own'

# POSIX calls through the eosxd mount. The FUSE server uses MD locks, as the
# other interfaces do.
load_op mount   mount_mkdir          'mkdir $mown'
load_op mount   mount_write          'cp $src $mown/f'
load_op mount   mount_append         'cat $src >> $mown/f'
load_op mount   mount_read           'cat $mown/f'
load_op mount   mount_truncate       'truncate -s 100 $mown/f'
load_op mount   mount_rename         'mv $mown/f $mown/g'
load_op mount   mount_hardlink       'ln $mown/g $mown/h'
load_op mount   mount_symlink        'ln -s g $mown/s'
load_op mount   mount_chmod          'chmod 600 $mown/g'
load_op mount   mount_xattr          'setfattr -n user.mount -v $i $mown/g'
load_op mount   mount_ls             'ls -la $mown'
load_op mount   other_rm_hardlink    'eos rm -F $own/h'
load_op mount   mount_unlink         'rm -f $mown/s $mown/g $mown/h'
load_op mount   mount_rmdir          'rmdir $mown'

@test "load: 2 workers of each kind" {
  (set +e; run_load)
}
