fs
--

.. code-block:: text

  fs add|boot|config|dropdeletion|dropghosts|dropfiles|dumpmd|ls|mv|rm|status [OPTIONS]
    Options:
    fs add [-m|--manual <fsid>] <uuid> <node-queue>|<host>[:<port>] <mountpoint> [<space>] [<status>]
    add and assign a filesystem based on the unique identifier of the disk <uuid>
    -m|--manual  : add with user specified <fsid> and <space>
    <fsid>       : numeric filesystem id 1...65535
    <uuid>       : unique string identifying current filesystem
    <node-queue> : internal EOS identifier for a node e.g /eos/<host>:<port>/fst
    it is preferable to use the host:port syntax
    <host>       : FQDN of host where filesystem is mounter
    <port>       : FST XRootD port number [usually 1095]
    <mountponit> : local path of the mounted filesystem e.g /data/
    <space>      : space in which to insert the filesystem, if nothing is
    specified then space "default" is used
    <status>     : set filesystem status after insertion e.g off|rw|ro etc.
.. code-block:: text

    fs boot <fsid>|<uuid>|<node-queue>|* [--syncmgm]
    boot - filesystem identified by <fsid> or <uuid>
    - all filesystems on a node identified by <node-queue>
    - all filesystems registered
    --syncmgm    : for MGM resynchronization during the booting
    fs clone <sourceid> <targetid>
    replicate files from the source to the target filesystem
    <sourceid>   : id of the source filesystem
    <targetid>   : id of the target filesystem
  
    fs compare <sourceid> <targetid>
    compares and reports which files are present on one filesystem and not on the other
    <sourceid>   : id of the source filesystem
    <targetid>   : id of the target filesystem
  
    fs config <fsid> <key>=<value>
    configure the filesystem parameter, where <key> and <value> can be:
    configstatus=rw|wo|ro|drain|draindead|off|empty [--comment "<comment>"]
    rw        : set filesystem in read-write mode
    wo        : set filesystem in write-only mode
    ro        : set filesystem in read-only mode
    drain     : set filesystem in drain mode
    draindead : set filesystem in draindead mode, unusable for any read
    off       : disable filesystem
    empty     : empty filesystem, possible only if there are no
    more files stored on it
    --comment : pass a reason for the status change
    headroom=<size>
    headroom to keep per filesystem. <size> can be (>0)[BMGT]
    scaninterval=<seconds>
    entry rescan interval (default 7 days), 0 disables scanning
    scanrate=<MB/s>
    maximum IO scan rate per filesystem
    scan_disk_interval=<seconds>
    disk consistency thread scan interval (default 4h)
    scan_ns_interval=<seconds>
    namespace consistency thread scan interval (default 3 days)
    scan_ns_rate=<entries/s>
    maximum scan rate of ns entries for the NS consistency. This
    is bound by the maxium number of IOPS per disk.
    graceperiod=<seconds>
    grace period before a filesystem with an operation error gets
    automatically drained
    drainperiod=<seconds>
    period a drain job is allowed to finish the drain procedure
    proxygroup=<proxy_grp_name>
    schedule a proxy for the current filesystem by taking it from
    the given proxy group. The special value "<none>" is the
    same as no value and means no proxy scheduling
    filestickyproxydepth=<depth>
    depth of the subtree to be considered for file-stickyness. A
    negative value means no file-stickyness
    forcegeotag=<geotag>
    set the filesystem's geotag, overriding the host geotag value.
    The special value "<none>" is the same as no value and means
    no override
    s3credentials=<accesskey>:<secretkey>
    the access and secret key pair used to authenticate
    with the S3 storage endpoint
    fs dropdeletion <fsid>
    drop all pending deletions on the filesystem
    fs dropghosts <fsid> [--fxid fid1 [fid2] ...]
    drop file ids (hex) without a corresponding metadata object in
    the namespace that are still accounted in the file system view.
    If no fxid is provided then all fids on the file system are checked.
    fs dropfiles <fsid> [-f]
    drop all files on the filesystem
    -f : unlink/remove files from the namespace (you have to remove
    the files from disk)
    fs dumpmd <fsid> [--fid] [--path] [--size] [-m|-s]
    dump all file metadata on this filesystem in query format
    --fid  : dump only the file ids
    --path : dump only the file paths
    --size : dump only the file sizes
    -m     : print full metadata record in env format
    -s     : silent mode (will keep an internal reference)
    fs ls [-m|-l|-e|--io|--fsck|[-d|--drain]|-D|-F] [-s] [-b|--brief] [[matchlist]]
    list filesystems using the default output format
    -m         : monitoring format
    -b|--brief : display hostnames without domain names
    -l         : display parameters in long format
    -e         : display filesystems in error state
    --io       : IO output format
    --fsck     : display filesystem check statistics
    -d|--drain : display filesystems in drain or draindead status
    along with drain progress and statistics
    -D|--drain_jobs :
    display ongoing drain transfers, matchlist needs to be an integer
    representing the drain file system id
    -F|--failed_drain_jobs :
    display failed drain transfers, matchlist needs to be an integer
    representing the drain file system id. This will only display
    information while the draining is ongoing
    -s         : silent mode
    [matchlist]
    -> can be the name of a space or a comma separated list of
    spaces e.g 'default,spare'
    -> can be a grep style list to filter certain filesystems
    e.g. 'fs ls -d drain,bootfailure'
    -> can be a combination of space filter and grep e.g.
    'fs ls -l default,drain,bootfailure'
    fs mv [--force] <src_fsid|src_grp|src_space> <dst_grp|dst_space>
    move filesystem(s) in different scheduling group or space
    --force   : force mode - allows to move non-empty filesystems bypassing group
    and node constraints
    src_fsid  : source filesystem id
    src_grp   : all filesystems from scheduling group are moved
    src_space : all filesystems from space are moved
    dst_grp   : destination scheduling group
    dst_space : destination space - best match scheduling group
    is auto-selected
    fs rm <fsid>|<mnt>|<node-queue> <mnt>|<hostname> <mnt>
    remove filesystem by various identifiers, where <mnt> is the
    mountpoint
    fs status [-r] [-l] <identifier>
    return all status variables of a filesystem and calculates
    the risk of data loss if this filesystem is removed
    <identifier> can be:
    <fsid> : filesystem id
    [<host>] <mountpoint> : if host is not specified then it's
    considered localhost
    -l : list all files which are at risk and offline files
    -r : show risk analysis
    Examples:
    fs ls --io -> list all filesystems with IO statistics
    fs boot *  -> send boot request to all filesystems
    fs dumpmd 100 -path -> dump all logical path names on filesystem 100
    fs mv 100 default.0 -> move filesystem 100 to scheduling group default.0
