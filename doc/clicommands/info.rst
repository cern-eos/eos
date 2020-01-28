info
----

.. code-block:: text

  file adjustreplica|check|convert|copy|drop|info|layout|move|purge|rename|replicate|verify|version ...
  '[eos] file ..' provides the file management interface of EOS.
  Options:
  file adjustreplica [--nodrop] <path>|fid:<fid-dec>|fxid:<fid-hex> [space [subgroup]] :
    tries to bring a files with replica layouts to the nominal replica level [ need to be root ]
  file check [<path>|fid:<fid-dec>|fxid:<fid-hex>] [%size%checksum%nrep%checksumattr%force%output%silent] :
    retrieves stat information from the physical replicas and verifies the correctness
    - %size                                                       :  return with error code EFAULT if there is a mismatch between the size meta data information
    - %checksum                                                   :  return with error code EFAULT if there is a mismatch between the checksum meta data information
    - %nrep                                                       :  return with error code EFAULT if there is a mismatch between the layout number of replicas and the existing replicas
    - %checksumattr                                               :  return with error code EFAULT if there is a mismatch between the checksum in the extended attributes on the FST and the FMD checksum
    - %silent                                                     :  suppresses all information for each replica to be printed
    - %force                                                      :  forces to get the MD even if the node is down
    - %output                                                     :  prints lines with inconsistency information
  file convert [--sync|--rewrite] [<path>|fid:<fid-dec>|fxid:<fid-hex>] [<layout>:<stripes> | <layout-id> | <sys.attribute.name>] [target-space] [placement-policy]:
    convert the layout of a file
    <layout>:<stripes>   : specify the target layout and number of stripes
    <layout-id>          : specify the hexadecimal layout id
    <conversion-name>    : specify the name of the attribute sys.conversion.<name> in the parent directory of <path> defining the target layout
    <target-space>       : optional name of the target space or group e.g. default or default.3
    <placement-policy>   : optional placement policy valid values are 'scattered','hybrid:<some_geotag>' and 'gathered:<some_geotag>'
    --sync               : run conversion in synchronous mode (by default conversions are asynchronous) - not supported yet
    --rewrite            : run conversion rewriting the file as is creating new copies and dropping old
  file copy [-f] [-s] [-c] <src> <dst>                                   :  synchronous third party copy from <src> to <dst>
    <src>                                                         :  source can be a file or a directory (<path>|fid:<fid-dec>|fxid:<fid-hex>)
    <dst>                                                         :  destination can be a file (if source is a file) or a directory
    -f                                                            :  force overwrite
    -s                                                            :  don't print output
    -c                                                            :  clone the file (keep ctime, mtime)
  file drop [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid> [-f] :
    drop the file <path> from <fsid> - force removes replica without trigger/wait for deletion (used to retire a filesystem)
  file info [<path>|fid:<fid-dec>|fxid:<fid-hex>] :
    convenience function aliasing to 'fileinfo' command
  file layout <path>|fid:<fid-dec>|fxid:<fid-hex>  -stripes <n> :
    change the number of stripes of a file with replica layout to <n>
  file layout <path>|fid:<fid-dec>|fxid:<fid-hex>  -checksum <checksum-type> :
    change the checksum-type of a file to <checksum-type>
  file move [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid1> <fsid2> :
    move the file <path> from  <fsid1> to <fsid2>
  file purge <path> [purge-version] :
    keep maximum <purge-version> versions of a file. If not specified apply the attribute definition from sys.versioning.
  file rename [<path>|fid:<fid-dec>|fxid:<fid-hex>] <new> :
    rename from <old> to <new> name (works for files and directories!).
  file replicate [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid1> <fsid2> :
    replicate file <path> part on <fsid1> to <fsid2>
  file symlink <name> <link-name> :
    create a symlink with <name> pointing to <link-name>
  file tag <name> +|-|~<fsid> :
    add/remove/unlink a filesystem location to/from a file in the location index - attention this does not move any data!
    unlink keeps the location in the list of deleted files e.g. the location get's a deletion request
  file touch [<path>|fid:<fid-dec>|fxid:<fid-hex>] :
    create a 0-size/0-replica file if <path> does not exist or update modification time of an existing file to the present time
  file verify <path>|fid:<fid-dec>|fxid:<fid-hex> [<fsid>] [-checksum] [-commitchecksum] [-commitsize] [-rate <rate>] : 
    verify a file against the disk images
  file verify <path|fid:<fid-dec>|fxid:<fid-hex> -resync : 
    ask all locations to resync their file md records
    <fsid>          : verifies only the replica on <fsid>
    -checksum       : trigger the checksum calculation during the verification process
    -commitchecksum : commit the computed checksum to the MGM
    -commitsize     : commit the file size to the MGM
    -rate <rate>    : restrict the verification speed to <rate> per node
  file version <path> [purge-version] :
    create a new version of a file by cloning
    <purge-version> : defines the max. number of versions to keep
  file versions [grab-version] :
    list versions of a file
    grab a version [grab-version] of a file
.. code-block:: text

    if not specified it will add a new version without purging any previous version
  file share <path> [lifetime] :
    <path>          : path to create a share link
    <lifetime>      : validity time of the share link like 1, 1s, 1d, 1w, 1mo, 1y, ... default is 28d
  file workflow <path>|fid:<fid-dec>|fxid:<fid-hex> <workflow> <event> :
    trigger workflow <workflow> with event <event> on <path>
