ns
--

.. code-block:: text

  ns [stat|mutex|compact|master|cache]
    print or configure basic namespace parameters
    ns stat [-a] [-m] [-n] [--reset]
    print namespace statistics
    -a      : break down by uid/gid
    -m      : display in monitoring format <key>=<value>
    -n      : display numerical uid/gid(s)
    --reset : reset namespace counters
.. code-block:: text

    ns mutex [<option>]
    manage mutex monitoring. Option can be:
    --toggletime     : toggle the timing
    --toggleorder    : toggle the order
    --toggledeadlock : toggle deadlock check
    --smplrate1      : set timing sample rate at 1% (default, no slow-down)
    --smplrate10     : set timing sample rate at 10% (medium slow-down)
    --smplrate100    : set timing sample rate at 100% (severe slow-down)
    ns compact off|on <delay> [<interval>] [<type>]
    enable online compaction after <delay> seconds
    <interval> : if >0 then compaction is repeated automatically
    after so many seconds
    <type>     : can be 'files', 'directories' or 'all'. By default  only the file
    changelog is compacted. The repair flag can be indicated by using
    'files-repair', 'directories-repair' or 'all-repair'.
    ns master [<option>]
    master/slave operations. Option can be:
    <master_hostname> : set hostname of MGM master RW daemon
    --log             : show master log
    --log-clear       : clean master log
    --enable          : enable the slave/master supervisor thread modifying stall/
    redirectorion rules
    --disable         : disable supervisor thread
    ns recompute_tree_size <path>|cid:<decimal_id>|cxid:<hex_id> [--depth <val>]
    recompute the tree size of a directory and all its subdirectories
    --depth : maximum depth for recomputation, default 0 i.e no limit
    ns recompute_quotanode <path>|cid:<decimal_id>|cxid:<hex_id>
    recompute the specified quotanode
    ns cache set|drop [-d|-f] [<max_num>] [<max_size>K|M|G...]
    set the max number of entries or the max size of the cache. Use the
    ns stat command to see the current values.
    set        : update cache size for files or directories
    drop       : drop cached file and/or directory entries
    -d         : control the directory cache
    -f         : control the file cache
    <max_num>  : max number of entries
    <max_size> : max size of the cache - not implemented yet
    ns cache drop-single-file <id of file to drop>
    force refresh of the given FileMD by dropping it from the cache
    ns cache drop-single-container <id of container to drop>
    force refresh of the given ContainerMD by dropping it from the cache
    ns max_drain_threads <num>
    set the max number of threads in the drain pool, default 400, minimum 4
    ns reserve-ids <file id> <container id>
    blacklist file and container IDs below the given threshold. The namespace
    will not allocate any file or container with IDs less than, or equal to the
    given blacklist thresholds.
  
