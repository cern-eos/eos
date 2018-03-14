space
-----

.. code-block:: text

  usage: space ls                                                      : list spaces
    space ls [-s|-g <depth>] [-m|-l|--io|--fsck] [<space>]        : list in all spaces or select only <space>. <space> is a substring match and can be a comma seperated list
    -s : silent mode
    -m : monitoring key=value output format
    -l : long output - list also file systems after each space
    -g : geo output - aggregate space information along the instance geotree down to <depth>
    --io : print IO satistics
    --fsck : print filesystem check statistics
    space config <space-name> space.nominalsize=<value>           : configure the nominal size for this space
    space config <space-name> space.balancer=on|off               : enable/disable the space balancer [default=off]
    space config <space-name> space.balancer.threshold=<percent>  : configure the used bytes deviation which triggers balancing            [ default=20 (%)     ]
    space config <space-name> space.balancer.node.rate=<MB/s>     : configure the nominal transfer bandwith per running transfer on a node [ default=25 (MB/s)   ]
    space config <space-name> space.balancer.node.ntx=<#>         : configure the number of parallel balancing transfers per node          [ default=2 (streams) ]
    space config <space-name> space.converter=on|off              : enable/disable the space converter [default=off]
    space config <space-name> space.converter.ntx=<#>             : configure the number of parallel conversions per space                 [ default=2 (streams) ]
    space config <space-name> space.drainer.node.rate=<MB/s >     : configure the nominal transfer bandwith per running transfer on a node [ default=25 (MB/s)   ]
    space config <space-name> space.drainer.node.ntx=<#>          : configure the number of parallel draining transfers per node           [ default=2 (streams) ]
    space config <space-name> space.drainer.node.nfs=<#>          : configure the number of max draining filesystems per node (Valid only for central drain)  [ default=5 ]
    space config <space-name> space.drainer.retries=<#>           : configure the number of retry for the draining process (Valid only for central drain)     [ default=1  ]
    space config <space-name> space.drainer.fs.ntx=<#>            : configure the number of parallel draining transfers per fs (Valid only for central drain) [ default=5   ]
    space config <space-name> space.lru=on|off                    : enable/disable the LRU policy engine [default=off]
    space config <space-name> space.lru.interval=<sec>            : configure the default lru scan interval
    space config <space-name> space.headroom=<size>               : configure the default disk headroom if not defined on a filesystem (see fs for details)
    space config <space-name> space.scaninterval=<sec>            : configure the default scan interval if not defined on a filesystem (see fs for details)
    space config <space-name> space.drainperiod=<sec>             : configure the default drain  period if not defined on a filesystem (see fs for details)
    space config <space-name> space.graceperiod=<sec>             : configure the default grace  period if not defined on a filesystem (see fs for details)
    space config <space-name> space.autorepair=on|off             : enable auto-repair of faulty replica's/files (the converter has to be enabled too)
    => size can be given also like 10T, 20G, 2P ... without space before the unit
    space config <space-name> space.geo.access.policy.write.exact=on|off   : if 'on' use exact matching geo replica (if available) , 'off' uses weighting [ for write case ]
    space config <space-name> space.geo.access.policy.read.exact=on|off    : if 'on' use exact matching geo replica (if available) , 'off' uses weighting [ for read case  ]
    space config <space-name> fs.<key>=<value>                    : configure file system parameters for each filesystem in this space (see help of 'fs config' for details)
    space define <space-name> [<groupsize> [<groupmod>]]          : define how many filesystems can end up in one scheduling group <groupsize> [default=0]
    => <groupsize>=0 means that no groups are built within a space, otherwise it should be the maximum number of nodes in a scheduling group
    => <groupmod> maximum number of groups in the space, which should be at least equal to the maximun number of filesystems per node
    space node-set <space-name> <node.key> <file-name>            : store the contents of <file-name> into the node configuration variable <node.key> visibile to all FSTs
    => if <file-name> matches file:<path> the file is loaded from the MGM and not from the client
    => local files cannot exceed 512 bytes - MGM files can be arbitrary length
    => the contents gets base64 encoded by default
    space node-get <space-name> <node.key>                        : get the value of <node.key> and base64 decode before output
    : if the value for <node.key> is identical for all nodes in the referenced space, it is dumped only once, otherwise the value is dumped for each node separately
    space kinetic-json-store cluster|security|location <local-file>
    : store a modified json file on the MGM (without publishing)
    space reset <space-name>  [--egroup|mapping|drain|scheduledrain|schedulebalance|ns|nsfilesystemview|nsfilemap|nsdirectorymap]
    : reset different space attributes
    --egroup : clear cached egroup information
    --mapping : clear all user/group uid/gid caches
    --drain : reset draining
    --scheduledrain : reset drain scheduling map
    --schedulebalance : reset balance scheduling map
    --ns : resize all namespace maps
    --nsfilesystemview : resize namespace filesystem view
    --nsfilemap : resize namespace file map
    --nsdirectorymap : resize namespace directory map
    space status <space-name> [-m]                                : print's all defined variables for space
    space set <space-name> on|off                                 : enables/disabels all groups under that space ( not the nodes !)
    space rm <space-name>                                         : remove space
    space quota <space-name> on|off                               : enable/disable quota
