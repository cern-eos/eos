space
-----

.. code-block:: text

   usage: space ls                                                  : list spaces
   usage: space ls [-s] [-m|-l|--io|--fsck] [<space>]                   : list in all spaces or select only <space>. <space> is a substring match and can be a comma seperated list
      -s : silent mode
      -m : monitoring key=value output format
      -l : long output - list also file systems after each space
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
      space config <space-name> space.lru=on|off                    : enable/disable the LRU policy engine [default=off]
      space config <space-name> space.lru.interval=<sec>            : configure the default lru scan interval
      space config <space-name> space.headroom=<size>               : configure the default disk headroom if not defined on a filesystem (see fs for details)
      space config <space-name> space.scaninterval=<sec>            : configure the default scan interval if not defined on a filesystem (see fs for details)
      space config <space-name> space.drainperiod=<sec>             : configure the default drain  period if not defined on a filesystem (see fs for details)
      space config <space-name> space.graceperiod=<sec>             : configure the default grace  period if not defined on a filesystem (see fs for details)
      space config <space-name> space.autorepair=on|off             : enable auto-repair of faulty replica's/files (the converter has to be enabled too)                                                                       => size can be given also like 10T, 20G, 2P ... without space before the unit
      space config <space-name> space.geo.access.policy.exact=on|off: if 'on' use exact matching geo replica (if available), 'off' uses weighting
      space config <space-name> fs.<key>=<value>                    : configure file system parameters for each filesystem in this space (see help of 'fs config' for details)
      space define <space-name> [<groupsize> [<groupmod>]]             : define how many filesystems can end up in one scheduling group <groupsize> [default=0]
      => <groupsize>=0 means, that no groups are built within a space, otherwise it should be the maximum number of nodes in a scheduling group
      => <groupmod> defines the maximun number of filesystems per node
      space reset <space-name>  [--egroup|drain|scheduledrain|schedulebalance]
      : reset a space e.g. recompute the drain state machine
      space status <space-name>                                     : print's all defined variables for space
      space set <space-name> on|off                                 : enables/disabels all groups under that space ( not the nodes !)
      space rm <space-name>                                         : remove space
      space quota <space-name> on|off                               : enable/disable quota
