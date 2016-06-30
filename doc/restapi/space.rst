.. highlight:: rst

.. index::
   single: Space API



space
=====

space ls
--------

List configured spaces.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=ls
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json
     [&mgm.outformat=l|m|io|fsck]
     [&mgm.selection=<match>]

CLI syntax
++++++++++

.. code-block:: text

   space ls [-s] [-m|-l|--io|--fsck] [<space>]                   : list in all spaces or select only <space>. <space> is a substring match and can be a comma seperated list
      -s : silent mode
      -m : monitoring key=value output format
      -l : long output - list also file systems after each space
      --io : print IO satistics
      --fsck : print filesystem check statistics


space config
------------

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=config
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json
     &mgm.space.name=<space>
     &mgm.space.key=<key>
     &mgm.space.value=<value>

CLI syntax
++++++++++

.. code-block:: text

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
      space config <space-name> space.geo.access.policy.write.exact=on|off   : if 'on' use exact matching geo replica (if available) , 'off' uses weighting [ for write case ]
      space config <space-name> space.geo.access.policy.read.exact=on|off    : if 'on' use exact matching geo replica (if available) , 'off' uses weighting [ for read case  ]
      space config <space-name> fs.<key>=<value>                    : configure file system parameters for each filesystem in this space (see help of 'fs config' for details)

space define
------------

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=define
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json
     &mgm.space=<space>
     &mgm.space.groumod=<groupmod>
     &mgm.space.groupsize=<groupsize>

CLI syntax
++++++++++

.. code-block:: text

      space define <space-name> [<groupsize> [<groupmod>]]          : define how many filesystems can end up in one scheduling group <groupsize> [default=0]
      => <groupsize>=0 means, that no groups are built within a space, otherwise it should be the maximum number of nodes in a scheduling group
      => <groupmod> defines the maximun number of filesystems per node



space reset
------------

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=reset
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json
     &mgm.space=<space>
     &[mgm.option=egroup|mapping|drain|scheduledrain|schedulebalance]

CLI syntax
++++++++++

.. code-block:: text

      space reset <space-name>  [--egroup|mapping|drain|scheduledrain|schedulebalance]
      : reset a space e.g. recompute the drain state machine

space status
------------

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=status
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json
     &mgm.space=<space>

CLI syntax
++++++++++

.. code-block::text

      space status <space-name>                                     : print's all defined variables for space

space set
---------

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=set
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json
     &mgm.space=<space>
     &mgm.space.state=on|off

CLI syntax
++++++++++

.. code-block::text

      space set <space-name> on|off                                 : enables/disabels all groups under that space ( not the nodes !)

space rm
--------

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=rm
     &mgm.space=<space>
     &mgm.format=json
     &eos.ruid=0
     &eos.rgid=0

CLI syntax
++++++++++

.. code-block:: text

      space rm <space-name>                                         : remove space

space quota
-----------

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=space
     &mgm.subcmd=quota
     &eos.ruid=0
     &eos.rgid=0
     &mgm.format=json
     &mgm.space=<space>
     &mgm.space.quota=on|off


CLI syntax
++++++++++

.. code-block:: text

      space quota <space-name> on|off                               : enable/disable quota


