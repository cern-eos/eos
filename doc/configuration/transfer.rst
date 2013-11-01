.. highlight:: rst

Transfer System
================

Overview
--------

EOS has three major interfaces to do file transfers.

**eos cp** is an EOS shell command which allows to im- and export files from/to 
EOS using XRootD, http, gsiftp and s3 protocol. 
By default all traffic flows via the client issuing the command. 
It is possible to use it in 'async' mode where IO is flowing through a third-party host.

**eos file copy** is a third party transfer interface supporting TPC transfers inside the EOS instance.

**eos transfer** allows to run scheduled transfers. 
IO is bridged via dedicated transfer gateways as explained in the following.

.. ::note

   The **Beryll** version of EOS supports the third-party-copy mechnism in XRootD >=3.3 using the standard
   **xrdcp --tpc** command.

eos cp
------

As a first overview we refer to the usage information of the EOS cp command. 
Currently the support of copy full directory trees is only supported for EOS 
type storage systems.

.. code-block:: bash

   Usage: cp [--async] [--rate=<rate>] [--streams=<n>] [--recursive|-R|-r] [-a] [-n] [-S] [-s|--silent] [-d] [--checksum] <src> <dst>

   '[eos] cp ..' provides copy functionality to EOS.
   Options:
   <src>|<dst> can be root://<host>/<path>, a local path /tmp/../ or an eos path /eos/ in the connected instanace...
   --async         : run an asynchronous transfer via a gateway server (see 'transfer submit --sync' for the full options)
   --rate          : limit the cp rate to <rate>
   --streams       : use <#> parallel streams
   --checksum      : output the checksums
   -a              : append to the target, don't truncate
   -n              : hide progress bar
   -S              : print summary
   -s --silent     : no output just return code
   -d              : enable debug information
   -k | --no-overwrite : disable overwriting of files

.. ::note

   If you deal with directories always add a '/' in the end of source or target 
   paths e.g. if the target should be a directory and not a file put a '/' in the end.
   To copy a directory hierarchy use '-r' and source and target directories terminated with '/' !

Examples
--------

.. code-block:: bash

   eos cp /var/data/myfile /eos/foo/user/data/                   : copy 'myfile' to /eos/foo/user/data/myfile
   eos cp /var/data/ /eos/foo/user/data/                         : copy all plain files in /var/data to /eos/foo/user/data/
   eos cp -r /var/data/ /eos/foo/user/data/                      : copy the full hierarchy from /var/data/ to /var/data to /eos/foo/user/data/ => empty directories won't show up on the target!
   eos cp -r --checksum --silent /var/data/ /eos/foo/user/data/  : copy the full hierarchy and just printout the checksum information for each file copied!

S3
++

URLs have to be written as:

.. code-block:: bash

   as3://<hostname>/<bucketname>/<filename> as implemented in ROOT
   or as3:<bucketname>/<filename> with environment variable S3_HOSTNAME set
   and as3:....?s3.id=<id>&s3.key=<key>

The access id can be defined in 3 ways:

.. code-block:: bash

   env S3_ACCESS_ID=<access-id>          [as used in ROOT  ]
   env S3_ACCESS_KEY_ID=<access-id>      [as used in libs3 ]

   <as3-url>?s3.id=<access-id>           [as used in EOS transfers ]


The access key can be defined in 3 ways:

.. code-block:: bash

   env S3_ACCESS_KEY=<access-key>        [as used in ROOT  ]
   env S3_SECRET_ACCESS_KEY=<access-key> [as used in libs3 ]
   <as3-url>?s3.key=<access-key>         [as used in EOS transfers ]

If <src> and <dst> are using S3, we are using the same credentials on both ends 
and the target credentials will overwrite source credentials!

 

Further Examples
++++++++++++++++

Import a file from an S3 storage into EOS:

.. code-block:: bash

   eos cp as3://swift.cern.ch/eos/bigfile?s3.id=<secret>&s3.key=<secret> /eos/local/bigfile

   [eos-cp] going to copy 1 files and 210.06 MB
   [eoscp] bigfile                  Total 200.32 MB    |====================| 100.00 % [26.7 MB/s]
   [eos-cp] copied 1/1 files and 210.06 MB in 8.63 seconds with 24.33 MB/s

Run the same import via a transfer gateway:

.. code-block:: bash

   eos cp --async as3://swift.cern.ch/eos/bigfile?s3.id=<secret>&s3.key=<secret> /eos/local/bigfile

   success: submitted transfer id=128095
   [eoscp TX] [ done       ]    |====================|  100.0% : 9s
   [eoscp] #################################################################
   [eoscp] # Date                     : ( 1343733064 ) Tue Jul 31 13:11:04 2012 

   ...

You can also easily import web files (no upload):

.. code-block:: bash


   eos cp http://root.cern.ch/drupal /eos/local/root.cern.ch


Transfer Gateways
-----------------

Every FST node in EOS can act as gateway. 
In fact it is possible to deploys FSTs only as gateways without any storage 
attached.

A gateway is enabled via the command:

.. code-block:: bash

   EOS Console [root://localhost] |/> node gw gateway1.cern.ch:1095 on

You can see the configuration state of nodes by doing:

.. code-block:: bash

   EOS Console [root://localhost] |/> node ls
   #-----------------------------------------------------------------------------------------------------------------------------
   #     type #                       hostport #   status #     status # txgw #gw-queued # gw-ntx #gw-rate # heartbeatdelta #nofs
   #-----------------------------------------------------------------------------------------------------------------------------
   nodesview            gateway1.cern.ch:1095     online           on     on          0       10      100                ~     0
   nodesview            storage1.cern.ch:1095     online           on    off          0       30      120                0    22

Do disable a gateway do:

.. code-block:: bash

   EOS Console [root://localhost] |/> node gw gateway1.cern.ch:1095 off

You see in the output of node ls that each node has two parameters for gateways:

.. epigraph::

   ======== ==================================================================================
   variable defition
   ======== ==================================================================================
   gw-ntx   number of parallel transfers on this node
   gw-rate  bandwith limitation used per transfer (if not specified differently by a transfer)
   ======== ==================================================================================

These paremeters are defined via:

.. code-block:: bash

   EOS Console [root://localhost] |/> node config gateway1.cern.ch gw.rate=100
   EOS Console [root://localhost] |/> node config gateway1.cern.ch gw.ntx=10

You can get a comprehansive summary of the configuration per node using the 
**eos node status** command:

.. code-block:: bash

   EOS Console [root://localhost] |/> node status eosdevsrv1.cern.ch
   # ------------------------------------------------------------------------------------
   # Node Variables
   # ....................................................................................
   gw.ntx                           := 10
   gw.rate                          := 100
   manager                          := eosdev.cern.ch:1094
   stat.balance.ntx                 := 2
   stat.balance.rate                := 25
   stat.gw.queued                   := 0
   status                           := on
   symkey                           := G41RrP1y/SLHsf9AhneqbxXaOSU=
   txgw                             := on

 
Transfer Queue and CLI
----------------------

The transfer state machine is as follows:

.. epigraph::
    
   ==========
   state
   ==========
   inserted
   validated
   scheduled
   stagein | stageout | running
   done | failed

Interaction with the transfer queue is done via the **eos transfer** CLI.

.. code-block:: bash

   EOS Console [root://localhost] |/> transfer
   Usage: transfer submit|cancel|ls|enable|disable|reset|clear|resubmit|log ..'[eos] transfer ..' provides the transfer interface of EOS.
   Options:
   transfer submit [--rate=<rate>] [--streams=<#>] [--group=<groupname>] [--sync] <URL1> <URL2> :
   transfer a file from URL1 to URL2
   <URL> can be root://<host>/<path> or a local path /eos/...
   --rate          : limit the transfer rate to <rate>
   --streams       : use <#> parallel streams

   --group         : set the group name for this transfer
   transfer cancel <id>|--group=<groupname>
   cancel transfer with ID <id> or by group <groupname>
   <id>=*          : cancel all transfers (only root can do that)

   transfer ls [-a] [-m] [s] [--group=<groupname>] [id]
   -a              : list all transfers not only of the current role
   -m              : list all transfers in monitoring format (key-val pairs)
   -s              : print transfer summary
   --group         : list all transfers in this group
   --sync          : follow the transfer in interactive mode (like interactive third party 'cp')
   <id> : id of the transfer to list

   transfer enable         : start the transfer engine (you have to be root to do that)
   transfer disable        : stop the transfer engine (you have to be root to do that)
   transfer reset [<id>|--group=<groupname>] 

                           : reset all transfers to 'inserted' state (you have to be root to do that)
   transfer clear          : clear's the transfer database (you have to be root to do that)
   transfer resubmit <id> [--group=<groupname>] 

                           : resubmit's a transfer
   transfer kill <id>|--group=<groupname> 

   transfer log <id>       : show the log of transfer <id>

                           : kill a running transfer
   transfer purge [<id>|--group=<groupname>]
                           : remove 'failed' transfers from the transfer queue by id, group or all if not specified

   When a transfer has been submitted using 'transfer submit' it will be in state inserted. When a transfer has been assigned to a transfer gateway it is in state scheduled. When a transfer is executed it will be either in status stagein (then stageout) or running. Certain protocols need a two stage process to bridge transfers. When transfer is going into status failed IT can be inspected using 'transfer log <id>'. Transfers moving into done state are automatically purged from the queue and put in the transfer archive.The transfer archive is a daily rotated log file in /var/eos/tx/transfer-archive.log storing all transfer logs. It is currently not accessible via the CLI.