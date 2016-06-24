.. highlight:: rst

.. index::
   single: Node API



node
=====

node ls
--------

List available nodes.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=node
     &mgm.subcmd=ls
     &eos.ruid=0
     &eos.rgid=0
     [&mgm.outformat=l|m|io|sys|fsck]
     [&mgm.outhost=brief]
     [&mgm.selection=<match>]

CLI syntax
++++++++++

.. code-block:: text

   node ls [-s] [-b|--brief] [-m|-l|--sys|--io|--fsck] [<node>]  : list all nodes or only <node>. <node> is a substring match and can be a comma seperated list
      -s : silent mode
      -b,--brief : display host names without domain names
      -m : monitoring key=value output format
      -l : long output - list also file systems after each node
      --io : print IO statistics
      --sys  : print SYS statistics (memory + threads)
      --fsck : print filesystem check statistcis

node config
-----------

Configure a node.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=node
     &mgm.subcmd=config
     &eos.ruid=0
     &eos.rgid=0
     &mgm.node.name=<node>
     &mgm.node.key=<key>
     &mgm.node.value=<value>

CLI syntax
++++++++++

.. code-block:: text

   node config <host:port> <key>=<value>                    : configure file system parameters for each filesystem of this node
      <key> : gw.rate=<mb/s> - set the transfer speed per gateway transfer
      <key> : gw.ntx=<#>     - set the number of concurrent transfers for a gateway node
      <key> : error.simulation=io_read|io_write|xs_read|xs_write|fmd_open
        io_read  : simulate read  errors
        io_write : simulate write errors
        xs_read  : simulate checksum errors when reading a file
        xs_write : simulate checksum errors when writing a file
        fmd_open : simulate fmd mismatch when opening a file
        <none>   : disable error simulation (every value than the previous ones are fine!)
      <key> : publish.interval=<sec> - set the filesystem state publication interval to <sec> seconds
      <key> : debug.level=<level> - set the node into debug level <level> [default=notice] -> see debug --help for available levels
      <key> : for other keys see help of 'fs config' for details

node set
--------

Activate/Deactive/Define a node.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=node
     &mgm.subcmd=set
     &eos.ruid=0
     &eos.rgid=0
     &mgm.node=<node>
     &mgm.node.state=on|off

CLI syntax
++++++++++

.. code-block:: text

      node set <queue-name>|<host:port> on|off                 : activate/deactivate node

node rm
--------

Remove a node.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=node
     &mgm.subcmd=rm
     &eos.ruid=0
     &eos.rgid=0
     &mgm.node=<node>

CLI syntax
++++++++++

.. code-block:: text

    node rm  <queue-name>|<host:port>                        : remove a node

node register
-------------

Register a node.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=node
     &mgm.subcmd=register
     &eos.ruid=0
     &eos.rgid=0
     &mgm.node.name=<node>
     &mgm.node.path2register=<path2register>
     &mgm.node.space2register=<space2register>
     [&mgm.node.force=true]
     [&mgm.node.root=true]

CLI syntax
++++++++++

.. code-block:: text

   node register <host:port|*> <path2register> <space2register> [--force] [--root] : register filesystems on node <host:port>
     <path2register> is used as match for the filesystems to register e.g. /data matches filesystems /data01 /data02 etc. ... /data/ registers all subdirectories in /data/
     <space2register> is formed as <space>:<n> where <space> is the space name and <n> must be equal to the number of filesystems which are matched by <path2register> e.g. data:4 or spare:22 ...
      --force : removes any existing filesystem label and re-registers
      --root  : allows to register paths on the root partition

node gw
--------

Enable/Disable a node as a transfer gateway

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=node
     &mgm.subcmd=GW
     &eos.ruid=0
     &eos.rgid=0
     &mgm.node=<node>
     &mgm.node.txgw=on|off

CLI syntax
++++++++++

.. code-block:: text

   node gw <queue-name>|<host:port> <on|off>                : enable (on) or disable (off) node as a transfer gateway

node status
--------

Show the status of a node.

REST syntax
+++++++++++

.. code-block:: text

   http://<host>:8000/proc/admin/ | root://<host>//proc/admin/
     ?mgm.cmd=node
     &mgm.subcmd=status
     &eos.ruid=0
     &eos.rgid=0
     &mgm.node=<node>

CLI syntax
++++++++++

.. code-block:: text

   node status <queue-name>|<host:port>                     : print's all defined variables for a node
