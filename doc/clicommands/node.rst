node
----

.. code-block:: text

   usage: node ls [-s] [-m|-l|--sys|--io|--fsck] [<node>]                     : list all nodes or only <node>. <node> is a substring match and can be a comma seperated list
      -s : silent mode
      -m : monitoring key=value output format
      -l : long output - list also file systems after each node
      --io : print IO statistics
      --sys  : print SYS statistics (memory + threads)
      --fsck : print filesystem check statistcis
      node config <host:port> <key>=<value>                    : configure file system parameters for each filesystem of this node
      <key> : gw.rate=<mb/s> - set the transfer speed per gateway transfer
      <key> : gw.ntx=<#>     - set the number of concurrent transfers for a gateway node
      <key> : error.simulation=io_read|io_write|xs_read|xs_write
      io_read  : simulate read  errors
      io_write : simulate write errors
      xs_read  : simulate checksum errors when reading a file
      xs_write : simulate checksum errors when writing a file
      <none>   : disable error simulation (every value than the previous ones are fine!)
      <key> : publish.interval=<sec> - set the filesystem state publication interval to <sec> seconds
      <key> : debug.level=<level> - set the node into debug level <level> [default=notice] -> see debug --help for available levels
      <key> : for other keys see help of 'fs config' for details
      node set <queue-name>|<host:port> on|off                 : activate/deactivate node
      node rm  <queue-name>|<host:port>                        : remove a node
      node register <host:port|*> <path2register> <space2register> [--force] [--root]
      node gw <queue-name>|<host:port> <on|off>                : enable (on) or disable (off) node as a transfer gateway
      : register filesystems on node <host:port>
      <path2register> is used as match for the filesystems to register e.g. /data matches filesystems /data01 /data02 etc. ... /data/ registers all subdirectories in /data/
      <space2register> is formed as <space>:<n> where <space> is the space name and <n> must be equal to the number of filesystems which are matched by <path2register> e.g. data:4 or spare:22 ...
      --force : removes any existing filesystem label and re-registers
      --root  : allows to register paths on the root partition
      node status <queue-name>|<host:port>                     : print's all defined variables for a node
