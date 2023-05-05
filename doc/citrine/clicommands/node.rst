node
----

.. code-block:: text

   usage:
  node ls [-s] [-b|--brief] [-m|-l|--sys|--io|--fsck] [<node>] : list all nodes or only <node>. <node> is a substring match and can be a comma seperated list
  	      -s : silent mode
  	      -b : display host names without domain names
  	      -m : monitoring key=value output format
  	      -l : long output - list also file systems after each node
  	    --io : print IO statistics
  	   --sys : print SYS statistics (memory + threads)
  	  --fsck : print filesystem check statistcis
  node config <host:port> <key>=<value : configure file system parameters for each filesystem of this node
  	    <key> : gw.rate=<mb/s> - set the transfer speed per gateway transfer
  	    <key> : gw.ntx=<#>     - set the number of concurrent transfers for a gateway node
  	    <key> : error.simulation=io_read|io_write|xs_read|xs_write|fmd_open
  	            If offset is given the the error will get triggered for request past the given value.
  	            Accepted format for offset: 8B, 10M, 20G etc.
  	            io_read[_<offset>]  : simulate read  errors
  	            io_write[_<offset>] : simulate write errors
  	            xs_read             : simulate checksum errors when reading a file
  	            xs_write            : simulate checksum errors when writing a file
  	            fmd_open            : simulate a file metadata mismatch when opening a file
  	            <none>              : disable error simulation (every value than the previous ones are fine!)
  	    <key> : publish.interval=<sec> - set the filesystem state publication interval to <sec> seconds
  	    <key> : debug.level=<level> - set the node into debug level <level> [default=notice] -> see debug --help for available levels
  	    <key> : for other keys see help of 'fs config' for details
  node set <queue-name>|<host:port> on|off                 : activate/deactivate node
  node rm  <queue-name>|<host:port>                        : remove a node
  node register <host:port|*> <path2register> <space2register> [--force] [--root] : register filesystems on node <host:port>
  	      <path2register> is used as match for the filesystems to register e.g. /data matches filesystems /data01 /data02 etc. ... /data/ registers all subdirectories in /data/
  	      <space2register> is formed as <space>:<n> where <space> is the space name and <n> must be equal to the number of filesystems which are matched by <path2register> e.g. data:4 or spare:22 ...
  	      --force : removes any existing filesystem label and re-registers
  	      --root  : allows to register paths on the root partition
  node txgw <queue-name>|<host:port> <on|off> : enable (on) or disable (off) node as a transfer gateway
  node proxygroupadd <group-name> <queue-name>|<host:port> : add a node to a proxy group
  node proxygrouprm <group-name> <queue-name>|<host:port> : rm a node from a proxy group
  node proxygroupclear <queue-name>|<host:port> : clear the list of groups a node belongs to
  node status <queue-name>|<host:port> : print's all defined variables for a node
  
