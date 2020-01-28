fusex
-----

.. code-block:: text

  usage: fusex ls [-l] [-f] [-m]                     :  print statistics about eosxd fuse clients
    [no option]                                          -  break down by client host [default]
    -l                                                   -  break down by client host and show statistics
    -f                                                   -  show ongoing flush locks
    -k                                                   -  show R/W locks
    -m                                                   -  show monitoring output format
    fuxex evict <uuid> [<reason>]                                 :  evict a fuse client
    <uuid> -  uuid of the client to evict
    <reason> -  optional text shown to the client why he has been evicted or an instruction for an action to the client
    - if the reason contains the keywoard 'abort' the abort handler will be called on client side (might create a stack trace/core)
    - if reason contains the keyword 'log2big' the client will effectily not be evicted, but will truncate his logfile to 0
    - if reason contains the keyword 'setlog' and 'debug','notice', 'error', 'crit', 'info', 'warning' the log level of the targeted mount is changed accordingly .e.g evict <uuid> "setlog error"
    - if reason contains the keyword 'stacktrace' the client will send a self-stacktrace with the next heartbeat message and it will be stored in /var/log/eos/mgm/eosxd-stacktraces.log e.g. evict <uuid> stacktrace
    - if reason contains the keyword 'sendlog' the client will send max. the last 512 lines of each log level and the log will be stored in /var/log/eos/mgm/eosxd-logtraces.log e.g. evict <uuid> sendlog
    - if reason contains the keyword 'resetbuffer' the client will reset the read-ahead and write-buffers in flight and possibly unlock a locked mount point
    fusex evict static|autofs mem:<size-in-mb>|idle:<seconds>     :  evict all autofs or static mounts which have a resident memory footprint larger than <size-in-mb> or are idle longer than <seconds>
    fusex dropcaps <uuid>                                         :  advice a client to drop all caps
    fusex droplocks <inode> <pid>                                 :  advice a client to drop for a given (hexadecimal) inode and process id
    fusex caps [-t | -i | -p [<regexp>] ]                         :  print caps
    -t                                                   -  sort by expiration time
    -i                                                   -  sort by inode
    -p                                                   -  display by path
    -t|i|p <regexp>>                                     -  display entries matching <regexp> for the used filter type
  examples:
    fusex caps -i ^0000abcd$                                  :  show caps for inode 0000abcd
    fusex caps -p ^/eos/$                                     :  show caps for path /eos
    fusex caps -p ^/eos/caps/                                 :  show all caps in subtree /eos/caps
    fusex conf [<heartbeat-in-seconds>] [quota-check-in-seconds]  :  show heartbeat and quota interval
    :  [ optional change heartbeat interval from [1-15] seconds ]
    :  [ optional set quota check interval from [1-16] seconds ]
  examples:
    fusex conf                                                :  show heartbeat and quota interval
    fusex conf 10                                             :  define heartbeat interval as 10 seconds
    fusex conf 10 10                                          :  define heartbeat and quota interval as 10 seconds
